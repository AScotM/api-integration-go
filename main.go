package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type headersFlag []string

func (h *headersFlag) String() string { return strings.Join(*h, ",") }
func (h *headersFlag) Set(v string) error {
	if !strings.Contains(v, ":") {
		return errors.New("header must be NAME:VALUE")
	}
	*h = append(*h, v)
	return nil
}

type result struct {
	Method      string            `json:"method,omitempty"`
	URL         string            `json:"url,omitempty"`
	StatusCode  int               `json:"status_code,omitempty"`
	Success     bool              `json:"success"`
	Elapsed     float64           `json:"elapsed_seconds,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	RateLimit   map[string]string `json:"rate_limit,omitempty"`
	Data        any               `json:"data,omitempty"`
	Error       string            `json:"error,omitempty"`
}

type client struct {
	base    *url.URL
	http    *http.Client
	headers http.Header
	retries int
	timeout time.Duration
}

func newClient(raw string, timeout time.Duration, retries int, token, apiKey, apiKeyHeader string, custom headersFlag) (*client, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, errors.New("base URL must be an HTTP(S) URL without userinfo")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("base URL cannot contain a query or fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/"
	h := make(http.Header)
	h.Set("Accept", "application/json")
	h.Set("User-Agent", "api-intergration-go/1.0")
	for _, entry := range custom {
		parts := strings.SplitN(entry, ":", 2)
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" || strings.ContainsAny(key, " \r\n\t") || strings.ContainsAny(value, "\r\n") {
			return nil, fmt.Errorf("invalid header %q", entry)
		}
		h.Set(key, value)
	}
	if token != "" {
		h.Set("Authorization", "Bearer "+token)
	}
	if apiKey != "" {
		if apiKeyHeader == "" || strings.ContainsAny(apiKeyHeader, " \r\n\t") {
			return nil, errors.New("invalid API key header")
		}
		h.Set(apiKeyHeader, apiKey)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 10
	return &client{base: u, http: &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}, headers: h, retries: retries, timeout: timeout}, nil
}

func (c *client) endpoint(raw string) (string, error) {
	if strings.HasPrefix(raw, "//") {
		return "", errors.New("network-path endpoints are not allowed")
	}
	ref, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if ref.User != nil || ref.Fragment != "" {
		return "", errors.New("endpoint userinfo and fragments are not allowed")
	}
	if ref.IsAbs() {
		if ref.Scheme != c.base.Scheme || ref.Host != c.base.Host {
			return "", errors.New("endpoint must use the configured API origin")
		}
		return ref.String(), nil
	}
	if ref.Host != "" {
		return "", errors.New("endpoint host is not allowed")
	}
	if strings.HasPrefix(raw, "/") {
		ref.Path = strings.TrimPrefix(ref.Path, "/")
		ref.RawPath = ""
	}
	out := c.base.ResolveReference(ref)
	basePath := path.Clean(c.base.Path)
	if basePath != "/" && out.Path != basePath && !strings.HasPrefix(out.Path, basePath+"/") {
		return "", errors.New("endpoint escapes the base URL path")
	}
	return out.String(), nil
}

func parseObject(raw string) (map[string]any, error) {
	if raw == "" {
		return nil, nil
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, errors.New("expected a JSON object")
	}
	return obj, nil
}

func (c *client) request(ctx context.Context, method, endpoint string, data any, params map[string]any) result {
	target, err := c.endpoint(endpoint)
	if err != nil {
		return result{Method: method, Success: false, Error: err.Error()}
	}
	u, err := url.Parse(target)
	if err != nil {
		return result{Method: method, Success: false, Error: err.Error()}
	}
	q := u.Query()
	for k, v := range params {
		if v == nil {
			continue
		}
		switch x := v.(type) {
		case []any:
			for _, item := range x {
				q.Add(k, fmt.Sprint(item))
			}
		default:
			q.Set(k, fmt.Sprint(v))
		}
	}
	u.RawQuery = q.Encode()
	target = u.String()
	var payload []byte
	if data != nil {
		payload, err = json.Marshal(data)
		if err != nil {
			return result{Method: method, URL: target, Success: false, Error: err.Error()}
		}
	}
	started := time.Now()
	maxAttempts := 1
	if method == http.MethodGet || method == http.MethodHead {
		maxAttempts += c.retries
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		req, reqErr := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
		if reqErr != nil {
			return result{Method: method, URL: target, Success: false, Error: reqErr.Error()}
		}
		req.Header = c.headers.Clone()
		if data != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, doErr := c.http.Do(req)
		if doErr != nil {
			if attempt+1 < maxAttempts && ctx.Err() == nil {
				if !wait(ctx, backoff(attempt)) {
					break
				}
				continue
			}
			return result{Method: method, URL: target, Success: false, Elapsed: seconds(started), Error: doErr.Error()}
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024+1))
		resp.Body.Close()
		if readErr != nil {
			return result{Method: method, URL: target, StatusCode: resp.StatusCode, Success: false, Elapsed: seconds(started), Error: readErr.Error()}
		}
		if len(body) > 10*1024*1024 {
			return result{Method: method, URL: target, StatusCode: resp.StatusCode, Success: false, Elapsed: seconds(started), Error: "response exceeds 10 MiB"}
		}
		if (resp.StatusCode == 429 || resp.StatusCode == 500 || resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504) && attempt+1 < maxAttempts {
			delay := backoff(attempt)
			if value := resp.Header.Get("Retry-After"); value != "" {
				if s, e := strconv.Atoi(value); e == nil && s >= 0 && s <= 30 {
					delay = time.Duration(s) * time.Second
				}
			}
			if !wait(ctx, delay) {
				break
			}
			continue
		}
		var decoded any
		if len(body) > 0 {
			if json.Unmarshal(body, &decoded) != nil {
				decoded = string(body)
			}
		}
		rates := map[string]string{}
		for _, key := range []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset", "Retry-After"} {
			if v := resp.Header.Get(key); v != "" {
				rates[key] = v
			}
		}
		r := result{Method: method, URL: target, StatusCode: resp.StatusCode, Success: resp.StatusCode >= 200 && resp.StatusCode < 300, Elapsed: seconds(started), ContentType: resp.Header.Get("Content-Type"), RateLimit: rates, Data: decoded}
		if !r.Success {
			r.Error = fmt.Sprintf("HTTP %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
		}
		return r
	}
	return result{Method: method, URL: target, Success: false, Elapsed: seconds(started), Error: ctx.Err().Error()}
}
func seconds(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1e6 }
func backoff(attempt int) time.Duration {
	if attempt > 5 {
		attempt = 5
	}
	return time.Duration(1<<attempt) * 500 * time.Millisecond
}
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func main() { os.Exit(run()) }
func run() int {
	fs := flag.NewFlagSet("api-intergration-go", flag.ContinueOnError)
	base := fs.String("base-url", "https://jsonplaceholder.typicode.com", "API base URL")
	dataRaw := fs.String("data", "", "JSON request body")
	paramsRaw := fs.String("params", "", "JSON query parameters")
	token := fs.String("token", os.Getenv("API_TOKEN"), "Bearer token")
	apiKey := fs.String("api-key", os.Getenv("API_KEY"), "API key")
	apiKeyHeader := fs.String("api-key-header", "X-API-Key", "API key header name")
	timeout := fs.Duration("timeout", 10*time.Second, "HTTP timeout, e.g. 10s")
	retries := fs.Int("retries", 3, "GET/HEAD retries")
	pages := fs.Int("pages", 1, "Number of pages")
	pageSize := fs.Int("page-size", 10, "Items per page")
	pageParam := fs.String("page-param", "_page", "Page query parameter")
	sizeParam := fs.String("size-param", "_limit", "Page size query parameter")
	delay := fs.Duration("delay", 0, "Delay between pages")
	output := fs.String("output", "", "Write response to JSON file")
	compact := fs.Bool("compact", false, "Compact JSON output")
	var headers headersFlag
	fs.Var(&headers, "header", "Custom NAME:VALUE header; repeatable")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: %s [options] METHOD ENDPOINT\n", os.Args[0])
		fs.PrintDefaults()
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		return 2
	}
	if fs.NArg() != 2 {
		fs.Usage()
		return 2
	}
	method := strings.ToUpper(fs.Arg(0))
	endpoint := fs.Arg(1)
	allowed := map[string]bool{"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true, "HEAD": true}
	if !allowed[method] {
		fmt.Fprintln(os.Stderr, "unsupported HTTP method")
		return 2
	}
	if *timeout <= 0 || *retries < 0 || *pages < 1 || *pageSize < 1 || *delay < 0 || (*pages > 1 && method != "GET") {
		fmt.Fprintln(os.Stderr, "invalid timeout, retries, pagination, or delay")
		return 2
	}
	params, err := parseObject(*paramsRaw)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid --params:", err)
		return 2
	}
	var body any
	if *dataRaw != "" {
		if err = json.Unmarshal([]byte(*dataRaw), &body); err != nil {
			fmt.Fprintln(os.Stderr, "invalid --data:", err)
			return 2
		}
		switch body.(type) {
		case map[string]any, []any:
		default:
			fmt.Fprintln(os.Stderr, "--data must be a JSON object or array")
			return 2
		}
	}
	if (method == "POST" || method == "PUT" || method == "PATCH") && body == nil {
		fmt.Fprintln(os.Stderr, "--data is required for write methods")
		return 2
	}
	c, err := newClient(*base, *timeout, *retries, *token, *apiKey, *apiKeyHeader, headers)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var resultValue any
	success := true
	if *pages == 1 {
		r := c.request(ctx, method, endpoint, body, params)
		resultValue = r
		success = r.Success
	} else {
		items := make([]any, 0)
		history := make([]map[string]any, 0)
		var failure string
		for p := 1; p <= *pages; p++ {
			q := make(map[string]any, len(params)+2)
			for k, v := range params {
				q[k] = v
			}
			q[*pageParam] = p
			q[*sizeParam] = *pageSize
			r := c.request(ctx, "GET", endpoint, nil, q)
			history = append(history, map[string]any{"page": p, "status_code": r.StatusCode, "success": r.Success, "elapsed_seconds": r.Elapsed})
			if !r.Success {
				failure = r.Error
				success = false
				break
			}
			arr, ok := r.Data.([]any)
			if !ok {
				failure = "paginated endpoint did not return a JSON array"
				success = false
				break
			}
			items = append(items, arr...)
			if len(arr) < *pageSize {
				break
			}
			if p < *pages && !wait(ctx, *delay) {
				failure = ctx.Err().Error()
				success = false
				break
			}
		}
		out := map[string]any{"success": success, "pages_requested": *pages, "pages_completed": len(history), "items_collected": len(items), "history": history, "data": items}
		if failure != "" {
			out["error"] = failure
		}
		resultValue = out
	}
	var encoded []byte
	if *compact {
		encoded, err = json.Marshal(resultValue)
	} else {
		encoded, err = json.MarshalIndent(resultValue, "", "  ")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	encoded = append(encoded, '\n')
	if *output != "" {
		target := *output
		temp, err := os.CreateTemp(path.Dir(target), ".api-output-*")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		tempName := temp.Name()
		defer os.Remove(tempName)
		if err = temp.Chmod(0600); err == nil {
			_, err = temp.Write(encoded)
		}
		if closeErr := temp.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			err = os.Rename(tempName, target)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	os.Stdout.Write(encoded)
	if !success {
		return 1
	}
	return 0
}
