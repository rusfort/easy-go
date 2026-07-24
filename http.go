package eg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	timeout     = 5 * time.Second
	goodCode    = "2"
	errCodeAuth = "401"
)

var ErrAuthFailed = errors.New("auth failed")

func RequestPost(
	ctx context.Context,
	endpoint, bearerToken string,
	body any, headers map[string]string,
	bodyIsRawBytes bool,
) ([]byte, error) {
	var (
		jsonBytes []byte
		err       error
	)

	if !bodyIsRawBytes {
		jsonBytes, err = jsoniter.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encoding JSON: %w", err)
		}
	} else {
		var ok bool
		jsonBytes, ok = body.([]byte)
		if !ok {
			return nil, fmt.Errorf("got bodyIsRawBytes flag true, but body is not bytes")
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}

	for header, value := range headers {
		req.Header.Set(header, value)
	}

	return doRequest(req)
}

func doRequest(req *http.Request) ([]byte, error) {
	client := &http.Client{
		Timeout: timeout,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if !strings.HasPrefix(resp.Status, goodCode) {
		if strings.HasPrefix(resp.Status, errCodeAuth) {
			return bodyBytes, ErrAuthFailed
		}

		return bodyBytes, fmt.Errorf("bad response status: %s", resp.Status)
	}

	return bodyBytes, nil
}
