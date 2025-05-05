package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
)

type File struct {
	Key            string `json:"key"`
	Size           int64  `json:"size"`
	SHA256Checksum string `json:"sha256_checksum"`
}

type Client struct {
	logger  *logrus.Logger
	baseURL string
	cli     *http.Client
}

func NewClient(logger *logrus.Logger, baseURL string) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse base url: %w", err)
	}

	return &Client{
		logger:  logger,
		baseURL: u.String(),
		cli: &http.Client{
			Timeout: 10 * time.Second,
		},
	}, nil
}

func (c *Client) UploadURL() string {
	result, _ := url.JoinPath(c.baseURL, "/v1/files/upload")
	return result
}

func (c *Client) Snapshot(ctx context.Context) (map[string]*File, error) {
	u, err := url.JoinPath(c.baseURL, "v1/snapshot")
	if err != nil {
		return nil, fmt.Errorf("create url: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create request: %w", err)
	}

	resp, err := c.doRequestWithRetry(req, "Snapshot")
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot with retrying: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.logger.WithField(
			"resp", fmt.Sprintf("%q", string(body)),
		).Error("Get snapshot failed with unexpected status code")
		return nil, fmt.Errorf("http get snapshot failed: %s", resp.Status)
	}

	type Response struct {
		KeyToMetadata map[string]*File `json:"key_to_metadata"`
	}

	var response Response
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("json decode response: %w", err)
	}

	return response.KeyToMetadata, nil
}

func (c *Client) Upload(ctx context.Context, r io.Reader, presignedURL string, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, presignedURL, r)
	if err != nil {
		return fmt.Errorf("could not create upload request: %w", err)
	}
	req.ContentLength = size
	req.Header.Set("Content-Length", strconv.FormatInt(size, 10))

	resp, err := c.doRequestWithRetry(req, "Upload")
	if err != nil {
		return fmt.Errorf("failed to upload with retry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		c.logger.WithField("resp", fmt.Sprintf("%q", string(body))).Error("Upload failed with unexpected status code")
		return fmt.Errorf("http upload failed: %s", resp.Status)
	}

	return nil
}

func (c *Client) Delete(ctx context.Context, fileKey string) error {
	u, err := url.JoinPath(c.baseURL, "v1/files", url.PathEscape(fileKey))
	if err != nil {
		return fmt.Errorf("create url: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return fmt.Errorf("could not create delete request: %w", err)
	}

	resp, err := c.doRequestWithRetry(req, "Delete")
	if err != nil {
		return fmt.Errorf("failed to delete snapshot with retry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.logger.WithField(
			"resp", fmt.Sprintf("%q", string(body)),
		).Error("Delete file failed with unexpected status code")
		return fmt.Errorf("http delete failed: %s", resp.Status)
	}

	return nil
}

func (c *Client) doRequestWithRetry(req *http.Request, method string) (*http.Response, error) {
	bk := newExponentialBackoffConfig()
	resp, err := backoff.RetryWithData[*http.Response](func() (*http.Response, error) {
		resp, err := c.cli.Do(req)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return nil, backoff.Permanent(fmt.Errorf("could not make http call: %w", err))
			}
			c.logger.WithField("method", method).WithError(err).Error("Failed to make http request, retrying...")
			return nil, fmt.Errorf("http request failed: %w", err)
		}
		return resp, nil
	}, bk)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func newExponentialBackoffConfig() *backoff.ExponentialBackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithMaxElapsedTime(time.Second*3),
		backoff.WithMaxInterval(time.Second),
		backoff.WithInitialInterval(time.Millisecond*100),
		backoff.WithMultiplier(2),
		backoff.WithRandomizationFactor(0.2),
	)
}
