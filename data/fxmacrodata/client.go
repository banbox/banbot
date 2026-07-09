package fxmacrodata

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Client struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

func NewClient(apiKey string) *Client {
	return &Client{
		BaseURL:    "https://api.fxmacrodata.com/v1",
		APIKey:     apiKey,
		HTTPClient: http.DefaultClient,
	}
}

func (c *Client) DataCatalogue(ctx context.Context, currency string) ([]byte, error) {
	return c.get(ctx, "/data_catalogue/"+norm(currency), nil)
}

func (c *Client) Announcements(ctx context.Context, currency, indicator string) ([]byte, error) {
	return c.get(ctx, "/announcements/"+norm(currency)+"/"+indicator, nil)
}

func (c *Client) Calendar(ctx context.Context, currency string) ([]byte, error) {
	return c.get(ctx, "/calendar/"+norm(currency), nil)
}

func (c *Client) Predictions(ctx context.Context, currency, indicator string) ([]byte, error) {
	return c.get(ctx, "/predictions/"+norm(currency)+"/"+indicator, nil)
}

func (c *Client) Forex(ctx context.Context, base, quote string) ([]byte, error) {
	return c.get(ctx, "/forex/"+norm(base)+"/"+norm(quote), nil)
}

func (c *Client) COT(ctx context.Context, currency string) ([]byte, error) {
	return c.get(ctx, "/cot/"+norm(currency), nil)
}

func (c *Client) CommoditiesLatest(ctx context.Context) ([]byte, error) {
	return c.get(ctx, "/commodities/latest", nil)
}

func (c *Client) Commodity(ctx context.Context, indicator string) ([]byte, error) {
	return c.get(ctx, "/commodities/"+indicator, nil)
}

func (c *Client) Curves(ctx context.Context, currency string) ([]byte, error) {
	return c.get(ctx, "/curves/"+norm(currency), nil)
}

func (c *Client) CurveProxies(ctx context.Context, currency string) ([]byte, error) {
	return c.get(ctx, "/curve_proxies/"+norm(currency), nil)
}

func (c *Client) ForwardCurves(ctx context.Context, currency string) ([]byte, error) {
	return c.get(ctx, "/forward_curves/"+norm(currency), nil)
}

func (c *Client) MarketSessions(ctx context.Context) ([]byte, error) {
	return c.get(ctx, "/market_sessions", nil)
}

func (c *Client) RiskSentiment(ctx context.Context) ([]byte, error) {
	return c.get(ctx, "/risk_sentiment", nil)
}

func (c *Client) News(ctx context.Context, currency string) ([]byte, error) {
	return c.get(ctx, "/news/"+norm(currency), nil)
}

func (c *Client) PressReleases(ctx context.Context, currency string) ([]byte, error) {
	return c.get(ctx, "/press-releases/"+norm(currency), nil)
}

func (c *Client) CentralBankers(ctx context.Context, currency string) ([]byte, error) {
	return c.get(ctx, "/central_bankers/"+norm(currency), nil)
}

func (c *Client) buildURL(path string, values url.Values) string {
	if values == nil {
		values = url.Values{}
	}
	if c.APIKey != "" {
		values.Set("api_key", c.APIKey)
	}
	base := strings.TrimRight(c.BaseURL, "/")
	if len(values) == 0 {
		return base + path
	}
	return base + path + "?" + values.Encode()
}

func (c *Client) get(ctx context.Context, path string, values url.Values) ([]byte, error) {
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.buildURL(path, values), nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fxmacrodata: status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func norm(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
