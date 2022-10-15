package rmq

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type HttpTask struct {
	Url    string            `json:"url"`
	Method string            `json:"method"`
	Header map[string]string `json:"header"`
	Body   []byte            `json:"body"`
}

func (h *HttpTask) Run(ctx context.Context, meta *Meta) (ret []byte, err error) {
	var (
		req  *http.Request
		resp *http.Response
		body []byte
	)
	if req, err = http.NewRequest(h.Method, h.Url, bytes.NewReader(h.Body)); err != nil {
		return
	}

	for k, v := range h.Header {
		req.Header.Set(k, v)
	}
	client := http.Client{Timeout: time.Duration(meta.Timeout) * time.Second}
	if resp, err = client.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()
	if ret, err = io.ReadAll(resp.Body); err != nil {
		return
	}
	if resp.StatusCode != 200 {
		err = fmt.Errorf("请求失败：%s", string(body))
	}
	return
}
