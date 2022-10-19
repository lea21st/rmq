package rmq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type HttpTask struct {
	Url    string            `json:"url"`
	Method string            `json:"method"`
	Header map[string]string `json:"header,omitempty"`
	Body   json.RawMessage   `json:"body,omitempty"` // 因为json序列号，保证消息好看一点
	msg    *Message
}

func NewHttpTaskGet(format string, arg ...any) *HttpTask {
	return &HttpTask{
		Url:    fmt.Sprintf(format, arg...),
		Method: http.MethodGet,
	}
}
func NewHttpTaskJsonPost(url string, data any) *HttpTask {
	body, _ := json.Marshal(data)
	return &HttpTask{
		Url:    url,
		Method: http.MethodPost,
		Header: map[string]string{
			"Content-Type": "application/json",
		},
		Body: body,
	}
}

func NewHttpTaskPostPostForm(url string, data url.Values) *HttpTask {
	return &HttpTask{
		Url:    url,
		Method: http.MethodPost,
		Header: map[string]string{
			"Content-Type": "application/x-www-form-urlencoded",
		},
		Body: []byte(data.Encode()),
	}
}

func (h *HttpTask) SetHeaders(headers map[string]string) *HttpTask {
	for k, v := range headers {
		h.Header[k] = v
	}
	return h
}

func (h *HttpTask) SetHeader(k, v string) *HttpTask {
	h.Header[k] = v
	return h
}

func (h *HttpTask) SetBody(data []byte) *HttpTask {
	h.Body = data
	return h
}
func (h *HttpTask) SetMethod(method string) *HttpTask {
	h.Method = method
	return h
}

func (h *HttpTask) Message() (msg *Message, err error) {
	msg, err = NewMsg().SetTask(h)
	return
}

func (h *HttpTask) TaskName() string {
	return "httpTask"
}

func (h *HttpTask) Scan(src []byte) (err error) {
	return json.Unmarshal(src, &h)
}

func (h *HttpTask) Load(ctx context.Context, msg *Message) (err error) {
	h.msg = msg
	return
}

func (h *HttpTask) Run(ctx context.Context) (result any, err error) {
	var (
		req  *http.Request
		resp *http.Response
		body []byte
	)
	if req, err = http.NewRequestWithContext(ctx, h.Method, h.Url, bytes.NewReader(h.Body)); err != nil {
		return
	}

	for k, v := range h.Header {
		req.Header.Set(k, v)
	}
	client := http.Client{Timeout: time.Duration(h.msg.Meta.Timeout) * time.Second}
	if resp, err = client.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()
	if body, err = io.ReadAll(resp.Body); err != nil {
		return
	}
	if resp.StatusCode != 200 {
		err = fmt.Errorf("请求失败：%s", string(body))
	}
	result = body
	return
}
