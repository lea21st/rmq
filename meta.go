package rmq

type Meta struct {
	CheckRule map[string]any `json:"check_rule,omitempty"` // 检查结果
	RetryRule []int          `json:"retry_rule,omitempty"` // 重试规则，单位秒
	Retry     [2]int         `json:"retry,omitempty"`      // 当前执行次数，总共重试测试
	Delay     int            `json:"delay,omitempty"`      // 延迟时间
	Timeout   int            `json:"timeout,omitempty"`    // 超时时间，单位秒
	TraceId   string         `json:"trace_id,omitempty"`   // 用于打通trace
}

var DefaultMeta = Meta{
	CheckRule: nil,
	RetryRule: nil,
	Retry:     [2]int{0, 0}, // 不重试
	Delay:     0,
	Timeout:   30,
	TraceId:   "",
}
