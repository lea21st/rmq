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

var RetryMeta = Meta{
	CheckRule: nil,
	RetryRule: []int{
		60 * 1,
		60 * 1,
		60 * 2,
		60 * 5,
		60 * 15,
		60 * 60,
		60 * 60 * 3,
	},
	Retry:   [2]int{0, 7}, // 正常执行1次+重试7次
	Delay:   0,
	Timeout: 30,
	TraceId: "",
}
