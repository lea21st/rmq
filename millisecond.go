package rmq

import "time"

type Millisecond int64

func (m Millisecond) Time() time.Time {
	return time.UnixMilli(int64(m))
}

func (m Millisecond) Add(d time.Duration) Millisecond {
	return CreateMillisecond(m.Time().Add(d))
}

func (m Millisecond) Format(layout string) string {
	return m.Time().Format(layout)
}

func (m Millisecond) DateTime() string {
	return m.Format("2006-01-02 15:04:05")
}

func Now() Millisecond {
	return CreateMillisecond(time.Now())
}

func CreateMillisecond(t time.Time) Millisecond {
	return Millisecond(t.UnixMilli())
}
