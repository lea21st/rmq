package rmq

import "time"

type Timestamp int64

func Now() Timestamp {
	return NewTimestamp(time.Now())
}

func NewTimestamp(t time.Time) Timestamp {
	return Timestamp(t.UnixMilli())
}

func (t Timestamp) Time() time.Time {
	return time.UnixMilli(int64(t))
}

func (t Timestamp) Add(d time.Duration) Timestamp {
	return NewTimestamp(t.Time().Add(d))
}

func (t Timestamp) Format(layout string) string {
	return t.Time().Format(layout)
}

func (t Timestamp) DateTime() string {
	return t.Format("2006-01-02 15:04:05")
}

func (t Timestamp) Date() string {
	return t.Format("2006-01-02")
}
