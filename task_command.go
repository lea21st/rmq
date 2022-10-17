package rmq

import (
	"bytes"
	"context"
	"os/exec"
)

type CommandTask struct {
	Shell    string
	Command  []string
	msg      *Message
	callback func(ctx context.Context, msg *Message) error
}

func (s *CommandTask) Run(ctx context.Context) (result any, err error) {
	var in, out bytes.Buffer
	cmd := exec.Command(s.Shell)
	cmd.Stdin = &in
	cmd.Stdout = &out
	for _, v := range s.Command {
		in.WriteString(v + "\n")
	}
	in.WriteString("exit\n")
	err = cmd.Run()
	result = out.String()
	return
}
