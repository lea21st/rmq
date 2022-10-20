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

func (c *CommandTask) TaskName() string {
	return "commandTask"
}

func (c *CommandTask) Run(ctx context.Context) (result string, err error) {
	var in, out bytes.Buffer
	cmd := exec.CommandContext(ctx, c.Shell)
	cmd.Stdin = &in
	cmd.Stdout = &out
	for _, v := range c.Command {
		in.WriteString(v + "\n")
	}
	in.WriteString("exit\n")
	err = cmd.Run()
	result = out.String()
	return
}
