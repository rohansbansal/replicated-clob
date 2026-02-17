package obs

import (
	"context"
	"fmt"
)

type Client struct{}

func New() *Client {
	return &Client{}
}

func (c *Client) LogNotice(ctx context.Context, msg string, args ...interface{}) {
	l := fmt.Sprintf(msg, args...)

	l = fmt.Sprintf("[NOTICE] %s\n", l)
	fmt.Printf(l)

}

func (c *Client) LogDebug(ctx context.Context, msg string, args ...interface{}) {
	l := fmt.Sprintf(msg, args...)

	var reqId string
	if reqIdVal, ok := ctx.Value("reqId").(string); ok {
		reqId = reqIdVal
	}
	l = fmt.Sprintf("[DEBUG][%v] %s\n", reqId, l)
	fmt.Printf(l)

}

func (c *Client) LogInfo(ctx context.Context, msg string, args ...interface{}) {
	l := fmt.Sprintf(msg, args...)

	var reqId string
	if reqIdVal, ok := ctx.Value("reqId").(string); ok {
		reqId = reqIdVal
	}
	l = fmt.Sprintf("[INFO][%v] %s\n", reqId, l)
	fmt.Printf(l)

}

func (c *Client) LogErr(ctx context.Context, msg string, args ...interface{}) {
	l := fmt.Sprintf(msg, args...)

	l = fmt.Sprintf("[ERROR] %s\n", l)
	fmt.Printf(l)

}

func (c *Client) LogAlert(ctx context.Context, msg string, args ...interface{}) {
	l := fmt.Sprintf(msg, args...)

	l = fmt.Sprintf("[ALERT] %s\n", l)
	fmt.Printf(l)

}
