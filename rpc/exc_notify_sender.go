package rpc

// NewExcNotifyWithSender creates an exception core that hands each aggregated
// alert to sender instead of a Session. The sender runs on a timer goroutine
// and must never panic or block; execution-backed runtimes pass a non-blocking
// queue so a failing channel cannot affect logging or trading.
func NewExcNotifyWithSender(sender func(map[string]interface{})) *ExcNotify {
	return newExcNotifyWithSender(sender)
}
