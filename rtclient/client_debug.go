package rtclient

import "context"

// RealtimeDumpStateCache dumps the state cache
func (rc *RealtimeClient) RealtimeDumpCache(ctx context.Context) error {
	var result interface{}
	err := rc.c.CallContext(ctx, &result, "eth_debugDumpRealtimeCache")
	if err != nil {
		return err
	}
	return nil
}
