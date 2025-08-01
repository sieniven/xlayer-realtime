package types

import (
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
	realtimeTypes "github.com/sieniven/xlayer-realtime/types"
)

type BlockMessage struct {
	Header        *types.Header
	PrevBlockInfo *realtimeTypes.BlockInfo
}

func (msg BlockMessage) GetBlockInfo() (*types.Header, *realtimeTypes.BlockInfo, error) {
	if msg.Header == nil {
		return nil, nil, fmt.Errorf("header is nil")
	}
	if msg.Header.Number.Uint64() == 0 {
		return nil, nil, fmt.Errorf("block number is 0")
	}

	return msg.Header, msg.PrevBlockInfo, nil
}

func (msg BlockMessage) MarshalJSON() ([]byte, error) {
	type BlockMessage struct {
		Header        *types.Header            `json:"header"`
		PrevBlockInfo *realtimeTypes.BlockInfo `json:"prevBlockInfo"`
	}

	var enc BlockMessage
	enc.Header = msg.Header
	enc.PrevBlockInfo = msg.PrevBlockInfo

	return json.Marshal(&enc)
}
