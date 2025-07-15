package types

import (
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
)

type BlockMessage struct {
	Header           *types.Header
	PrevBlockTxCount int64
}

func ToKafkaBlockMessage(header *types.Header, prevBlockTxCount int64) (blockMsg BlockMessage, err error) {
	blockMsg = BlockMessage{
		Header:           header,
		PrevBlockTxCount: prevBlockTxCount,
	}

	return blockMsg, nil
}

func (msg BlockMessage) GetBlockInfo() (*types.Header, int64, error) {
	if msg.Header == nil {
		return nil, 0, fmt.Errorf("header is nil")
	}
	if msg.Header.Number.Uint64() == 0 {
		return nil, 0, fmt.Errorf("block number is 0")
	}

	return msg.Header, msg.PrevBlockTxCount, nil
}

func (msg BlockMessage) MarshalJSON() ([]byte, error) {
	type BlockMessage struct {
		Header           *types.Header `json:"header"`
		PrevBlockTxCount int64         `json:"prevBlockTxCount"`
	}

	var enc BlockMessage
	enc.Header = msg.Header
	enc.PrevBlockTxCount = msg.PrevBlockTxCount

	return json.Marshal(&enc)
}
