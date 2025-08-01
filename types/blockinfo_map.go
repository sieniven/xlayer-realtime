package types

import (
	"path/filepath"
	"sync"

	libcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type BlockInfo struct {
	Header  *types.Header  `json:"header"`
	TxCount int64          `json:"txCount"`
	Hash    libcommon.Hash `json:"hash"`
}

type BlockInfoMap struct {
	blockInfos map[uint64]*BlockInfo
	mu         sync.RWMutex
}

func NewBlockInfoMap(size int) *BlockInfoMap {
	return &BlockInfoMap{
		blockInfos: make(map[uint64]*BlockInfo, size),
	}
}

func (bm *BlockInfoMap) Get(blockNum uint64) (*types.Header, int64, libcommon.Hash, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	blockInfo, exists := bm.blockInfos[blockNum]
	if exists {
		return blockInfo.Header, blockInfo.TxCount, blockInfo.Hash, true
	}
	return nil, 0, libcommon.Hash{}, exists
}

func (bm *BlockInfoMap) PutHeader(blockNum uint64, header *types.Header, prevBlockInfo *BlockInfo) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.blockInfos[blockNum] = &BlockInfo{
		Header:  header,
		TxCount: -1,
		Hash:    libcommon.Hash{},
	}

	// Update previous block header tx count
	prevBlockNum := blockNum - 1
	_, exists := bm.blockInfos[prevBlockNum]
	if exists {
		// Update previous block info
		bm.blockInfos[prevBlockNum] = prevBlockInfo
	}
}

func (bm *BlockInfoMap) Delete(blockNum uint64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	delete(bm.blockInfos, blockNum)
}

func (bm *BlockInfoMap) Clear() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	for k := range bm.blockInfos {
		delete(bm.blockInfos, k)
	}
}

// -------------- Debug operations --------------
func (bm *BlockInfoMap) DebugDumpToFile(cacheDumpPath string) error {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	return WriteToJSON(filepath.Join(cacheDumpPath, "block_info_map.json"), bm.blockInfos)
}
