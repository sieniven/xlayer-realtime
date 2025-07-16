package cache

import (
	libcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	realtimeTypes "github.com/sieniven/xlayer-realtime/types"
)

type StatelessCache struct {
	blockInfoMap *realtimeTypes.BlockInfoMap
	txInfoMap    *realtimeTypes.TxInfoMap
}

func NewStatelessCache(blockCacheSize int, txCacheSize int) *StatelessCache {
	return &StatelessCache{
		blockInfoMap: realtimeTypes.NewBlockInfoMap(blockCacheSize),
		txInfoMap:    realtimeTypes.NewTxInfoMap(blockCacheSize, txCacheSize),
	}
}

func (cache *StatelessCache) Clear() {
	cache.blockInfoMap.Clear()
	cache.txInfoMap.Clear()
}

// -------------- Read operations --------------
func (cache *StatelessCache) GetHeader(blockNum uint64) (*types.Header, int64, bool) {
	return cache.blockInfoMap.Get(blockNum)
}

func (cache *StatelessCache) GetTxInfo(txHash libcommon.Hash) (*types.Transaction, *types.Receipt, uint64, []*types.InnerTx, bool) {
	return cache.txInfoMap.GetTx(txHash)
}

func (cache *StatelessCache) GetBlockTxs(blockNum uint64) ([]libcommon.Hash, bool) {
	return cache.txInfoMap.GetBlockTxs(blockNum)
}

// -------------- Write operations --------------
func (cache *StatelessCache) PutHeader(blockNum uint64, header *types.Header, prevTxCount int64) {
	cache.blockInfoMap.PutHeader(blockNum, header, prevTxCount)
}

func (cache *StatelessCache) PutTxInfo(blockNum uint64, txHash libcommon.Hash, tx *types.Transaction, receipt *types.Receipt, innerTxs []*types.InnerTx) {
	cache.txInfoMap.Put(blockNum, txHash, tx, receipt, innerTxs)
}

func (cache *StatelessCache) DeleteBlock(blockNum uint64) {
	cache.blockInfoMap.Delete(blockNum)
	cache.txInfoMap.Delete(blockNum)
}

// -------------- Debug operations --------------
func (cache *StatelessCache) DebugDumpToFile(cacheDumpPath string) error {
	err := cache.blockInfoMap.DebugDumpToFile(cacheDumpPath)
	if err != nil {
		return err
	}
	return cache.txInfoMap.DebugDumpToFile(cacheDumpPath)
}
