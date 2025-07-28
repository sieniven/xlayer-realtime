package realtimeapi

import (
	"context"
	"fmt"
	"math/big"
	"time"

	libcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/filters"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
	realtimeCache "github.com/sieniven/xlayer-realtime/cache"
	"github.com/sieniven/xlayer-realtime/subscription"
)

var (
	MockBlockHash         = libcommon.BytesToHash([]byte{1})
	EmptyBlockHash        = libcommon.Hash{}
	ErrRealtimeNotEnabled = fmt.Errorf("realtime is not enabled")
)

type RealtimeAPIImpl struct {
	cacheDB    *realtimeCache.RealtimeCache
	subService *subscription.RealtimeSubscription
	b          ethapi.Backend
	filterApi  *filters.FilterAPI
}

func NewRealtimeAPI(
	cacheDB *realtimeCache.RealtimeCache,
	subService *subscription.RealtimeSubscription,
	base ethapi.Backend,
	filterApi *filters.FilterAPI,
) *RealtimeAPIImpl {
	return &RealtimeAPIImpl{
		cacheDB:    cacheDB,
		subService: subService,
		b:          base,
		filterApi:  filterApi,
	}
}

func (api *RealtimeAPIImpl) GetStateDB(ctx context.Context) (*state.StateDB, error) {
	if api.cacheDB == nil || !api.cacheDB.ReadyFlag.Load() {
		return nil, ErrRealtimeNotEnabled
	}

	statedb, _, err := api.b.StateAndHeaderByNumber(ctx, rpc.LatestBlockNumber)
	if err != nil {
		return nil, err
	}
	// Override the reader with the realtime state cache layer
	statedb.SetReader(api.cacheDB.State)

	return statedb, nil
}

func (api *RealtimeAPIImpl) getBlockNumber(blockNr rpc.BlockNumber) (uint64, bool, error) {
	if api.cacheDB == nil || !api.cacheDB.ReadyFlag.Load() {
		return 0, false, ErrRealtimeNotEnabled
	}

	confirmHeight := api.cacheDB.GetHighestConfirmHeight()
	if confirmHeight == 0 {
		return 0, false, fmt.Errorf("no block number found in stateless cache")
	}

	switch blockNr {
	case rpc.LatestBlockNumber:
		return confirmHeight, true, nil
	case rpc.EarliestBlockNumber:
		// Unsupported
		return 0, false, fmt.Errorf("earliest block number is not supported")
	case rpc.FinalizedBlockNumber:
		return confirmHeight, true, nil
	case rpc.SafeBlockNumber:
		return confirmHeight, true, nil
	case rpc.PendingBlockNumber:
		pendingHeight := api.cacheDB.GetHighestPendingHeight()
		if pendingHeight == 0 {
			return 0, false, fmt.Errorf("no block number found in stateless cache")
		}
		return pendingHeight, true, nil
	default:
		blockNumber := uint64(blockNr.Int64())
		if blockNumber > confirmHeight {
			return 0, false, fmt.Errorf("block with number %d not found", blockNumber)
		}
		return blockNumber, blockNumber == confirmHeight, nil
	}
}

func (api *RealtimeAPIImpl) ChainConfig() *params.ChainConfig {
	return api.b.ChainConfig()
}

func (api *RealtimeAPIImpl) Engine() consensus.Engine {
	return api.b.Engine()
}

func (api *RealtimeAPIImpl) GetEVM(ctx context.Context, state *state.StateDB, header *types.Header, vmConfig *vm.Config, blockCtx *vm.BlockContext) *vm.EVM {
	return api.b.GetEVM(ctx, state, header, vmConfig, blockCtx)
}

func (api *RealtimeAPIImpl) RPCEVMTimeout() time.Duration {
	return api.b.RPCEVMTimeout()
}

func (api *RealtimeAPIImpl) RPCTxFeeCap() float64 {
	return api.b.RPCTxFeeCap()
}

// newRPCTransaction_realtime returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
// Note that realtime API do not support blockHash.
func newRPCTransaction_realtime(tx *types.Transaction, txblockhash libcommon.Hash, blockNumber uint64, blockTime uint64, index uint64, baseFee *big.Int, config *params.ChainConfig, receipt *types.Receipt) *ethapi.RPCTransaction {
	blockhash := txblockhash
	if blockhash == EmptyBlockHash {
		blockhash = MockBlockHash
	}

	result := ethapi.NewRPCTransaction(tx, blockhash, blockNumber, blockTime, index, baseFee, config, receipt)
	result.BlockHash = &libcommon.Hash{}
	return result
}
