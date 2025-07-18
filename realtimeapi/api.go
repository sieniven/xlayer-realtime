package realtimeapi

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
	realtimeCache "github.com/sieniven/xlayer-realtime/cache"
	"github.com/sieniven/xlayer-realtime/subscription"
)

var (
	ErrRealtimeNotEnabled           = fmt.Errorf("realtime is not enabled")
	ErrRealtimeNotSupported         = fmt.Errorf("unsupported on realtime backend")
	ErrRealtimeConfirmBlockNotFound = fmt.Errorf("realtime confirm block not found")
)

type RealtimeAPIImpl struct {
	cacheDB    *realtimeCache.RealtimeCache
	subService *subscription.RealtimeSubscription
	b          ethapi.Backend
	enableFlag bool
}

func NewRealtimeAPI(
	cacheDB *realtimeCache.RealtimeCache,
	subService *subscription.RealtimeSubscription,
	base ethapi.Backend,
	enableFlag bool,
) *RealtimeAPIImpl {
	return &RealtimeAPIImpl{
		cacheDB:    cacheDB,
		subService: subService,
		b:          base,
		enableFlag: enableFlag,
	}
}

func (b *RealtimeAPIImpl) ChainConfig() *params.ChainConfig {
	return b.b.ChainConfig()
}

func (b *RealtimeAPIImpl) Engine() consensus.Engine {
	return b.b.Engine()
}

func (b *RealtimeAPIImpl) GetEVM(ctx context.Context, state *state.StateDB, header *types.Header, vmConfig *vm.Config, blockCtx *vm.BlockContext) *vm.EVM {
	return b.b.GetEVM(ctx, state, header, vmConfig, blockCtx)
}

func (b *RealtimeAPIImpl) RPCEVMTimeout() time.Duration {
	return b.b.RPCEVMTimeout()
}

func (b *RealtimeAPIImpl) RPCTxFeeCap() float64 {
	return b.b.RPCTxFeeCap()
}

func (b *RealtimeAPIImpl) GetStateDB(ctx context.Context) (*state.StateDB, error) {
	if !b.enableFlag || b.cacheDB == nil {
		return nil, ErrRealtimeNotEnabled
	}

	statedb, _, err := b.b.StateAndHeaderByNumber(ctx, rpc.LatestBlockNumber)
	if err != nil {
		return nil, err
	}
	// Override the reader with the realtime state cache layer
	statedb.SetReader(b.cacheDB.State)

	return statedb, nil
}

func (api *RealtimeAPIImpl) getBlockNumber(blockNr rpc.BlockNumber) (uint64, bool, error) {
	if !api.enableFlag || api.cacheDB == nil {
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
