package cache

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	libcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	realtimeTypes "github.com/sieniven/xlayer-realtime/types"
)

var (
	ErrNotReady = fmt.Errorf("state cache not initialized")
)

// StateCache implements the state.Reader interface (ContractCodeReader, StateReader).
// The cache holds the chainstate db, with a changeset cache layer that stores
// in-memory the state changes.
type StateCache struct {
	ctx        context.Context
	db         state.Database
	initHeight atomic.Uint64

	cacheLock    sync.RWMutex
	latestRoot   libcommon.Hash
	accountCache map[libcommon.Address]*types.StateAccount
	storageCache map[string]libcommon.Hash
	codeCache    map[libcommon.Hash][]byte
}

func NewStateCache(ctx context.Context, db state.Database, size int) (*StateCache, error) {
	return &StateCache{
		ctx:          ctx,
		db:           db,
		initHeight:   atomic.Uint64{},
		accountCache: make(map[libcommon.Address]*types.StateAccount, size),
		storageCache: make(map[string]libcommon.Hash, size),
		codeCache:    make(map[libcommon.Hash][]byte, size),
	}, nil
}

func (cache *StateCache) TryInitCache(executionHeight uint64) error {
	cache.cacheLock.Lock()
	defer cache.cacheLock.Unlock()

	if cache.initHeight.Load() != 0 {
		return fmt.Errorf("state cache already initialized")
	}
	cache.initHeight.Store(executionHeight)

	return nil
}

func (cache *StateCache) UpdateLatestRoot(latestRoot libcommon.Hash) {
	cache.cacheLock.Lock()
	defer cache.cacheLock.Unlock()
	cache.latestRoot = latestRoot
}

func (cache *StateCache) GetLatestRoot() libcommon.Hash {
	cache.cacheLock.RLock()
	defer cache.cacheLock.RUnlock()
	return cache.latestRoot
}

func (cache *StateCache) Clear() {
	cache.cacheLock.Lock()
	defer cache.cacheLock.Unlock()

	// Clear all caches
	for k := range cache.accountCache {
		delete(cache.accountCache, k)
	}
	for k := range cache.storageCache {
		delete(cache.storageCache, k)
	}
	for k := range cache.codeCache {
		delete(cache.codeCache, k)
	}
	cache.initHeight.Store(0)
}

func (cache *StateCache) GetInitHeight() uint64 {
	return cache.initHeight.Load()
}

// -------------- Cache operations --------------
func (cache *StateCache) ApplyChangeset(changeset *realtimeTypes.Changeset, blockNumber uint64, txIndex uint) error {
	// Handle account data changes
	addressChanges := make(map[libcommon.Address]*types.StateAccount)
	cache.applyChangesetToAccountData(changeset, addressChanges)

	cache.cacheLock.Lock()
	defer cache.cacheLock.Unlock()

	// Apply code changes
	for codeHash, code := range changeset.CodeChanges {
		cache.codeCache[codeHash] = code
	}

	// Apply storage changes
	for address, storage := range changeset.StorageChanges {
		for key, value := range storage {
			compositeKey := GenerateCompositeStorageKey(address.Bytes(), key.Bytes())
			cache.storageCache[string(compositeKey)] = value
		}
	}

	// Apply deleted accounts changes
	for address := range changeset.DeletedAccounts {
		// Non-existent / deleted accounts are set to nil
		addressChanges[address] = nil
	}

	// Apply account changes
	for address, account := range addressChanges {
		delete(cache.accountCache, address)
		cache.accountCache[address] = account
		log.Debug("[Realtime] ApplyChangeset: ", address)
	}

	log.Debug(fmt.Sprintf("[Realtime] Apply changeset from tx with height: %d, txIndex: %d\n", blockNumber, txIndex))

	return nil
}

func (cache *StateCache) applyChangesetToAccountData(changeset *realtimeTypes.Changeset, addressChanges map[libcommon.Address]*types.StateAccount) (err error) {
	// Apply balance changes
	for address, balance := range changeset.BalanceChanges {
		if _, ok := changeset.DeletedAccounts[address]; ok {
			continue
		}

		account, err := cache.getOrCreateAccount(address, addressChanges)
		if err != nil {
			return fmt.Errorf("apply balance changes failed: %v", err)
		}
		account.Balance.Set(balance)
	}

	// Apply nonce changes
	for address, nonce := range changeset.NonceChanges {
		if _, ok := changeset.DeletedAccounts[address]; ok {
			continue
		}

		account, err := cache.getOrCreateAccount(address, addressChanges)
		if err != nil {
			return fmt.Errorf("apply nonce changes failed: %v", err)
		}
		account.Nonce = nonce
	}

	// Apply code hash changes
	for address, codeHash := range changeset.CodeHashChanges {
		if _, ok := changeset.DeletedAccounts[address]; ok {
			continue
		}

		account, err := cache.getOrCreateAccount(address, addressChanges)
		if err != nil {
			return fmt.Errorf("apply code hash changes failed: %v", err)
		}
		account.CodeHash = codeHash[:]
	}

	return nil
}

func (cache *StateCache) getOrCreateAccount(address libcommon.Address, addressChanges map[libcommon.Address]*types.StateAccount) (*types.StateAccount, error) {
	account, ok := addressChanges[address]
	if !ok {
		var err error
		account, err = cache.unsafeReadAccountData(address)
		if err != nil {
			return nil, err
		}

		if account == nil {
			// Non-existent account, create new account
			account = cache.createAccount()
		}
		addressChanges[address] = account
	}

	return account, nil
}

func (cache *StateCache) unsafeReadAccountData(address libcommon.Address) (*types.StateAccount, error) {
	acc, ok := cache.accountCache[address]
	if ok {
		return acc, nil
	}

	// Cache miss, read from chainstate db
	return cache.GetAccountFromChainDb(address)
}

func (cache *StateCache) createAccount() *types.StateAccount {
	return types.NewEmptyStateAccount()
}

// -------------- StateReader implementation --------------
func (cache *StateCache) Account(addr libcommon.Address) (*types.StateAccount, error) {
	if cache.initHeight.Load() == 0 {
		return nil, ErrNotReady
	}

	cache.cacheLock.RLock()
	acc, ok := cache.accountCache[addr]
	if ok {
		accCopy := acc.Copy()
		cache.cacheLock.RUnlock()
		return accCopy, nil
	}
	cache.cacheLock.RUnlock()

	// Cache miss, read from chainstate db
	return cache.GetAccountFromChainDb(addr)
}

func (cache *StateCache) Storage(addr libcommon.Address, slot libcommon.Hash) (libcommon.Hash, error) {
	if cache.initHeight.Load() == 0 {
		return libcommon.Hash{}, ErrNotReady
	}

	compositeKey := GenerateCompositeStorageKey(addr.Bytes(), slot.Bytes())

	cache.cacheLock.RLock()
	storage, ok := cache.storageCache[string(compositeKey)]
	if ok {
		cache.cacheLock.RUnlock()
		return storage, nil
	}
	cache.cacheLock.RUnlock()

	// Cache miss, read from chainstate db
	return cache.GetAccountStorageFromChainDb(addr, slot)
}

func (cache *StateCache) Code(addr libcommon.Address, codeHash libcommon.Hash) ([]byte, error) {
	if bytes.Equal(codeHash.Bytes(), types.EmptyCodeHash[:]) {
		return nil, nil
	}

	if cache.initHeight.Load() == 0 {
		return nil, ErrNotReady
	}

	cache.cacheLock.RLock()
	code, ok := cache.codeCache[codeHash]
	if ok {
		cache.cacheLock.RUnlock()
		return code, nil
	}
	cache.cacheLock.RUnlock()

	// Cache miss, read from chainstate db
	return cache.GetAccountCodeFromChainDb(addr, codeHash)
}

func (cache *StateCache) CodeSize(addr libcommon.Address, codeHash libcommon.Hash) (int, error) {
	code, err := cache.Code(addr, codeHash)
	return len(code), err
}

// -------------- Chainstate db reader operations --------------
func (cache *StateCache) GetAccountFromChainDb(address libcommon.Address) (*types.StateAccount, error) {
	reader, err := cache.db.Reader(cache.latestRoot)
	if err != nil {
		return nil, err
	}
	return reader.Account(address)
}

func (cache *StateCache) GetAccountStorageFromChainDb(address libcommon.Address, key libcommon.Hash) (libcommon.Hash, error) {
	reader, err := cache.db.Reader(cache.latestRoot)
	if err != nil {
		return libcommon.Hash{}, err
	}
	return reader.Storage(address, key)
}

func (cache *StateCache) GetAccountCodeFromChainDb(address libcommon.Address, codeHash libcommon.Hash) ([]byte, error) {
	reader, err := cache.db.Reader(cache.latestRoot)
	if err != nil {
		return nil, err
	}
	return reader.Code(address, codeHash)
}

func (cache *StateCache) GetAccountCodeSizeFromChainDb(address libcommon.Address, codeHash libcommon.Hash) (int, error) {
	reader, err := cache.db.Reader(cache.latestRoot)
	if err != nil {
		return 0, err
	}
	return reader.CodeSize(address, codeHash)
}

// -------------- Debug operations --------------
func (cache *StateCache) DebugDumpToFile(cacheDumpPath string) error {
	cache.cacheLock.RLock()
	defer cache.cacheLock.RUnlock()

	accountData := make(map[string]string)
	for addr, acc := range cache.accountCache {
		value := types.SlimAccountRLP(*acc)
		accountData[hex.EncodeToString(addr[:])] = hex.EncodeToString(value)
	}
	if err := realtimeTypes.WriteToJSON(filepath.Join(cacheDumpPath, "account_cache.json"), accountData); err != nil {
		return fmt.Errorf("failed to dump account cache: %v", err)
	}

	storageData := make(map[string]string)
	for key, value := range cache.storageCache {
		storageData[hex.EncodeToString([]byte(key))] = hex.EncodeToString(value.Bytes())
	}
	if err := realtimeTypes.WriteToJSON(filepath.Join(cacheDumpPath, "storage_cache.json"), storageData); err != nil {
		return fmt.Errorf("failed to dump storage cache: %v", err)
	}

	codeData := make(map[string]string)
	for hash, code := range cache.codeCache {
		codeData[hex.EncodeToString(hash[:])] = hex.EncodeToString(code)
	}
	if err := realtimeTypes.WriteToJSON(filepath.Join(cacheDumpPath, "code_cache.json"), codeData); err != nil {
		return fmt.Errorf("failed to dump code cache: %v", err)
	}

	return nil
}

// DebugCompare compares the state cache with the chain-state db, and returns the
// list of account addresses that have differing states.
func (cache *StateCache) DebugCompare(reader state.Reader) []string {
	cache.cacheLock.RLock()
	defer cache.cacheLock.RUnlock()

	mismatches := []string{}
	for addr, accCache := range cache.accountCache {
		log.Info("[Realtime] Comparing account", "address", addr.String())
		accDb, err := reader.Account(addr)
		if err != nil {
			mismatch := fmt.Sprintf("chain-state db reader error, failed to read account. address: %s, error: %v", addr.String(), err)
			mismatches = append(mismatches, mismatch)
			continue
		}
		if accDb == nil {
			mismatch := fmt.Sprintf("account %s not found in database", addr.String())
			mismatches = append(mismatches, mismatch)
			continue
		}

		if accCache.Nonce != accDb.Nonce {
			mismatch := fmt.Sprintf("nonce mismatch, account %s, cache nonce: %d, db nonce: %d", addr.String(), accCache.Nonce, accDb.Nonce)
			mismatches = append(mismatches, mismatch)
		}

		if accCache.Balance.Cmp(accDb.Balance) != 0 {
			mismatch := fmt.Sprintf("balance mismatch, account %s, cache balance: %d, db balance: %d", addr.String(), accCache.Balance.ToBig(), accDb.Balance.ToBig())
			mismatches = append(mismatches, mismatch)
		}

		// Note that realtime state cache does not update the state account storage trie root
		if accCache.Root != types.EmptyRootHash {
			mismatch := fmt.Sprintf("root mismatch, account %s, cache root: %s, db root: %s", addr.String(), accCache.Root.String(), accDb.Root.String())
			mismatches = append(mismatches, mismatch)
		}

		if !bytes.Equal(accCache.CodeHash, accDb.CodeHash) {
			mismatch := fmt.Sprintf("codehash mismatch, account %s, cache codehash: %s, db codehash: %s", addr.String(), hex.EncodeToString(accCache.CodeHash), hex.EncodeToString(accDb.CodeHash))
			mismatches = append(mismatches, mismatch)
		}
	}

	return mismatches
}
