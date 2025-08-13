package test

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/realtime/rtclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"
)

// TestRealtimeComparison is the main test function that compares various RPC methods
// between realtime and non-realtime enabled nodes to ensure output is identical
func TestRealtimeComparison(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(DefaultL2AdminPrivateKey, "0x"))
	require.NoError(t, err)
	ec, err := ethclient.Dial(DefaultL2NetworkRealtimeURL)
	require.NoError(t, err)
	client, err := rtclient.NewRealtimeClient(ctx, ec, DefaultL2NetworkRealtimeURL)
	require.NoError(t, err)

	// Create shared RPC client for direct JSON RPC calls to non-realtime node
	nonRealtimeRPCClient, err := rpc.Dial(DefaultL2NetworkNoRealtimeURL)
	require.NoError(t, err)
	defer nonRealtimeRPCClient.Close()

	latestBlockNumber, err := client.RealtimeBlockNumber(ctx)
	require.NoError(t, err)
	log.Info(fmt.Sprintf("Latest block number at test start: %d", latestBlockNumber))

	testBlocks := []string{}

	for i := 0; i < 10; i++ {
		testBlocks = append(testBlocks, fmt.Sprintf("0x%x", latestBlockNumber-uint64(i)))
	}

	fromAddress := common.HexToAddress(DefaultL2AdminAddress)
	log.Info(fmt.Sprintf("Sender: %s", fromAddress))

	testAddress := common.HexToAddress("0x1234567890123456789012345678901234567890")
	txHash := transToken(t, context.Background(), client, big.NewInt(Gwei), testAddress.String())
	txHashCommon := common.HexToHash(txHash)
	time.Sleep(1 * time.Second)

	erc20Address := deployERC20Contract(t, ctx, privateKey, client)

	log.Info("Starting realtime comparison test", "realtimeURL", DefaultL2NetworkRealtimeURL, "nonRealtimeURL", DefaultL2NetworkNoRealtimeURL)

	// TestStatelessAPIs - Block and Transaction Data
	t.Run("TestStatelessAPIs", func(t *testing.T) {
		log.Info("Running stateless comparison tests")

		t.Run("getBlockByNumber", func(t *testing.T) {
			allPassed := true

			for _, blockParam := range testBlocks {
				blockNumber, err := convertBlockParam(ctx, client, blockParam)
				if err != nil {
					t.Errorf("Failed to convert block parameter %v: %v", blockParam, err)
					allPassed = false
					continue
				}

				// Get block from realtime node
				realtimeBlock, err := client.RealtimeGetBlockByNumber(ctx, blockNumber)
				if err != nil {
					t.Errorf("Failed to get block from realtime node for %v: %v", blockParam, err)
					allPassed = false
					continue
				}

				// Make direct RPC call to non-realtime node to get JSON response
				var nonRealtimeMap map[string]interface{}
				err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeMap, "eth_getBlockByNumber", blockParam, true)
				if err != nil {
					t.Errorf("Failed to get block from non-realtime node for %v: %v", blockParam, err)
					allPassed = false
					continue
				}

				err = CompareBlock(realtimeBlock, nonRealtimeMap, fmt.Sprintf("block_%v", blockParam))
				if err != nil {
					t.Errorf("Block responses differ for %v: %v", blockParam, err)
					allPassed = false
				}
			}

			require.True(t, allPassed, "getBlockByNumber test failed - some scenarios did not pass")
		})

		t.Run("getBlockByHash", func(t *testing.T) {
			allPassed := true

			// add pending to test getBlockByHash
			testBlocks = append(testBlocks, "pending")

			for _, blockParam := range testBlocks {
				blockNumber, err := convertBlockParam(ctx, client, blockParam)
				if err != nil {
					t.Logf("Failed to convert block parameter %v: %v", blockParam, err)
					continue
				}

				blockByNumber, err := client.RealtimeGetBlockByNumber(ctx, blockNumber)
				if err != nil {
					t.Logf("Could not get block %v by number: %v", blockParam, err)
					continue
				}

				blockHash, ok := extractBlockHash(blockByNumber, blockParam)
				if !ok {
					t.Logf("Block %v does not have a valid hash", blockParam)
					continue
				}
				log.Info(fmt.Sprintf("Comparing block %v by hash: %s", blockParam, blockHash.Hex()))

				// Get block from realtime node
				realtimeBlock, err := client.RealtimeGetBlockByHash(ctx, blockHash, true)
				if err != nil {
					t.Errorf("Failed to get block from realtime node for %v: %v", blockParam, err)
					allPassed = false
					continue
				}

				var nonRealtimeMap map[string]interface{}
				err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeMap, "eth_getBlockByHash", blockHash, true)
				if err != nil {
					t.Errorf("Failed to get block from non-realtime node for %v: %v", blockParam, err)
					allPassed = false
					continue
				}

				err = CompareBlock(realtimeBlock, nonRealtimeMap, fmt.Sprintf("block_%v_hash", blockParam))
				if err != nil {
					t.Errorf("Block responses differ for %v hash %s: %v", blockParam, blockHash.Hex(), err)
					allPassed = false
				}
			}

			require.True(t, allPassed, "getBlockByHash test failed - some scenarios did not pass")
		})

		t.Run("getBlockTransactionCountByNumber", func(t *testing.T) {
			numberOfTransactions := 5
			txHashes := transTokenBatch(t, context.Background(), client, big.NewInt(Gwei), testAddress.String(), numberOfTransactions)
			lastTxHash := txHashes[len(txHashes)-1]

			// Get the block information from the last transaction's receipt
			receipt, err := client.RealtimeGetTransactionReceipt(ctx, common.HexToHash(lastTxHash))
			require.NoError(t, err)
			require.NotNil(t, receipt, "Transaction receipt should not be nil")

			targetBlockNumber := receipt.BlockNumber.Uint64()

			// Get transaction count from realtime node
			realtimeTxCount, err := client.RealtimeGetBlockTransactionCountByNumber(ctx, targetBlockNumber)
			require.NoError(t, err)

			time.Sleep(1 * time.Second)

			blockNumberHex := fmt.Sprintf("0x%x", targetBlockNumber)
			var nonRealtimeTxCountHex string
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeTxCountHex, "eth_getBlockTransactionCountByNumber", blockNumberHex)
			require.NoError(t, err)

			nonRealtimeTxCount, err := strconv.ParseUint(strings.TrimPrefix(nonRealtimeTxCountHex, "0x"), 16, 64)
			require.NoError(t, err)

			require.Equal(t, realtimeTxCount, nonRealtimeTxCount, fmt.Sprintf("Transaction counts should match for block %d: realtime=%d, non-realtime=%d", targetBlockNumber, realtimeTxCount, nonRealtimeTxCount))
		})

		t.Run("getBlockTransactionCountByHash", func(t *testing.T) {
			numberOfTransactions := 5
			txHashes := transTokenBatch(t, context.Background(), client, big.NewInt(Gwei), testAddress.String(), numberOfTransactions)
			lastTxHash := txHashes[len(txHashes)-1]

			receipt, err := client.RealtimeGetTransactionReceipt(ctx, common.HexToHash(lastTxHash))
			require.NoError(t, err)
			require.NotNil(t, receipt, "Transaction receipt should not be nil")

			targetBlockNumber := receipt.BlockNumber.Uint64()
			targetBlockHash := receipt.BlockHash

			// Get the actual transaction count for this block by number
			actualTxCount, err := client.RealtimeGetBlockTransactionCountByNumber(ctx, targetBlockNumber)
			require.NoError(t, err)

			realtimeTxCount, err := client.RealtimeGetBlockTransactionCountByHash(ctx, targetBlockHash)
			require.NoError(t, err)

			time.Sleep(1 * time.Second)

			var nonRealtimeTxCountHex string
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeTxCountHex, "eth_getBlockTransactionCountByHash", targetBlockHash)
			require.NoError(t, err)

			// Convert hex string to uint64
			nonRealtimeTxCount, err := strconv.ParseUint(strings.TrimPrefix(nonRealtimeTxCountHex, "0x"), 16, 64)
			require.NoError(t, err)

			require.Equal(t, actualTxCount, realtimeTxCount, fmt.Sprintf("Transaction count by hash should match count by number (%d)", actualTxCount))
			require.Equal(t, realtimeTxCount, nonRealtimeTxCount, fmt.Sprintf("Transaction counts should match for block %d hash %s: realtime=%d, non-realtime=%d", targetBlockNumber, targetBlockHash.Hex(), realtimeTxCount, nonRealtimeTxCount))

		})

		// Test getBlockInternalTransactions
		t.Run("getBlockInternalTransactions", func(t *testing.T) {
			txHash := transToken(t, context.Background(), client, big.NewInt(Gwei), testAddress.String())

			receipt, err := client.RealtimeGetTransactionReceipt(ctx, common.HexToHash(txHash))
			require.NoError(t, err)
			require.NotNil(t, receipt, "Transaction receipt should not be nil")

			targetBlockNumber := receipt.BlockNumber.Uint64()

			// Get internal transactions from realtime node
			realtimeInternalTxs, err := client.RealtimeGetBlockInternalTransactions(ctx, targetBlockNumber)
			require.NoError(t, err)
			require.NotNil(t, realtimeInternalTxs, "Realtime internal transactions map should not be nil")

			time.Sleep(1 * time.Second)

			blockNumberHex := fmt.Sprintf("0x%x", targetBlockNumber)
			var nonRealtimeInternalTxs map[common.Hash][]*types.InnerTx
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeInternalTxs, "eth_getBlockInternalTransactions", blockNumberHex)
			require.NoError(t, err)
			require.NotNil(t, nonRealtimeInternalTxs, "Non-realtime internal transactions should not be nil")

			require.Equal(t, realtimeInternalTxs, nonRealtimeInternalTxs, fmt.Sprintf("Internal transactions should be identical for block %d", targetBlockNumber))
		})

		t.Run("getTransactionByHash", func(t *testing.T) {
			txHashNew := transToken(t, context.Background(), client, big.NewInt(Gwei), testAddress.String())
			realtimeTransaction, err := client.RealtimeGetTransactionByHash(ctx, common.HexToHash(txHashNew))
			require.NoError(t, err)

			// Make direct RPC call to non-realtime node to get JSON response
			var nonRealtimeTransaction *types.Transaction
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeTransaction, "eth_getTransactionByHash", common.HexToHash(txHashNew))
			require.NoError(t, err)

			require.Equal(t, realtimeTransaction, nonRealtimeTransaction, fmt.Sprintf("Transactions should be identical for hash %s", txHash))
		})

		t.Run("getRawTransactionByHash", func(t *testing.T) {
			realtimeTransactionBytes, err := client.RealtimeGetRawTransactionByHash(ctx, txHashCommon)
			require.NoError(t, err)

			var nonRealtimeTransaction string
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeTransaction, "eth_getRawTransactionByHash", txHashCommon)
			require.NoError(t, err)
			require.NotEmpty(t, nonRealtimeTransaction, "Non-realtime transaction should not be empty")

			realtimeTransactionHex := "0x" + hex.EncodeToString(realtimeTransactionBytes)

			require.Equal(t, realtimeTransactionHex, nonRealtimeTransaction, fmt.Sprintf("Raw transactions should be identical for hash %s", txHash))
		})

		t.Run("getTransactionReceipt", func(t *testing.T) {
			realtimeReceipt, err := client.RealtimeGetTransactionReceipt(ctx, txHashCommon)
			require.NoError(t, err)

			var nonRealtimeReceipt *types.Receipt
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeReceipt, "eth_getTransactionReceipt", txHashCommon)
			require.NoError(t, err)
			require.NotNil(t, nonRealtimeReceipt, "Non-realtime receipt should not be nil")

			require.Equal(t, realtimeReceipt, nonRealtimeReceipt, fmt.Sprintf("Transaction receipts should be identical for hash %s", txHash))
		})

		t.Run("getInternalTransactions", func(t *testing.T) {
			realtimeInternalTxs, err := client.RealtimeGetInternalTransactions(ctx, txHashCommon)
			require.NoError(t, err)

			var nonRealtimeInternalTxs []*types.InnerTx
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeInternalTxs, "eth_getInternalTransactions", txHashCommon)
			require.NoError(t, err)
			require.NotNil(t, nonRealtimeInternalTxs, "Non-realtime internal transactions should not be nil")

			require.Equal(t, realtimeInternalTxs, nonRealtimeInternalTxs, fmt.Sprintf("Internal transactions should be identical for hash %s", txHash))
		})
	})

	// TestStateAPIs - Balances, Code, Storage, and Contract Calls
	t.Run("TestStateAPIs", func(t *testing.T) {
		log.Info("Running state comparison tests")

		t.Run("blockNumber", func(t *testing.T) {
			realtimeBlockNumber, err := client.RealtimeBlockNumber(ctx)
			require.NoError(t, err)

			var nonRealtimeBlockNumber string
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeBlockNumber, "eth_blockNumber")
			require.NoError(t, err)
			require.Equal(t, "0x"+strconv.FormatUint(realtimeBlockNumber, 16), nonRealtimeBlockNumber, "Block numbers should match")
		})

		t.Run("call", func(t *testing.T) {
			data, err := erc20ABI.Pack("balanceOf", fromAddress)
			require.NoError(t, err)

			realtimeCall, err := client.RealtimeCall(ctx, testAddress, erc20Address, "0x100000", "0x1", "0x0", fmt.Sprintf("0x%x", data))
			require.NoError(t, err)

			callArgs := map[string]interface{}{
				"from":     testAddress.Hex(),
				"to":       erc20Address.Hex(),
				"gas":      "0x100000",
				"gasPrice": "0x1",
				"value":    "0x0",
				"data":     fmt.Sprintf("0x%x", data),
			}

			var nonRealtimeCall string
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeCall, "eth_call", callArgs, "latest")
			require.NoError(t, err)
			require.Equal(t, realtimeCall, nonRealtimeCall, "Calls should match")
		})

		t.Run("getBalance", func(t *testing.T) {
			realtimeBalance, err := client.RealtimeGetBalance(ctx, testAddress)
			require.NoError(t, err)

			var nonRealtimeBalance string
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeBalance, "eth_getBalance", testAddress, "latest")
			require.NoError(t, err)

			require.Equal(t, "0x"+realtimeBalance.Text(16), nonRealtimeBalance, "Balances should match")
		})

		t.Run("getTransactionCount", func(t *testing.T) {
			realtimeTransactionCount, err := client.RealtimeGetTransactionCount(ctx, testAddress)
			require.NoError(t, err)

			var nonRealtimeTransactionCount string
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeTransactionCount, "eth_getTransactionCount", testAddress, "latest")
			require.NoError(t, err)

			require.Equal(t, "0x"+strconv.FormatUint(realtimeTransactionCount, 16), nonRealtimeTransactionCount, "Transaction counts should match")
		})

		t.Run("getCode", func(t *testing.T) {
			realtimeCode, err := client.RealtimeGetCode(ctx, erc20Address)
			require.NoError(t, err)

			var nonRealtimeCode string
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeCode, "eth_getCode", erc20Address, "latest")
			require.NoError(t, err)

			require.Equal(t, realtimeCode, nonRealtimeCode, "Codes should match")
		})

		t.Run("getStorageAt", func(t *testing.T) {
			realtimeStorage, err := client.RealtimeGetStorageAt(ctx, erc20Address, "0x2")
			require.NoError(t, err)

			var nonRealtimeStorage string
			err = nonRealtimeRPCClient.CallContext(context.Background(), &nonRealtimeStorage, "eth_getStorageAt", erc20Address, "0x2", "latest")
			require.NoError(t, err)

			require.Equal(t, realtimeStorage, nonRealtimeStorage, "Storages should match")
		})
	})
}
