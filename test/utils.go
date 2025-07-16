package test

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/sieniven/xlayer-realtime/rtclient"
	"github.com/stretchr/testify/require"
)

var (
	erc20ABI, _         = abi.JSON(strings.NewReader(erc20ABIJson))
	factoryABI, _       = abi.JSON(strings.NewReader(factoryABIJson))
	destroyABI, _       = abi.JSON(strings.NewReader(destroyABIJson))
	createDestroyABI, _ = abi.JSON(strings.NewReader(createDestroyABIJson))
)

type RealtimeResult struct {
	Header   *types.Header      `json:"Header,omitempty"`
	TxHash   string             `json:"TxHash,omitempty"`
	TxData   *types.Transaction `json:"TxData,omitempty"`
	Receipt  *types.Receipt     `json:"Receipt,omitempty"`
	InnerTxs []*types.InnerTx   `json:"InnerTxs,omitempty"`
}

// setupRealtimeTestEnvironment creates a test environment with necessary data for tests
func setupRealtimeTestEnvironment(t *testing.T, ctx context.Context, rtclient *rtclient.RealtimeClient) uint64 {
	// Wait for at least one block to be available
	var blockNumber uint64
	var err error
	for i := 0; i < 30; i++ {
		blockNumber, err = rtclient.RealtimeBlockNumber(ctx)
		require.NoError(t, err)
		fmt.Printf("Realtime block number: %d, attempt: %v\n", blockNumber, i)
		if blockNumber > 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	require.Greater(t, blockNumber, uint64(0), "Realtime block number should be greater than 0")
	return blockNumber
}

// GetAuth configures and returns an auth object.
func GetAuth(privateKeyStr string, chainID uint64) (*bind.TransactOpts, error) {
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(privateKeyStr, "0x"))
	if err != nil {
		return nil, err
	}

	return bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(0).SetUint64(chainID))
}

func GetTestChainConfig(chainID uint64) *params.ChainConfig {
	return params.TestChainConfig
}

func nativeTransferTx(t *testing.T, ctx context.Context, client *ethclient.Client, amount *big.Int, toAddress string) *types.Transaction {
	chainID, err := client.ChainID(ctx)
	require.NoError(t, err)
	auth, err := GetAuth(DefaultL2AdminPrivateKey, chainID.Uint64())
	require.NoError(t, err)
	nonce, err := client.PendingNonceAt(ctx, auth.From)
	require.NoError(t, err)
	gasPrice, err := client.SuggestGasPrice(ctx)
	require.NoError(t, err)

	to := common.HexToAddress(toAddress)
	gas, err := client.EstimateGas(ctx, ethereum.CallMsg{
		From:  auth.From,
		To:    &to,
		Value: amount,
	})
	require.NoError(t, err)

	tx := types.NewTransaction(
		nonce,
		to,
		amount,
		gas,
		gasPrice,
		nil,
	)

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(DefaultL2AdminPrivateKey, "0x"))
	require.NoError(t, err)

	signer := types.MakeSigner(GetTestChainConfig(DefaultL2ChainID), big.NewInt(1), 0)
	signedTx, err := types.SignTx(tx, signer, privateKey)
	require.NoError(t, err)

	err = client.SendTransaction(ctx, signedTx)
	require.NoError(t, err)

	return signedTx
}

func deployERC20Contract(
	t *testing.T,
	ctx context.Context,
	privateKey *ecdsa.PrivateKey,
	client *ethclient.Client,
) common.Address {
	publicKey := privateKey.Public()
	tmpPublicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	require.True(t, ok)
	fromAddress := crypto.PubkeyToAddress(*tmpPublicKeyECDSA)
	fmt.Printf("Sender: %s\n", fromAddress)

	nonce, err := client.PendingNonceAt(ctx, fromAddress)
	require.NoError(t, err)

	// Define gas parameters
	gasPrice, err := client.SuggestGasPrice(ctx)
	require.NoError(t, err)

	// Set up transaction options
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(195))
	require.NoError(t, err)

	auth.Nonce = big.NewInt(int64(nonce))
	auth.Value = big.NewInt(0)
	auth.GasLimit = uint64(3000000)
	auth.GasPrice = gasPrice

	// Deploy the contract
	erc20Bytecode, err := hex.DecodeString(erc20BytecodeStr)
	require.NoError(t, err)
	erc20Address, tx, _, err := bind.DeployContract(auth, erc20ABI, erc20Bytecode, client)
	require.NoError(t, err)

	fmt.Printf("ERC20 Contract deployed at: %s, transaction hash: %s\n", erc20Address.Hex(), tx.Hash().Hex())
	// Wait for contract deployment to be mined
	bind.WaitDeployed(ctx, client, tx)

	return erc20Address
}

func erc20TransferTx(
	t *testing.T,
	ctx context.Context,
	privateKey *ecdsa.PrivateKey,
	client *ethclient.Client,
	amount *big.Int,
	toAddress common.Address,
	erc20Address common.Address,
	nonce uint64,
) *types.Transaction {

	// Prepare transfer data
	data, err := erc20ABI.Pack("transfer", toAddress, amount)
	require.NoError(t, err)

	gasPrice, err := client.SuggestGasPrice(ctx)
	require.NoError(t, err)
	transferERC20TokenTx := types.NewTransaction(
		nonce,
		erc20Address,
		amount,
		60000,
		gasPrice,
		data,
	)

	signer := types.MakeSigner(GetTestChainConfig(DefaultL2ChainID), big.NewInt(1), 0)
	signedTx, err := types.SignTx(transferERC20TokenTx, signer, privateKey)
	require.NoError(t, err)

	err = client.SendTransaction(ctx, signedTx)
	require.NoError(t, err)

	return signedTx
}

func transToken(t *testing.T, ctx context.Context, client *ethclient.Client, amount *big.Int, toAddress string) string {
	return transTokenWithFrom(t, ctx, client, DefaultL2AdminPrivateKey, amount, toAddress)
}

func transTokenWithFrom(t *testing.T, ctx context.Context, client *ethclient.Client, fromPrivateKey string, amount *big.Int, toAddress string) string {
	chainID, err := client.ChainID(ctx)
	require.NoError(t, err)
	auth, err := GetAuth(fromPrivateKey, chainID.Uint64())
	require.NoError(t, err)
	nonce, err := client.PendingNonceAt(ctx, auth.From)
	require.NoError(t, err)
	gasPrice, err := client.SuggestGasPrice(ctx)
	require.NoError(t, err)

	to := common.HexToAddress(toAddress)
	gas, err := client.EstimateGas(ctx, ethereum.CallMsg{
		From:  auth.From,
		To:    &to,
		Value: amount,
	})
	require.NoError(t, err)
	log.Info(fmt.Sprintf("gas: %d", gas))
	log.Info(fmt.Sprintf("gasPrice: %d", gasPrice))

	tx := types.NewTransaction(
		nonce,
		to,
		amount,
		gas,
		gasPrice,
		nil,
	)

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(fromPrivateKey, "0x"))
	require.NoError(t, err)

	signer := types.MakeSigner(GetTestChainConfig(DefaultL2ChainID), big.NewInt(1), 0)
	signedTx, err := types.SignTx(tx, signer, privateKey)
	require.NoError(t, err)

	err = client.SendTransaction(ctx, signedTx)
	require.NoError(t, err)

	err = WaitTxToBeMined(ctx, *client, signedTx, DefaultTimeoutTxToBeMined)
	require.NoError(t, err)

	return signedTx.Hash().String()
}

func DeployFactoryContract(t *testing.T, ctx context.Context, client *ethclient.Client) common.Address {
	// Deploy Factory contract
	chainID, err := client.ChainID(ctx)
	require.NoError(t, err)
	auth, err := GetAuth(DefaultL2AdminPrivateKey, chainID.Uint64())
	require.NoError(t, err)
	nonce, err := client.PendingNonceAt(ctx, auth.From)
	require.NoError(t, err)
	gasPrice, err := client.SuggestGasPrice(ctx)
	require.NoError(t, err)

	auth.Nonce = big.NewInt(int64(nonce))
	auth.Value = big.NewInt(0)
	auth.GasLimit = uint64(3000000)
	auth.GasPrice = gasPrice

	factoryBytecode, err := hex.DecodeString(factoryBytecodeStr)
	require.NoError(t, err)
	factoryAddr, tx, _, err := bind.DeployContract(auth, factoryABI, factoryBytecode, client)
	require.NoError(t, err)

	fmt.Printf("Factory contract deployed at: %s, transaction hash: %s\n", factoryAddr.Hex(), tx.Hash().Hex())
	bind.WaitDeployed(ctx, client, tx)

	return factoryAddr
}

func SendDeployDestroyContractTx(t *testing.T, ctx context.Context, client *ethclient.Client, privateKey *ecdsa.PrivateKey, factoryAddr common.Address, salt *big.Int) {
	destroyBytecode, err := hex.DecodeString(destroyBytecodeStr)
	require.NoError(t, err)
	data, err := factoryABI.Pack("deploy", destroyBytecode, salt)
	require.NoError(t, err)

	nonce, err := client.PendingNonceAt(ctx, common.HexToAddress(DefaultL2AdminAddress))
	require.NoError(t, err)
	gasPrice, err := client.SuggestGasPrice(ctx)
	require.NoError(t, err)
	deployTx := types.NewTransaction(
		nonce,
		factoryAddr,
		big.NewInt(0),
		3000000,
		gasPrice,
		data,
	)

	signer := types.MakeSigner(GetTestChainConfig(DefaultL2ChainID), big.NewInt(1), 0)
	require.NoError(t, err)
	signedTx, err := types.SignTx(deployTx, signer, privateKey)
	require.NoError(t, err)
	err = client.SendTransaction(ctx, signedTx)
	require.NoError(t, err)
}

func SendDestroyContractTx(t *testing.T, ctx context.Context, client *ethclient.Client, privateKey *ecdsa.PrivateKey, destroyAddr common.Address) {
	data, err := destroyABI.Pack("destroy", common.HexToAddress(DefaultL2AdminAddress))
	require.NoError(t, err)

	nonce, err := client.PendingNonceAt(ctx, common.HexToAddress(DefaultL2AdminAddress))
	require.NoError(t, err)
	gasPrice, err := client.SuggestGasPrice(ctx)
	require.NoError(t, err)
	destroyTx := types.NewTransaction(
		nonce,
		destroyAddr,
		big.NewInt(0),
		3000000,
		gasPrice,
		data,
	)

	signer := types.MakeSigner(GetTestChainConfig(DefaultL2ChainID), big.NewInt(1), 0)
	require.NoError(t, err)
	signedTx, err := types.SignTx(destroyTx, signer, privateKey)
	require.NoError(t, err)
	err = client.SendTransaction(ctx, signedTx)
	require.NoError(t, err)
}

func GetContractAddressFromFactory(t *testing.T, ctx context.Context, rtclient *rtclient.RealtimeClient, ethclient *ethclient.Client, factoryAddr common.Address, salt *big.Int) common.Address {
	destroyBytecode, err := hex.DecodeString(destroyBytecodeStr)
	require.NoError(t, err)

	// Pack the call to computeAddress
	computeInput, err := factoryABI.Pack("computeAddress", destroyBytecode, salt)
	require.NoError(t, err)
	result, err := rtclient.RealtimeCall(ctx, common.HexToAddress(DefaultL2AdminAddress), factoryAddr, "0x300000", "0x1", "0x0", fmt.Sprintf("0x%x", computeInput))
	require.NoError(t, err)

	// Unpack the result
	return common.HexToAddress(result)
}

func DeployCreateDestroyContract(t *testing.T, ctx context.Context, client *ethclient.Client) common.Address {
	// Deploy Factory contract
	chainID, err := client.ChainID(ctx)
	require.NoError(t, err)
	auth, err := GetAuth(DefaultL2AdminPrivateKey, chainID.Uint64())
	require.NoError(t, err)
	nonce, err := client.PendingNonceAt(ctx, auth.From)
	require.NoError(t, err)
	gasPrice, err := client.SuggestGasPrice(ctx)
	require.NoError(t, err)

	auth.Nonce = big.NewInt(int64(nonce))
	auth.Value = big.NewInt(0)
	auth.GasLimit = uint64(3000000)
	auth.GasPrice = gasPrice

	createDestroyBytecode, err := hex.DecodeString(createDestroyBytecodeStr)
	require.NoError(t, err)
	createDestroyAddr, tx, _, err := bind.DeployContract(auth, createDestroyABI, createDestroyBytecode, client)
	require.NoError(t, err)

	fmt.Printf("Create Destroy contract deployed at: %s, transaction hash: %s\n", createDestroyAddr.Hex(), tx.Hash().Hex())
	bind.WaitDeployed(ctx, client, tx)

	return createDestroyAddr
}

func SendCreateAndDestroyTx(t *testing.T, ctx context.Context, client *ethclient.Client, privateKey *ecdsa.PrivateKey, createDestroyAddr common.Address, salt *big.Int) {
	data, err := createDestroyABI.Pack("createAndDestroy", salt, common.HexToAddress(DefaultL2AdminAddress))
	require.NoError(t, err)

	nonce, err := client.PendingNonceAt(ctx, common.HexToAddress(DefaultL2AdminAddress))
	require.NoError(t, err)
	gasPrice, err := client.SuggestGasPrice(ctx)
	require.NoError(t, err)
	createAndDestroyTx := types.NewTransaction(
		nonce,
		createDestroyAddr,
		big.NewInt(0),
		3000000,
		gasPrice,
		data,
	)

	signer := types.MakeSigner(GetTestChainConfig(DefaultL2ChainID), big.NewInt(1), 0)
	require.NoError(t, err)
	signedTx, err := types.SignTx(createAndDestroyTx, signer, privateKey)
	require.NoError(t, err)
	err = client.SendTransaction(ctx, signedTx)
	require.NoError(t, err)
}

func GetContractAddressFromCreateDestroy(t *testing.T, ctx context.Context, rtclient *rtclient.RealtimeClient, ethclient *ethclient.Client, createDestroyAddr common.Address, salt *big.Int) common.Address {
	destroyBytecode, err := hex.DecodeString(destroyBytecodeStr)
	require.NoError(t, err)

	// Pack the call to computeAddress
	computeInput, err := factoryABI.Pack("computeAddress", destroyBytecode, salt)
	require.NoError(t, err)
	result, err := rtclient.RealtimeCall(ctx, common.HexToAddress(DefaultL2AdminAddress), createDestroyAddr, "0x300000", "0x1", "0x0", fmt.Sprintf("0x%x", computeInput))
	require.NoError(t, err)

	// Unpack the result
	return common.HexToAddress(result)
}

// RevertReason returns the revert reason for a tx that has a receipt with failed status
func RevertReason(
	ctx context.Context,
	client ethclient.Client,
	tx *types.Transaction,
	blockNumber *big.Int,
) (string, error) {
	if tx == nil {
		return "", nil
	}

	msg := ethereum.CallMsg{
		From: tx.From(),
		To:   tx.To(),
		Gas:  tx.Gas(),

		Value: tx.Value(),
		Data:  tx.Data(),
	}
	hex, err := client.CallContract(ctx, msg, blockNumber)
	if err != nil {
		return "", err
	}

	unpackedMsg, err := abi.UnpackRevert(hex)
	if err != nil {
		fmt.Printf("failed to get the revert message for tx %v: %v\n", tx.Hash(), err)
		return "", errors.New("execution reverted")
	}

	return unpackedMsg, nil
}

func toLogFilterArg(q ethereum.FilterQuery) (interface{}, error) {
	arg := map[string]interface{}{
		"address": q.Addresses,
		"topics":  q.Topics,
	}
	return arg, nil
}
