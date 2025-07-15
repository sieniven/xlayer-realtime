package test

import (
	"math/big"
	"testing"

	libcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/holiman/uint256"
	kafkaTypes "github.com/sieniven/xlayer-realtime/kafka/types"
	realtimeTypes "github.com/sieniven/xlayer-realtime/types"
	"gotest.tools/assert"
)

var (
	dynFeeTx = &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce: 3,
			To:    &testToAddr,
			Value: big.NewInt(10),
			Gas:   25000,
			Data:  libcommon.FromHex("5544"),
		},
		ChainID:    big.NewInt(1),
		Tip:        big.NewInt(1),
		FeeCap:     big.NewInt(1),
		AccessList: accesses,
	}

	blobTx = &types.BlobTx{
		DynamicFeeTransaction: *dynFeeTx,
		MaxFeePerBlobGas:      big.NewInt(10),
		BlobVersionedHashes:   []libcommon.Hash{{0}},
	}
)

func TestLegacyTx(t *testing.T) {
	// Test from
	emptyTx := types.NewTransaction(
		0,
		libcommon.HexToAddress(testToAddr.String()),
		big.NewInt(0), 0, big.NewInt(10),
		nil,
	)
	emptyTx.SetSender(testFromAddr)

	emptyTxReceipt := types.NewReceipt(false, 1000)

	blockNumber := uint64(100)
	emptyMsg, err := kafkaTypes.ToKafkaTransactionMessage(emptyTx, emptyTxReceipt, nil, nil, blockNumber)
	assert.NilError(t, err)
	AssertCommonTx(t, emptyMsg, emptyTx, blockNumber, types.LegacyTxType)
	assert.Equal(t, emptyMsg.GasPrice, emptyTx.GetPrice().String())
	AssertReceipt(t, emptyMsg, emptyTxReceipt)
	AssertInnerTxs(t, emptyMsg, nil)

	sigBytes := "98ff921201554726367d2be8c804a7ff89ccf285ebc57dff8ae4c44b9c19ac4a8887321be575c8095f789dd4c743dfe42c1820f9231f98a962b210e3ac2452a301"
	rightvrsTx, _ := types.NewTransaction(
		3,
		testToAddr,
		uint256.NewInt(10),
		2000,
		big.NewInt(1),
		libcommon.FromHex("5544"),
	).WithSignature(
		*types.LatestSignerForChainID(nil),
		libcommon.Hex2Bytes(sigBytes),
	)
	rightvrsTx.SetSender(testFromAddr)

	rightvrsTxReceipt := &types.Receipt{
		PostState:         libcommon.Hash{2}.Bytes(),
		CumulativeGasUsed: 3,
		Logs: []*types.Log{
			{Address: libcommon.BytesToAddress([]byte{0x22})},
			{Address: libcommon.BytesToAddress([]byte{0x02, 0x22})},
		},
		TxHash:          rightvrsTx.Hash(),
		ContractAddress: libcommon.BytesToAddress([]byte{0x02, 0x22, 0x22}),
		GasUsed:         2,
	}

	rightvrsTxInnerTxs := []*types.InnerTx{
		{
			Name:     "innerTx1",
			CallType: vm.CALL_TYP,
		},
	}

	rightvrsTxChangeset := &realtimeTypes.Changeset{
		BalanceChanges: map[libcommon.Address]*uint256.Int{
			testToAddr: uint256.NewInt(10),
		},
	}

	msg, err := kafkaTypes.ToKafkaTransactionMessage(rightvrsTx, rightvrsTxReceipt, rightvrsTxInnerTxs, rightvrsTxChangeset, blockNumber)
	assert.NilError(t, err)
	AssertCommonTx(t, msg, rightvrsTx, blockNumber, types.LegacyTxType)
	assert.Equal(t, msg.GasPrice, rightvrsTx.GetPrice().String())
	AssertReceipt(t, msg, rightvrsTxReceipt)
	AssertInnerTxs(t, msg, rightvrsTxInnerTxs)
	AssertChangeseet(t, msg, rightvrsTxChangeset)

	// Test to
	convertEmptyTx, convertBlockNumber, err := emptyMsg.GetTransaction()
	assert.NilError(t, err)
	assert.Equal(t, convertBlockNumber, blockNumber)
	AssertCommonTx(t, emptyMsg, convertEmptyTx, convertBlockNumber, types.LegacyTxType)
	assert.Equal(t, emptyMsg.GasPrice, convertEmptyTx.GetPrice().String())

	convertRightvsTx, convertBlockNumber, err := msg.GetTransaction()
	assert.NilError(t, err)
	assert.Equal(t, convertBlockNumber, blockNumber)
	AssertCommonTx(t, msg, convertRightvsTx, convertBlockNumber, types.LegacyTxType)
	assert.Equal(t, msg.GasPrice, convertRightvsTx.GetPrice().String())
}

func TestAccessListTx(t *testing.T) {
	// Test from
	sigBytes := "c9519f4f2b30335884581971573fadf60c6204f59a911df35ee8a540456b266032f1e8e2c5dd761f9e4f88f41c8310aeaba26a8bfcdacfedfa12ec3862d3752101"
	accessListTx := &types.AccessListTx{
		ChainID: big.NewInt(1),
		LegacyTx: types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce: 3,
				To:    &testToAddr,
				Value: uint256.NewInt(10),
				Gas:   25000,
				Data:  libcommon.FromHex("5544"),
			},
			GasPrice: uint256.NewInt(1),
		},
		AccessList: accesses,
	}

	signedAccessListTx, _ := accessListTx.WithSignature(
		*types.LatestSignerForChainID(big.NewInt(1)),
		libcommon.Hex2Bytes(sigBytes),
	)
	signedAccessListTx.SetSender(testFromAddr)

	signedAccessListTxReceipt := &types.Receipt{
		Type:              types.AccessListTxType,
		PostState:         libcommon.Hash{3}.Bytes(),
		CumulativeGasUsed: 6,
		Logs: []*types.Log{
			{Address: libcommon.BytesToAddress([]byte{0x33})},
			{Address: libcommon.BytesToAddress([]byte{0x03, 0x33})},
		},
		TxHash:          signedAccessListTx.Hash(),
		ContractAddress: libcommon.BytesToAddress([]byte{0x03, 0x33, 0x33}),
		GasUsed:         3,
	}
	signedAccessListTxInnerTxs := []*types.InnerTx{
		{
			Name:     "innerTx1",
			CallType: vm.CALL_TYP,
		},
	}
	signedAccessListTxChangeset := &realtimeTypes.Changeset{
		BalanceChanges: map[libcommon.Address]*uint256.Int{
			testToAddr: uint256.NewInt(10),
		},
	}

	blockNumber := uint64(100)
	msg, err := kafkaTypes.ToKafkaTransactionMessage(signedAccessListTx, signedAccessListTxReceipt, signedAccessListTxInnerTxs, signedAccessListTxChangeset, blockNumber)
	assert.NilError(t, err)
	AssertCommonTx(t, msg, signedAccessListTx, blockNumber, types.AccessListTxType)
	assert.Equal(t, msg.GasPrice, signedAccessListTx.GetPrice().String())
	AssertReceipt(t, msg, signedAccessListTxReceipt)
	AssertInnerTxs(t, msg, signedAccessListTxInnerTxs)
	AssertAccessList(t, msg.AccessList)
	AssertChangeseet(t, msg, signedAccessListTxChangeset)

	// Test to
	convertAccessListTx, convertBlockNumber, err := msg.GetTransaction()
	assert.NilError(t, err)
	assert.Equal(t, convertBlockNumber, blockNumber)
	AssertCommonTx(t, msg, convertAccessListTx, convertBlockNumber, types.AccessListTxType)
	assertTxAccessList(t, convertAccessListTx.GetAccessList())
	assert.Equal(t, msg.GasPrice, convertAccessListTx.GetPrice().String())
}

func TestDynamicFeeTx(t *testing.T) {
	// Test from
	sigBytes := "c9519f4f2b30335884581971573fadf60c6204f59a911df35ee8a540456b266032f1e8e2c5dd761f9e4f88f41c8310aeaba26a8bfcdacfedfa12ec3862d3752101"
	signedDynFeeTx, _ := dynFeeTx.WithSignature(
		*types.LatestSignerForChainID(big.NewInt(1)),
		libcommon.Hex2Bytes(sigBytes),
	)
	signedDynFeeTx.SetSender(testFromAddr)

	signedDynFeeTxReceipt := &types.Receipt{
		Type:              types.DynamicFeeTxType,
		PostState:         libcommon.Hash{4}.Bytes(),
		CumulativeGasUsed: 10,
		Logs: []*types.Log{
			{Address: libcommon.BytesToAddress([]byte{0x33})},
			{Address: libcommon.BytesToAddress([]byte{0x03, 0x33})},
		},
		TxHash:          signedDynFeeTx.Hash(),
		ContractAddress: libcommon.BytesToAddress([]byte{0x03, 0x33, 0x33}),
		GasUsed:         3,
	}
	signedDynFeeTxInnerTxs := []*types.InnerTx{
		{
			Name:     "innerTx1",
			CallType: types.CALL_TYP,
		},
	}
	signedDynFeeTxChangeset := &realtimeTypes.Changeset{
		BalanceChanges: map[libcommon.Address]*uint256.Int{
			testToAddr: uint256.NewInt(10),
		},
	}

	blockNumber := uint64(100)
	msg, err := kafkaTypes.ToKafkaTransactionMessage(signedDynFeeTx, signedDynFeeTxReceipt, signedDynFeeTxInnerTxs, signedDynFeeTxChangeset, blockNumber)
	assert.NilError(t, err)
	AssertCommonTx(t, msg, signedDynFeeTx, blockNumber, types.DynamicFeeTxType)
	assert.Equal(t, msg.Tip, signedDynFeeTx.GetTip().String())
	assert.Equal(t, msg.FeeCap, signedDynFeeTx.GetFeeCap().String())
	AssertReceipt(t, msg, signedDynFeeTxReceipt)
	AssertInnerTxs(t, msg, signedDynFeeTxInnerTxs)
	AssertAccessList(t, msg.AccessList)
	AssertChangeseet(t, msg, signedDynFeeTxChangeset)

	// Test to
	convertDynFeeTx, convertBlockNumber, err := msg.GetTransaction()
	assert.NilError(t, err)
	assert.Equal(t, convertBlockNumber, blockNumber)
	AssertCommonTx(t, msg, convertDynFeeTx, convertBlockNumber, types.DynamicFeeTxType)
	assertTxAccessList(t, convertDynFeeTx.GetAccessList())
	assert.Equal(t, msg.Tip, convertDynFeeTx.GetTip().String())
	assert.Equal(t, msg.FeeCap, convertDynFeeTx.GetFeeCap().String())
}

func TestFromBlobTx(t *testing.T) {
	// Test from
	blobTx.SetSender(testFromAddr)
	blobTxReceipt := &types.Receipt{
		PostState:         libcommon.Hash{2}.Bytes(),
		CumulativeGasUsed: 15,
		Logs: []*types.Log{
			{Address: libcommon.BytesToAddress([]byte{0x22})},
			{Address: libcommon.BytesToAddress([]byte{0x02, 0x22})},
		},
		TxHash:          blobTx.Hash(),
		ContractAddress: libcommon.BytesToAddress([]byte{0x02, 0x22, 0x22}),
		GasUsed:         5,
	}
	blobTxInnerTxs := []*types.InnerTx{
		{
			Name:     "innerTx1",
			CallType: types.CALL_TYP,
		},
	}
	blobTxChangeset := &realtimeTypes.Changeset{
		BalanceChanges: map[libcommon.Address]*uint256.Int{
			testToAddr: uint256.NewInt(10),
		},
	}

	blockNumber := uint64(100)
	msg, err := kafkaTypes.ToKafkaTransactionMessage(blobTx, blobTxReceipt, blobTxInnerTxs, blobTxChangeset, blockNumber)
	assert.NilError(t, err)
	AssertCommonTx(t, msg, blobTx, blockNumber, types.BlobTxType)
	assert.Equal(t, msg.Tip, blobTx.GetTip().String())
	assert.Equal(t, msg.FeeCap, blobTx.GetFeeCap().String())
	AssertReceipt(t, msg, blobTxReceipt)
	AssertInnerTxs(t, msg, blobTxInnerTxs)
	AssertAccessList(t, msg.AccessList)

	assert.Equal(t, msg.MaxFeePerBlobGas, "10")
	assert.Equal(t, len(msg.BlobVersionedHashes), 1)
	for _, hash := range msg.BlobVersionedHashes {
		assert.Equal(t, hash, "0x0000000000000000000000000000000000000000000000000000000000000000")
	}

	// Test to
	convertBlobTx, convertBlockNumber, err := msg.GetTransaction()
	assert.NilError(t, err)
	AssertCommonTx(t, msg, convertBlobTx, convertBlockNumber, types.BlobTxType)
	assert.Equal(t, msg.Tip, convertBlobTx.GetTip().String())
	assert.Equal(t, msg.FeeCap, convertBlobTx.GetFeeCap().String())
	assertTxAccessList(t, convertBlobTx.GetAccessList())
}
