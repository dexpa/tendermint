package mempool_test

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/tendermint/abci/example/dummy"
	abci "github.com/tendermint/abci/types"
	cmn "github.com/tendermint/tmlibs/common"
	dbm "github.com/tendermint/tmlibs/db"
	"github.com/tendermint/tmlibs/log"

	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/types"
	mpool "github.com/tendermint/tendermint/mempool"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/iavl"
	"github.com/tendermint/abci/example/code"
)

type OrderedApplication struct {
	dummy.DummyApplication
	state *iavl.VersionedTree
	txWeight uint64
}

/**
	Custom Ordered Application that implements correct conditions for ordered mempool in DeliverTx
 */
func newOrderedApplication() *OrderedApplication {
	state := iavl.NewVersionedTree(0, dbm.NewMemDB())
	return &OrderedApplication{
		DummyApplication: *dummy.NewDummyApplication(),
		state: state,
	}
}

func (app *OrderedApplication) DeliverTx(tx []byte) abci.ResponseDeliverTx {
	if len(tx) > 8 {
		return abci.ResponseDeliverTx{
			Code: code.CodeTypeBadNonce,
			Log:  fmt.Sprintf("Max tx size is 8 bytes, got %d", len(tx))}
	}
	tx8 := make([]byte, 8)
	copy(tx8[len(tx8)-len(tx):], tx)
	txValue := binary.BigEndian.Uint64(tx8)

	if app.txWeight == 0 {
		app.txWeight = txValue
	}

	if txValue > app.txWeight {
		return abci.ResponseDeliverTx{
			Code: code.CodeTypeBadNonce,
			Log:  fmt.Sprintf("Invalid nonce. Expected nounce less or equal %v, got %v", app.txWeight, txValue)}
	}
	app.state.Set(tx8, tx8)
	app.txWeight = txValue
	return abci.ResponseDeliverTx{Code: abci.CodeTypeOK, Data: tx8}
}

func (app *OrderedApplication) Commit() abci.ResponseCommit {
	// Save a new version
	var hash []byte
	var err error

	if app.state.Size() > 0 {
		// just add one more to height (kind of arbitrarily stupid)
		height := app.state.LatestVersion() + 1
		hash, err = app.state.SaveVersion(height)
		if err != nil {
			// if this wasn't a dummy app, we'd do something smarter
			panic(err)
		}
	}

	return abci.ResponseCommit{Code: code.CodeTypeOK, Data: hash}
}



func newMempoolWithApp(cc proxy.ClientCreator) *mpool.OrderedMempool {
	config := cfg.ResetTestRoot("mempool_test")

	appConnMem, _ := cc.NewABCIClient()
	appConnMem.SetLogger(log.TestingLogger().With("module", "abci-client", "connection", "mempool"))
	appConnMem.Start()
	mempool := mpool.NewOrderedMempool(config.Mempool, appConnMem, 0)
	mempool.SetLogger(log.TestingLogger())
	return mempool
}

func ensureNoFire(t *testing.T, ch <-chan int64, timeoutMS int) {
	timer := time.NewTimer(time.Duration(timeoutMS) * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("Expected not to fire")
	case <-timer.C:
	}
}

func ensureFire(t *testing.T, ch <-chan int64, timeoutMS int) {
	timer := time.NewTimer(time.Duration(timeoutMS) * time.Millisecond)
	select {
	case <-ch:
	case <-timer.C:
		t.Fatal("Expected to fire")
	}
}

func checkTxs(t *testing.T, mempool *mpool.OrderedMempool, count int) types.Txs {
	txs := make(types.Txs, count)
	for i := 0; i < count; i++ {
		txBytes := make([]byte, 20)
		txs[i] = txBytes
		_, err := rand.Read(txBytes)
		if err != nil {
			t.Error(err)
		}
		if err := mempool.CheckTx(txBytes, nil); err != nil {
			t.Fatalf("Error after CheckTx: %v", err)
		}
	}
	return txs
}

func TestTxsAvailable(t *testing.T) {
	app := newOrderedApplication()
	cc := proxy.NewLocalClientCreator(app)
	mempool := newMempoolWithApp(cc)
	mempool.EnableTxsAvailable()

	timeoutMS := 500

	// with no txs, it shouldnt fire
	ensureNoFire(t, mempool.TxsAvailable(), timeoutMS)

	// send a bunch of txs, it should only fire once
	txs := checkTxs(t, mempool, 100)
	ensureFire(t, mempool.TxsAvailable(), timeoutMS)
	ensureNoFire(t, mempool.TxsAvailable(), timeoutMS)

	// call update with half the txs.
	// it should fire once now for the new height
	// since there are still txs left
	committedTxs, txs := txs[:50], txs[50:]
	if err := mempool.Update(1, committedTxs); err != nil {
		t.Error(err)
	}
	ensureFire(t, mempool.TxsAvailable(), timeoutMS)
	ensureNoFire(t, mempool.TxsAvailable(), timeoutMS)

	// send a bunch more txs. we already fired for this height so it shouldnt fire again
	moreTxs := checkTxs(t, mempool, 50)
	ensureNoFire(t, mempool.TxsAvailable(), timeoutMS)

	// now call update with all the txs. it should not fire as there are no txs left
	committedTxs = append(txs, moreTxs...)
	if err := mempool.Update(2, committedTxs); err != nil {
		t.Error(err)
	}
	ensureNoFire(t, mempool.TxsAvailable(), timeoutMS)

	// send a bunch more txs, it should only fire once
	checkTxs(t, mempool, 100)
	ensureFire(t, mempool.TxsAvailable(), timeoutMS)
	ensureNoFire(t, mempool.TxsAvailable(), timeoutMS)
}

func TestSerialReap(t *testing.T) {
	app := newOrderedApplication()
	cc := proxy.NewLocalClientCreator(app)

	mempool := newMempoolWithApp(cc)
	appConnCon, _ := cc.NewABCIClient()
	appConnCon.SetLogger(log.TestingLogger().With("module", "abci-client", "connection", "consensus"))
	err := appConnCon.Start()
	require.Nil(t, err)

	cacheMap := make(map[string]struct{})
	deliverTxsRange := func(list []uint64) {
		// Deliver some txs.
		for _, i := range list {

			// This will succeed
			txBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(txBytes, uint64(i))
			err := mempool.CheckTx(txBytes, nil)
			_, cached := cacheMap[string(txBytes)]
			if cached {
				require.NotNil(t, err, "expected error for cached tx")
			} else {
				require.Nil(t, err, "expected no err for uncached tx")
			}
			cacheMap[string(txBytes)] = struct{}{}

			// Duplicates are cached and should return error
			err = mempool.CheckTx(txBytes, nil)
			require.NotNil(t, err, "Expected error after CheckTx on duplicated tx")
		}
	}

	reapCheck := func(exp int) {
		txs := mempool.Reap(-1)
		require.Equal(t, len(txs), exp, cmn.Fmt("Expected to reap %v txs but got %v", exp, len(txs)))
	}

	reapOrderChech := func(exp int) {
		txs := mempool.Reap(-1)
		var max uint64
		for _, tx := range txs {
			var u64 uint64
			u64 = binary.BigEndian.Uint64(tx)
			if max == 0 {
				max = u64
			}
			require.True(t, u64 <= max , cmn.Fmt("Expected value %v less or equal %v", u64, max))
		}
	}

	updateRange := func(list []uint64) {
		txs := make([]types.Tx, 0)
		for _, i := range list {
			txBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(txBytes, uint64(i))
			txs = append(txs, txBytes)
		}
		if err := mempool.Update(0, txs); err != nil {
			t.Error(err)
		}
	}

	commitRange := func(list []uint64) {
		// Deliver some txs.
		for _, i := range list {
			txBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(txBytes, uint64(i))
			res, err := appConnCon.DeliverTxSync(txBytes)
			if err != nil {
				t.Errorf("Client error committing tx: %v", err)
			}
			if res.IsErr() {
				t.Errorf("Error committing tx. Code:%v result:%X log:%v",
					res.Code, res.Data, res.Log)
			}
		}
		res, err := appConnCon.CommitSync()
		if err != nil {
			t.Errorf("Client error committing: %v", err)
		}
		if len(res.Data) == 0 {
			t.Errorf("Error committing. Hash:%X log:%v", res.Data, res.Log)
		}
	}

	//----------------------------------------

	txListOne := []uint64{6,9,5,2,1,3,7,8,4}
	txListTwo := []uint64{16,19,15,12,11,13,17,18,14}
	txListThree := []uint64{116,119,115,112,111,113,117,118}

	// Deliver some txs.
	deliverTxsRange(txListOne)

	// Reap the txs.
	reapCheck(9)
	reapOrderChech(9)
	reapOrderChech(18)

	// Reap again.  We should get the same amount
	reapCheck(9)
	reapOrderChech(9)
	reapOrderChech(18)

	// Deliver 0 to 999, we should reap 900 new txs
	// because 100 were already counted.
	deliverTxsRange(txListTwo)

	// Reap the txs.
	reapCheck(18)
	reapOrderChech(9)
	reapOrderChech(18)
	reapOrderChech(28)

	// Reap again.  We should get the same amount
	reapCheck(18)

	// Commit from the consensus AppConn
	txs := mempool.Reap(5)
	commitList := []uint64{}
	for _, i := range txs {
		var txInt uint64
		txInt = binary.BigEndian.Uint64(i)
		commitList = append(commitList, txInt)
	}
	commitRange(commitList)
	updateRange(txListOne)

	// We should have 9 left.
	reapCheck(9)
	reapOrderChech(9)
	reapOrderChech(18)

	// Deliver 100 invalid txs and 100 valid txs
	deliverTxsRange(append(txListOne, txListThree...))

	// We should have 17 now.
	reapCheck(17)
	reapOrderChech(9)
	reapOrderChech(18)
}


func checksumIt(data []byte) string {
	h := md5.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func checksumFile(p string, t *testing.T) string {
	data, err := ioutil.ReadFile(p)
	require.Nil(t, err, "expecting successful read of %q", p)
	return checksumIt(data)
}
