package mempool

import (
	"github.com/tendermint/tmlibs/clist"
	"github.com/tendermint/tmlibs/log"

	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/types"
	"time"
	"sync/atomic"
	cmn "github.com/tendermint/tmlibs/common"
	"sort"
	"encoding/binary"
)

//type OrderedMempool interface {
//	types.Mempool
//	SetLogger(logger log.Logger)
//	TxsFrontWait() *clist.CElement
//}

type OrderedMempool struct {
	Mempool
}

func NewOrderedMempool(config *cfg.MempoolConfig, proxyAppConn proxy.AppConnMempool, height int64) *OrderedMempool {
	mempool := &OrderedMempool{Mempool{
		config:        config,
		proxyAppConn:  proxyAppConn,
		txs:           clist.New(),
		counter:       0,
		height:        height,
		rechecking:    0,
		recheckCursor: nil,
		recheckEnd:    nil,
		logger:        log.NewNopLogger(),
		cache:         newTxCache(cacheSize),
	}}
	mempool.initWAL()
	proxyAppConn.SetResponseCallback(mempool.resCb)
	return mempool
}

func (mem *OrderedMempool) Reap(maxTxs int) types.Txs {
	mem.proxyMtx.Lock()
	defer mem.proxyMtx.Unlock()

	for atomic.LoadInt32(&mem.rechecking) > 0 {
		// TODO: Something better?
		time.Sleep(time.Millisecond * 10)
	}

	txs := mem.collectTxs(maxTxs)
	return txs
}

func (mem *OrderedMempool) collectTxs(maxTxs int) types.Txs {
	if maxTxs == 0 {
		return []types.Tx{}
	} else if maxTxs < 0 {
		maxTxs = mem.txs.Len()
	}

	txs := make([]types.Tx, 0, cmn.MaxInt(mem.txs.Len(), maxTxs))

	// @todo fix: do not sort if maxTxs > mem.txs.Len()
	for e := mem.txs.Front(); e != nil; e = e.Next() {
		memTx := e.Value.(*mempoolTx)
		txs = append(txs, memTx.tx)
	}

	if mem.txs.Len() < maxTxs {
		return txs
	}

	sort.SliceStable(txs[:], func(i, j int) bool {
		txA := binary.BigEndian.Uint64([]byte(txs[i]))
		txB := binary.BigEndian.Uint64([]byte(txs[j]))
		return txA > txB
	})

	return txs[:cmn.MinInt(mem.txs.Len(), maxTxs)]
}

func (mem *OrderedMempool) SetLogger(logger log.Logger) {
	mem.logger = logger
}
