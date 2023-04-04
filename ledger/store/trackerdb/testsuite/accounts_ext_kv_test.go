// Copyright (C) 2019-2023 Algorand, Inc.
// This file is part of go-algorand
//
// go-algorand is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// go-algorand is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with go-algorand.  If not, see <https://www.gnu.org/licenses/>.

package testsuite

import (
	"context"

	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/algorand/go-algorand/ledger/store/trackerdb"
	"github.com/algorand/go-algorand/protocol"
	"github.com/stretchr/testify/require"
)

func init() {
	// register tests that will run on each KV implementation
	registerTest("global-round-update", CustomTestRoundUpdate)
	registerTest("global-totals", CustomTestTotals)
	registerTest("txtail-update", CustomTestTxTail)
	registerTest("online_accounts-round_params-update", CustomTestOnlineAccountParams)
}

func CustomTestRoundUpdate(t *customT) {
	aw, err := t.db.MakeAccountsWriter()
	require.NoError(t, err)

	ar, err := t.db.MakeAccountsReader()
	require.NoError(t, err)

	// update the round
	err = aw.UpdateAccountsRound(basics.Round(1))
	require.NoError(t, err)

	// read the round
	rnd, err := ar.AccountsRound()
	require.NoError(t, err)
	require.Equal(t, basics.Round(1), rnd)
}

func CustomTestTotals(t *customT) {
	aw, err := t.db.MakeAccountsWriter()
	require.NoError(t, err)

	ar, err := t.db.MakeAccountsReader()
	require.NoError(t, err)

	// generate some test data
	totals := ledgercore.AccountTotals{
		Online:           ledgercore.AlgoCount{Money: basics.MicroAlgos{Raw: 42}},
		Offline:          ledgercore.AlgoCount{Money: basics.MicroAlgos{Raw: 1000}},
		NotParticipating: ledgercore.AlgoCount{Money: basics.MicroAlgos{Raw: 8}},
		RewardsLevel:     9000,
	}

	// update the totals
	err = aw.AccountsPutTotals(totals, false)
	require.NoError(t, err)

	// read the totals
	read_totals, err := ar.AccountsTotals(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, totals, read_totals)

	// generate some staging values
	staging_totals := ledgercore.AccountTotals{
		Online:           ledgercore.AlgoCount{Money: basics.MicroAlgos{Raw: 1}},
		Offline:          ledgercore.AlgoCount{Money: basics.MicroAlgos{Raw: 2}},
		NotParticipating: ledgercore.AlgoCount{Money: basics.MicroAlgos{Raw: 3}},
		RewardsLevel:     4,
	}

	// update the (staging) totals
	err = aw.AccountsPutTotals(staging_totals, true)
	require.NoError(t, err)

	// read the totals
	read_totals, err = ar.AccountsTotals(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, staging_totals, read_totals)

	// double check the live data is still there
	read_totals, err = ar.AccountsTotals(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, totals, read_totals)
}

func CustomTestTxTail(t *customT) {
	aw, err := t.db.MakeAccountsWriter()
	require.NoError(t, err)

	ar, err := t.db.MakeAccountsReader()
	require.NoError(t, err)

	// generate some test data
	baseRound := basics.Round(0)
	roundData := []*trackerdb.TxTailRound{
		{
			TxnIDs: []transactions.Txid{transactions.Txid(crypto.Hash([]byte("tx-0")))},
		},
		{
			TxnIDs: []transactions.Txid{transactions.Txid(crypto.Hash([]byte("tx-1")))},
		},
		{
			TxnIDs: []transactions.Txid{transactions.Txid(crypto.Hash([]byte("tx-2")))},
		},
	}
	// TODO: remove this conversion once we change the API to take the actual types
	var rawRoundData [][]byte
	for _, tail := range roundData {
		raw := protocol.Encode(tail)
		rawRoundData = append(rawRoundData, raw)
	}

	// write TxTail's
	err = aw.TxtailNewRound(context.Background(), baseRound, rawRoundData, baseRound)
	require.NoError(t, err)

	// load  TxTail's (error, must be the latest round)
	_, _, _, err = ar.LoadTxTail(context.Background(), basics.Round(1))
	require.Error(t, err)

	// load  TxTail's
	txtails, hashes, readBaseRound, err := ar.LoadTxTail(context.Background(), basics.Round(2))
	require.NoError(t, err)
	require.Len(t, txtails, 3)                 // assert boundries
	require.Equal(t, roundData[0], txtails[0]) // assert ordering
	require.Len(t, hashes, 3)
	require.Equal(t, basics.Round(0), readBaseRound)

	// generate some more test data
	roundData = []*trackerdb.TxTailRound{
		{
			TxnIDs: []transactions.Txid{transactions.Txid(crypto.Hash([]byte("tx-3")))},
		},
	}
	// reset data
	rawRoundData = make([][]byte, 0)
	// TODO: remove this conversion once we change the API to take the actual types
	for _, tail := range roundData {
		raw := protocol.Encode(tail)
		rawRoundData = append(rawRoundData, raw)
	}
	// write TxTail's (delete everything before round 2)
	err = aw.TxtailNewRound(context.Background(), basics.Round(3), rawRoundData, basics.Round(2))
	require.NoError(t, err)

	// load  TxTail's
	txtails, hashes, readBaseRound, err = ar.LoadTxTail(context.Background(), basics.Round(3))
	require.NoError(t, err)
	require.Len(t, txtails, 2)
	require.Len(t, hashes, 2)
	require.Equal(t, basics.Round(2), readBaseRound)
}

func CustomTestOnlineAccountParams(t *customT) {
	aw, err := t.db.MakeAccountsWriter()
	require.NoError(t, err)

	ar, err := t.db.MakeAccountsReader()
	require.NoError(t, err)

	// generate some test data
	startRound := basics.Round(0)
	roundParams := []ledgercore.OnlineRoundParamsData{
		{OnlineSupply: 100},
		{OnlineSupply: 42},
		{OnlineSupply: 9000},
	}

	// clean up the db before starting with the test
	// Note: some engines might start with some data built-in data for round 0
	err = aw.AccountsPruneOnlineRoundParams(basics.Round(42))
	require.NoError(t, err)

	// write round params
	err = aw.AccountsPutOnlineRoundParams(roundParams, startRound)
	require.NoError(t, err)

	// read round params
	readParams, endRound, err := ar.AccountsOnlineRoundParams()
	require.NoError(t, err)
	require.Len(t, readParams, 3)                   // assert boundries
	require.Equal(t, roundParams[0], readParams[0]) // assert ordering
	require.Equal(t, basics.Round(2), endRound)     // check round

	// prune params
	err = aw.AccountsPruneOnlineRoundParams(basics.Round(1))
	require.NoError(t, err)

	// read round params (again, after prunning)
	readParams, endRound, err = ar.AccountsOnlineRoundParams()
	require.NoError(t, err)
	require.Len(t, readParams, 2)                   // assert boundries
	require.Equal(t, roundParams[1], readParams[0]) // assert ordering, and first item
	require.Equal(t, basics.Round(2), endRound)     // check round
}
