// Copyright (C) 2019-2022 Algorand, Inc.
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

package ledger

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/algorand/go-algorand/data/transactions/logic"
	"github.com/algorand/go-algorand/encoded"

	"github.com/stretchr/testify/require"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/crypto/merklesignature"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/algorand/go-algorand/ledger/store"
	storetesting "github.com/algorand/go-algorand/ledger/store/testing"
	ledgertesting "github.com/algorand/go-algorand/ledger/testing"
	"github.com/algorand/go-algorand/logging"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/go-algorand/test/partitiontest"
	"github.com/algorand/go-algorand/util/db"
)

func checkAccounts(t *testing.T, tx *sql.Tx, rnd basics.Round, accts map[basics.Address]basics.AccountData) {
	arw := store.NewAccountsSQLReaderWriter(tx)

	r, err := arw.AccountsRound()
	require.NoError(t, err)
	require.Equal(t, r, rnd)

	aq, err := store.AccountsInitDbQueries(tx)
	require.NoError(t, err)
	defer aq.Close()

	var totalOnline, totalOffline, totalNotPart uint64

	for addr, data := range accts {
		expected := ledgercore.ToAccountData(data)
		pad, err := aq.LookupAccount(addr)
		require.NoError(t, err)
		d := pad.AccountData.GetLedgerCoreAccountData()
		require.Equal(t, expected, d)

		switch d.Status {
		case basics.Online:
			totalOnline += d.MicroAlgos.Raw
		case basics.Offline:
			totalOffline += d.MicroAlgos.Raw
		case basics.NotParticipating:
			totalNotPart += d.MicroAlgos.Raw
		default:
			t.Errorf("unknown status %v", d.Status)
		}
	}

	all, err := accountsAll(tx)
	require.NoError(t, err)
	require.Equal(t, all, accts)

	totals, err := arw.AccountsTotals(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, totalOnline, totals.Online.Money.Raw, "mismatching total online money")
	require.Equal(t, totalOffline, totals.Offline.Money.Raw)
	require.Equal(t, totalNotPart, totals.NotParticipating.Money.Raw)
	require.Equal(t, totalOnline+totalOffline, totals.Participating().Raw)
	require.Equal(t, totalOnline+totalOffline+totalNotPart, totals.All().Raw)

	d, err := aq.LookupAccount(ledgertesting.RandomAddress())
	require.NoError(t, err)
	require.Equal(t, rnd, d.Round)
	require.Equal(t, d.AccountData, store.BaseAccountData{})

	proto := config.Consensus[protocol.ConsensusCurrentVersion]

	onlineAccounts := make(map[basics.Address]*ledgercore.OnlineAccount)
	for addr, data := range accts {
		if data.Status == basics.Online {
			ad := ledgercore.ToAccountData(data)
			onlineAccounts[addr] = accountDataToOnline(addr, &ad, proto)
		}
	}

	// Compute the top-N accounts ourselves
	var testtop []ledgercore.OnlineAccount
	for _, data := range onlineAccounts {
		testtop = append(testtop, *data)
	}

	sort.Slice(testtop, func(i, j int) bool {
		ibal := testtop[i].NormalizedOnlineBalance
		jbal := testtop[j].NormalizedOnlineBalance
		if ibal > jbal {
			return true
		}
		if ibal < jbal {
			return false
		}
		return bytes.Compare(testtop[i].Address[:], testtop[j].Address[:]) > 0
	})

	for i := 0; i < len(onlineAccounts); i++ {
		dbtop, err := arw.AccountsOnlineTop(rnd, 0, uint64(i), proto)
		require.NoError(t, err)
		require.Equal(t, i, len(dbtop))

		for j := 0; j < i; j++ {
			_, ok := dbtop[testtop[j].Address]
			require.True(t, ok)
		}
	}

	top, err := arw.AccountsOnlineTop(rnd, 0, uint64(len(onlineAccounts)+1), proto)
	require.NoError(t, err)
	require.Equal(t, len(top), len(onlineAccounts))
}

func TestAccountDBInit(t *testing.T) {
	partitiontest.PartitionTest(t)

	proto := config.Consensus[protocol.ConsensusCurrentVersion]

	dbs, _ := storetesting.DbOpenTest(t, true)
	storetesting.SetDbLogging(t, dbs)
	defer dbs.Close()

	tx, err := dbs.Wdb.Handle.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	accts := ledgertesting.RandomAccounts(20, true)
	newDB := store.AccountsInitTest(t, tx, accts, protocol.ConsensusCurrentVersion)
	require.True(t, newDB)

	checkAccounts(t, tx, 0, accts)

	newDB, err = store.AccountsInitLightTest(t, tx, accts, proto)
	require.NoError(t, err)
	require.False(t, newDB)
	checkAccounts(t, tx, 0, accts)
}

// creatablesFromUpdates calculates creatables from updates
func creatablesFromUpdates(base map[basics.Address]basics.AccountData, updates ledgercore.AccountDeltas, seen map[basics.CreatableIndex]bool) map[basics.CreatableIndex]ledgercore.ModifiedCreatable {
	known := make(map[basics.CreatableIndex]struct{}, len(seen))
	for aidx := range seen {
		known[aidx] = struct{}{}
	}
	for _, ad := range base {
		for aidx := range ad.AppParams {
			known[basics.CreatableIndex(aidx)] = struct{}{}
		}
		for aidx := range ad.AssetParams {
			known[basics.CreatableIndex(aidx)] = struct{}{}
		}
	}
	return updates.ToModifiedCreatables(known)
}

func applyPartialDeltas(base map[basics.Address]basics.AccountData, deltas ledgercore.AccountDeltas) map[basics.Address]basics.AccountData {
	result := make(map[basics.Address]basics.AccountData, len(base)+deltas.Len())
	for addr, ad := range base {
		result[addr] = ad
	}

	for i := 0; i < deltas.Len(); i++ {
		addr, _ := deltas.GetByIdx(i)
		ad, ok := result[addr]
		if !ok {
			ad, _ = deltas.GetBasicsAccountData(addr)
		} else {
			ad = deltas.ApplyToBasicsAccountData(addr, ad)
		}
		result[addr] = ad
	}
	return result
}

func TestAccountDBRound(t *testing.T) {
	partitiontest.PartitionTest(t)

	proto := config.Consensus[protocol.ConsensusCurrentVersion]

	dbs, _ := storetesting.DbOpenTest(t, true)
	storetesting.SetDbLogging(t, dbs)
	defer dbs.Close()

	tx, err := dbs.Wdb.Handle.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	arw := store.NewAccountsSQLReaderWriter(tx)

	accts := ledgertesting.RandomAccounts(20, true)
	store.AccountsInitTest(t, tx, accts, protocol.ConsensusCurrentVersion)
	checkAccounts(t, tx, 0, accts)
	totals, err := arw.AccountsTotals(context.Background(), false)
	require.NoError(t, err)
	expectedOnlineRoundParams, endRound, err := arw.AccountsOnlineRoundParams()
	require.NoError(t, err)
	require.Equal(t, 1, len(expectedOnlineRoundParams))
	require.Equal(t, 0, int(endRound))

	// used to determine how many creatables element will be in the test per iteration
	numElementsPerSegment := 10

	// lastCreatableID stores asset or app max used index to get rid of conflicts
	lastCreatableID := basics.CreatableIndex(crypto.RandUint64() % 512)
	ctbsList, randomCtbs := randomCreatables(numElementsPerSegment)
	expectedDbImage := make(map[basics.CreatableIndex]ledgercore.ModifiedCreatable)
	var baseAccounts lruAccounts
	var baseResources lruResources
	var baseOnlineAccounts lruOnlineAccounts
	var newacctsTotals map[basics.Address]ledgercore.AccountData
	baseAccounts.init(nil, 100, 80)
	baseResources.init(nil, 100, 80)
	baseOnlineAccounts.init(nil, 100, 80)
	for i := 1; i < 10; i++ {
		var updates ledgercore.AccountDeltas
		updates, newacctsTotals, _ = ledgertesting.RandomDeltasFull(20, accts, 0, &lastCreatableID)
		totals = ledgertesting.CalculateNewRoundAccountTotals(t, updates, 0, proto, accts, totals)
		accts = applyPartialDeltas(accts, updates)
		ctbsWithDeletes := randomCreatableSampling(i, ctbsList, randomCtbs,
			expectedDbImage, numElementsPerSegment)

		oldBase := i - 1
		updatesCnt := makeCompactAccountDeltas([]ledgercore.StateDelta{{Accts: updates}}, basics.Round(oldBase), true, baseAccounts)
		resourceUpdatesCnt := makeCompactResourceDeltas([]ledgercore.StateDelta{{Accts: updates}}, basics.Round(oldBase), true, baseAccounts, baseResources)
		updatesOnlineCnt := makeCompactOnlineAccountDeltas([]ledgercore.AccountDeltas{updates}, basics.Round(oldBase), baseOnlineAccounts)

		err = updatesCnt.accountsLoadOld(tx)
		require.NoError(t, err)

		err = updatesOnlineCnt.accountsLoadOld(tx)
		require.NoError(t, err)

		knownAddresses := make(map[basics.Address]int64)
		for _, delta := range updatesCnt.deltas {
			knownAddresses[delta.oldAcct.Addr] = delta.oldAcct.Rowid
		}

		err = resourceUpdatesCnt.resourcesLoadOld(tx, knownAddresses)
		require.NoError(t, err)

		err = arw.AccountsPutTotals(totals, false)
		require.NoError(t, err)
		onlineRoundParams := ledgercore.OnlineRoundParamsData{RewardsLevel: totals.RewardsLevel, OnlineSupply: totals.Online.Money.Raw, CurrentProtocol: protocol.ConsensusCurrentVersion}
		err = arw.AccountsPutOnlineRoundParams([]ledgercore.OnlineRoundParamsData{onlineRoundParams}, basics.Round(i))
		require.NoError(t, err)
		expectedOnlineRoundParams = append(expectedOnlineRoundParams, onlineRoundParams)

		updatedAccts, updatesResources, updatedKVs, err := accountsNewRound(tx, updatesCnt, resourceUpdatesCnt, nil, ctbsWithDeletes, proto, basics.Round(i))
		require.NoError(t, err)
		require.Equal(t, updatesCnt.len(), len(updatedAccts))
		numResUpdates := 0
		for _, rs := range updatesResources {
			numResUpdates += len(rs)
		}
		require.Equal(t, resourceUpdatesCnt.len(), numResUpdates)
		require.Empty(t, updatedKVs)

		updatedOnlineAccts, err := onlineAccountsNewRound(tx, updatesOnlineCnt, proto, basics.Round(i))
		require.NoError(t, err)

		err = arw.UpdateAccountsRound(basics.Round(i))
		require.NoError(t, err)

		// TODO: calculate exact number of updates?
		// newly created online accounts + accounts went offline + voting data/stake modifed accounts
		require.NotEmpty(t, updatedOnlineAccts)

		checkAccounts(t, tx, basics.Round(i), accts)
		checkCreatables(t, tx, i, expectedDbImage)
	}

	// test the accounts totals
	var updates ledgercore.AccountDeltas
	for addr, acctData := range newacctsTotals {
		updates.Upsert(addr, acctData)
	}

	expectedTotals := ledgertesting.CalculateNewRoundAccountTotals(t, updates, 0, proto, nil, ledgercore.AccountTotals{})
	actualTotals, err := arw.AccountsTotals(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, expectedTotals, actualTotals)

	actualOnlineRoundParams, endRound, err := arw.AccountsOnlineRoundParams()
	require.NoError(t, err)
	require.Equal(t, expectedOnlineRoundParams, actualOnlineRoundParams)
	require.Equal(t, 9, int(endRound))

	// check LoadAllFullAccounts
	loaded := make(map[basics.Address]basics.AccountData, len(accts))
	acctCb := func(addr basics.Address, data basics.AccountData) {
		loaded[addr] = data
	}
	count, err := arw.LoadAllFullAccounts(context.Background(), "accountbase", "resources", acctCb)
	require.NoError(t, err)
	require.Equal(t, count, len(accts))
	require.Equal(t, count, len(loaded))
	require.Equal(t, accts, loaded)
}

// TestAccountDBInMemoryAcct checks in-memory only account modifications are handled correctly by
// makeCompactAccountDeltas, makeCompactResourceDeltas and accountsNewRound
func TestAccountDBInMemoryAcct(t *testing.T) {
	partitiontest.PartitionTest(t)

	proto := config.Consensus[protocol.ConsensusCurrentVersion]
	type testfunc func(basics.Address) ([]ledgercore.StateDelta, int, int)
	var tests = []testfunc{
		func(addr basics.Address) ([]ledgercore.StateDelta, int, int) {
			const numRounds = 4
			stateDeltas := make([]ledgercore.StateDelta, numRounds)
			stateDeltas[0].Accts.Upsert(addr, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 1000000}}})
			stateDeltas[0].Accts.UpsertAssetResource(addr, 100, ledgercore.AssetParamsDelta{}, ledgercore.AssetHoldingDelta{Holding: &basics.AssetHolding{Amount: 0}})
			// transfer some asset
			stateDeltas[1].Accts.UpsertAssetResource(addr, 100, ledgercore.AssetParamsDelta{}, ledgercore.AssetHoldingDelta{Holding: &basics.AssetHolding{Amount: 100}})
			// close out the asset
			stateDeltas[2].Accts.UpsertAssetResource(addr, 100, ledgercore.AssetParamsDelta{}, ledgercore.AssetHoldingDelta{Deleted: true})
			// close the account
			stateDeltas[3].Accts.Upsert(addr, ledgercore.AccountData{})
			return stateDeltas, 2, 3
		},
		func(addr basics.Address) ([]ledgercore.StateDelta, int, int) {
			const numRounds = 4
			stateDeltas := make([]ledgercore.StateDelta, numRounds)
			stateDeltas[0].Accts.Upsert(addr, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 1000000}}})
			stateDeltas[1].Accts.UpsertAssetResource(addr, 100, ledgercore.AssetParamsDelta{}, ledgercore.AssetHoldingDelta{Holding: &basics.AssetHolding{Amount: 0}})
			// close out the asset
			stateDeltas[2].Accts.UpsertAssetResource(addr, 100, ledgercore.AssetParamsDelta{}, ledgercore.AssetHoldingDelta{Deleted: true})
			// close the account
			stateDeltas[3].Accts.Upsert(addr, ledgercore.AccountData{})
			return stateDeltas, 2, 2
		},
	}

	for i, test := range tests {

		dbs, _ := storetesting.DbOpenTest(t, true)
		storetesting.SetDbLogging(t, dbs)
		defer dbs.Close()

		tx, err := dbs.Wdb.Handle.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		accts := ledgertesting.RandomAccounts(1, true)
		store.AccountsInitTest(t, tx, accts, protocol.ConsensusCurrentVersion)
		addr := ledgertesting.RandomAddress()

		// lastCreatableID stores asset or app max used index to get rid of conflicts
		var baseAccounts lruAccounts
		var baseResources lruResources
		baseAccounts.init(nil, 100, 80)
		baseResources.init(nil, 100, 80)

		t.Run(fmt.Sprintf("test%d", i), func(t *testing.T) {

			stateDeltas, numAcctDeltas, numResDeltas := test(addr)
			lastRound := uint64(len(stateDeltas) + 1)

			outAccountDeltas := makeCompactAccountDeltas(stateDeltas, basics.Round(1), true, baseAccounts)
			require.Equal(t, 1, len(outAccountDeltas.deltas))
			require.Equal(t, accountDelta{newAcct: store.BaseAccountData{UpdateRound: lastRound}, nAcctDeltas: numAcctDeltas, address: addr}, outAccountDeltas.deltas[0])
			require.Equal(t, 1, len(outAccountDeltas.misses))

			outResourcesDeltas := makeCompactResourceDeltas(stateDeltas, basics.Round(1), true, baseAccounts, baseResources)
			require.Equal(t, 1, len(outResourcesDeltas.deltas))
			require.Equal(t,
				resourceDelta{
					oldResource: store.PersistedResourcesData{Aidx: 100}, newResource: store.MakeResourcesData(lastRound - 1),
					nAcctDeltas: numResDeltas, address: addr,
				},
				outResourcesDeltas.deltas[0],
			)
			require.Equal(t, 1, len(outAccountDeltas.misses))

			err = outAccountDeltas.accountsLoadOld(tx)
			require.NoError(t, err)

			knownAddresses := make(map[basics.Address]int64)
			for _, delta := range outAccountDeltas.deltas {
				knownAddresses[delta.oldAcct.Addr] = delta.oldAcct.Rowid
			}

			err = outResourcesDeltas.resourcesLoadOld(tx, knownAddresses)
			require.NoError(t, err)

			updatedAccts, updatesResources, updatedKVs, err := accountsNewRound(tx, outAccountDeltas, outResourcesDeltas, nil, nil, proto, basics.Round(lastRound))
			require.NoError(t, err)
			require.Equal(t, 1, len(updatedAccts)) // we store empty even for deleted accounts
			require.Equal(t,
				store.PersistedAccountData{Addr: addr, Round: basics.Round(lastRound)},
				updatedAccts[0],
			)

			require.Equal(t, 1, len(updatesResources[addr])) // we store empty even for deleted resources
			require.Equal(t,
				store.PersistedResourcesData{Addrid: 0, Aidx: 100, Data: store.MakeResourcesData(0), Round: basics.Round(lastRound)},
				updatesResources[addr][0],
			)

			require.Empty(t, updatedKVs)
		})
	}
}

func TestAccountStorageWithStateProofID(t *testing.T) {
	partitiontest.PartitionTest(t)

	dbs, _ := storetesting.DbOpenTest(t, true)
	storetesting.SetDbLogging(t, dbs)
	defer dbs.Close()

	tx, err := dbs.Wdb.Handle.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	accts := ledgertesting.RandomAccounts(20, false)
	_ = store.AccountsInitTest(t, tx, accts, protocol.ConsensusCurrentVersion)
	checkAccounts(t, tx, 0, accts)
	require.True(t, allAccountsHaveStateProofPKs(accts))
}

func allAccountsHaveStateProofPKs(accts map[basics.Address]basics.AccountData) bool {
	for _, data := range accts {
		if data.Status == basics.Online && data.StateProofID.IsEmpty() {
			return false
		}
	}
	return true
}

// checkCreatables compares the expected database image to the actual databse content
func checkCreatables(t *testing.T,
	tx *sql.Tx, iteration int,
	expectedDbImage map[basics.CreatableIndex]ledgercore.ModifiedCreatable) {

	stmt, err := tx.Prepare("SELECT asset, creator, ctype FROM assetcreators")
	require.NoError(t, err)

	defer stmt.Close()
	rows, err := stmt.Query()
	if err != sql.ErrNoRows {
		require.NoError(t, err)
	}
	defer rows.Close()
	counter := 0
	for rows.Next() {
		counter++
		mc := ledgercore.ModifiedCreatable{}
		var buf []byte
		var asset basics.CreatableIndex
		err := rows.Scan(&asset, &buf, &mc.Ctype)
		require.NoError(t, err)
		copy(mc.Creator[:], buf)

		require.NotNil(t, expectedDbImage[asset])
		require.Equal(t, expectedDbImage[asset].Creator, mc.Creator)
		require.Equal(t, expectedDbImage[asset].Ctype, mc.Ctype)
		require.True(t, expectedDbImage[asset].Created)
	}
	require.Equal(t, len(expectedDbImage), counter)
}

// randomCreatableSampling sets elements to delete from previous iteration
// It consideres 10 elements in an iteration.
// loop 0: returns the first 10 elements
// loop 1: returns:
// - the second 10 elements
// - random sample of elements from the first 10: created changed from true -> false
// loop 2: returns:
// - the elements 20->30
// - random sample of elements from 10->20: created changed from true -> false
func randomCreatableSampling(iteration int, crtbsList []basics.CreatableIndex,
	creatables map[basics.CreatableIndex]ledgercore.ModifiedCreatable,
	expectedDbImage map[basics.CreatableIndex]ledgercore.ModifiedCreatable,
	numElementsPerSegement int) map[basics.CreatableIndex]ledgercore.ModifiedCreatable {

	iteration-- // 0-based here

	delSegmentEnd := iteration * numElementsPerSegement
	delSegmentStart := delSegmentEnd - numElementsPerSegement
	if delSegmentStart < 0 {
		delSegmentStart = 0
	}

	newSample := make(map[basics.CreatableIndex]ledgercore.ModifiedCreatable)
	stop := delSegmentEnd + numElementsPerSegement

	for i := delSegmentStart; i < delSegmentEnd; i++ {
		ctb := creatables[crtbsList[i]]
		if ctb.Created &&
			// Always delete the first element, to make sure at least one
			// element is always deleted.
			(i == delSegmentStart || (crypto.RandUint64()%2) == 1) {
			ctb.Created = false
			newSample[crtbsList[i]] = ctb
			delete(expectedDbImage, crtbsList[i])
		}
	}

	for i := delSegmentEnd; i < stop; i++ {
		newSample[crtbsList[i]] = creatables[crtbsList[i]]
		if creatables[crtbsList[i]].Created {
			expectedDbImage[crtbsList[i]] = creatables[crtbsList[i]]
		}
	}

	return newSample
}

func randomCreatables(numElementsPerSegement int) ([]basics.CreatableIndex,
	map[basics.CreatableIndex]ledgercore.ModifiedCreatable) {
	creatables := make(map[basics.CreatableIndex]ledgercore.ModifiedCreatable)
	creatablesList := make([]basics.CreatableIndex, numElementsPerSegement*10)
	uniqueAssetIds := make(map[basics.CreatableIndex]bool)

	for i := 0; i < numElementsPerSegement*10; i++ {
		assetIndex, mc := randomCreatable(uniqueAssetIds)
		creatables[assetIndex] = mc
		creatablesList[i] = assetIndex
	}
	return creatablesList, creatables // creatablesList is needed for maintaining the order
}

// randomCreatable generates a random creatable.
func randomCreatable(uniqueAssetIds map[basics.CreatableIndex]bool) (
	assetIndex basics.CreatableIndex, mc ledgercore.ModifiedCreatable) {

	var ctype basics.CreatableType

	switch crypto.RandUint64() % 2 {
	case 0:
		ctype = basics.AssetCreatable
	case 1:
		ctype = basics.AppCreatable
	}

	creatable := ledgercore.ModifiedCreatable{
		Ctype:   ctype,
		Created: (crypto.RandUint64() % 2) == 1,
		Creator: ledgertesting.RandomAddress(),
		Ndeltas: 1,
	}

	var assetIdx basics.CreatableIndex
	for {
		assetIdx = basics.CreatableIndex(crypto.RandUint64() % (uint64(2) << 50))
		_, found := uniqueAssetIds[assetIdx]
		if !found {
			uniqueAssetIds[assetIdx] = true
			break
		}
	}
	return assetIdx, creatable
}

func generateRandomTestingAccountBalances(numAccounts int) (updates map[basics.Address]basics.AccountData) {
	secrets := crypto.GenerateOneTimeSignatureSecrets(15, 500)
	pubVrfKey, _ := crypto.VrfKeygenFromSeed([32]byte{0, 1, 2, 3})
	var stateProofID merklesignature.Verifier
	crypto.RandBytes(stateProofID.Commitment[:])
	updates = make(map[basics.Address]basics.AccountData, numAccounts)

	for i := 0; i < numAccounts; i++ {
		addr := ledgertesting.RandomAddress()
		updates[addr] = basics.AccountData{
			MicroAlgos:         basics.MicroAlgos{Raw: 0x000ffffffffffffff / uint64(numAccounts)},
			Status:             basics.NotParticipating,
			RewardsBase:        uint64(i),
			RewardedMicroAlgos: basics.MicroAlgos{Raw: 0x000ffffffffffffff / uint64(numAccounts)},
			VoteID:             secrets.OneTimeSignatureVerifier,
			SelectionID:        pubVrfKey,
			StateProofID:       stateProofID.Commitment,
			VoteFirstValid:     basics.Round(0x000ffffffffffffff),
			VoteLastValid:      basics.Round(0x000ffffffffffffff),
			VoteKeyDilution:    0x000ffffffffffffff,
			AssetParams: map[basics.AssetIndex]basics.AssetParams{
				0x000ffffffffffffff: {
					Total:         0x000ffffffffffffff,
					Decimals:      0x2ffffff,
					DefaultFrozen: true,
					UnitName:      "12345678",
					AssetName:     "12345678901234567890123456789012",
					URL:           "12345678901234567890123456789012",
					MetadataHash:  pubVrfKey,
					Manager:       addr,
					Reserve:       addr,
					Freeze:        addr,
					Clawback:      addr,
				},
			},
			Assets: map[basics.AssetIndex]basics.AssetHolding{
				0x000ffffffffffffff: {
					Amount: 0x000ffffffffffffff,
					Frozen: true,
				},
			},
		}
	}
	return
}

func benchmarkInitBalances(b testing.TB, numAccounts int, dbs db.Pair, proto protocol.ConsensusVersion) (updates map[basics.Address]basics.AccountData) {
	tx, err := dbs.Wdb.Handle.Begin()
	require.NoError(b, err)

	updates = generateRandomTestingAccountBalances(numAccounts)

	store.AccountsInitTest(b, tx, updates, proto)
	err = tx.Commit()
	require.NoError(b, err)
	return
}

func cleanupTestDb(dbs db.Pair, dbName string, inMemory bool) {
	dbs.Close()
	if !inMemory {
		os.Remove(dbName)
	}
}

func benchmarkReadingAllBalances(b *testing.B, inMemory bool) {
	dbs, fn := storetesting.DbOpenTest(b, inMemory)
	storetesting.SetDbLogging(b, dbs)
	defer cleanupTestDb(dbs, fn, inMemory)

	benchmarkInitBalances(b, b.N, dbs, protocol.ConsensusCurrentVersion)
	tx, err := dbs.Rdb.Handle.Begin()
	require.NoError(b, err)

	b.ResetTimer()
	// read all the balances in the database.
	bal, err2 := accountsAll(tx)
	require.NoError(b, err2)
	tx.Commit()

	prevHash := crypto.Digest{}
	for _, accountBalance := range bal {
		encodedAccountBalance := protocol.Encode(&accountBalance)
		prevHash = crypto.Hash(append(encodedAccountBalance, []byte(prevHash[:])...))
	}
	require.Equal(b, b.N, len(bal))
}

func BenchmarkReadingAllBalancesRAM(b *testing.B) {
	benchmarkReadingAllBalances(b, true)
}

func BenchmarkReadingAllBalancesDisk(b *testing.B) {
	benchmarkReadingAllBalances(b, false)
}

func benchmarkReadingRandomBalances(b *testing.B, inMemory bool) {
	dbs, fn := storetesting.DbOpenTest(b, inMemory)
	storetesting.SetDbLogging(b, dbs)
	defer cleanupTestDb(dbs, fn, inMemory)

	accounts := benchmarkInitBalances(b, b.N, dbs, protocol.ConsensusCurrentVersion)

	qs, err := store.AccountsInitDbQueries(dbs.Rdb.Handle)
	require.NoError(b, err)
	defer qs.Close()

	// read all the balances in the database, shuffled
	addrs := make([]basics.Address, len(accounts))
	pos := 0
	for addr := range accounts {
		addrs[pos] = addr
		pos++
	}
	rand.Shuffle(len(addrs), func(i, j int) { addrs[i], addrs[j] = addrs[j], addrs[i] })

	// only measure the actual fetch time
	b.ResetTimer()
	for _, addr := range addrs {
		_, err = qs.LookupAccount(addr)
		require.NoError(b, err)
	}
}

func BenchmarkReadingRandomBalancesRAM(b *testing.B) {
	benchmarkReadingRandomBalances(b, true)
}

func BenchmarkReadingRandomBalancesDisk(b *testing.B) {
	benchmarkReadingRandomBalances(b, false)
}
func BenchmarkWritingRandomBalancesDisk(b *testing.B) {
	totalStartupAccountsNumber := 5000000
	batchCount := 1000
	startupAcct := 5
	initDatabase := func() (*sql.Tx, func(), error) {
		dbs, fn := storetesting.DbOpenTest(b, false)
		storetesting.SetDbLogging(b, dbs)
		cleanup := func() {
			cleanupTestDb(dbs, fn, false)
		}

		benchmarkInitBalances(b, startupAcct, dbs, protocol.ConsensusCurrentVersion)
		dbs.Wdb.SetSynchronousMode(context.Background(), db.SynchronousModeOff, false)

		// insert 1M accounts data, in batches of 1000
		for batch := 0; batch <= batchCount; batch++ {
			fmt.Printf("\033[M\r %d / %d accounts written", totalStartupAccountsNumber*batch/batchCount, totalStartupAccountsNumber)

			tx, err := dbs.Wdb.Handle.Begin()

			require.NoError(b, err)

			acctsData := generateRandomTestingAccountBalances(totalStartupAccountsNumber / batchCount)
			replaceStmt, err := tx.Prepare("INSERT INTO accountbase (address, normalizedonlinebalance, data) VALUES (?, ?, ?)")
			require.NoError(b, err)
			defer replaceStmt.Close()
			for addr, acctData := range acctsData {
				_, err = replaceStmt.Exec(addr[:], uint64(0), protocol.Encode(&acctData))
				require.NoError(b, err)
			}

			err = tx.Commit()
			require.NoError(b, err)
		}
		dbs.Wdb.SetSynchronousMode(context.Background(), db.SynchronousModeFull, true)
		tx, err := dbs.Wdb.Handle.Begin()
		require.NoError(b, err)
		fmt.Printf("\033[M\r")
		return tx, cleanup, err
	}

	selectAccounts := func(tx *sql.Tx) (accountsAddress [][]byte, accountsRowID []int) {
		accountsAddress = make([][]byte, 0, totalStartupAccountsNumber+startupAcct)
		accountsRowID = make([]int, 0, totalStartupAccountsNumber+startupAcct)

		// read all the accounts to obtain the addrs.
		rows, err := tx.Query("SELECT rowid, address FROM accountbase")
		require.NoError(b, err)
		defer rows.Close()
		for rows.Next() {
			var addrbuf []byte
			var rowid int
			err = rows.Scan(&rowid, &addrbuf)
			require.NoError(b, err)
			accountsAddress = append(accountsAddress, addrbuf)
			accountsRowID = append(accountsRowID, rowid)
		}
		return
	}

	tx, cleanup, err := initDatabase()
	require.NoError(b, err)
	defer cleanup()

	accountsAddress, accountsRowID := selectAccounts(tx)

	b.Run("ByAddr", func(b *testing.B) {
		preparedUpdate, err := tx.Prepare("UPDATE accountbase SET data = ? WHERE address = ?")
		require.NoError(b, err)
		defer preparedUpdate.Close()
		// updates accounts by address
		randomAccountData := make([]byte, 200)
		crypto.RandBytes(randomAccountData)
		updateOrder := rand.Perm(len(accountsRowID))
		b.ResetTimer()
		startTime := time.Now()
		for n := 0; n < b.N; n++ {
			for _, acctIdx := range updateOrder {
				res, err := preparedUpdate.Exec(randomAccountData[:], accountsAddress[acctIdx])
				require.NoError(b, err)
				rowsAffected, err := res.RowsAffected()
				require.NoError(b, err)
				require.Equal(b, int64(1), rowsAffected)
				n++
				if n == b.N {
					break
				}
			}

		}
		b.ReportMetric(float64(int(time.Since(startTime))/b.N), "ns/acct_update")
	})

	b.Run("ByRowID", func(b *testing.B) {
		preparedUpdate, err := tx.Prepare("UPDATE accountbase SET data = ? WHERE rowid = ?")
		require.NoError(b, err)
		defer preparedUpdate.Close()
		// updates accounts by address
		randomAccountData := make([]byte, 200)
		crypto.RandBytes(randomAccountData)
		updateOrder := rand.Perm(len(accountsRowID))
		b.ResetTimer()
		startTime := time.Now()
		for n := 0; n < b.N; n++ {
			for _, acctIdx := range updateOrder {
				res, err := preparedUpdate.Exec(randomAccountData[:], accountsRowID[acctIdx])
				require.NoError(b, err)
				rowsAffected, err := res.RowsAffected()
				require.NoError(b, err)
				require.Equal(b, int64(1), rowsAffected)
				n++
				if n == b.N {
					break
				}
			}
		}
		b.ReportMetric(float64(int(time.Since(startTime))/b.N), "ns/acct_update")

	})

	err = tx.Commit()
	require.NoError(b, err)
}

// TestAccountsDbQueriesCreateClose tests to see that we can create the accountsDbQueries and close it.
// it also verify that double-closing it doesn't create an issue.
func TestAccountsDbQueriesCreateClose(t *testing.T) {
	partitiontest.PartitionTest(t)

	dbs, _ := storetesting.DbOpenTest(t, true)
	storetesting.SetDbLogging(t, dbs)
	defer dbs.Close()

	err := dbs.Wdb.Atomic(func(ctx context.Context, tx *sql.Tx) (err error) {
		store.AccountsInitTest(t, tx, make(map[basics.Address]basics.AccountData), protocol.ConsensusCurrentVersion)
		return nil
	})
	require.NoError(t, err)
	qs, err := store.AccountsInitDbQueries(dbs.Rdb.Handle)
	require.NoError(t, err)
	// TODO[store-refactor]: internals are opaque, once we move the the remainder of accountdb we can mvoe this too
	// require.NotNil(t, qs.listCreatablesStmt)
	qs.Close()
	// require.Nil(t, qs.listCreatablesStmt)
	qs.Close()
	// require.Nil(t, qs.listCreatablesStmt)
}

func benchmarkWriteCatchpointStagingBalancesSub(b *testing.B, ascendingOrder bool) {
	proto := config.Consensus[protocol.ConsensusCurrentVersion]
	genesisInitState, _ := ledgertesting.GenerateInitState(b, protocol.ConsensusCurrentVersion, 100)
	const inMem = false
	log := logging.TestingLog(b)
	cfg := config.GetDefaultLocal()
	cfg.Archival = false
	log.SetLevel(logging.Warn)
	dbBaseFileName := strings.Replace(b.Name(), "/", "_", -1)
	l, err := OpenLedger(log, dbBaseFileName, inMem, genesisInitState, cfg)
	require.NoError(b, err, "could not open ledger")
	defer func() {
		l.Close()
		os.Remove(dbBaseFileName + ".block.sqlite")
		os.Remove(dbBaseFileName + ".tracker.sqlite")
	}()
	catchpointAccessor := MakeCatchpointCatchupAccessor(l, log)
	catchpointAccessor.ResetStagingBalances(context.Background(), true)
	targetAccountsCount := uint64(b.N)
	accountsLoaded := uint64(0)
	var last64KStart time.Time
	last64KSize := uint64(0)
	last64KAccountCreationTime := time.Duration(0)
	accountsWritingStarted := time.Now()
	accountsGenerationDuration := time.Duration(0)
	b.ResetTimer()
	for accountsLoaded < targetAccountsCount {
		b.StopTimer()
		balancesLoopStart := time.Now()
		// generate a chunk;
		chunkSize := targetAccountsCount - accountsLoaded
		if chunkSize > BalancesPerCatchpointFileChunk {
			chunkSize = BalancesPerCatchpointFileChunk
		}
		last64KSize += chunkSize
		if accountsLoaded >= targetAccountsCount-64*1024 && last64KStart.IsZero() {
			last64KStart = time.Now()
			last64KSize = chunkSize
			last64KAccountCreationTime = time.Duration(0)
		}
		var chunk catchpointFileChunkV6
		chunk.Balances = make([]encoded.BalanceRecordV6, chunkSize)
		for i := uint64(0); i < chunkSize; i++ {
			var randomAccount encoded.BalanceRecordV6
			accountData := store.BaseAccountData{RewardsBase: accountsLoaded + i}
			accountData.MicroAlgos.Raw = crypto.RandUint63()
			randomAccount.AccountData = protocol.Encode(&accountData)
			crypto.RandBytes(randomAccount.Address[:])
			if ascendingOrder {
				binary.LittleEndian.PutUint64(randomAccount.Address[:], accountsLoaded+i)
			}
			chunk.Balances[i] = randomAccount
		}
		balanceLoopDuration := time.Since(balancesLoopStart)
		last64KAccountCreationTime += balanceLoopDuration
		accountsGenerationDuration += balanceLoopDuration

		normalizedAccountBalances, err := prepareNormalizedBalancesV6(chunk.Balances, proto)
		require.NoError(b, err)
		b.StartTimer()
		err = l.trackerDBs.Wdb.Atomic(func(ctx context.Context, tx *sql.Tx) (err error) {
			crw := store.NewCatchpointSQLReaderWriter(tx)
			err = crw.WriteCatchpointStagingBalances(ctx, normalizedAccountBalances)
			return
		})

		require.NoError(b, err)
		accountsLoaded += chunkSize
	}
	if !last64KStart.IsZero() {
		last64KDuration := time.Since(last64KStart) - last64KAccountCreationTime
		fmt.Printf("%-82s%-7d (last 64k) %-6d ns/account       %d accounts/sec\n", b.Name(), last64KSize, (last64KDuration / time.Duration(last64KSize)).Nanoseconds(), int(float64(last64KSize)/float64(last64KDuration.Seconds())))
	}
	stats, err := l.trackerDBs.Wdb.Vacuum(context.Background())
	require.NoError(b, err)
	fmt.Printf("%-82sdb fragmentation   %.1f%%\n", b.Name(), float32(stats.PagesBefore-stats.PagesAfter)*100/float32(stats.PagesBefore))
	b.ReportMetric(float64(b.N)/float64((time.Since(accountsWritingStarted)-accountsGenerationDuration).Seconds()), "accounts/sec")
}

func BenchmarkWriteCatchpointStagingBalances(b *testing.B) {
	benchSizes := []int{1024 * 100, 1024 * 200, 1024 * 400}
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("RandomInsertOrder-%d", size), func(b *testing.B) {
			b.N = size
			benchmarkWriteCatchpointStagingBalancesSub(b, false)
		})
	}
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("AscendingInsertOrder-%d", size), func(b *testing.B) {
			b.N = size
			benchmarkWriteCatchpointStagingBalancesSub(b, true)
		})
	}
}

func TestLookupKeysByPrefix(t *testing.T) {
	partitiontest.PartitionTest(t)
	t.Parallel()

	dbs, fn := storetesting.DbOpenTest(t, false)
	storetesting.SetDbLogging(t, dbs)
	defer cleanupTestDb(dbs, fn, false)

	// return account data, initialize DB tables from AccountsInitTest
	_ = benchmarkInitBalances(t, 1, dbs, protocol.ConsensusCurrentVersion)

	qs, err := store.AccountsInitDbQueries(dbs.Rdb.Handle)
	require.NoError(t, err)
	defer qs.Close()

	kvPairDBPrepareSet := []struct {
		key   []byte
		value []byte
	}{
		{key: []byte{0xFF, 0x12, 0x34, 0x56, 0x78}, value: []byte("val0")},
		{key: []byte{0xFF, 0xFF, 0x34, 0x56, 0x78}, value: []byte("val1")},
		{key: []byte{0xFF, 0xFF, 0xFF, 0x56, 0x78}, value: []byte("val2")},
		{key: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x78}, value: []byte("val3")},
		{key: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, value: []byte("val4")},
		{key: []byte{0xFF, 0xFE, 0xFF}, value: []byte("val5")},
		{key: []byte{0xFF, 0xFF, 0x00, 0xFF, 0xFF}, value: []byte("val6")},
		{key: []byte{0xFF, 0xFF}, value: []byte("should not confuse with 0xFF-0xFE")},
		{key: []byte{0xBA, 0xDD, 0xAD, 0xFF, 0xFF}, value: []byte("baddadffff")},
		{key: []byte{0xBA, 0xDD, 0xAE, 0x00}, value: []byte("baddae00")},
		{key: []byte{0xBA, 0xDD, 0xAE}, value: []byte("baddae")},
		{key: []byte("TACOCAT"), value: []byte("val6")},
		{key: []byte("TACOBELL"), value: []byte("2bucks50cents?")},
		{key: []byte("DingHo-SmallPack"), value: []byte("3bucks75cents")},
		{key: []byte("DingHo-StandardPack"), value: []byte("5bucks25cents")},
		{key: []byte("BostonKitchen-CheeseSlice"), value: []byte("3bucks50cents")},
		{key: []byte(`™£´´∂ƒ∂ƒßƒ©∑®ƒß∂†¬∆`), value: []byte("random Bluh")},
	}

	tx, err := dbs.Wdb.Handle.Begin()
	require.NoError(t, err)

	// writer is only for kvstore
	writer, err := store.MakeAccountsSQLWriter(tx, true, true, true, true)
	if err != nil {
		return
	}

	for i := 0; i < len(kvPairDBPrepareSet); i++ {
		err := writer.UpsertKvPair(string(kvPairDBPrepareSet[i].key), kvPairDBPrepareSet[i].value)
		require.NoError(t, err)
	}

	err = tx.Commit()
	require.NoError(t, err)
	writer.Close()

	testCases := []struct {
		prefix        []byte
		expectedNames [][]byte
		err           string
	}{
		{
			prefix: []byte{0xFF},
			err:    "strange prefix",
		},
		{
			prefix: []byte{0xFF, 0xFE},
			expectedNames: [][]byte{
				{0xFF, 0xFE, 0xFF},
			},
		},
		{
			prefix: []byte{0xFF, 0xFE, 0xFF},
			expectedNames: [][]byte{
				{0xFF, 0xFE, 0xFF},
			},
		},
		{
			prefix: []byte{0xFF, 0xFF},
			err:    "strange prefix",
		},
		{
			prefix: []byte{0xFF, 0xFF, 0xFF},
			err:    "strange prefix",
		},
		{
			prefix: []byte{0xFF, 0xFF, 0xFF, 0xFF},
			err:    "strange prefix",
		},
		{
			prefix: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			err:    "strange prefix",
		},
		{
			prefix: []byte{0xBA, 0xDD, 0xAD, 0xFF},
			expectedNames: [][]byte{
				{0xBA, 0xDD, 0xAD, 0xFF, 0xFF},
			},
		},
		{
			prefix: []byte{0xBA, 0xDD, 0xAD, 0xFF, 0xFF},
			expectedNames: [][]byte{
				{0xBA, 0xDD, 0xAD, 0xFF, 0xFF},
			},
		},
		{
			prefix: []byte{0xBA, 0xDD},
			expectedNames: [][]byte{
				{0xBA, 0xDD, 0xAE},
				{0xBA, 0xDD, 0xAE, 0x00},
				{0xBA, 0xDD, 0xAD, 0xFF, 0xFF},
			},
		},
		{
			prefix: []byte{0xBA, 0xDD, 0xAE},
			expectedNames: [][]byte{
				{0xBA, 0xDD, 0xAE},
				{0xBA, 0xDD, 0xAE, 0x00},
			},
		},
		{
			prefix: []byte("TACO"),
			expectedNames: [][]byte{
				[]byte("TACOCAT"),
				[]byte("TACOBELL"),
			},
		},
		{
			prefix:        []byte("TACOC"),
			expectedNames: [][]byte{[]byte("TACOCAT")},
		},
		{
			prefix: []byte("DingHo"),
			expectedNames: [][]byte{
				[]byte("DingHo-SmallPack"),
				[]byte("DingHo-StandardPack"),
			},
		},
		{
			prefix: []byte("DingHo-S"),
			expectedNames: [][]byte{
				[]byte("DingHo-SmallPack"),
				[]byte("DingHo-StandardPack"),
			},
		},
		{
			prefix:        []byte("DingHo-Small"),
			expectedNames: [][]byte{[]byte("DingHo-SmallPack")},
		},
		{
			prefix:        []byte("BostonKitchen"),
			expectedNames: [][]byte{[]byte("BostonKitchen-CheeseSlice")},
		},
		{
			prefix:        []byte(`™£´´∂ƒ∂ƒßƒ©`),
			expectedNames: [][]byte{[]byte(`™£´´∂ƒ∂ƒßƒ©∑®ƒß∂†¬∆`)},
		},
		{
			prefix: []byte{},
			err:    "strange prefix",
		},
	}

	for index, testCase := range testCases {
		t.Run("lookupKVByPrefix-testcase-"+strconv.Itoa(index), func(t *testing.T) {
			actual := make(map[string]bool)
			_, err := qs.LookupKeysByPrefix(string(testCase.prefix), uint64(len(kvPairDBPrepareSet)), actual, 0)
			if err != nil {
				require.NotEmpty(t, testCase.err, testCase.prefix)
				require.Contains(t, err.Error(), testCase.err)
			} else {
				require.Empty(t, testCase.err)
				expected := make(map[string]bool)
				for _, name := range testCase.expectedNames {
					expected[string(name)] = true
				}
				require.Equal(t, actual, expected)
			}
		})
	}
}

func BenchmarkLookupKeyByPrefix(b *testing.B) {
	// learn something from BenchmarkWritingRandomBalancesDisk

	dbs, fn := storetesting.DbOpenTest(b, false)
	storetesting.SetDbLogging(b, dbs)
	defer cleanupTestDb(dbs, fn, false)

	// return account data, initialize DB tables from AccountsInitTest
	_ = benchmarkInitBalances(b, 1, dbs, protocol.ConsensusCurrentVersion)

	qs, err := store.AccountsInitDbQueries(dbs.Rdb.Handle)
	require.NoError(b, err)
	defer qs.Close()

	currentDBSize := 0
	nextDBSize := 2
	increment := 2

	nameBuffer := make([]byte, 5)
	valueBuffer := make([]byte, 5)

	// from 2^1 -> 2^2 -> ... -> 2^22 sized DB
	for bIndex := 0; bIndex < 22; bIndex++ {
		// make writer to DB
		tx, err := dbs.Wdb.Handle.Begin()
		require.NoError(b, err)

		// writer is only for kvstore
		writer, err := store.MakeAccountsSQLWriter(tx, true, true, true, true)
		if err != nil {
			return
		}

		var prefix string
		// how to write to dbs a bunch of stuffs?
		for i := 0; i < nextDBSize-currentDBSize; i++ {
			crypto.RandBytes(nameBuffer)
			crypto.RandBytes(valueBuffer)
			appID := basics.AppIndex(crypto.RandUint64())
			boxKey := logic.MakeBoxKey(appID, string(nameBuffer))
			err = writer.UpsertKvPair(boxKey, valueBuffer)
			require.NoError(b, err)

			if i == 0 {
				prefix = logic.MakeBoxKey(appID, "")
			}
		}
		err = tx.Commit()
		require.NoError(b, err)
		writer.Close()

		// benchmark the query against large DB, see if we have O(log N) speed
		currentDBSize = nextDBSize
		nextDBSize *= increment

		b.Run("lookupKVByPrefix-DBsize"+strconv.Itoa(currentDBSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				results := make(map[string]bool)
				_, err := qs.LookupKeysByPrefix(prefix, uint64(currentDBSize), results, 0)
				require.NoError(b, err)
				require.True(b, len(results) >= 1)
			}
		})
	}
}

// upsert updates existing or inserts a new entry
func (a *compactResourcesDeltas) upsert(delta resourceDelta) {
	if idx, exist := a.cache[accountCreatable{address: delta.address, index: delta.oldResource.Aidx}]; exist {
		a.deltas[idx] = delta
		return
	}
	a.insert(delta)
}

// upsertOld updates existing or inserts a new partial entry with only old field filled
func (a *compactAccountDeltas) upsertOld(old store.PersistedAccountData) {
	addr := old.Addr
	if idx, exist := a.cache[addr]; exist {
		a.deltas[idx].oldAcct = old
		return
	}
	a.insert(accountDelta{oldAcct: old, address: old.Addr})
}

// upsert updates existing or inserts a new entry
func (a *compactAccountDeltas) upsert(addr basics.Address, delta accountDelta) {
	if idx, exist := a.cache[addr]; exist { // nil map lookup is OK
		a.deltas[idx] = delta
		return
	}
	a.insert(delta)
}
func TestCompactAccountDeltas(t *testing.T) {
	partitiontest.PartitionTest(t)

	a := require.New(t)

	ad := compactAccountDeltas{}
	data, idx := ad.get(basics.Address{})
	a.Equal(-1, idx)
	a.Equal(accountDelta{}, data)

	addr := ledgertesting.RandomAddress()
	data, idx = ad.get(addr)
	a.Equal(-1, idx)
	a.Equal(accountDelta{}, data)

	a.Zero(ad.len())
	a.Panics(func() { ad.getByIdx(0) })

	sample1 := accountDelta{newAcct: store.BaseAccountData{MicroAlgos: basics.MicroAlgos{Raw: 123}}, address: addr}
	ad.upsert(addr, sample1)
	data, idx = ad.get(addr)
	a.NotEqual(-1, idx)
	a.Equal(sample1, data)

	a.Equal(1, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(sample1, data)

	sample2 := accountDelta{newAcct: store.BaseAccountData{MicroAlgos: basics.MicroAlgos{Raw: 456}}, address: addr}
	ad.upsert(addr, sample2)
	data, idx = ad.get(addr)
	a.NotEqual(-1, idx)
	a.Equal(sample2, data)

	a.Equal(1, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(sample2, data)

	ad.update(idx, sample2)
	data, idx2 := ad.get(addr)
	a.Equal(idx, idx2)
	a.Equal(sample2, data)

	a.Equal(1, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(sample2, data)

	old1 := store.PersistedAccountData{Addr: addr, AccountData: store.BaseAccountData{MicroAlgos: basics.MicroAlgos{Raw: 789}}}
	ad.upsertOld(old1)
	a.Equal(1, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(accountDelta{newAcct: sample2.newAcct, oldAcct: old1, address: addr}, data)

	addr1 := ledgertesting.RandomAddress()
	old2 := store.PersistedAccountData{Addr: addr1, AccountData: store.BaseAccountData{MicroAlgos: basics.MicroAlgos{Raw: 789}}}
	ad.upsertOld(old2)
	a.Equal(2, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(accountDelta{newAcct: sample2.newAcct, oldAcct: old1, address: addr}, data)

	data = ad.getByIdx(1)
	a.Equal(addr1, data.oldAcct.Addr)
	a.Equal(accountDelta{oldAcct: old2, address: addr1}, data)

	// apply old on empty delta object, expect no changes
	ad.updateOld(0, old2)
	a.Equal(2, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(accountDelta{newAcct: sample2.newAcct, oldAcct: old2, address: addr}, data)

	addr2 := ledgertesting.RandomAddress()
	sample2.address = addr2
	idx = ad.insert(sample2)
	a.Equal(3, ad.len())
	a.Equal(2, idx)
	data = ad.getByIdx(idx)
	a.Equal(addr2, data.address)
	a.Equal(sample2, data)
}

// upsertOld updates existing or inserts a new partial entry with only old field filled
func (a *compactResourcesDeltas) upsertOld(addr basics.Address, old store.PersistedResourcesData) {
	if idx, exist := a.cache[accountCreatable{address: addr, index: old.Aidx}]; exist {
		a.deltas[idx].oldResource = old
		return
	}
	idx := a.insert(resourceDelta{oldResource: old, address: addr})
	a.deltas[idx].address = addr
}

func TestCompactResourceDeltas(t *testing.T) {
	partitiontest.PartitionTest(t)

	a := require.New(t)

	ad := compactResourcesDeltas{}
	data, idx := ad.get(basics.Address{}, 0)
	a.Equal(-1, idx)
	a.Equal(resourceDelta{}, data)

	addr := ledgertesting.RandomAddress()
	data, idx = ad.get(addr, 0)
	a.Equal(-1, idx)
	a.Equal(resourceDelta{}, data)

	a.Zero(ad.len())
	a.Panics(func() { ad.getByIdx(0) })

	sample1 := resourceDelta{newResource: store.ResourcesData{Total: 123}, address: addr, oldResource: store.PersistedResourcesData{Aidx: 1}}
	ad.upsert(sample1)
	data, idx = ad.get(addr, 1)
	a.NotEqual(-1, idx)
	a.Equal(sample1, data)

	a.Equal(1, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(sample1, data)

	sample2 := resourceDelta{newResource: store.ResourcesData{Total: 456}, address: addr, oldResource: store.PersistedResourcesData{Aidx: 1}}
	ad.upsert(sample2)
	data, idx = ad.get(addr, 1)
	a.NotEqual(-1, idx)
	a.Equal(sample2, data)

	a.Equal(1, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(sample2, data)

	ad.update(idx, sample2)
	data, idx2 := ad.get(addr, 1)
	a.Equal(idx, idx2)
	a.Equal(sample2, data)

	a.Equal(1, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(sample2, data)

	old1 := store.PersistedResourcesData{Addrid: 111, Aidx: 1, Data: store.ResourcesData{Total: 789}}
	ad.upsertOld(addr, old1)
	a.Equal(1, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(resourceDelta{newResource: sample2.newResource, oldResource: old1, address: addr}, data)

	addr1 := ledgertesting.RandomAddress()
	old2 := store.PersistedResourcesData{Addrid: 222, Aidx: 2, Data: store.ResourcesData{Total: 789}}
	ad.upsertOld(addr1, old2)
	a.Equal(2, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(resourceDelta{newResource: sample2.newResource, oldResource: old1, address: addr}, data)

	data = ad.getByIdx(1)
	a.Equal(addr1, data.address)
	a.Equal(resourceDelta{oldResource: old2, address: addr1}, data)

	ad.updateOld(0, old2)
	a.Equal(2, ad.len())
	data = ad.getByIdx(0)
	a.Equal(addr, data.address)
	a.Equal(resourceDelta{newResource: sample2.newResource, oldResource: old2, address: addr}, data)

	addr2 := ledgertesting.RandomAddress()
	sample2.oldResource.Aidx = 2
	sample2.address = addr2
	idx = ad.insert(sample2)
	a.Equal(3, ad.len())
	a.Equal(2, idx)
	data = ad.getByIdx(idx)
	a.Equal(addr2, data.address)
	cachedData, pos := ad.get(addr2, 2)
	a.Equal(2, pos)
	a.Equal(data, cachedData)
	a.Equal(sample2, data)
}

func TestLookupAccountAddressFromAddressID(t *testing.T) {
	partitiontest.PartitionTest(t)

	dbs, _ := storetesting.DbOpenTest(t, true)
	storetesting.SetDbLogging(t, dbs)
	defer dbs.Close()

	addrs := make([]basics.Address, 100)
	for i := range addrs {
		addrs[i] = ledgertesting.RandomAddress()
	}
	addrsids := make(map[basics.Address]int64)
	err := dbs.Wdb.Atomic(func(ctx context.Context, tx *sql.Tx) (err error) {
		store.AccountsInitTest(t, tx, make(map[basics.Address]basics.AccountData), protocol.ConsensusCurrentVersion)

		for i := range addrs {
			res, err := tx.ExecContext(ctx, "INSERT INTO accountbase (address, data) VALUES (?, ?)", addrs[i][:], []byte{12, 3, 4})
			if err != nil {
				return err
			}
			rowid, err := res.LastInsertId()
			if err != nil {
				return err
			}
			addrsids[addrs[i]] = rowid
		}
		return nil
	})
	require.NoError(t, err)

	err = dbs.Rdb.Atomic(func(ctx context.Context, tx *sql.Tx) (err error) {
		arw := store.NewAccountsSQLReaderWriter(tx)

		for addr, addrid := range addrsids {
			retAddr, err := arw.LookupAccountAddressFromAddressID(ctx, addrid)
			if err != nil {
				return err
			}
			if retAddr != addr {
				return fmt.Errorf("mismatching addresses")
			}
		}
		// test fail case:
		retAddr, err := arw.LookupAccountAddressFromAddressID(ctx, -1)

		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("unexpected error : %w", err)
		}
		if !retAddr.IsZero() {
			return fmt.Errorf("unexpected address; should have been empty")
		}
		return nil
	})
	require.NoError(t, err)
}

type mockResourcesKey struct {
	addrid int64
	aidx   basics.CreatableIndex
}
type mockAccountWriter struct {
	// rowid to data
	accounts map[int64]ledgercore.AccountData
	// addr to rowid
	addresses map[basics.Address]int64
	// rowid to addr
	rowids    map[int64]basics.Address
	resources map[mockResourcesKey]ledgercore.AccountResource

	kvStore map[string][]byte

	lastRowid   int64
	availRowIds []int64
}

func makeMockAccountWriter() (m mockAccountWriter) {
	m.accounts = make(map[int64]ledgercore.AccountData)
	m.resources = make(map[mockResourcesKey]ledgercore.AccountResource)
	m.addresses = make(map[basics.Address]int64)
	m.rowids = make(map[int64]basics.Address)
	return
}

func (m mockAccountWriter) clone() (m2 mockAccountWriter) {
	m2.accounts = make(map[int64]ledgercore.AccountData, len(m.accounts))
	m2.resources = make(map[mockResourcesKey]ledgercore.AccountResource, len(m.resources))
	m2.addresses = make(map[basics.Address]int64, len(m.resources))
	m2.rowids = make(map[int64]basics.Address, len(m.rowids))
	for k, v := range m.accounts {
		m2.accounts[k] = v
	}
	for k, v := range m.resources {
		m2.resources[k] = v
	}
	for k, v := range m.addresses {
		m2.addresses[k] = v
	}
	for k, v := range m.rowids {
		m2.rowids[k] = v
	}
	m2.lastRowid = m.lastRowid
	m2.availRowIds = m.availRowIds
	return m2
}

func (m *mockAccountWriter) nextRowid() (rowid int64) {
	if len(m.availRowIds) > 0 {
		rowid = m.availRowIds[len(m.availRowIds)-1]
		m.availRowIds = m.availRowIds[:len(m.availRowIds)-1]
	} else {
		m.lastRowid++
		rowid = m.lastRowid
	}
	return
}

func (m *mockAccountWriter) setAccount(addr basics.Address, data ledgercore.AccountData) {
	var rowid int64
	var ok bool
	if rowid, ok = m.addresses[addr]; !ok {
		rowid = m.nextRowid()
		m.rowids[rowid] = addr
		m.addresses[addr] = rowid
	}
	m.accounts[rowid] = data
}

func (m *mockAccountWriter) setResource(addr basics.Address, cidx basics.CreatableIndex, data ledgercore.AccountResource) error {
	var rowid int64
	var ok bool
	if rowid, ok = m.addresses[addr]; !ok {
		return fmt.Errorf("account %s does not exist", addr.String())
	}
	key := mockResourcesKey{rowid, cidx}
	m.resources[key] = data

	return nil
}

func (m *mockAccountWriter) Lookup(addr basics.Address) (pad store.PersistedAccountData, ok bool, err error) {
	rowid, ok := m.addresses[addr]
	if !ok {
		return
	}
	data, ok := m.accounts[rowid]
	if !ok {
		err = fmt.Errorf("not found %s", addr.String())
		return
	}
	pad.AccountData.SetCoreAccountData(&data)
	pad.Addr = addr
	pad.Rowid = rowid
	return
}

func (m *mockAccountWriter) LookupResource(addr basics.Address, cidx basics.CreatableIndex) (prd store.PersistedResourcesData, ok bool, err error) {
	rowid, ok := m.addresses[addr]
	if !ok {
		return
	}
	res, ok := m.resources[mockResourcesKey{rowid, cidx}]
	if !ok {
		err = fmt.Errorf("not found (%s, %d)", addr.String(), cidx)
		return
	}
	if res.AppLocalState != nil {
		prd.Data.SetAppLocalState(*res.AppLocalState)
	}
	if res.AppParams != nil {
		prd.Data.SetAppParams(*res.AppParams, prd.Data.IsHolding())
	}
	if res.AssetHolding != nil {
		prd.Data.SetAssetHolding(*res.AssetHolding)
	}
	if res.AssetParams != nil {
		prd.Data.SetAssetParams(*res.AssetParams, prd.Data.IsHolding())
	}
	prd.Addrid = rowid
	prd.Aidx = cidx
	return
}

func (m *mockAccountWriter) InsertAccount(addr basics.Address, normBalance uint64, data store.BaseAccountData) (rowid int64, err error) {
	rowid, ok := m.addresses[addr]
	if ok {
		err = fmt.Errorf("insertAccount: addr %s, rowid %d: UNIQUE constraint failed", addr.String(), rowid)
		return
	}
	rowid = m.nextRowid()
	m.addresses[addr] = rowid
	m.rowids[rowid] = addr
	m.accounts[rowid] = data.GetLedgerCoreAccountData()
	return
}

func (m *mockAccountWriter) DeleteAccount(rowid int64) (rowsAffected int64, err error) {
	var addr basics.Address
	var ok bool
	if addr, ok = m.rowids[rowid]; !ok {
		return 0, nil
	}

	delete(m.addresses, addr)
	delete(m.rowids, rowid)
	delete(m.accounts, rowid)
	m.availRowIds = append(m.availRowIds, rowid)
	return 1, nil
}

func (m *mockAccountWriter) UpdateAccount(rowid int64, normBalance uint64, data store.BaseAccountData) (rowsAffected int64, err error) {
	if _, ok := m.rowids[rowid]; !ok {
		return 0, fmt.Errorf("updateAccount: not found rowid %d", rowid)
	}
	old, ok := m.accounts[rowid]
	if !ok {
		return 0, fmt.Errorf("updateAccount: not found data for %d", rowid)
	}
	if old == data.GetLedgerCoreAccountData() {
		return 0, nil
	}
	m.accounts[rowid] = data.GetLedgerCoreAccountData()
	return 1, nil
}

func (m *mockAccountWriter) InsertResource(addrid int64, aidx basics.CreatableIndex, data store.ResourcesData) (rowid int64, err error) {
	key := mockResourcesKey{addrid, aidx}
	if _, ok := m.resources[key]; ok {
		return 0, fmt.Errorf("insertResource: (%d, %d): UNIQUE constraint failed", addrid, aidx)
	}
	// use persistedResourcesData.AccountResource for conversion
	prd := store.PersistedResourcesData{Data: data}
	new := prd.AccountResource()
	m.resources[key] = new
	return 1, nil
}

func (m *mockAccountWriter) DeleteResource(addrid int64, aidx basics.CreatableIndex) (rowsAffected int64, err error) {
	key := mockResourcesKey{addrid, aidx}
	if _, ok := m.resources[key]; !ok {
		return 0, nil
	}
	delete(m.resources, key)
	return 1, nil
}

func (m *mockAccountWriter) UpdateResource(addrid int64, aidx basics.CreatableIndex, data store.ResourcesData) (rowsAffected int64, err error) {
	key := mockResourcesKey{addrid, aidx}
	old, ok := m.resources[key]
	if !ok {
		return 0, fmt.Errorf("updateResource: not found (%d, %d)", addrid, aidx)
	}
	// use persistedResourcesData.AccountResource for conversion
	prd := store.PersistedResourcesData{Data: data}
	new := prd.AccountResource()
	if new == old {
		return 0, nil
	}
	m.resources[key] = new
	return 1, nil
}

func (m *mockAccountWriter) UpsertKvPair(key string, value []byte) error {
	m.kvStore[key] = value
	return nil
}

func (m *mockAccountWriter) DeleteKvPair(key string) error {
	delete(m.kvStore, key)
	return nil
}

func (m *mockAccountWriter) InsertCreatable(cidx basics.CreatableIndex, ctype basics.CreatableType, creator []byte) (rowid int64, err error) {
	return 0, fmt.Errorf("insertCreatable: not implemented")
}

func (m *mockAccountWriter) DeleteCreatable(cidx basics.CreatableIndex, ctype basics.CreatableType) (rowsAffected int64, err error) {
	return 0, fmt.Errorf("deleteCreatable: not implemented")
}

func (m *mockAccountWriter) Close() {
}

func factorial(n int) int {
	res := 1
	for i := 1; i <= n; i++ {
		res = res * i
	}
	return res
}

// permHeap generates all permutations for an integer array from 0 to n-1 inclusive
// uses Heap's non-recursive algorithm
func permHeap(n int) (result [][]int) {
	numResults := factorial(n)
	result = make([][]int, 0, numResults)
	input := make([]int, n)
	for i := 0; i < n; i++ {
		input[i] = i
	}
	temp := make([]int, n)
	copy(temp, input)
	result = append(result, temp)

	c := make([]int, n)

	i := 0
	for i < n {
		if c[i] < i {
			if i%2 == 0 {
				input[0], input[i] = input[i], input[0]
			} else {
				input[c[i]], input[i] = input[i], input[c[i]]
			}
			temp := make([]int, n)
			copy(temp, input)
			result = append(result, temp)
			c[i]++
			i = 0
		} else {
			c[i] = 0
			i++
		}
	}
	return
}

func TestFactorialPerm(t *testing.T) {
	partitiontest.PartitionTest(t)
	a := require.New(t)

	a.Equal(1, factorial(0))
	a.Equal(1, factorial(1))
	a.Equal(2, factorial(2))
	a.Equal(6, factorial(3))
	a.Equal(120, factorial(5))

	perms := permHeap(5)
	dict := make(map[string]struct{}, len(perms))
	for _, perm := range perms {
		var key string
		for _, i := range perm {
			key += strconv.Itoa(i)
		}
		dict[key] = struct{}{}
	}
	a.Equal(len(perms), len(dict))
}

func compactAccountDeltasPermutations(a *require.Assertions, cad compactAccountDeltas) []compactAccountDeltas {
	a.Empty(cad.misses)

	size := cad.len()
	result := make([]compactAccountDeltas, 0, factorial(size))

	perms := permHeap(size)
	// remap existing deltas to permutated one
	for _, perm := range perms {
		new := compactAccountDeltas{}
		new.cache = make(map[basics.Address]int, size)
		new.deltas = make([]accountDelta, size)
		for i, k := range perm {
			new.deltas[k] = cad.deltas[i]
		}
		for key, i := range cad.cache {
			new.cache[key] = perm[i]
		}
		result = append(result, new)
	}

	// ensure remapping
	for _, new := range result {
		for key, idx := range new.cache {
			d1 := cad.getByIdx(cad.cache[key])
			d2 := new.getByIdx(idx)
			a.Equal(d1, d2)
		}
	}

	return result
}

func compactResourcesDeltasPermutations(a *require.Assertions, crd compactResourcesDeltas) []compactResourcesDeltas {

	size := crd.len()
	result := make([]compactResourcesDeltas, 0, factorial(size))

	perms := permHeap(size)
	// remap existing deltas to permutated one
	for _, perm := range perms {
		new := compactResourcesDeltas{}
		new.cache = make(map[accountCreatable]int, size)
		new.deltas = make([]resourceDelta, size)
		new.misses = make([]int, len(crd.misses))
		for i, k := range perm {
			new.deltas[k] = crd.deltas[i]
		}
		for key, i := range crd.cache {
			new.cache[key] = perm[i]
		}
		copy(new.misses, crd.misses)
		result = append(result, new)
	}

	// ensure remapping
	for _, new := range result {
		for key, idx := range new.cache {
			d1 := crd.getByIdx(crd.cache[key])
			d2 := new.getByIdx(idx)
			a.Equal(d1, d2)
		}
	}

	return result
}

// TestAccountUnorderedUpdates ensures rowid reuse in accountbase does not lead to
// resources insertion problems.
// This test simulates a problem found while testing resources deltas on testnet:
//
// unable to advance tracker db snapshot (16541781-16541801): db op failed:
// addr RGJVDTZIFR7VIHI4QMSA6Y7H3FCHUXIBS5H26UKDGMWHALTMW3ZRGMNX3M addrid 515356, aidx 22045503, err: UNIQUE constraint failed
//
// Investigation shown there was another account YF5GJTPPMOUPU2GRGGVP2PGJTQZWGSWZISFHNIKDJSZ2CDPPWN4KKKYVQE
// opted in into the same app 22045503. During the commit range the following happened:
// at 16541783 YF5 made a payment txn (one acct delta)
// at 16541785 RGJ has been funded and and opted in into app 22045503 (one acct delta, one res delta)
// at 16541788 YF5 address had clear state txn for 22045503, and close out txn for the entire account (one acct delta, one res delta)
// Because YF5 had modifications before RGJ, all its acct deltas were compacted into a single entry before RGJ (delete, create)
// In the same time, the order in resources delta remained the same (opt-in, delete).
// While processing acct deltas (delete, create) SQLite reused on old rowid for new account.
// Then this rowid was discovered as addrid for opt-in operation and the "UNIQUE constraint failed" error happened.
func TestAccountUnorderedUpdates(t *testing.T) {
	partitiontest.PartitionTest(t)
	a := require.New(t)

	mock := makeMockAccountWriter()
	addr1 := ledgertesting.RandomAddress()
	addr2 := ledgertesting.RandomAddress()
	observer := ledgertesting.RandomAddress()
	aidx := basics.AppIndex(22045503)

	// set a base state: fund couple accounts, create an app and opt-in
	mock.setAccount(observer, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 10000000}, TotalAppParams: 1}})
	err := mock.setResource(observer, basics.CreatableIndex(aidx), ledgercore.AccountResource{AppParams: &basics.AppParams{ApprovalProgram: []byte{1, 2, 3}}})
	a.NoError(err)
	mock.setAccount(addr1, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 10000000}, TotalAppLocalStates: 1}})
	err = mock.setResource(addr1, basics.CreatableIndex(aidx), ledgercore.AccountResource{AppLocalState: &basics.AppLocalState{Schema: basics.StateSchema{NumUint: 10}}})
	a.NoError(err)

	updates := make([]ledgercore.StateDelta, 4)
	// payment addr1 -> observer
	updates[0].Accts.Upsert(addr1, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 9000000}, TotalAppLocalStates: 1}})
	updates[0].Accts.Upsert(observer, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 11000000}, TotalAppParams: 1}})

	// fund addr2, opt-in
	updates[1].Accts.Upsert(observer, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 10000000}, TotalAppParams: 1}})
	updates[1].Accts.Upsert(addr2, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 1000000}, TotalAppLocalStates: 1}})
	updates[1].Accts.UpsertAppResource(addr2, aidx, ledgercore.AppParamsDelta{}, ledgercore.AppLocalStateDelta{LocalState: &basics.AppLocalState{Schema: basics.StateSchema{NumUint: 10}}})

	// close addr1: delete app, move funds
	updates[2].Accts.UpsertAppResource(addr1, aidx, ledgercore.AppParamsDelta{}, ledgercore.AppLocalStateDelta{Deleted: true})
	updates[2].Accts.Upsert(observer, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 19000000}, TotalAppParams: 1}})
	updates[2].Accts.Upsert(addr1, ledgercore.AccountData{})

	// this is not required but adds one more resource entry and helps in combinations testing
	// update the app
	updates[3].Accts.UpsertAppResource(observer, aidx, ledgercore.AppParamsDelta{Params: &basics.AppParams{ApprovalProgram: []byte{4, 5, 6}}}, ledgercore.AppLocalStateDelta{})

	dbRound := basics.Round(16541781)
	latestRound := basics.Round(16541801)

	// we want to have all accounts to be found: addr1, observer existed, and addr2 non-existed
	// this would have compact deltas current and without missing entries
	var baseAccounts lruAccounts
	baseAccounts.init(nil, 100, 80)

	pad, ok, err := mock.Lookup(addr1)
	a.NoError(err)
	a.True(ok)
	baseAccounts.write(pad)
	pad, ok, err = mock.Lookup(observer)
	a.NoError(err)
	a.True(ok)
	baseAccounts.write(pad)
	baseAccounts.write(store.PersistedAccountData{Addr: addr2})

	acctDeltas := makeCompactAccountDeltas(updates, dbRound, false, baseAccounts)
	a.Empty(acctDeltas.misses)
	a.Equal(3, acctDeltas.len())

	// we want to have (addr1, aidx) and (observer, aidx)
	var baseResources lruResources
	baseResources.init(nil, 100, 80)

	prd, ok, err := mock.LookupResource(addr1, basics.CreatableIndex(aidx))
	a.NoError(err)
	a.True(ok)
	baseResources.write(prd, addr1)
	prd, ok, err = mock.LookupResource(observer, basics.CreatableIndex(aidx))
	a.NoError(err)
	a.True(ok)
	baseResources.write(prd, observer)

	resDeltas := makeCompactResourceDeltas(updates, dbRound, false, baseAccounts, baseResources)
	a.Equal(1, len(resDeltas.misses)) // (addr2, aidx) does not exist
	a.Equal(3, resDeltas.len())       // (addr1, aidx), (observer, aidx) found

	acctVariants := compactAccountDeltasPermutations(a, acctDeltas)
	resVariants := compactResourcesDeltasPermutations(a, resDeltas)
	for i, acctVariant := range acctVariants {
		for j, resVariant := range resVariants {
			t.Run(fmt.Sprintf("acct-perm-%d|res-perm-%d", i, j), func(t *testing.T) {
				a := require.New(t)
				mock2 := mock.clone()
				updatedAccounts, updatedResources, updatedKVs, err := accountsNewRoundImpl(
					&mock2, acctVariant, resVariant, nil, nil, config.ConsensusParams{}, latestRound,
				)
				a.NoError(err)
				a.Len(updatedAccounts, 3)
				a.Len(updatedResources, 3)
				a.Empty(updatedKVs)
			})
		}
	}
}

// TestAccountsNewRoundDeletedResourceEntries checks that accountsNewRound
// returns updated entries with empty addrid as an indication of deleted entry
func TestAccountsNewRoundDeletedResourceEntries(t *testing.T) {
	partitiontest.PartitionTest(t)
	a := require.New(t)

	mock := makeMockAccountWriter()
	addr1 := ledgertesting.RandomAddress()
	addr2 := ledgertesting.RandomAddress()
	observer := ledgertesting.RandomAddress()
	aidx := basics.AppIndex(22045503)

	// set a base state: fund couple accounts, create an app and opt-in
	mock.setAccount(observer, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 10000000}, TotalAppParams: 1}})
	err := mock.setResource(observer, basics.CreatableIndex(aidx), ledgercore.AccountResource{AppParams: &basics.AppParams{ApprovalProgram: []byte{1, 2, 3}}})
	a.NoError(err)
	mock.setAccount(addr1, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 10000000}, TotalAppLocalStates: 1}})
	err = mock.setResource(addr1, basics.CreatableIndex(aidx), ledgercore.AccountResource{AppLocalState: &basics.AppLocalState{Schema: basics.StateSchema{NumUint: 10}}})
	a.NoError(err)

	updates := make([]ledgercore.StateDelta, 3)
	// fund addr2, opt-in, delete app, move funds
	updates[0].Accts.Upsert(addr2, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 1000000}, TotalAppLocalStates: 1}})
	updates[0].Accts.UpsertAppResource(addr2, aidx, ledgercore.AppParamsDelta{}, ledgercore.AppLocalStateDelta{LocalState: &basics.AppLocalState{Schema: basics.StateSchema{NumUint: 10}}})

	// close addr1: delete app, move funds
	updates[1].Accts.UpsertAppResource(addr1, aidx, ledgercore.AppParamsDelta{}, ledgercore.AppLocalStateDelta{Deleted: true})
	updates[1].Accts.Upsert(observer, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 20000000}, TotalAppParams: 1}})
	updates[1].Accts.Upsert(addr1, ledgercore.AccountData{})

	// close addr2: delete app, move funds
	updates[2].Accts.UpsertAppResource(addr2, aidx, ledgercore.AppParamsDelta{}, ledgercore.AppLocalStateDelta{Deleted: true})
	updates[2].Accts.Upsert(observer, ledgercore.AccountData{AccountBaseData: ledgercore.AccountBaseData{MicroAlgos: basics.MicroAlgos{Raw: 11000000}, TotalAppParams: 1}})
	updates[2].Accts.Upsert(addr2, ledgercore.AccountData{})

	dbRound := basics.Round(1)
	latestRound := basics.Round(10)

	var baseAccounts lruAccounts
	baseAccounts.init(nil, 100, 80)
	var baseResources lruResources
	baseResources.init(nil, 100, 80)

	pad, ok, err := mock.Lookup(addr1)
	a.NoError(err)
	a.True(ok)
	baseAccounts.write(pad)
	pad, ok, err = mock.Lookup(observer)
	a.NoError(err)
	a.True(ok)
	baseAccounts.write(pad)
	baseAccounts.write(store.PersistedAccountData{Addr: addr2}) // put an empty record for addr2 to get rid of lookups

	acctDeltas := makeCompactAccountDeltas(updates, dbRound, false, baseAccounts)
	a.Empty(acctDeltas.misses)
	a.Equal(3, acctDeltas.len())

	// we want to have (addr1, aidx) and (observer, aidx)
	prd, ok, err := mock.LookupResource(addr1, basics.CreatableIndex(aidx))
	a.NoError(err)
	a.True(ok)
	baseResources.write(prd, addr1)
	prd, ok, err = mock.LookupResource(observer, basics.CreatableIndex(aidx))
	a.NoError(err)
	a.True(ok)
	baseResources.write(prd, observer)

	resDeltas := makeCompactResourceDeltas(updates, dbRound, false, baseAccounts, baseResources)
	a.Equal(1, len(resDeltas.misses)) // (addr2, aidx) does not exist
	a.Equal(2, resDeltas.len())       // (addr1, aidx) found

	updatedAccounts, updatedResources, updatedKVs, err := accountsNewRoundImpl(
		&mock, acctDeltas, resDeltas, nil, nil, config.ConsensusParams{}, latestRound,
	)
	a.NoError(err)
	a.Equal(3, len(updatedAccounts))
	a.Equal(2, len(updatedResources))
	a.Equal(0, len(updatedKVs))

	// one deletion entry for pre-existing account addr1, and one entry for in-memory account addr2
	// in base accounts updates and in resources updates
	addressesToCheck := map[basics.Address]bool{addr1: true, addr2: true}
	matches := 0
	for _, upd := range updatedAccounts {
		if addressesToCheck[upd.Addr] {
			a.Equal(int64(0), upd.Rowid)
			a.Empty(upd.AccountData)
			matches++
		}
	}
	a.Equal(len(addressesToCheck), matches)

	for addr := range addressesToCheck {
		upd := updatedResources[addr]
		a.Equal(1, len(upd))
		a.Equal(int64(0), upd[0].Addrid)
		a.Equal(basics.CreatableIndex(aidx), upd[0].Aidx)
		a.Equal(store.MakeResourcesData(uint64(0)), upd[0].Data)
	}
}

func BenchmarkLRUResources(b *testing.B) {
	var baseResources lruResources
	baseResources.init(nil, 1000, 850)

	var data store.PersistedResourcesData
	var has bool
	addrs := make([]basics.Address, 850)
	for i := 0; i < 850; i++ {
		data.Data.ApprovalProgram = make([]byte, 8096*4)
		data.Aidx = basics.CreatableIndex(1)
		addrBytes := ([]byte(fmt.Sprintf("%d", i)))[:32]
		var addr basics.Address
		for i, b := range addrBytes {
			addr[i] = b
		}
		addrs[i] = addr
		baseResources.write(data, addr)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := i % 850
		data, has = baseResources.read(addrs[pos], basics.CreatableIndex(1))
		require.True(b, has)
	}
}

func initBoxDatabase(b *testing.B, totalBoxes, boxSize int) (db.Pair, func(), error) {
	batchCount := 100
	if batchCount > totalBoxes {
		batchCount = 1
	}

	proto := config.Consensus[protocol.ConsensusCurrentVersion]
	dbs, fn := storetesting.DbOpenTest(b, false)
	storetesting.SetDbLogging(b, dbs)
	cleanup := func() {
		cleanupTestDb(dbs, fn, false)
	}

	tx, err := dbs.Wdb.Handle.Begin()
	require.NoError(b, err)
	_, err = store.AccountsInitLightTest(b, tx, make(map[basics.Address]basics.AccountData), proto)
	require.NoError(b, err)
	err = tx.Commit()
	require.NoError(b, err)
	err = dbs.Wdb.SetSynchronousMode(context.Background(), db.SynchronousModeOff, false)
	require.NoError(b, err)

	cnt := 0
	for batch := 0; batch <= batchCount; batch++ {
		tx, err = dbs.Wdb.Handle.Begin()
		require.NoError(b, err)
		writer, err := store.MakeAccountsSQLWriter(tx, false, false, true, false)
		require.NoError(b, err)
		for boxIdx := 0; boxIdx < totalBoxes/batchCount; boxIdx++ {
			err = writer.UpsertKvPair(fmt.Sprintf("%d", cnt), make([]byte, boxSize))
			require.NoError(b, err)
			cnt++
		}

		err = tx.Commit()
		require.NoError(b, err)
		writer.Close()
	}
	err = dbs.Wdb.SetSynchronousMode(context.Background(), db.SynchronousModeFull, true)
	return dbs, cleanup, err
}

func BenchmarkBoxDatabaseRead(b *testing.B) {
	getBoxNamePermutation := func(totalBoxes int) []int {
		rand.Seed(time.Now().UnixNano())
		boxNames := make([]int, totalBoxes)
		for i := 0; i < totalBoxes; i++ {
			boxNames[i] = i
		}
		rand.Shuffle(len(boxNames), func(x, y int) { boxNames[x], boxNames[y] = boxNames[y], boxNames[x] })
		return boxNames
	}

	boxCnt := []int{10, 1000, 100000}
	boxSizes := []int{2, 2048, 4 * 8096}
	for _, totalBoxes := range boxCnt {
		for _, boxSize := range boxSizes {
			b.Run(fmt.Sprintf("totalBoxes=%d/boxSize=%d", totalBoxes, boxSize), func(b *testing.B) {
				b.StopTimer()

				dbs, cleanup, err := initBoxDatabase(b, totalBoxes, boxSize)
				require.NoError(b, err)

				boxNames := getBoxNamePermutation(totalBoxes)
				lookupStmt, err := dbs.Wdb.Handle.Prepare("SELECT rnd, value FROM acctrounds LEFT JOIN kvstore ON key = ? WHERE id='acctbase';")
				require.NoError(b, err)
				var v sql.NullString
				for i := 0; i < b.N; i++ {
					var pv store.PersistedKVData
					boxName := boxNames[i%totalBoxes]
					b.StartTimer()
					err = lookupStmt.QueryRow([]byte(fmt.Sprintf("%d", boxName))).Scan(&pv.Round, &v)
					b.StopTimer()
					require.NoError(b, err)
					require.True(b, v.Valid)
				}

				cleanup()
			})
		}
	}

	// test caching performance
	lookbacks := []int{1, 32, 256, 2048}
	for _, lookback := range lookbacks {
		for _, boxSize := range boxSizes {
			totalBoxes := 100000

			b.Run(fmt.Sprintf("lookback=%d/boxSize=%d", lookback, boxSize), func(b *testing.B) {
				b.StopTimer()

				dbs, cleanup, err := initBoxDatabase(b, totalBoxes, boxSize)
				require.NoError(b, err)

				boxNames := getBoxNamePermutation(totalBoxes)
				lookupStmt, err := dbs.Wdb.Handle.Prepare("SELECT rnd, value FROM acctrounds LEFT JOIN kvstore ON key = ? WHERE id='acctbase';")
				require.NoError(b, err)
				var v sql.NullString
				for i := 0; i < b.N+lookback; i++ {
					var pv store.PersistedKVData
					boxName := boxNames[i%totalBoxes]
					err = lookupStmt.QueryRow([]byte(fmt.Sprintf("%d", boxName))).Scan(&pv.Round, &v)
					require.NoError(b, err)
					require.True(b, v.Valid)

					// benchmark reading the potentially cached value that was read lookback boxes ago
					if i >= lookback {
						boxName = boxNames[(i-lookback)%totalBoxes]
						b.StartTimer()
						err = lookupStmt.QueryRow([]byte(fmt.Sprintf("%d", boxName))).Scan(&pv.Round, &v)
						b.StopTimer()
						require.NoError(b, err)
						require.True(b, v.Valid)
					}
				}

				cleanup()
			})
		}
	}
}

// TestAccountTopOnline ensures accountsOnlineTop return a right subset of accounts
// from the history table.
// Start with two online accounts A, B at round 1
// At round 2 make A offline.
// At round 3 make B offline and add a new online account C.
//
// addr | rnd | status
// -----|-----|--------
// A    |  1  |    1
// B    |  1  |    1
// A    |  2  |    0
// B    |  3  |    0
// C    |  3  |    1
//
// Ensure
// - for round 1 A and B returned
// - for round 2 only B returned
// - for round 3 only C returned
// The test also checks accountsDbQueries.lookupOnline
func TestAccountOnlineQueries(t *testing.T) {
	partitiontest.PartitionTest(t)

	proto := config.Consensus[protocol.ConsensusCurrentVersion]

	dbs, _ := storetesting.DbOpenTest(t, true)
	storetesting.SetDbLogging(t, dbs)
	defer dbs.Close()

	tx, err := dbs.Wdb.Handle.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	arw := store.NewAccountsSQLReaderWriter(tx)

	var accts map[basics.Address]basics.AccountData
	store.AccountsInitTest(t, tx, accts, protocol.ConsensusCurrentVersion)
	totals, err := arw.AccountsTotals(context.Background(), false)
	require.NoError(t, err)

	var baseAccounts lruAccounts
	var baseResources lruResources
	var baseOnlineAccounts lruOnlineAccounts
	baseAccounts.init(nil, 100, 80)
	baseResources.init(nil, 100, 80)
	baseOnlineAccounts.init(nil, 100, 80)

	addrA := basics.Address(crypto.Hash([]byte("A")))
	addrB := basics.Address(crypto.Hash([]byte("B")))
	addrC := basics.Address(crypto.Hash([]byte("C")))

	var voteIDA crypto.OneTimeSignatureVerifier
	crypto.RandBytes(voteIDA[:])
	var voteIDB crypto.OneTimeSignatureVerifier
	crypto.RandBytes(voteIDB[:])
	var voteIDC crypto.OneTimeSignatureVerifier
	crypto.RandBytes(voteIDC[:])

	dataA1 := ledgercore.AccountData{
		AccountBaseData: ledgercore.AccountBaseData{
			MicroAlgos: basics.MicroAlgos{Raw: 100_000_000},
			Status:     basics.Online,
		},
		VotingData: ledgercore.VotingData{
			VoteID: voteIDA,
		},
	}

	dataB1 := ledgercore.AccountData{
		AccountBaseData: ledgercore.AccountBaseData{
			MicroAlgos: basics.MicroAlgos{Raw: 200_000_000},
			Status:     basics.Online,
		},
		VotingData: ledgercore.VotingData{
			VoteID: voteIDB,
		},
	}

	dataC3 := ledgercore.AccountData{
		AccountBaseData: ledgercore.AccountBaseData{
			MicroAlgos: basics.MicroAlgos{Raw: 300_000_000},
			Status:     basics.Online,
		},
		VotingData: ledgercore.VotingData{
			VoteID: voteIDC,
		},
	}

	dataA2 := dataA1
	dataA2.Status = basics.Offline
	dataA2.VoteID = crypto.OneTimeSignatureVerifier{}

	dataB2 := dataB1
	dataB2.Status = basics.Offline
	dataB2.VoteID = crypto.OneTimeSignatureVerifier{}

	delta1 := ledgercore.AccountDeltas{}
	delta1.Upsert(addrA, dataA1)
	delta1.Upsert(addrB, dataB1)

	delta2 := ledgercore.AccountDeltas{}
	delta2.Upsert(addrA, dataA2)

	delta3 := ledgercore.AccountDeltas{}
	delta3.Upsert(addrB, dataB2)
	delta3.Upsert(addrC, dataC3)

	addRound := func(rnd basics.Round, updates ledgercore.StateDelta) {
		totals = ledgertesting.CalculateNewRoundAccountTotals(t, updates.Accts, 0, proto, accts, totals)
		accts = applyPartialDeltas(accts, updates.Accts)

		oldBase := rnd - 1
		updatesCnt := makeCompactAccountDeltas([]ledgercore.StateDelta{updates}, oldBase, true, baseAccounts)
		updatesOnlineCnt := makeCompactOnlineAccountDeltas([]ledgercore.AccountDeltas{updates.Accts}, oldBase, baseOnlineAccounts)

		err = updatesCnt.accountsLoadOld(tx)
		require.NoError(t, err)

		err = updatesOnlineCnt.accountsLoadOld(tx)
		require.NoError(t, err)

		err = arw.AccountsPutTotals(totals, false)
		require.NoError(t, err)
		updatedAccts, _, _, err := accountsNewRound(tx, updatesCnt, compactResourcesDeltas{}, nil, nil, proto, rnd)
		require.NoError(t, err)
		require.Equal(t, updatesCnt.len(), len(updatedAccts))

		updatedOnlineAccts, err := onlineAccountsNewRound(tx, updatesOnlineCnt, proto, rnd)
		require.NoError(t, err)
		require.NotEmpty(t, updatedOnlineAccts)

		err = arw.UpdateAccountsRound(rnd)
		require.NoError(t, err)
	}

	addRound(1, ledgercore.StateDelta{Accts: delta1})
	addRound(2, ledgercore.StateDelta{Accts: delta2})
	addRound(3, ledgercore.StateDelta{Accts: delta3})

	queries, err := store.OnlineAccountsInitDbQueries(tx)
	require.NoError(t, err)

	// check round 1
	rnd := basics.Round(1)
	online, err := arw.AccountsOnlineTop(rnd, 0, 10, proto)
	require.NoError(t, err)
	require.Equal(t, 2, len(online))
	require.NotContains(t, online, addrC)

	onlineAcctA, ok := online[addrA]
	require.True(t, ok)
	require.NotNil(t, onlineAcctA)
	require.Equal(t, addrA, onlineAcctA.Address)
	require.Equal(t, dataA1.AccountBaseData.MicroAlgos, onlineAcctA.MicroAlgos)

	onlineAcctB, ok := online[addrB]
	require.True(t, ok)
	require.NotNil(t, onlineAcctB)
	require.Equal(t, addrB, onlineAcctB.Address)
	require.Equal(t, dataB1.AccountBaseData.MicroAlgos, onlineAcctB.MicroAlgos)

	paod, err := queries.LookupOnline(addrA, rnd)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), paod.Round)
	require.Equal(t, addrA, paod.Addr)
	require.Equal(t, dataA1.AccountBaseData.MicroAlgos, paod.AccountData.MicroAlgos)
	require.Equal(t, voteIDA, paod.AccountData.VoteID)

	paod, err = queries.LookupOnline(addrB, rnd)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), paod.Round)
	require.Equal(t, addrB, paod.Addr)
	require.Equal(t, dataB1.AccountBaseData.MicroAlgos, paod.AccountData.MicroAlgos)
	require.Equal(t, voteIDB, paod.AccountData.VoteID)

	paod, err = queries.LookupOnline(addrC, rnd)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), paod.Round)
	require.Equal(t, addrC, paod.Addr)
	require.Empty(t, paod.AccountData)

	// check round 2
	rnd = basics.Round(2)
	online, err = arw.AccountsOnlineTop(rnd, 0, 10, proto)
	require.NoError(t, err)
	require.Equal(t, 1, len(online))
	require.NotContains(t, online, addrA)
	require.NotContains(t, online, addrC)

	onlineAcctB, ok = online[addrB]
	require.True(t, ok)
	require.NotNil(t, onlineAcctB)
	require.Equal(t, addrB, onlineAcctB.Address)
	require.Equal(t, dataB1.AccountBaseData.MicroAlgos, onlineAcctB.MicroAlgos)

	paod, err = queries.LookupOnline(addrA, rnd)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), paod.Round)
	require.Equal(t, addrA, paod.Addr)
	require.Empty(t, paod.AccountData)

	paod, err = queries.LookupOnline(addrB, rnd)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), paod.Round)
	require.Equal(t, addrB, paod.Addr)
	require.Equal(t, dataB1.AccountBaseData.MicroAlgos, paod.AccountData.MicroAlgos)
	require.Equal(t, voteIDB, paod.AccountData.VoteID)

	paod, err = queries.LookupOnline(addrC, rnd)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), paod.Round)
	require.Equal(t, addrC, paod.Addr)
	require.Empty(t, paod.AccountData)

	// check round 3
	rnd = basics.Round(3)
	online, err = arw.AccountsOnlineTop(rnd, 0, 10, proto)
	require.NoError(t, err)
	require.Equal(t, 1, len(online))
	require.NotContains(t, online, addrA)
	require.NotContains(t, online, addrB)

	onlineAcctC, ok := online[addrC]
	require.True(t, ok)
	require.NotNil(t, onlineAcctC)
	require.Equal(t, addrC, onlineAcctC.Address)
	require.Equal(t, dataC3.AccountBaseData.MicroAlgos, onlineAcctC.MicroAlgos)

	paod, err = queries.LookupOnline(addrA, rnd)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), paod.Round)
	require.Equal(t, addrA, paod.Addr)
	require.Empty(t, paod.AccountData)

	paod, err = queries.LookupOnline(addrB, rnd)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), paod.Round)
	require.Equal(t, addrB, paod.Addr)
	require.Empty(t, paod.AccountData)

	paod, err = queries.LookupOnline(addrC, rnd)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), paod.Round)
	require.Equal(t, addrC, paod.Addr)
	require.Equal(t, dataC3.AccountBaseData.MicroAlgos, paod.AccountData.MicroAlgos)
	require.Equal(t, voteIDC, paod.AccountData.VoteID)

	paods, err := arw.OnlineAccountsAll(0)
	require.NoError(t, err)
	require.Equal(t, 5, len(paods))

	// expect:
	//
	// addr | rnd | status
	// -----|-----|--------
	//    B |   1 |      1
	//    B |   3 |      0
	//    C |   3 |      1
	//    A |   1 |      1
	//    A |   2 |      0

	checkAddrB := func() {
		require.Equal(t, int64(2), paods[0].Rowid)
		require.Equal(t, basics.Round(1), paods[0].UpdRound)
		require.Equal(t, addrB, paods[0].Addr)
		require.Equal(t, int64(4), paods[1].Rowid)
		require.Equal(t, basics.Round(3), paods[1].UpdRound)
		require.Equal(t, addrB, paods[1].Addr)
	}

	checkAddrC := func() {
		require.Equal(t, int64(5), paods[2].Rowid)
		require.Equal(t, basics.Round(3), paods[2].UpdRound)
		require.Equal(t, addrC, paods[2].Addr)
	}

	checkAddrA := func() {
		require.Equal(t, int64(1), paods[3].Rowid)
		require.Equal(t, basics.Round(1), paods[3].UpdRound)
		require.Equal(t, addrA, paods[3].Addr)
		require.Equal(t, int64(3), paods[4].Rowid)
		require.Equal(t, basics.Round(2), paods[4].UpdRound)
		require.Equal(t, addrA, paods[4].Addr)
	}

	checkAddrB()
	checkAddrC()
	checkAddrA()

	paods, err = arw.OnlineAccountsAll(3)
	require.NoError(t, err)
	require.Equal(t, 5, len(paods))
	checkAddrB()
	checkAddrC()
	checkAddrA()

	paods, err = arw.OnlineAccountsAll(2)
	require.NoError(t, err)
	require.Equal(t, 3, len(paods))
	checkAddrB()
	checkAddrC()

	paods, err = arw.OnlineAccountsAll(1)
	require.NoError(t, err)
	require.Equal(t, 2, len(paods))
	checkAddrB()

	paods, rnd, err = queries.LookupOnlineHistory(addrA)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), rnd)
	require.Equal(t, 2, len(paods))
	require.Equal(t, int64(1), paods[0].Rowid)
	require.Equal(t, basics.Round(1), paods[0].UpdRound)
	require.Equal(t, int64(3), paods[1].Rowid)
	require.Equal(t, basics.Round(2), paods[1].UpdRound)

	paods, rnd, err = queries.LookupOnlineHistory(addrB)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), rnd)
	require.Equal(t, 2, len(paods))
	require.Equal(t, int64(2), paods[0].Rowid)
	require.Equal(t, basics.Round(1), paods[0].UpdRound)
	require.Equal(t, int64(4), paods[1].Rowid)
	require.Equal(t, basics.Round(3), paods[1].UpdRound)

	paods, rnd, err = queries.LookupOnlineHistory(addrC)
	require.NoError(t, err)
	require.Equal(t, basics.Round(3), rnd)
	require.Equal(t, 1, len(paods))
	require.Equal(t, int64(5), paods[0].Rowid)
	require.Equal(t, basics.Round(3), paods[0].UpdRound)
}

type mockOnlineAccountsWriter struct {
	rowid int64
}

func (w *mockOnlineAccountsWriter) InsertOnlineAccount(addr basics.Address, normBalance uint64, data store.BaseOnlineAccountData, updRound uint64, voteLastValid uint64) (rowid int64, err error) {
	w.rowid++
	return w.rowid, nil
}

func (w *mockOnlineAccountsWriter) Close() {}

func TestAccountOnlineAccountsNewRound(t *testing.T) {
	partitiontest.PartitionTest(t)

	proto := config.Consensus[protocol.ConsensusCurrentVersion]
	writer := &mockOnlineAccountsWriter{rowid: 100}

	updates := compactOnlineAccountDeltas{}
	addrA := ledgertesting.RandomAddress()
	addrB := ledgertesting.RandomAddress()
	addrC := ledgertesting.RandomAddress()
	addrD := ledgertesting.RandomAddress()
	addrE := ledgertesting.RandomAddress()

	// acct A is empty
	deltaA := onlineAccountDelta{
		address: addrA,
	}
	// acct B is new and offline
	deltaB := onlineAccountDelta{
		address: addrB,
		newAcct: []store.BaseOnlineAccountData{{
			MicroAlgos: basics.MicroAlgos{Raw: 200_000_000},
		}},
		updRound:  []uint64{1},
		newStatus: []basics.Status{basics.Offline},
	}
	// acct C is new and online
	deltaC := onlineAccountDelta{
		address: addrC,
		newAcct: []store.BaseOnlineAccountData{{
			MicroAlgos:     basics.MicroAlgos{Raw: 300_000_000},
			BaseVotingData: store.BaseVotingData{VoteFirstValid: 500},
		}},
		newStatus: []basics.Status{basics.Online},
		updRound:  []uint64{2},
	}
	// acct D is old and went offline
	deltaD := onlineAccountDelta{
		address: addrD,
		oldAcct: store.PersistedOnlineAccountData{
			Addr: addrD,
			AccountData: store.BaseOnlineAccountData{
				MicroAlgos:     basics.MicroAlgos{Raw: 400_000_000},
				BaseVotingData: store.BaseVotingData{VoteFirstValid: 500},
			},
			Rowid: 1,
		},
		newAcct: []store.BaseOnlineAccountData{{
			MicroAlgos: basics.MicroAlgos{Raw: 400_000_000},
		}},
		newStatus: []basics.Status{basics.Offline},
		updRound:  []uint64{3},
	}

	// acct E is old online
	deltaE := onlineAccountDelta{
		address: addrE,
		oldAcct: store.PersistedOnlineAccountData{
			Addr: addrE,
			AccountData: store.BaseOnlineAccountData{
				MicroAlgos:     basics.MicroAlgos{Raw: 500_000_000},
				BaseVotingData: store.BaseVotingData{VoteFirstValid: 500},
			},
			Rowid: 2,
		},
		newAcct: []store.BaseOnlineAccountData{{
			MicroAlgos:     basics.MicroAlgos{Raw: 500_000_000},
			BaseVotingData: store.BaseVotingData{VoteFirstValid: 600},
		}},
		newStatus: []basics.Status{basics.Online},
		updRound:  []uint64{4},
	}

	updates.deltas = append(updates.deltas, deltaA, deltaB, deltaC, deltaD, deltaE)
	lastUpdateRound := basics.Round(1)
	updated, err := onlineAccountsNewRoundImpl(writer, updates, proto, lastUpdateRound)
	require.NoError(t, err)

	require.Len(t, updated, 3)
	require.Equal(t, updated[0].Addr, addrC)
	require.Equal(t, updated[1].Addr, addrD)
	require.Equal(t, updated[2].Addr, addrE)

	// check errors: new online with empty voting data
	deltaC.newStatus[0] = basics.Online
	deltaC.newAcct[0].VoteFirstValid = 0
	updates.deltas = []onlineAccountDelta{deltaC}
	_, err = onlineAccountsNewRoundImpl(writer, updates, proto, lastUpdateRound)
	require.Error(t, err)

	// check errors: new non-online with non-empty voting data
	deltaB.newStatus[0] = basics.Offline
	deltaB.newAcct[0].VoteFirstValid = 1
	updates.deltas = []onlineAccountDelta{deltaB}
	_, err = onlineAccountsNewRoundImpl(writer, updates, proto, lastUpdateRound)
	require.Error(t, err)

	// check errors: new online with empty voting data
	deltaD.newStatus[0] = basics.Online
	updates.deltas = []onlineAccountDelta{deltaD}
	_, err = onlineAccountsNewRoundImpl(writer, updates, proto, lastUpdateRound)
	require.Error(t, err)
}

func TestAccountOnlineAccountsNewRoundFlip(t *testing.T) {
	partitiontest.PartitionTest(t)

	proto := config.Consensus[protocol.ConsensusCurrentVersion]
	writer := &mockOnlineAccountsWriter{rowid: 100}

	updates := compactOnlineAccountDeltas{}
	addrA := ledgertesting.RandomAddress()
	addrB := ledgertesting.RandomAddress()
	addrC := ledgertesting.RandomAddress()

	// acct A is new, offline and then online
	deltaA := onlineAccountDelta{
		address: addrA,
		newAcct: []store.BaseOnlineAccountData{
			{
				MicroAlgos: basics.MicroAlgos{Raw: 100_000_000},
			},
			{
				MicroAlgos:     basics.MicroAlgos{Raw: 100_000_000},
				BaseVotingData: store.BaseVotingData{VoteFirstValid: 100},
			},
		},
		updRound:  []uint64{1, 2},
		newStatus: []basics.Status{basics.Offline, basics.Online},
	}
	// acct B is new and online and then offline
	deltaB := onlineAccountDelta{
		address: addrB,
		newAcct: []store.BaseOnlineAccountData{
			{
				MicroAlgos:     basics.MicroAlgos{Raw: 200_000_000},
				BaseVotingData: store.BaseVotingData{VoteFirstValid: 200},
			},
			{
				MicroAlgos: basics.MicroAlgos{Raw: 200_000_000},
			},
		},
		updRound:  []uint64{3, 4},
		newStatus: []basics.Status{basics.Online, basics.Offline},
	}
	// acct C is old online, then online and then offline
	deltaC := onlineAccountDelta{
		address: addrC,
		oldAcct: store.PersistedOnlineAccountData{
			Addr: addrC,
			AccountData: store.BaseOnlineAccountData{
				MicroAlgos:     basics.MicroAlgos{Raw: 300_000_000},
				BaseVotingData: store.BaseVotingData{VoteFirstValid: 300},
			},
			Rowid: 1,
		},
		newAcct: []store.BaseOnlineAccountData{
			{
				MicroAlgos:     basics.MicroAlgos{Raw: 300_000_000},
				BaseVotingData: store.BaseVotingData{VoteFirstValid: 301},
			},
			{
				MicroAlgos: basics.MicroAlgos{Raw: 300_000_000},
			},
		},
		newStatus: []basics.Status{basics.Online, basics.Offline},
		updRound:  []uint64{5, 6},
	}

	updates.deltas = append(updates.deltas, deltaA, deltaB, deltaC)
	lastUpdateRound := basics.Round(1)
	updated, err := onlineAccountsNewRoundImpl(writer, updates, proto, lastUpdateRound)
	require.NoError(t, err)

	require.Len(t, updated, 5)
	require.Equal(t, updated[0].Addr, addrA)
	require.Equal(t, updated[1].Addr, addrB)
	require.Equal(t, updated[2].Addr, addrB)
	require.Equal(t, updated[3].Addr, addrC)
	require.Equal(t, updated[4].Addr, addrC)
}

func TestAccountOnlineRoundParams(t *testing.T) {
	partitiontest.PartitionTest(t)

	dbs, _ := storetesting.DbOpenTest(t, true)
	storetesting.SetDbLogging(t, dbs)
	defer dbs.Close()

	tx, err := dbs.Wdb.Handle.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	arw := store.NewAccountsSQLReaderWriter(tx)

	var accts map[basics.Address]basics.AccountData
	store.AccountsInitTest(t, tx, accts, protocol.ConsensusCurrentVersion)

	// entry i is for round i+1 since db initialized with entry for round 0
	const maxRounds = 40 // any number
	onlineRoundParams := make([]ledgercore.OnlineRoundParamsData, maxRounds)
	for i := range onlineRoundParams {
		onlineRoundParams[i].OnlineSupply = uint64(i + 1)
		onlineRoundParams[i].CurrentProtocol = protocol.ConsensusCurrentVersion
		onlineRoundParams[i].RewardsLevel = uint64(i + 1)
	}

	err = arw.AccountsPutOnlineRoundParams(onlineRoundParams, 1)
	require.NoError(t, err)

	dbOnlineRoundParams, endRound, err := arw.AccountsOnlineRoundParams()
	require.NoError(t, err)
	require.Equal(t, maxRounds+1, len(dbOnlineRoundParams)) // +1 comes from init state
	require.Equal(t, onlineRoundParams, dbOnlineRoundParams[1:])
	require.Equal(t, maxRounds, int(endRound))

	err = arw.AccountsPruneOnlineRoundParams(10)
	require.NoError(t, err)

	dbOnlineRoundParams, endRound, err = arw.AccountsOnlineRoundParams()
	require.NoError(t, err)
	require.Equal(t, onlineRoundParams[9:], dbOnlineRoundParams)
	require.Equal(t, maxRounds, int(endRound))
}

// TestOnlineAccountsDeletion checks the onlineAccountsDelete preseves online accounts entries
// and deleted only expired offline and online rows
// Round    1   2   3   4   5   6   7
// Acct A  On     Off          On
// Acct B          On              On
// Expectations:
// onlineAccountsDelete(1): A online
// onlineAccountsDelete(2): A online
// onlineAccountsDelete(3): A offline, B online
// etc
func TestOnlineAccountsDeletion(t *testing.T) {
	partitiontest.PartitionTest(t)

	dbs, _ := storetesting.DbOpenTest(t, true)
	storetesting.SetDbLogging(t, dbs)
	defer dbs.Close()

	tx, err := dbs.Wdb.Handle.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	var accts map[basics.Address]basics.AccountData
	store.AccountsInitTest(t, tx, accts, protocol.ConsensusCurrentVersion)

	arw := store.NewAccountsSQLReaderWriter(tx)

	updates := compactOnlineAccountDeltas{}
	addrA := ledgertesting.RandomAddress()
	addrB := ledgertesting.RandomAddress()

	deltaA := onlineAccountDelta{
		address: addrA,
		newAcct: []store.BaseOnlineAccountData{
			{
				MicroAlgos:     basics.MicroAlgos{Raw: 100_000_000},
				BaseVotingData: store.BaseVotingData{VoteFirstValid: 100},
			},
			{
				MicroAlgos: basics.MicroAlgos{Raw: 100_000_000},
			},
			{
				MicroAlgos:     basics.MicroAlgos{Raw: 100_000_000},
				BaseVotingData: store.BaseVotingData{VoteFirstValid: 600},
			},
		},
		updRound:  []uint64{1, 3, 6},
		newStatus: []basics.Status{basics.Online, basics.Offline, basics.Online},
	}
	// acct B is new and online and then offline
	deltaB := onlineAccountDelta{
		address: addrB,
		newAcct: []store.BaseOnlineAccountData{
			{
				MicroAlgos:     basics.MicroAlgos{Raw: 200_000_000},
				BaseVotingData: store.BaseVotingData{VoteFirstValid: 300},
			},
			{
				MicroAlgos:     basics.MicroAlgos{Raw: 200_000_000},
				BaseVotingData: store.BaseVotingData{VoteFirstValid: 700},
			},
		},
		updRound:  []uint64{3, 7},
		newStatus: []basics.Status{basics.Online, basics.Online},
	}

	updates.deltas = append(updates.deltas, deltaA, deltaB)
	writer, err := store.MakeOnlineAccountsSQLWriter(tx, updates.len() > 0)
	if err != nil {
		return
	}
	defer writer.Close()

	lastUpdateRound := basics.Round(10)
	proto := config.Consensus[protocol.ConsensusCurrentVersion]
	updated, err := onlineAccountsNewRoundImpl(writer, updates, proto, lastUpdateRound)
	require.NoError(t, err)
	require.Len(t, updated, 5)

	queries, err := store.OnlineAccountsInitDbQueries(tx)
	require.NoError(t, err)

	var count int64
	var history []store.PersistedOnlineAccountData
	var validThrough basics.Round
	for _, rnd := range []basics.Round{1, 2, 3} {
		err = arw.OnlineAccountsDelete(rnd)
		require.NoError(t, err)

		err = tx.QueryRow("SELECT COUNT(1) FROM onlineaccounts").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, int64(5), count)

		history, validThrough, err = queries.LookupOnlineHistory(addrA)
		require.NoError(t, err)
		require.Equal(t, basics.Round(0), validThrough) // not set
		require.Len(t, history, 3)
		history, validThrough, err = queries.LookupOnlineHistory(addrB)
		require.NoError(t, err)
		require.Equal(t, basics.Round(0), validThrough)
		require.Len(t, history, 2)
	}

	for _, rnd := range []basics.Round{4, 5, 6, 7} {
		err = arw.OnlineAccountsDelete(rnd)
		require.NoError(t, err)

		err = tx.QueryRow("SELECT COUNT(1) FROM onlineaccounts").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, int64(3), count)

		history, validThrough, err = queries.LookupOnlineHistory(addrA)
		require.NoError(t, err)
		require.Equal(t, basics.Round(0), validThrough)
		require.Len(t, history, 1)
		history, validThrough, err = queries.LookupOnlineHistory(addrB)
		require.NoError(t, err)
		require.Equal(t, basics.Round(0), validThrough)
		require.Len(t, history, 2)
	}

	for _, rnd := range []basics.Round{8, 9} {
		err = arw.OnlineAccountsDelete(rnd)
		require.NoError(t, err)

		err = tx.QueryRow("SELECT COUNT(1) FROM onlineaccounts").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, int64(2), count)

		history, validThrough, err = queries.LookupOnlineHistory(addrA)
		require.NoError(t, err)
		require.Equal(t, basics.Round(0), validThrough)
		require.Len(t, history, 1)
		history, validThrough, err = queries.LookupOnlineHistory(addrB)
		require.NoError(t, err)
		require.Equal(t, basics.Round(0), validThrough)
		require.Len(t, history, 1)
	}
}

func randomBaseAccountData() store.BaseAccountData {
	vd := store.BaseVotingData{
		VoteFirstValid:  basics.Round(crypto.RandUint64()),
		VoteLastValid:   basics.Round(crypto.RandUint64()),
		VoteKeyDilution: crypto.RandUint64(),
	}
	crypto.RandBytes(vd.VoteID[:])
	crypto.RandBytes(vd.StateProofID[:])
	crypto.RandBytes(vd.SelectionID[:])

	baseAD := store.BaseAccountData{
		Status:                     basics.Online,
		MicroAlgos:                 basics.MicroAlgos{Raw: crypto.RandUint64()},
		RewardsBase:                crypto.RandUint64(),
		RewardedMicroAlgos:         basics.MicroAlgos{Raw: crypto.RandUint64()},
		AuthAddr:                   ledgertesting.RandomAddress(),
		TotalAppSchemaNumUint:      crypto.RandUint64(),
		TotalAppSchemaNumByteSlice: crypto.RandUint64(),
		TotalExtraAppPages:         uint32(crypto.RandUint63() % uint64(math.MaxUint32)),
		TotalAssetParams:           crypto.RandUint64(),
		TotalAssets:                crypto.RandUint64(),
		TotalAppParams:             crypto.RandUint64(),
		TotalAppLocalStates:        crypto.RandUint64(),
		BaseVotingData:             vd,
		UpdateRound:                crypto.RandUint64(),
	}

	return baseAD
}

func TestEncodedBaseAccountDataSize(t *testing.T) {
	partitiontest.PartitionTest(t)
	t.Parallel()

	baseAD := randomBaseAccountData()
	encoded := baseAD.MarshalMsg(nil)
	require.GreaterOrEqual(t, MaxEncodedBaseAccountDataSize, len(encoded))
}

func makeString(len int) string {
	s := ""
	for i := 0; i < len; i++ {
		s += string(byte(i))
	}
	return s
}

func randomAssetResourceData() store.ResourcesData {
	currentConsensusParams := config.Consensus[protocol.ConsensusCurrentVersion]

	// resourcesData is suiteable for keeping asset params, holding, app params, app local state
	// but only asset + holding or app + local state can appear there
	rdAsset := store.ResourcesData{
		Total:         crypto.RandUint64(),
		Decimals:      uint32(crypto.RandUint63() % uint64(math.MaxUint32)),
		DefaultFrozen: true,
		// MetadataHash
		UnitName:  makeString(currentConsensusParams.MaxAssetUnitNameBytes),
		AssetName: makeString(currentConsensusParams.MaxAssetNameBytes),
		URL:       makeString(currentConsensusParams.MaxAssetURLBytes),
		Manager:   ledgertesting.RandomAddress(),
		Reserve:   ledgertesting.RandomAddress(),
		Freeze:    ledgertesting.RandomAddress(),
		Clawback:  ledgertesting.RandomAddress(),

		Amount: crypto.RandUint64(),
		Frozen: true,
	}
	crypto.RandBytes(rdAsset.MetadataHash[:])

	return rdAsset
}

func randomAppResourceData() store.ResourcesData {
	currentConsensusParams := config.Consensus[protocol.ConsensusCurrentVersion]

	rdApp := store.ResourcesData{

		SchemaNumUint:      crypto.RandUint64(),
		SchemaNumByteSlice: crypto.RandUint64(),
		// KeyValue

		// ApprovalProgram
		// ClearStateProgram
		// GlobalState
		LocalStateSchemaNumUint:       crypto.RandUint64(),
		LocalStateSchemaNumByteSlice:  crypto.RandUint64(),
		GlobalStateSchemaNumUint:      crypto.RandUint64(),
		GlobalStateSchemaNumByteSlice: crypto.RandUint64(),
		ExtraProgramPages:             uint32(crypto.RandUint63() % uint64(math.MaxUint32)),

		ResourceFlags: 255,
		UpdateRound:   crypto.RandUint64(),
	}

	// MaxAvailableAppProgramLen is conbined size of approval and clear state since it is bound by proto.MaxAppTotalProgramLen
	rdApp.ApprovalProgram = make([]byte, config.MaxAvailableAppProgramLen/2)
	crypto.RandBytes(rdApp.ApprovalProgram)
	rdApp.ClearStateProgram = make([]byte, config.MaxAvailableAppProgramLen/2)
	crypto.RandBytes(rdApp.ClearStateProgram)

	maxGlobalState := make(basics.TealKeyValue, currentConsensusParams.MaxGlobalSchemaEntries)
	for globalKey := uint64(0); globalKey < currentConsensusParams.MaxGlobalSchemaEntries; globalKey++ {
		prefix := fmt.Sprintf("%d|", globalKey)
		padding := makeString(currentConsensusParams.MaxAppKeyLen - len(prefix))
		maxKey := prefix + padding
		maxValue := basics.TealValue{
			Type:  basics.TealBytesType,
			Bytes: makeString(currentConsensusParams.MaxAppSumKeyValueLens - len(maxKey)),
		}
		maxGlobalState[maxKey] = maxValue
	}

	maxLocalState := make(basics.TealKeyValue, currentConsensusParams.MaxLocalSchemaEntries)
	for localKey := uint64(0); localKey < currentConsensusParams.MaxLocalSchemaEntries; localKey++ {
		prefix := fmt.Sprintf("%d|", localKey)
		padding := makeString(currentConsensusParams.MaxAppKeyLen - len(prefix))
		maxKey := prefix + padding
		maxValue := basics.TealValue{
			Type:  basics.TealBytesType,
			Bytes: makeString(currentConsensusParams.MaxAppSumKeyValueLens - len(maxKey)),
		}
		maxLocalState[maxKey] = maxValue
	}

	rdApp.GlobalState = maxGlobalState
	rdApp.KeyValue = maxLocalState

	return rdApp
}

func TestEncodedBaseResourceSize(t *testing.T) {
	partitiontest.PartitionTest(t)
	t.Parallel()

	// resourcesData is suiteable for keeping asset params, holding, app params, app local state
	// but only asset + holding or app + local state can appear there
	rdAsset := randomAssetResourceData()
	rdApp := randomAppResourceData()

	encodedAsset := rdAsset.MarshalMsg(nil)
	encodedApp := rdApp.MarshalMsg(nil)

	require.Less(t, len(encodedAsset), len(encodedApp))
	require.GreaterOrEqual(t, MaxEncodedBaseResourceDataSize, len(encodedApp))
}
