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

package ledger

import (
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/ledger/store/trackerdb"
	"github.com/algorand/go-algorand/logging"
	"github.com/algorand/go-deadlock"
)

// lruAccounts provides a storage class for the most recently used accounts data.
// It doesn't have any synchronization primitive on its own and require to be
// synchronized by the caller.
type lruAccounts struct {
	mus []deadlock.RWMutex

	// accountsList contain the list of persistedAccountData, where the front ones are the most "fresh"
	// and the ones on the back are the oldest.
	accountsList []*persistedAccountDataList
	// accounts provides fast access to the various elements in the list by using the account address
	// if lruAccounts is set with pendingWrites 0, then accounts is nil
	accounts []map[basics.Address]*persistedAccountDataListNode
	// pendingAccounts are used as a way to avoid taking a write-lock. When the caller needs to "materialize" these,
	// it would call flushPendingWrites and these would be merged into the accounts/accountsList
	// if lruAccounts is set with pendingWrites 0, then pendingAccounts is nil
	pendingAccounts []chan trackerdb.PersistedAccountData
	// log interface; used for logging the threshold event.
	log logging.Logger
	// pendingWritesWarnThreshold is the threshold beyond we would write a warning for exceeding the number of pendingAccounts entries
	pendingWritesWarnThreshold int

	// if lruAccounts is set with pendingWrites 0, then pendingNotFound and notFound is nil
	pendingNotFound []chan basics.Address
	notFound        []map[basics.Address]struct{}
}

var buckets int = 256

// init initializes the lruAccounts for use.
// thread locking semantics : write lock
func (m *lruAccounts) init(log logging.Logger, pendingWrites int, pendingWritesWarnThreshold int) {
	// divide the address space into 256 buckets
	// this is done to reduce the contention on the locks
	m.mus = make([]deadlock.RWMutex, buckets)

	// equally divide the pendingWrites into the buckets
	// pendingWrites = pendingWrites / buckets

	if pendingWrites > 0 {
		m.mus = make([]deadlock.RWMutex, buckets)
		m.accountsList = make([]*persistedAccountDataList, buckets)
		m.accounts = make([]map[basics.Address]*persistedAccountDataListNode, buckets)
		m.pendingAccounts = make([]chan trackerdb.PersistedAccountData, buckets)
		m.notFound = make([]map[basics.Address]struct{}, buckets)
		m.pendingNotFound = make([]chan basics.Address, buckets)

		for i := 0; i < buckets; i++ {
			m.accountsList[i] = newPersistedAccountList().allocateFreeNodes(pendingWrites)
			m.accounts[i] = make(map[basics.Address]*persistedAccountDataListNode, pendingWrites)
			m.pendingAccounts[i] = make(chan trackerdb.PersistedAccountData, pendingWrites)
			m.notFound[i] = make(map[basics.Address]struct{}, pendingWrites)
			m.pendingNotFound[i] = make(chan basics.Address, pendingWrites)
		}
	}

	m.log = log
	m.pendingWritesWarnThreshold = pendingWritesWarnThreshold
}

func (m *lruAccounts) buckets() int {
	return buckets
}

func (m *lruAccounts) address_to_bucket(addr *basics.Address) int {
	// TODO: we need to handle special addresses like the sink separatly or that bucket will have perf issues

	// we should grab some hash of the address to avoid addresses with similar prefixes to end up in the same bucket
	// Note: this only works up to 256 buckets
	return int(addr[21]) % buckets
}

func (m *lruAccounts) Lock(addr *basics.Address) {
	i := m.address_to_bucket(addr)
	m.mus[i].Lock()
}

func (m *lruAccounts) Unlock(addr *basics.Address) {
	i := m.address_to_bucket(addr)
	m.mus[i].Unlock()
}

func (m *lruAccounts) RLock(addr *basics.Address) {
	i := m.address_to_bucket(addr)
	m.mus[i].RLock()
}

func (m *lruAccounts) RUnlock(addr *basics.Address) {
	i := m.address_to_bucket(addr)
	m.mus[i].RUnlock()
}

// read the persistedAccountData object that the lruAccounts has for the given address.
// thread locking semantics : read lock
func (m *lruAccounts) read(addr basics.Address) (data trackerdb.PersistedAccountData, has bool) {
	i := m.address_to_bucket(&addr)
	if el := m.accounts[i][addr]; el != nil {
		return *el.Value, true
	}
	return trackerdb.PersistedAccountData{}, false
}

// readNotFound returns whether we have attempted to read this address but it did not exist in the db.
// thread locking semantics : read lock
func (m *lruAccounts) readNotFound(addr basics.Address) bool {
	i := m.address_to_bucket(&addr)
	_, ok := m.notFound[i][addr]
	return ok
}

// flushPendingWrites flushes the pending writes to the main lruAccounts cache.
// thread locking semantics : write lock
func (m *lruAccounts) flushPendingWrites(hint *basics.Address, takeLock bool) {
	from := 0
	to := buckets
	// if we have a hint then we only flush the bucket that the hint is in
	if hint != nil {
		from = m.address_to_bucket(hint)
		to = from + 1
	}
	// flush the pending writes for each bucket
	for i := from; i < to; i++ {
		if takeLock {
			m.mus[i].Lock()
		}

		pendingEntriesCount := len(m.pendingAccounts[i])
		if pendingEntriesCount >= m.pendingWritesWarnThreshold {
			m.log.Warnf("lruAccounts: number of entries in pendingAccounts(%d) exceed the warning threshold of %d", pendingEntriesCount, m.pendingWritesWarnThreshold)
		}

	outer:
		for ; pendingEntriesCount > 0; pendingEntriesCount-- {
			select {
			case pendingAccountData := <-m.pendingAccounts[i]:
				m.write(pendingAccountData)
			default:
				break outer
			}
		}

		pendingEntriesCount = len(m.pendingNotFound[i])
	outer2:
		for ; pendingEntriesCount > 0; pendingEntriesCount-- {
			select {
			case addr := <-m.pendingNotFound[i]:
				m.writeNotFound(addr)
			default:
				break outer2
			}
		}

		if takeLock {
			m.mus[i].Unlock()
		}
	}
}

// writePending write a single persistedAccountData entry to the pendingAccounts buffer.
// the function doesn't block, and in case of a buffer overflow the entry would not be added.
// thread locking semantics : no lock is required.
func (m *lruAccounts) writePending(acct trackerdb.PersistedAccountData) {
	i := m.address_to_bucket(&acct.Addr)
	select {
	case m.pendingAccounts[i] <- acct:
	default:
	}
}

// writeNotFoundPending tags an address as not existing in the db.
// the function doesn't block, and in case of a buffer overflow the entry would not be added.
// thread locking semantics : no lock is required.
func (m *lruAccounts) writeNotFoundPending(addr basics.Address) {
	i := m.address_to_bucket(&addr)
	select {
	case m.pendingNotFound[i] <- addr:
	default:
	}
}

// write a single persistedAccountData to the lruAccounts cache.
// when writing the entry, the round number would be used to determine if it's a newer
// version of what's already on the cache or not. In all cases, the entry is going
// to be promoted to the front of the list.
// thread locking semantics : write lock
func (m *lruAccounts) write(acctData trackerdb.PersistedAccountData) {
	i := m.address_to_bucket(&acctData.Addr)
	if m.accounts[i] == nil {
		return
	}
	if el := m.accounts[i][acctData.Addr]; el != nil {
		// already exists; is it a newer ?
		if el.Value.Before(&acctData) {
			// we update with a newer version.
			el.Value = &acctData
		}
		m.accountsList[i].moveToFront(el)
	} else {
		// new entry.
		m.accounts[i][acctData.Addr] = m.accountsList[i].pushFront(&acctData)
	}
}

// write a single account as not found on the db
// thread locking semantics : write lock
func (m *lruAccounts) writeNotFound(addr basics.Address) {
	i := m.address_to_bucket(&addr)
	m.notFound[i][addr] = struct{}{}
}

// prune adjust the current size of the lruAccounts cache, by dropping the least
// recently used entries.
// thread locking semantics : write lock
func (m *lruAccounts) prune(newSize int, takeLock bool) (removed int) {
	for i := 0; i < buckets; i++ {
		if takeLock {
			m.mus[i].Lock()
		}

		if m.accounts[i] == nil {
			if takeLock {
				m.mus[i].Unlock()
			}
			continue
		}
		for {
			if len(m.accounts[i]) <= newSize {
				break
			}
			back := m.accountsList[i].back()
			delete(m.accounts[i], back.Value.Addr)
			m.accountsList[i].remove(back)
			removed++
		}

		// clear the notFound list
		m.notFound[i] = make(map[basics.Address]struct{}, len(m.notFound[i]))

		if takeLock {
			m.mus[i].Unlock()
		}
	}
	return
}
