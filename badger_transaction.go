/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package badgerstore

import (
	"github.com/keyvalstore/store"
	"github.com/dgraph-io/badger/v3"
)

type implBadgerTransaction struct {
	tx *badger.Txn
	readOnly bool
}

func NewTransaction(tx *badger.Txn, readOnly bool) store.Transaction {
	return &implBadgerTransaction{tx: tx, readOnly: readOnly}
}

func (t *implBadgerTransaction) ReadOnly() bool {
	return t.readOnly
}

func (t *implBadgerTransaction) Commit() error {
	err := t.tx.Commit()
	if err != nil {
		return wrapError(err)
	}
	return nil
}

func (t *implBadgerTransaction) Rollback() {
	t.tx.Discard()
}

func (t *implBadgerTransaction) Instance() interface{} {
	return t.tx
}
