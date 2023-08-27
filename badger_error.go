/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package badgerstore

import (
	"context"
	"github.com/keyvalstore/store"
	"github.com/dgraph-io/badger/v3"
)

func wrapError(err error) error {
	switch err {
	case context.DeadlineExceeded:
		return err
	case context.Canceled:
		return err
	case badger.ErrConflict:
		return store.ErrConcurrentTxn
	case badger.ErrReadOnlyTxn:
		return store.ErrReadOnlyTxn
	case badger.ErrInvalidRequest:
		return store.ErrInvalidRequest
	case badger.ErrKeyNotFound:
		return store.ErrNotFound
	case badger.ErrEmptyKey:
		return store.ErrEmptyKey
	case badger.ErrInvalidKey:
		return store.ErrInvalidKey
	case badger.ErrDiscardedTxn:
		return store.ErrDiscardedTxn
	case badger.ErrTxnTooBig:
		return store.ErrTooBigTxn
	case badger.ErrDBClosed:
		return store.ErrAlreadyClosed
	case ErrTransactionCanceled:
		return store.ErrCanceledTxn
	default:
		return err
	}
}
