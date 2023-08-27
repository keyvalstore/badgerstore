/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package badgerstore

import (
	"github.com/dgraph-io/badger/v3"
	"reflect"
	"strings"
	"time"
)

func OpenDatabase(dataDir string, options ...Option) (*badger.DB, *StoreOptions, error) {

	storeOpts := DefaultStoreOptions()

	opts := badger.DefaultOptions(dataDir)
	opts.ValueLogMaxEntries = DefaultValueLogMaxEntries

	for _, opt := range options {
		opt.apply(&opts, storeOpts)
	}

	deadline := time.Now().Add(storeOpts.OpenTimeout)
	for {

		db, err := badger.Open(opts)
		if err != nil {
			if strings.Contains(err.Error(), "Cannot acquire directory lock") && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return nil, storeOpts, err
		}

		return db, storeOpts, nil
	}

}

func ObjectType() reflect.Type {
	return BadgerStoreClass
}


