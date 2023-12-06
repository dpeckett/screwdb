/* SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2023 Damian Peckett <damian@pecke.tt>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package screwdb

// #cgo CFLAGS: -Wno-address-of-packed-member
// #cgo LDFLAGS: -lcrypto
// #include <stdlib.h>
// #include <string.h>
// #include "btree.h"
import "C"
import (
	"fmt"
	"os"
	"unsafe"
)

type Flags uint

const (
	NoSync   Flags = C.BT_NOSYNC
	ReadOnly Flags = C.BT_RDONLY
)

type DB struct {
	bt *C.struct_btree
}

func Open(path string, flags Flags, mode os.FileMode) (*DB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	bt, err := C.btree_open(cpath, C.uint(flags), C.mode_t(mode))
	if bt == nil {
		return nil, fmt.Errorf("open failed: %w", err)
	}

	return &DB{bt}, nil
}

func (db *DB) Close() error {
	C.btree_close(db.bt)

	return nil
}

func (db *DB) SetCacheSize(cacheSize uint) {
	C.btree_set_cache_size(db.bt, C.uint(cacheSize))
}

func (db *DB) Sync() error {
	rc, err := C.btree_sync(db.bt)
	if rc != 0 {
		return fmt.Errorf("sync failed: %w", err)
	}

	return nil
}

func (db *DB) Compact() error {
	rc, err := C.btree_compact(db.bt)
	if rc != 0 {
		return fmt.Errorf("compact failed: %w", err)
	}

	return nil
}

func (db *DB) Revert() error {
	rc, err := C.btree_revert(db.bt)
	if rc != 0 {
		return fmt.Errorf("revert failed: %w", err)
	}

	return nil
}

func (db *DB) Compare(a, b []byte) int {
	cA := C.struct_btval{
		data: C.CBytes(a),
		size: C.ulong(len(a)),
	}
	defer C.free(unsafe.Pointer(cA.data))

	cB := C.struct_btval{
		data: C.CBytes(b),
		size: C.ulong(len(b)),
	}

	return int(C.btree_cmp(db.bt, &cA, &cB))

}

type Tx struct {
	bt *C.struct_btree
	tx *C.struct_btree_txn
}

func (db *DB) View(fn func(*Tx) error) error {
	tx := &Tx{
		bt: db.bt,
	}

	var err error
	tx.tx, err = C.btree_txn_begin(db.bt, 1)
	if tx.tx == nil {
		return fmt.Errorf("transaction begin failed: %w", err)
	}
	defer C.btree_txn_abort(tx.tx)

	return fn(tx)
}

func (db *DB) Update(fn func(*Tx) error) error {
	tx := &Tx{
		bt: db.bt,
	}

	var err error
	tx.tx, err = C.btree_txn_begin(db.bt, 0)
	if tx.tx == nil {
		return fmt.Errorf("transaction begin failed: %w", err)
	}

	if err = fn(tx); err != nil {
		C.btree_txn_abort(tx.tx)

		return err
	}

	rc, err := C.btree_txn_commit(tx.tx)
	if rc != 0 {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	return nil
}

func (tx *Tx) Get(key []byte) ([]byte, error) {
	cKey := C.struct_btval{
		data: C.CBytes(key),
		size: C.ulong(len(key)),
	}
	defer C.free(unsafe.Pointer(cKey.data))

	var cValue C.struct_btval
	rc, err := C.btree_txn_get(tx.bt, tx.tx, &cKey, &cValue)
	if rc != 0 {
		return nil, fmt.Errorf("get failed: %w", err)
	}

	return C.GoBytes(cValue.data, C.int(cValue.size)), nil
}

func (tx *Tx) Put(key, value []byte) error {
	cKey := C.struct_btval{
		data: C.CBytes(key),
		size: C.ulong(len(key)),
	}
	defer C.free(unsafe.Pointer(cKey.data))

	cValue := C.struct_btval{
		data: C.CBytes(value),
		size: C.ulong(len(value)),
	}
	defer C.free(unsafe.Pointer(cValue.data))

	rc, err := C.btree_txn_put(tx.bt, tx.tx, &cKey, &cValue)
	if rc != 0 {
		return fmt.Errorf("put failed: %w", err)
	}

	return nil
}

func (tx *Tx) Delete(key []byte) error {
	cKey := C.struct_btval{
		data: C.CBytes(key),
		size: C.ulong(len(key)),
	}
	defer C.free(unsafe.Pointer(cKey.data))

	rc, err := C.btree_txn_del(tx.bt, tx.tx, &cKey, nil)
	if rc != 0 {
		return fmt.Errorf("delete failed: %w", err)
	}

	return nil
}

type Cursor struct {
	cursor *C.struct_cursor
}

func (tx *Tx) Cursor() (*Cursor, error) {
	cursor, err := C.btree_txn_cursor_open(tx.bt, tx.tx)
	if cursor == nil {
		return nil, fmt.Errorf("cursor open failed: %w", err)
	}

	return &Cursor{cursor}, nil
}

func (c *Cursor) Close() {
	C.btree_cursor_close(c.cursor)
}

func (c *Cursor) First() ([]byte, []byte, error) {
	var cKey, cValue C.struct_btval

	rc, err := C.btree_cursor_get(c.cursor, &cKey, &cValue, C.BT_FIRST)
	if rc != 0 {
		return nil, nil, fmt.Errorf("cursor get failed: %w", err)
	}

	return C.GoBytes(cKey.data, C.int(cKey.size)), C.GoBytes(cValue.data, C.int(cValue.size)), nil
}

func (c *Cursor) Next() ([]byte, []byte, error) {
	var cKey, cValue C.struct_btval

	rc, err := C.btree_cursor_get(c.cursor, &cKey, &cValue, C.BT_NEXT)
	if rc != 0 {
		return nil, nil, fmt.Errorf("cursor get failed: %w", err)
	}

	return C.GoBytes(cKey.data, C.int(cKey.size)), C.GoBytes(cValue.data, C.int(cValue.size)), nil
}

func (c *Cursor) Seek(key []byte) ([]byte, []byte, error) {
	var cValue C.struct_btval

	cKey := C.struct_btval{
		data: C.CBytes(key),
		size: C.ulong(len(key)),
	}

	rc, err := C.btree_cursor_get(c.cursor, &cKey, &cValue, C.BT_CURSOR_EXACT)
	if rc != 0 {
		return nil, nil, fmt.Errorf("cursor get failed: %w", err)
	}

	return C.GoBytes(cKey.data, C.int(cKey.size)), C.GoBytes(cValue.data, C.int(cValue.size)), nil
}
