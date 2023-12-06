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

package screwdb_test

import (
	"bufio"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/dpeckett/screwdb"
	"github.com/stretchr/testify/require"
)

func TestScrewDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "screwdb_test.db")

	db, err := screwdb.Open(path, screwdb.NoSync, 0o644)
	require.NoError(t, err)
	defer db.Close()

	f, err := os.Open("testdata/words.txt")
	require.NoError(t, err)
	defer f.Close()

	err = db.Update(func(tx *screwdb.Tx) error {
		scanner := bufio.NewScanner(f)
		for i := uint64(0); scanner.Scan(); i++ {
			var value [8]byte
			binary.LittleEndian.PutUint64(value[:], i)

			if err := tx.Put([]byte(scanner.Text()), value[:], false); err != nil {
				return err
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *screwdb.Tx) error {
		testEntries := map[string]uint64{
			"bowelless":    25272,
			"oxygenation":  136539,
			"betwit":       21629,
			"acentric":     1114,
			"furthermore":  74432,
			"pretrain":     155325,
			"interciliary": 95964,
			"oxbiter":      136400,
			"Babylonian":   17207,
			"rinderpest":   170019,
		}

		for key, expectedValue := range testEntries {
			value, err := tx.Get([]byte(key))
			require.NoError(t, err)

			require.Equal(t, expectedValue, binary.LittleEndian.Uint64(value))
		}

		c, err := tx.Cursor()
		require.NoError(t, err)

		key, value, err := c.Seek([]byte("betwit"))
		require.NoError(t, err)

		require.Equal(t, "betwit", string(key))
		require.Equal(t, uint64(21629), binary.LittleEndian.Uint64(value))

		key, value, err = c.Next()
		require.NoError(t, err)

		require.Equal(t, "betwixen", string(key))
		require.Equal(t, uint64(21630), binary.LittleEndian.Uint64(value))

		c.Close()

		return nil
	})
	require.NoError(t, err)
}
