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
 *
 * Based on btree.c from the ldapd project:
 *
 * Copyright (c) 2009, 2010 Martin Hedenfalk <martin@bzero.se>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef _btree_h_
#define _btree_h_

#include <sys/types.h>

struct mpage;
struct cursor;
struct btree_txn;

struct btval {
  void *data;
  size_t size;
  int free_data;    /* true if data malloc'd */
  struct mpage *mp; /* ref'd memory page */
};

typedef int (*bt_cmp_func)(const struct btval *a, const struct btval *b);
typedef void (*bt_prefix_func)(const struct btval *a, const struct btval *b,
                               struct btval *sep);

enum cursor_op {
  BT_CURSOR,       /* cursor operations */
  BT_CURSOR_EXACT, /* position at given key */
  BT_FIRST,        /* position at key, or fail */
  BT_NEXT
};

/* return codes */
#define BT_FAIL -1
#define BT_SUCCESS 0

/* btree flags */
#define BT_NOSYNC 0x02 /* don't fsync after commit */
#define BT_RDONLY 0x04 /* read only */

struct btree *btree_open(const char *path, unsigned int flags, mode_t mode);
void btree_close(struct btree *bt);

struct btree_txn *btree_txn_begin(struct btree *bt, int rdonly);
int btree_txn_commit(struct btree_txn *txn);
void btree_txn_abort(struct btree_txn *txn);

int btree_txn_get(struct btree *bt, struct btree_txn *txn, struct btval *key,
                  struct btval *data);
int btree_txn_put(struct btree *bt, struct btree_txn *txn, struct btval *key,
                  struct btval *data);
int btree_txn_del(struct btree *bt, struct btree_txn *txn, struct btval *key,
                  struct btval *data);

void btree_set_cache_size(struct btree *bt, unsigned int cache_size);

struct cursor *btree_txn_cursor_open(struct btree *bt, struct btree_txn *txn);
void btree_cursor_close(struct cursor *cursor);
int btree_cursor_get(struct cursor *cursor, struct btval *key,
                     struct btval *data, enum cursor_op op);

int btree_sync(struct btree *bt);
int btree_compact(struct btree *bt);

int btree_cmp(struct btree *bt, const struct btval *a, const struct btval *b);

#endif
