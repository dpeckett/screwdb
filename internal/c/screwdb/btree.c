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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/file.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <openssl/sha.h>

#include "sys_tree.h"

#include "btree.h"

#define PAGESIZE 4096
#define BT_MINKEYS 4
#define BT_MAGIC 0xB3DBB3DB
#define BT_VERSION 4
#define MAXKEYSIZE 255

#define P_INVALID 0xFFFFFFFF

#define F_ISSET(w, f) (((w) & (f)) == (f))
#define MINIMUM(a, b) ((a) < (b) ? (a) : (b))

typedef uint32_t pgno_t;
typedef uint16_t indx_t;

/* There are four page types: meta, index, leaf and overflow.
 * They all share the same page header.
 */
struct page {           /* represents an on-disk page */
  pgno_t pgno;          /* page number */
#define P_BRANCH 0x01   /* branch page */
#define P_LEAF 0x02     /* leaf page */
#define P_OVERFLOW 0x04 /* overflow page */
#define P_META 0x08     /* meta page */
#define P_HEAD 0x10     /* header page */
  uint32_t flags;
#define lower b.fb.fb_lower
#define upper b.fb.fb_upper
#define p_next_pgno b.pb_next_pgno
  union page_bounds {
    struct {
      indx_t fb_lower; /* lower bound of free space */
      indx_t fb_upper; /* upper bound of free space */
    } fb;
    pgno_t pb_next_pgno; /* overflow page linked list */
  } b;
  indx_t ptrs[1]; /* dynamic size */
} __attribute__((packed));

#define PAGEHDRSZ offsetof(struct page, ptrs)

#define NUMKEYSP(p) (((p)->lower - PAGEHDRSZ) >> 1)
#define NUMKEYS(mp) (((mp)->page->lower - PAGEHDRSZ) >> 1)
#define SIZELEFT(mp) (indx_t)((mp)->page->upper - (mp)->page->lower)
#define PAGEFILL(bt, mp)                                                       \
  (1000 * ((bt)->head.psize - PAGEHDRSZ - SIZELEFT(mp)) /                      \
   ((bt)->head.psize - PAGEHDRSZ))
#define IS_LEAF(mp) F_ISSET((mp)->page->flags, P_LEAF)
#define IS_BRANCH(mp) F_ISSET((mp)->page->flags, P_BRANCH)
#define IS_OVERFLOW(mp) F_ISSET((mp)->page->flags, P_OVERFLOW)

struct bt_head { /* header page content */
  uint32_t magic;
  uint32_t version;
  uint32_t flags;
  uint32_t psize; /* page size */
} __attribute__((packed));

struct bt_meta {          /* meta (footer) page content */
#define BT_TOMBSTONE 0x01 /* file is replaced */
  uint32_t flags;
  pgno_t root;      /* page number of root page */
  pgno_t prev_meta; /* previous meta page number */
  time_t created_at;
  uint32_t branch_pages;
  uint32_t leaf_pages;
  uint32_t overflow_pages;
  uint32_t revisions;
  uint32_t depth;
  uint64_t entries;
  unsigned char hash[SHA256_DIGEST_LENGTH];
} __attribute__((packed));

struct btkey {
  size_t len;
  char str[MAXKEYSIZE];
};

struct mpage {                 /* an in-memory cached page */
  RB_ENTRY(mpage) entry;       /* page cache entry */
  SIMPLEQ_ENTRY(mpage) next;   /* queue of dirty pages */
  TAILQ_ENTRY(mpage) lru_next; /* LRU queue */
  struct mpage *parent;        /* NULL if root */
  unsigned int parent_index;   /* keep track of node index */
  struct btkey prefix;
  struct page *page;
  pgno_t pgno; /* copy of page->pgno */
  short ref;   /* increased by cursors */
  short dirty; /* 1 if on dirty queue */
};
RB_HEAD(page_cache, mpage);
SIMPLEQ_HEAD(dirty_queue, mpage);
TAILQ_HEAD(lru_queue, mpage);

static int mpage_cmp(struct mpage *a, struct mpage *b);
static struct mpage *mpage_lookup(struct btree *bt, pgno_t pgno);
static void mpage_add(struct btree *bt, struct mpage *mp);
static void mpage_free(struct mpage *mp);
static void mpage_del(struct btree *bt, struct mpage *mp);
static void mpage_flush(struct btree *bt);
static struct mpage *mpage_copy(struct btree *bt, struct mpage *mp);
static void mpage_prune(struct btree *bt);
static void mpage_dirty(struct btree *bt, struct mpage *mp);
static struct mpage *mpage_touch(struct btree *bt, struct mpage *mp);

RB_PROTOTYPE(page_cache, mpage, entry, mpage_cmp);
RB_GENERATE(page_cache, mpage, entry, mpage_cmp);

struct ppage { /* ordered list of pages */
  SLIST_ENTRY(ppage) entry;
  struct mpage *mpage;
  unsigned int ki; /* cursor index on page */
};
SLIST_HEAD(page_stack, ppage);

#define CURSOR_EMPTY(c) SLIST_EMPTY(&(c)->stack)
#define CURSOR_TOP(c) SLIST_FIRST(&(c)->stack)
#define CURSOR_POP(c) SLIST_REMOVE_HEAD(&(c)->stack, entry)
#define CURSOR_PUSH(c, p) SLIST_INSERT_HEAD(&(c)->stack, p, entry)

struct cursor {
  struct btree *bt;
  struct btree_txn *txn;
  struct page_stack stack; /* stack of parent pages */
  short initialized;       /* 1 if initialized */
  short eof;               /* 1 if end is reached */
};

#define METAHASHLEN offsetof(struct bt_meta, hash)
#define METADATA(p) ((void *)((char *)p + PAGEHDRSZ))

struct node {
#define n_pgno p.np_pgno
#define n_dsize p.np_dsize
  union {
    pgno_t np_pgno;    /* child page number */
    uint32_t np_dsize; /* leaf data size */
  } p;
  uint16_t ksize;      /* key size */
#define F_BIGDATA 0x01 /* data put on overflow page */
  uint8_t flags;
  char data[1];
} __attribute__((packed));

struct btree_txn {
  pgno_t root;                     /* current / new root page */
  pgno_t next_pgno;                /* next unallocated page */
  struct btree *bt;                /* btree is ref'd */
  struct dirty_queue *dirty_queue; /* modified pages */
#define BT_TXN_RDONLY 0x01         /* read-only transaction */
#define BT_TXN_ERROR 0x02          /* an error has occurred */
  unsigned int flags;
};

struct btree {
  int fd;
  char *path;
#define BT_FIXPADDING 0x01 /* internal */
  unsigned int flags;
  struct bt_head head;
  struct bt_meta meta;
  struct page_cache *page_cache;
  struct lru_queue *lru_queue;
  struct btree_txn *txn; /* current write transaction */
  int ref;               /* increased by cursors & txn */
  unsigned int cache_size;
  unsigned int max_cache;
  off_t size; /* current file size */
};

#define NODESIZE offsetof(struct node, data)

#define INDXSIZE(k) (NODESIZE + ((k) == NULL ? 0 : (k)->size))
#define LEAFSIZE(k, d) (NODESIZE + (k)->size + (d)->size)
#define NODEPTRP(p, i) ((struct node *)((char *)(p) + (p)->ptrs[i]))
#define NODEPTR(mp, i) NODEPTRP((mp)->page, i)
#define NODEKEY(node) (void *)((node)->data)
#define NODEDATA(node) (void *)((char *)(node)->data + (node)->ksize)
#define NODEPGNO(node) ((node)->p.np_pgno)
#define NODEDSZ(node) ((node)->p.np_dsize)

#define BT_COMMIT_PAGES 64   /* max number of pages to write in one commit */
#define BT_MAXCACHE_DEF 1024 /* max number of pages to keep in cache  */

static int btree_read_page(struct btree *bt, pgno_t pgno, struct page *page);
static struct mpage *btree_get_mpage(struct btree *bt, pgno_t pgno);
static int btree_search_page_root(struct btree *bt, struct mpage *root,
                                  struct btval *key, struct cursor *cursor,
                                  int modify, struct mpage **mpp);
static int btree_search_page(struct btree *bt, struct btree_txn *txn,
                             struct btval *key, struct cursor *cursor,
                             int modify, struct mpage **mpp);

static int btree_write_header(struct btree *bt, int fd);
static int btree_read_header(struct btree *bt);
static int btree_is_meta_page(struct page *p);
static int btree_read_meta(struct btree *bt, pgno_t *p_next);
static int btree_write_meta(struct btree *bt, pgno_t root, unsigned int flags);
static void btree_ref(struct btree *bt);

static struct node *btree_search_node(struct btree *bt, struct mpage *mp,
                                      struct btval *key, int *exactp,
                                      unsigned int *kip);
static int btree_add_node(struct btree *bt, struct mpage *mp, indx_t indx,
                          struct btval *key, struct btval *data, pgno_t pgno,
                          uint8_t flags);
static void btree_del_node(struct btree *bt, struct mpage *mp, indx_t indx);
static int btree_read_data(struct btree *bt, struct mpage *mp,
                           struct node *leaf, struct btval *data);

static int btree_rebalance(struct btree *bt, struct mpage *mp);
static int btree_update_key(struct btree *bt, struct mpage *mp, indx_t indx,
                            struct btval *key);
static int btree_adjust_prefix(struct btree *bt, struct mpage *src, int delta);
static int btree_move_node(struct btree *bt, struct mpage *src, indx_t srcindx,
                           struct mpage *dst, indx_t dstindx);
static int btree_merge(struct btree *bt, struct mpage *src, struct mpage *dst);
static int btree_split(struct btree *bt, struct mpage **mpp,
                       unsigned int *newindxp, struct btval *newkey,
                       struct btval *newdata, pgno_t newpgno);
static struct mpage *btree_new_page(struct btree *bt, uint32_t flags);
static int btree_write_overflow_data(struct btree *bt, struct page *p,
                                     struct btval *data);

static void cursor_pop_page(struct cursor *cursor);
static struct ppage *cursor_push_page(struct cursor *cursor, struct mpage *mp);

static int bt_set_key(struct btree *bt, struct mpage *mp, struct node *node,
                      struct btval *key);
static int btree_sibling(struct cursor *cursor, int move_right);
static int btree_cursor_next(struct cursor *cursor, struct btval *key,
                             struct btval *data);
static int btree_cursor_set(struct cursor *cursor, struct btval *key,
                            struct btval *data, int *exactp);
static int btree_cursor_first(struct cursor *cursor, struct btval *key,
                              struct btval *data);

static void bt_reduce_separator(struct btree *bt, struct node *min,
                                struct btval *sep);
static void remove_prefix(struct btree *bt, struct btval *key, size_t pfxlen);
static void expand_prefix(struct btree *bt, struct mpage *mp, indx_t indx,
                          struct btkey *expkey);
static void concat_prefix(struct btree *bt, char *s1, size_t n1, char *s2,
                          size_t n2, char *cs, size_t *cn);
static void common_prefix(struct btree *bt, struct btkey *min,
                          struct btkey *max, struct btkey *pfx);
static void find_common_prefix(struct btree *bt, struct mpage *mp);

static size_t bt_leaf_size(struct btree *bt, struct btval *key,
                           struct btval *data);
static size_t bt_branch_size(struct btree *bt, struct btval *key);

static pgno_t btree_compact_tree(struct btree *bt, pgno_t pgno,
                                 struct btree *btc);

static int memncmp(const void *s1, size_t n1, const void *s2, size_t n2);

static int memncmp(const void *s1, size_t n1, const void *s2, size_t n2) {
  if (n1 < n2 && memcmp(s1, s2, n1) == 0) {
    return -1;
  } else if (n1 > n2 && memcmp(s1, s2, n2) == 0) {
    return 1;
  }

  return memcmp(s1, s2, n1);
}

int btree_cmp(struct btree *bt, const struct btval *a, const struct btval *b) {
  return memncmp(a->data, a->size, b->data, b->size);
}

static void common_prefix(struct btree *bt, struct btkey *min,
                          struct btkey *max, struct btkey *pfx) {
  size_t n = 0;
  char *p1;
  char *p2;

  if (min->len == 0 || max->len == 0) {
    pfx->len = 0;
    return;
  }

  p1 = min->str;
  p2 = max->str;

  while (*p1 == *p2) {
    if (n == min->len || n == max->len) {
      break;
    }

    p1++;
    p2++;
    n++;
  }

  pfx->len = n;
  memmove(pfx->str, max->str, n);
}

static void remove_prefix(struct btree *bt, struct btval *key, size_t pfxlen) {
  if (pfxlen == 0) {
    return;
  }

  key->size -= pfxlen;
  key->data = (char *)key->data + pfxlen;
}

static void expand_prefix(struct btree *bt, struct mpage *mp, indx_t indx,
                          struct btkey *expkey) {
  struct node *node;

  node = NODEPTR(mp, indx);
  expkey->len = sizeof(expkey->str);
  concat_prefix(bt, mp->prefix.str, mp->prefix.len, NODEKEY(node), node->ksize,
                expkey->str, &expkey->len);
}

static int bt_cmp(struct btree *bt, const struct btval *key1,
                  const struct btval *key2, struct btkey *pfx) {
  return memncmp((char *)key1->data + pfx->len, key1->size - pfx->len,
                 key2->data, key2->size);
}

static int mpage_cmp(struct mpage *a, struct mpage *b) {
  if (a->pgno > b->pgno) {
    return 1;
  }
  if (a->pgno < b->pgno) {
    return -1;
  }
  return 0;
}

static struct mpage *mpage_lookup(struct btree *bt, pgno_t pgno) {
  struct mpage find, *mp;

  find.pgno = pgno;
  mp = RB_FIND(page_cache, bt->page_cache, &find);
  if (mp) {
    /* Update LRU queue. Move page to the end. */
    TAILQ_REMOVE(bt->lru_queue, mp, lru_next);
    TAILQ_INSERT_TAIL(bt->lru_queue, mp, lru_next);
  }
  return mp;
}

static void mpage_add(struct btree *bt, struct mpage *mp) {
  RB_INSERT(page_cache, bt->page_cache, mp);
  bt->cache_size++;
  TAILQ_INSERT_TAIL(bt->lru_queue, mp, lru_next);
}

static void mpage_free(struct mpage *mp) {
  if (mp != NULL) {
    free(mp->page);
    free(mp);
  }
}

static void mpage_del(struct btree *bt, struct mpage *mp) {
  RB_REMOVE(page_cache, bt->page_cache, mp);
  bt->cache_size--;
  TAILQ_REMOVE(bt->lru_queue, mp, lru_next);
}

static void mpage_flush(struct btree *bt) {
  struct mpage *mp;

  while ((mp = RB_MIN(page_cache, bt->page_cache)) != NULL) {
    mpage_del(bt, mp);
    mpage_free(mp);
  }
}

static struct mpage *mpage_copy(struct btree *bt, struct mpage *mp) {
  struct mpage *copy;

  if ((copy = calloc(1, sizeof(*copy))) == NULL) {
    return NULL;
  }
  if ((copy->page = malloc(bt->head.psize)) == NULL) {
    free(copy);
    return NULL;
  }
  memmove(copy->page, mp->page, bt->head.psize);
  memmove(&copy->prefix, &mp->prefix, sizeof(mp->prefix));
  copy->parent = mp->parent;
  copy->parent_index = mp->parent_index;
  copy->pgno = mp->pgno;

  return copy;
}

/* Remove the least recently used memory pages until the cache size is
 * within the configured bounds. Pages referenced by cursors or returned
 * key/data are not pruned.
 */
static void mpage_prune(struct btree *bt) {
  struct mpage *mp, *next;

  for (mp = TAILQ_FIRST(bt->lru_queue); mp; mp = next) {
    if (bt->cache_size <= bt->max_cache) {
      break;
    }

    next = TAILQ_NEXT(mp, lru_next);
    if (!mp->dirty && mp->ref <= 0) {
      mpage_del(bt, mp);
      mpage_free(mp);
    }
  }
}

/* Mark a page as dirty and push it on the dirty queue. */
static void mpage_dirty(struct btree *bt, struct mpage *mp) {
  if (!mp->dirty) {
    mp->dirty = 1;
    SIMPLEQ_INSERT_TAIL(bt->txn->dirty_queue, mp, next);
  }
}

/* Touch a page: make it dirty and re-insert into tree with updated pgno. */
static struct mpage *mpage_touch(struct btree *bt, struct mpage *mp) {
  if (!mp->dirty) {
    if (mp->ref == 0) {
      mpage_del(bt, mp);
    } else if ((mp = mpage_copy(bt, mp)) == NULL) {
      return NULL;
    }
    mp->pgno = mp->page->pgno = bt->txn->next_pgno++;
    mpage_dirty(bt, mp);
    mpage_add(bt, mp);

    /* Update the page number to new touched page. */
    if (mp->parent != NULL)
      NODEPGNO(NODEPTR(mp->parent, mp->parent_index)) = mp->pgno;
  }

  return mp;
}

static int btree_read_page(struct btree *bt, pgno_t pgno, struct page *page) {
  ssize_t rc;

  if ((rc = pread(bt->fd, page, bt->head.psize,
                  (off_t)pgno * bt->head.psize)) == 0) {
    errno = ENOENT;
    return BT_FAIL;
  } else if (rc != (ssize_t)bt->head.psize) {
    if (rc > 0) {
      errno = EINVAL;
    }
    return BT_FAIL;
  }

  if (page->pgno != pgno) {
    errno = EINVAL;
    return BT_FAIL;
  }

  return BT_SUCCESS;
}

int btree_sync(struct btree *bt) {
  if (!F_ISSET(bt->flags, BT_NOSYNC)) {
    return fsync(bt->fd);
  }

  return 0;
}

struct btree_txn *btree_txn_begin(struct btree *bt, int rdonly) {
  struct btree_txn *txn;

  if (!rdonly && bt->txn != NULL) {
    errno = EBUSY;
    return NULL;
  }

  if ((txn = calloc(1, sizeof(*txn))) == NULL) {
    return NULL;
  }

  if (rdonly) {
    txn->flags |= BT_TXN_RDONLY;
  } else {
    txn->dirty_queue = calloc(1, sizeof(*txn->dirty_queue));
    if (txn->dirty_queue == NULL) {
      free(txn);
      return NULL;
    }
    SIMPLEQ_INIT(txn->dirty_queue);

    if (flock(bt->fd, LOCK_EX | LOCK_NB) != 0) {
      errno = EBUSY;
      free(txn->dirty_queue);
      free(txn);
      return NULL;
    }
    bt->txn = txn;
  }

  txn->bt = bt;
  btree_ref(bt);

  if (btree_read_meta(bt, &txn->next_pgno) != BT_SUCCESS) {
    btree_txn_abort(txn);
    return NULL;
  }

  txn->root = bt->meta.root;

  return txn;
}

void btree_txn_abort(struct btree_txn *txn) {
  struct mpage *mp;
  struct btree *bt;

  if (txn == NULL) {
    return;
  }

  bt = txn->bt;

  if (!F_ISSET(txn->flags, BT_TXN_RDONLY)) {
    /* Discard all dirty pages. */
    while (!SIMPLEQ_EMPTY(txn->dirty_queue)) {
      mp = SIMPLEQ_FIRST(txn->dirty_queue);
      mpage_del(bt, mp);
      SIMPLEQ_REMOVE_HEAD(txn->dirty_queue, next);
      mpage_free(mp);
    }

    txn->bt->txn = NULL;
    flock(txn->bt->fd, LOCK_UN);
    free(txn->dirty_queue);
  }

  btree_close(txn->bt);
  free(txn);
}

int btree_txn_commit(struct btree_txn *txn) {
  int n, done;
  ssize_t rc;
  off_t size;
  struct mpage *mp;
  struct btree *bt;
  struct iovec iov[BT_COMMIT_PAGES];

  bt = txn->bt;

  if (F_ISSET(txn->flags, BT_TXN_RDONLY)) {
    btree_txn_abort(txn);
    errno = EPERM;
    return BT_FAIL;
  }

  if (txn != bt->txn) {
    btree_txn_abort(txn);
    errno = EINVAL;
    return BT_FAIL;
  }

  if (F_ISSET(txn->flags, BT_TXN_ERROR)) {
    btree_txn_abort(txn);
    errno = EINVAL;
    return BT_FAIL;
  }

  if (SIMPLEQ_EMPTY(txn->dirty_queue)) {
    goto done;
  }

  if (F_ISSET(bt->flags, BT_FIXPADDING)) {
    size = lseek(bt->fd, 0, SEEK_END);
    size += bt->head.psize - (size % bt->head.psize);
    if (ftruncate(bt->fd, size) != 0) {
      btree_txn_abort(txn);
      return BT_FAIL;
    }
    bt->flags &= ~BT_FIXPADDING;
  }

  /* Commit up to BT_COMMIT_PAGES dirty pages to disk until done. */
  do {
    n = 0;
    done = 1;
    SIMPLEQ_FOREACH(mp, txn->dirty_queue, next) {
      iov[n].iov_len = bt->head.psize;
      iov[n].iov_base = mp->page;
      if (++n >= BT_COMMIT_PAGES) {
        done = 0;
        break;
      }
    }

    if (n == 0) {
      break;
    }

    rc = writev(bt->fd, iov, n);
    if (rc != (ssize_t)bt->head.psize * n) {
      btree_txn_abort(txn);
      return BT_FAIL;
    }

    /* Remove the dirty flag from the written pages. */
    while (!SIMPLEQ_EMPTY(txn->dirty_queue)) {
      mp = SIMPLEQ_FIRST(txn->dirty_queue);
      mp->dirty = 0;
      SIMPLEQ_REMOVE_HEAD(txn->dirty_queue, next);
      if (--n == 0) {
        break;
      }
    }
  } while (!done);

  if (btree_sync(bt) != 0 || btree_write_meta(bt, txn->root, 0) != BT_SUCCESS ||
      btree_sync(bt) != 0) {
    btree_txn_abort(txn);
    return BT_FAIL;
  }

done:
  mpage_prune(bt);
  btree_txn_abort(txn);

  return BT_SUCCESS;
}

static int btree_write_header(struct btree *bt, int fd) {
  struct stat sb;
  struct bt_head *h;
  struct page *p;
  ssize_t rc;
  unsigned int psize;

  /* Ask stat for 'optimal blocksize for I/O', but cap to fit in indx_t. */
  if (fstat(fd, &sb) == 0) {
    psize = MINIMUM(32 * 1024, sb.st_blksize);
  } else {
    psize = PAGESIZE;
  }

  if ((p = calloc(1, psize)) == NULL) {
    return -1;
  }
  p->flags = P_HEAD;

  h = METADATA(p);
  h->magic = BT_MAGIC;
  h->version = BT_VERSION;
  h->psize = psize;
  memmove(&bt->head, h, sizeof(*h));

  rc = write(fd, p, bt->head.psize);
  free(p);
  if (rc != (ssize_t)bt->head.psize) {
    return BT_FAIL;
  }

  return BT_SUCCESS;
}

static int btree_read_header(struct btree *bt) {
  char page[PAGESIZE];
  struct page *p;
  struct bt_head *h;
  int rc;

  /* We don't know the page size yet, so use a minimum value. */
  if ((rc = pread(bt->fd, page, PAGESIZE, 0)) == 0) {
    errno = ENOENT;
    return -1;
  } else if (rc != PAGESIZE) {
    if (rc > 0) {
      errno = EINVAL;
    }
    return -1;
  }

  p = (struct page *)page;

  if (!F_ISSET(p->flags, P_HEAD)) {
    errno = EINVAL;
    return -1;
  }

  h = METADATA(p);
  if (h->magic != BT_MAGIC) {
    errno = EINVAL;
    return -1;
  }

  if (h->version != BT_VERSION) {
    errno = EINVAL;
    return -1;
  }

  memmove(&bt->head, h, sizeof(*h));
  return 0;
}

static int btree_write_meta(struct btree *bt, pgno_t root, unsigned int flags) {
  struct mpage *mp;
  struct bt_meta *meta;
  ssize_t rc;

  if ((mp = btree_new_page(bt, P_META)) == NULL) {
    return -1;
  }

  bt->meta.prev_meta = bt->meta.root;
  bt->meta.root = root;
  bt->meta.flags = flags;
  bt->meta.created_at = time(0);
  bt->meta.revisions++;
  SHA256((unsigned char *)&bt->meta, METAHASHLEN, bt->meta.hash);

  /* Copy the meta data changes to the new meta page. */
  meta = METADATA(mp->page);
  memmove(meta, &bt->meta, sizeof(*meta));

  rc = write(bt->fd, mp->page, bt->head.psize);
  mp->dirty = 0;
  SIMPLEQ_REMOVE_HEAD(bt->txn->dirty_queue, next);
  if (rc != (ssize_t)bt->head.psize) {
    return BT_FAIL;
  }

  if ((bt->size = lseek(bt->fd, 0, SEEK_END)) == -1) {
    bt->size = 0;
  }

  return BT_SUCCESS;
}

/* Returns true if page p is a valid meta page, false otherwise. */
static int btree_is_meta_page(struct page *p) {
  struct bt_meta *m;
  unsigned char hash[SHA256_DIGEST_LENGTH];

  m = METADATA(p);
  if (!F_ISSET(p->flags, P_META)) {
    errno = EINVAL;
    return 0;
  }

  if (m->root >= p->pgno && m->root != P_INVALID) {
    errno = EINVAL;
    return 0;
  }

  SHA256((unsigned char *)m, METAHASHLEN, hash);
  if (memcmp(hash, m->hash, SHA256_DIGEST_LENGTH) != 0) {
    errno = EINVAL;
    return 0;
  }

  return 1;
}

static int btree_read_meta(struct btree *bt, pgno_t *p_next) {
  struct mpage *mp;
  struct bt_meta *meta;
  pgno_t meta_pgno, next_pgno;
  off_t size;

  if ((size = lseek(bt->fd, 0, SEEK_END)) == -1) {
    goto fail;
  }

  if (size < bt->size) {
    errno = EIO;
    goto fail;
  }

  if (size == bt->head.psize) { /* there is only the header */
    if (p_next != NULL) {
      *p_next = 1;
    }
    return BT_SUCCESS; /* new file */
  }

  next_pgno = size / bt->head.psize;
  if (next_pgno == 0) {
    errno = EIO;
    goto fail;
  }

  meta_pgno = next_pgno - 1;

  if (size % bt->head.psize != 0) {
    bt->flags |= BT_FIXPADDING;
    next_pgno++;
  }

  if (p_next != NULL) {
    *p_next = next_pgno;
  }

  if (size == bt->size) {
    if (F_ISSET(bt->meta.flags, BT_TOMBSTONE)) {
      errno = ESTALE;
      return BT_FAIL;
    } else {
      return BT_SUCCESS;
    }
  }
  bt->size = size;

  while (meta_pgno > 0) {
    if ((mp = btree_get_mpage(bt, meta_pgno)) == NULL) {
      break;
    }
    if (btree_is_meta_page(mp->page)) {
      meta = METADATA(mp->page);
      if (F_ISSET(meta->flags, BT_TOMBSTONE)) {
        errno = ESTALE;
        return BT_FAIL;
      } else {
        /* Make copy of last meta page. */
        memmove(&bt->meta, meta, sizeof(bt->meta));
        return BT_SUCCESS;
      }
    }
    --meta_pgno; /* scan backwards to first valid meta page */
  }

  errno = EIO;
fail:
  if (p_next != NULL) {
    *p_next = P_INVALID;
  }

  return BT_FAIL;
}

struct btree *btree_open_fd(int fd, unsigned int flags) {
  struct btree *bt;
  int fl;

  fl = fcntl(fd, F_GETFL);
  if (fcntl(fd, F_SETFL, fl | O_APPEND) == -1) {
    return NULL;
  }

  if ((bt = calloc(1, sizeof(*bt))) == NULL) {
    return NULL;
  }
  bt->fd = fd;
  bt->flags = flags;
  bt->flags &= ~BT_FIXPADDING;
  bt->ref = 1;
  bt->meta.root = P_INVALID;

  if ((bt->page_cache = calloc(1, sizeof(*bt->page_cache))) == NULL) {
    goto fail;
  }
  bt->max_cache = BT_MAXCACHE_DEF;
  RB_INIT(bt->page_cache);

  if ((bt->lru_queue = calloc(1, sizeof(*bt->lru_queue))) == NULL) {
    goto fail;
  }
  TAILQ_INIT(bt->lru_queue);

  if (btree_read_header(bt) != 0) {
    if (errno != ENOENT) {
      goto fail;
    }

    btree_write_header(bt, bt->fd);
  }

  if (btree_read_meta(bt, NULL) != 0) {
    goto fail;
  }

  return bt;

fail:
  free(bt->lru_queue);
  free(bt->page_cache);
  free(bt);
  return NULL;
}

struct btree *btree_open(const char *path, unsigned int flags, mode_t mode) {
  int fd, oflags;
  struct btree *bt;

  if (F_ISSET(flags, BT_RDONLY)) {
    oflags = O_RDONLY;
  } else {
    oflags = O_RDWR | O_CREAT | O_APPEND;
  }

  if ((fd = open(path, oflags, mode)) == -1) {
    return NULL;
  }

  if ((bt = btree_open_fd(fd, flags)) == NULL) {
    close(fd);
  } else {
    bt->path = strdup(path);
  }

  return bt;
}

static void btree_ref(struct btree *bt) { bt->ref++; }

void btree_close(struct btree *bt) {
  if (bt == NULL) {
    return;
  }

  if (--bt->ref == 0) {
    close(bt->fd);
    mpage_flush(bt);
    free(bt->lru_queue);
    free(bt->path);
    free(bt->page_cache);
    free(bt);
  }
}

/* Search for key within a leaf page, using binary search.
 * Returns the smallest entry larger or equal to the key.
 * If exactp is non-null, stores whether the found entry was an exact match
 * in *exactp (1 or 0).
 * If kip is non-null, stores the index of the found entry in *kip.
 * If no entry larger of equal to the key is found, returns NULL.
 */
static struct node *btree_search_node(struct btree *bt, struct mpage *mp,
                                      struct btval *key, int *exactp,
                                      unsigned int *kip) {
  unsigned int i = 0;
  int low, high;
  int rc = 0;
  struct node *node;
  struct btval nodekey;

  memset(&nodekey, 0, sizeof(nodekey));

  low = IS_LEAF(mp) ? 0 : 1;
  high = NUMKEYS(mp) - 1;
  while (low <= high) {
    i = (low + high) >> 1;
    node = NODEPTR(mp, i);

    nodekey.size = node->ksize;
    nodekey.data = NODEKEY(node);
 
    rc = bt_cmp(bt, key, &nodekey, &mp->prefix);
    if (rc == 0) {
      break;
    }
    if (rc > 0) {
      low = i + 1;
    } else {
      high = i - 1;
    }
  }

  if (rc > 0) { /* Found entry is less than the key. */
    i++;        /* Skip to get the smallest entry larger than key. */
    if (i >= NUMKEYS(mp)) {
      /* There is no entry larger or equal to the key. */
      return NULL;
    }
  }
  if (exactp) {
    *exactp = (rc == 0);
  }
  /* Store the key index if requested. */
  if (kip) {
    *kip = i;
  }

  return NODEPTR(mp, i);
}

static void cursor_pop_page(struct cursor *cursor) {
  struct ppage *top;

  top = CURSOR_TOP(cursor);
  CURSOR_POP(cursor);
  top->mpage->ref--;

  free(top);
}

static struct ppage *cursor_push_page(struct cursor *cursor, struct mpage *mp) {
  struct ppage *ppage;

  if ((ppage = calloc(1, sizeof(*ppage))) == NULL) {
    return NULL;
  }
  ppage->mpage = mp;
  mp->ref++;
  CURSOR_PUSH(cursor, ppage);
  return ppage;
}

static struct mpage *btree_get_mpage(struct btree *bt, pgno_t pgno) {
  struct mpage *mp;

  mp = mpage_lookup(bt, pgno);
  if (mp == NULL) {
    if ((mp = calloc(1, sizeof(*mp))) == NULL) {
      return NULL;
    }
    if ((mp->page = malloc(bt->head.psize)) == NULL) {
      free(mp);
      return NULL;
    }
    if (btree_read_page(bt, pgno, mp->page) != BT_SUCCESS) {
      mpage_free(mp);
      return NULL;
    }
    mp->pgno = pgno;
    mpage_add(bt, mp);
  }

  return mp;
}

static void concat_prefix(struct btree *bt, char *s1, size_t n1, char *s2,
                          size_t n2, char *cs, size_t *cn) {
  memmove(cs, s1, n1);
  memmove(cs + n1, s2, n2);
  *cn = n1 + n2;
}

static void find_common_prefix(struct btree *bt, struct mpage *mp) {
  indx_t lbound = 0, ubound = 0;
  struct mpage *lp, *up;
  struct btkey lprefix, uprefix;

  mp->prefix.len = 0;

  lp = mp;
  while (lp->parent != NULL) {
    if (lp->parent_index > 0) {
      lbound = lp->parent_index;
      break;
    }
    lp = lp->parent;
  }

  up = mp;
  while (up->parent != NULL) {
    if (up->parent_index + 1 < (indx_t)NUMKEYS(up->parent)) {
      ubound = up->parent_index + 1;
      break;
    }
    up = up->parent;
  }

  if (lp->parent != NULL && up->parent != NULL) {
    expand_prefix(bt, lp->parent, lbound, &lprefix);
    expand_prefix(bt, up->parent, ubound, &uprefix);
    common_prefix(bt, &lprefix, &uprefix, &mp->prefix);
  } else if (mp->parent) {
    memmove(&mp->prefix, &mp->parent->prefix, sizeof(mp->prefix));
  }
}

static int btree_search_page_root(struct btree *bt, struct mpage *root,
                                  struct btval *key, struct cursor *cursor,
                                  int modify, struct mpage **mpp) {
  struct mpage *mp, *parent;

  if (cursor && cursor_push_page(cursor, root) == NULL) {
    return BT_FAIL;
  }

  mp = root;
  while (IS_BRANCH(mp)) {
    unsigned int i = 0;
    struct node *node;

    if (key == NULL) { /* Initialize cursor to first page. */
      i = 0;
    } else {
      int exact;
      node = btree_search_node(bt, mp, key, &exact, &i);
      if (node == NULL) {
        i = NUMKEYS(mp) - 1;
      } else if (!exact) {
        i--;
      }
    }

    node = NODEPTR(mp, i);

    if (cursor) {
      CURSOR_TOP(cursor)->ki = i;
    }

    parent = mp;
    if ((mp = btree_get_mpage(bt, NODEPGNO(node))) == NULL) {
      return BT_FAIL;
    }
    mp->parent = parent;
    mp->parent_index = i;
    find_common_prefix(bt, mp);

    if (cursor && cursor_push_page(cursor, mp) == NULL) {
      return BT_FAIL;
    }

    if (modify && (mp = mpage_touch(bt, mp)) == NULL) {
      return BT_FAIL;
    }
  }

  if (!IS_LEAF(mp)) {
    return BT_FAIL;
  }

  *mpp = mp;
  return BT_SUCCESS;
}

/* Search for the page a given key should be in.
 * Stores a pointer to the found page in *mpp.
 * If key is NULL, search for the lowest page (used by btree_cursor_first).
 * If cursor is non-null, pushes parent pages on the cursor stack.
 * If modify is true, visited pages are updated with new page numbers.
 */
static int btree_search_page(struct btree *bt, struct btree_txn *txn,
                             struct btval *key, struct cursor *cursor,
                             int modify, struct mpage **mpp) {
  int rc;
  pgno_t root;
  struct mpage *mp;

  /* Can't modify pages outside a transaction. */
  if (txn == NULL && modify) {
    errno = EINVAL;
    return BT_FAIL;
  }

  /* Choose which root page to start with. If a transaction is given
   * use the root page from the transaction, otherwise read the last
   * committed root page.
   */
  if (txn == NULL) {
    if ((rc = btree_read_meta(bt, NULL)) != BT_SUCCESS) {
      return rc;
    }
    root = bt->meta.root;
  } else if (F_ISSET(txn->flags, BT_TXN_ERROR)) {
    errno = EINVAL;
    return BT_FAIL;
  } else
    root = txn->root;

  if (root == P_INVALID) { /* Tree is empty. */
    errno = ENOENT;
    return BT_FAIL;
  }

  if ((mp = btree_get_mpage(bt, root)) == NULL) {
    return BT_FAIL;
  }

  if (modify && !mp->dirty) {
    if ((mp = mpage_touch(bt, mp)) == NULL) {
      return BT_FAIL;
    }
    txn->root = mp->pgno;
  }

  return btree_search_page_root(bt, mp, key, cursor, modify, mpp);
}

static int btree_read_data(struct btree *bt, struct mpage *mp,
                           struct node *leaf, struct btval *data) {
  struct mpage *omp; /* overflow mpage */
  size_t psz;
  size_t max;
  size_t sz = 0;
  pgno_t pgno;

  memset(data, 0, sizeof(*data));
  max = bt->head.psize - PAGEHDRSZ;

  if (!F_ISSET(leaf->flags, F_BIGDATA)) {
    data->size = leaf->n_dsize;
    if (data->size > 0) {
      if (mp == NULL) {
        if ((data->data = malloc(data->size)) == NULL) {
          return BT_FAIL;
        }
        memmove(data->data, NODEDATA(leaf), data->size);
        data->free_data = 1;
        data->mp = NULL;
      } else {
        data->data = NODEDATA(leaf);
        data->free_data = 0;
        data->mp = mp;
        mp->ref++;
      }
    }
    return BT_SUCCESS;
  }

  /* Read overflow data. */
  if ((data->data = malloc(leaf->n_dsize)) == NULL) {
    return BT_FAIL;
  }
  data->size = leaf->n_dsize;
  data->free_data = 1;
  data->mp = NULL;
  memmove(&pgno, NODEDATA(leaf), sizeof(pgno));
  for (sz = 0; sz < data->size;) {
    if ((omp = btree_get_mpage(bt, pgno)) == NULL ||
        !F_ISSET(omp->page->flags, P_OVERFLOW)) {
      free(data->data);
      mpage_free(omp);
      return BT_FAIL;
    }
    psz = data->size - sz;
    if (psz > max) {
      psz = max;
    }
    memmove((char *)data->data + sz, omp->page->ptrs, psz);
    sz += psz;
    pgno = omp->page->p_next_pgno;
  }

  return BT_SUCCESS;
}

int btree_txn_get(struct btree *bt, struct btree_txn *txn, struct btval *key,
                  struct btval *data) {
  int rc, exact;
  struct node *leaf;
  struct mpage *mp;

  if (bt != NULL && txn != NULL && bt != txn->bt) {
    errno = EINVAL;
    return BT_FAIL;
  }

  if (bt == NULL) {
    if (txn == NULL) {
      errno = EINVAL;
      return BT_FAIL;
    }
    bt = txn->bt;
  }

  if (key->size == 0 || key->size > MAXKEYSIZE) {
    errno = EINVAL;
    return BT_FAIL;
  }

  if ((rc = btree_search_page(bt, txn, key, NULL, 0, &mp)) != BT_SUCCESS) {
    return rc;
  }

  leaf = btree_search_node(bt, mp, key, &exact, NULL);
  if (leaf && exact) {
    rc = btree_read_data(bt, mp, leaf, data);
  } else {
    errno = ENOENT;
    rc = BT_FAIL;
  }

  mpage_prune(bt);
  return rc;
}

static int btree_sibling(struct cursor *cursor, int move_right) {
  int rc;
  struct node *indx;
  struct ppage *parent, *top;
  struct mpage *mp;

  top = CURSOR_TOP(cursor);
  if ((parent = SLIST_NEXT(top, entry)) == NULL) {
    errno = ENOENT;
    return BT_FAIL; /* root has no siblings */
  }

  cursor_pop_page(cursor);
  if (move_right ? (parent->ki + 1 >= NUMKEYS(parent->mpage))
                 : (parent->ki == 0)) {
    if ((rc = btree_sibling(cursor, move_right)) != BT_SUCCESS) {
      return rc;
    }
    parent = CURSOR_TOP(cursor);
  } else {
    if (move_right) {
      parent->ki++;
    } else {
      parent->ki--;
    }
  }

  indx = NODEPTR(parent->mpage, parent->ki);
  if ((mp = btree_get_mpage(cursor->bt, indx->n_pgno)) == NULL) {
    return BT_FAIL;
  }
  mp->parent = parent->mpage;
  mp->parent_index = parent->ki;

  cursor_push_page(cursor, mp);
  find_common_prefix(cursor->bt, mp);

  return BT_SUCCESS;
}

static int bt_set_key(struct btree *bt, struct mpage *mp, struct node *node,
                      struct btval *key) {
  if (key == NULL) {
    return 0;
  }

  if (mp->prefix.len > 0) {
    key->size = node->ksize + mp->prefix.len;
    key->data = malloc(key->size);
    if (key->data == NULL) {
      return -1;
    }
    concat_prefix(bt, mp->prefix.str, mp->prefix.len, NODEKEY(node),
                  node->ksize, key->data, &key->size);
    key->free_data = 1;
  } else {
    key->size = node->ksize;
    key->data = NODEKEY(node);
    key->free_data = 0;
    key->mp = mp;
    mp->ref++;
  }

  return 0;
}

static int btree_cursor_next(struct cursor *cursor, struct btval *key,
                             struct btval *data) {
  struct ppage *top;
  struct mpage *mp;
  struct node *leaf;

  if (cursor->eof) {
    errno = ENOENT;
    return BT_FAIL;
  }

  top = CURSOR_TOP(cursor);
  mp = top->mpage;

  if (top->ki + 1 >= NUMKEYS(mp)) {
    if (btree_sibling(cursor, 1) != BT_SUCCESS) {
      cursor->eof = 1;
      return BT_FAIL;
    }
    top = CURSOR_TOP(cursor);
    mp = top->mpage;
  } else {
    top->ki++;
  }

  leaf = NODEPTR(mp, top->ki);

  if (data && btree_read_data(cursor->bt, mp, leaf, data) != BT_SUCCESS) {
    return BT_FAIL;
  }

  if (bt_set_key(cursor->bt, mp, leaf, key) != 0) {
    return BT_FAIL;
  }

  return BT_SUCCESS;
}

static int btree_cursor_set(struct cursor *cursor, struct btval *key,
                            struct btval *data, int *exactp) {
  int rc;
  struct node *leaf;
  struct mpage *mp;
  struct ppage *top;

  rc = btree_search_page(cursor->bt, cursor->txn, key, cursor, 0, &mp);
  if (rc != BT_SUCCESS) {
    return rc;
  }

  top = CURSOR_TOP(cursor);
  leaf = btree_search_node(cursor->bt, mp, key, exactp, &top->ki);
  if (exactp != NULL && !*exactp) {
    /* BT_CURSOR_EXACT specified and not an exact match. */
    errno = ENOENT;
    return BT_FAIL;
  }

  if (leaf == NULL) {
    if (btree_sibling(cursor, 1) != BT_SUCCESS) {
      return BT_FAIL; /* no entries matched */
    }
    top = CURSOR_TOP(cursor);
    top->ki = 0;
    mp = top->mpage;
    leaf = NODEPTR(mp, 0);
  }

  cursor->initialized = 1;
  cursor->eof = 0;

  if (data && btree_read_data(cursor->bt, mp, leaf, data) != BT_SUCCESS) {
    return BT_FAIL;
  }

  if (bt_set_key(cursor->bt, mp, leaf, key) != 0) {
    return BT_FAIL;
  }

  return BT_SUCCESS;
}

static int btree_cursor_first(struct cursor *cursor, struct btval *key,
                              struct btval *data) {
  int rc;
  struct mpage *mp;
  struct node *leaf;

  rc = btree_search_page(cursor->bt, cursor->txn, NULL, cursor, 0, &mp);
  if (rc != BT_SUCCESS) {
    return rc;
  }

  leaf = NODEPTR(mp, 0);
  cursor->initialized = 1;
  cursor->eof = 0;

  if (data && btree_read_data(cursor->bt, mp, leaf, data) != BT_SUCCESS) {
    return BT_FAIL;
  }

  if (bt_set_key(cursor->bt, mp, leaf, key) != 0) {
    return BT_FAIL;
  }

  return BT_SUCCESS;
}

int btree_cursor_get(struct cursor *cursor, struct btval *key,
                     struct btval *data, enum cursor_op op) {
  int rc;
  int exact = 0;

  switch (op) {
  case BT_CURSOR:
  case BT_CURSOR_EXACT:
    while (CURSOR_TOP(cursor) != NULL) {
      cursor_pop_page(cursor);
    }
    if (key == NULL || key->size == 0 || key->size > MAXKEYSIZE) {
      errno = EINVAL;
      rc = BT_FAIL;
    } else if (op == BT_CURSOR_EXACT) {
      rc = btree_cursor_set(cursor, key, data, &exact);
    } else {
      rc = btree_cursor_set(cursor, key, data, NULL);
    }
    break;
  case BT_NEXT:
    if (!cursor->initialized) {
      rc = btree_cursor_first(cursor, key, data);
    } else {
      rc = btree_cursor_next(cursor, key, data);
    }
    break;
  case BT_FIRST:
    while (CURSOR_TOP(cursor) != NULL) {
      cursor_pop_page(cursor);
    }
    rc = btree_cursor_first(cursor, key, data);
    break;
  default:
    rc = BT_FAIL;
    break;
  }

  mpage_prune(cursor->bt);

  return rc;
}

static struct mpage *btree_new_page(struct btree *bt, uint32_t flags) {
  struct mpage *mp;

  if ((mp = calloc(1, sizeof(*mp))) == NULL) {
    return NULL;
  }
  if ((mp->page = malloc(bt->head.psize)) == NULL) {
    free(mp);
    return NULL;
  }
  mp->pgno = mp->page->pgno = bt->txn->next_pgno++;
  mp->page->flags = flags;
  mp->page->lower = PAGEHDRSZ;
  mp->page->upper = bt->head.psize;

  if (IS_BRANCH(mp)) {
    bt->meta.branch_pages++;
  } else if (IS_LEAF(mp)) {
    bt->meta.leaf_pages++;
  } else if (IS_OVERFLOW(mp)) {
    bt->meta.overflow_pages++;
  }

  mpage_add(bt, mp);
  mpage_dirty(bt, mp);

  return mp;
}

static size_t bt_leaf_size(struct btree *bt, struct btval *key,
                           struct btval *data) {
  size_t sz;

  sz = LEAFSIZE(key, data);
  if (data->size >= bt->head.psize / BT_MINKEYS) {
    /* put on overflow page */
    sz -= data->size - sizeof(pgno_t);
  }

  return sz + sizeof(indx_t);
}

static size_t bt_branch_size(struct btree *bt, struct btval *key) {
  return INDXSIZE(key) + sizeof(indx_t);
}

static int btree_write_overflow_data(struct btree *bt, struct page *p,
                                     struct btval *data) {
  size_t done = 0;
  size_t sz;
  size_t max;
  pgno_t *linkp; /* linked page stored here */
  struct mpage *next = NULL;

  max = bt->head.psize - PAGEHDRSZ;

  while (done < data->size) {
    if (next != NULL) {
      p = next->page;
    }
    linkp = &p->p_next_pgno;
    if (data->size - done > max) {
      /* need another overflow page */
      if ((next = btree_new_page(bt, P_OVERFLOW)) == NULL) {
        return BT_FAIL;
      }
      *linkp = next->pgno;
    } else {
      *linkp = 0; /* indicates end of list */
    }
    sz = data->size - done;
    if (sz > max) {
      sz = max;
    }
    memmove(p->ptrs, (char *)data->data + done, sz);
    done += sz;
  }

  return BT_SUCCESS;
}

/* Key prefix should already be stripped. */
static int btree_add_node(struct btree *bt, struct mpage *mp, indx_t indx,
                          struct btval *key, struct btval *data, pgno_t pgno,
                          uint8_t flags) {
  unsigned int i;
  size_t node_size = NODESIZE;
  indx_t ofs;
  struct node *node;
  struct page *p;
  struct mpage *ofp = NULL; /* overflow page */

  p = mp->page;

  if (key != NULL) {
    node_size += key->size;
  }

  if (IS_LEAF(mp)) {
    node_size += data->size;
    if (F_ISSET(flags, F_BIGDATA)) {
      /* Data already on overflow page. */
      node_size -= data->size - sizeof(pgno_t);
    } else if (data->size >= bt->head.psize / BT_MINKEYS) {
      /* Put data on overflow page. */
      node_size -= data->size - sizeof(pgno_t);
      if ((ofp = btree_new_page(bt, P_OVERFLOW)) == NULL) {
        return BT_FAIL;
      }
      flags |= F_BIGDATA;
    }
  }

  if (node_size + sizeof(indx_t) > SIZELEFT(mp)) {
    return BT_FAIL;
  }

  /* Move higher pointers up one slot. */
  for (i = NUMKEYS(mp); i > indx; i--) {
    p->ptrs[i] = p->ptrs[i - 1];
  }

  /* Adjust free space offsets. */
  ofs = p->upper - node_size;
  p->ptrs[indx] = ofs;
  p->upper = ofs;
  p->lower += sizeof(indx_t);

  /* Write the node data. */
  node = NODEPTR(mp, indx);
  node->ksize = (key == NULL) ? 0 : key->size;
  node->flags = flags;
  if (IS_LEAF(mp)) {
    node->n_dsize = data->size;
  } else {
    node->n_pgno = pgno;
  }

  if (key) {
    memmove(NODEKEY(node), key->data, key->size);
  }

  if (IS_LEAF(mp)) {
    if (ofp == NULL) {
      if (F_ISSET(flags, F_BIGDATA)) {
        memmove(node->data + key->size, data->data, sizeof(pgno_t));
      } else {
        memmove(node->data + key->size, data->data, data->size);
      }
    } else {
      memmove(node->data + key->size, &ofp->pgno, sizeof(pgno_t));
      if (btree_write_overflow_data(bt, ofp->page, data) == BT_FAIL) {
        return BT_FAIL;
      }
    }
  }

  return BT_SUCCESS;
}

static void btree_del_node(struct btree *bt, struct mpage *mp, indx_t indx) {
  unsigned int sz;
  indx_t i, j, numkeys, ptr;
  struct node *node;
  char *base;

  node = NODEPTR(mp, indx);
  sz = NODESIZE + node->ksize;
  if (IS_LEAF(mp)) {
    if (F_ISSET(node->flags, F_BIGDATA)) {
      sz += sizeof(pgno_t);
    } else {
      sz += NODEDSZ(node);
    }
  }

  ptr = mp->page->ptrs[indx];
  numkeys = NUMKEYS(mp);
  for (i = j = 0; i < numkeys; i++) {
    if (i != indx) {
      mp->page->ptrs[j] = mp->page->ptrs[i];
      if (mp->page->ptrs[i] < ptr) {
        mp->page->ptrs[j] += sz;
      }
      j++;
    }
  }

  base = (char *)mp->page + mp->page->upper;
  memmove(base + sz, base, ptr - mp->page->upper);

  mp->page->lower -= sizeof(indx_t);
  mp->page->upper += sz;
}

struct cursor *btree_txn_cursor_open(struct btree *bt, struct btree_txn *txn) {
  struct cursor *cursor;

  if (bt != NULL && txn != NULL && bt != txn->bt) {
    errno = EINVAL;
    return NULL;
  }

  if (bt == NULL) {
    if (txn == NULL) {
      errno = EINVAL;
      return NULL;
    }
    bt = txn->bt;
  }

  if ((cursor = calloc(1, sizeof(*cursor))) != NULL) {
    SLIST_INIT(&cursor->stack);
    cursor->bt = bt;
    cursor->txn = txn;
    btree_ref(bt);
  }

  return cursor;
}

void btree_cursor_close(struct cursor *cursor) {
  if (cursor != NULL) {
    while (!CURSOR_EMPTY(cursor)) {
      cursor_pop_page(cursor);
    }

    btree_close(cursor->bt);
    free(cursor);
  }
}

static int btree_update_key(struct btree *bt, struct mpage *mp, indx_t indx,
                            struct btval *key) {
  indx_t ptr, i, numkeys;
  int delta;
  size_t len;
  struct node *node;
  char *base;

  node = NODEPTR(mp, indx);
  ptr = mp->page->ptrs[indx];

  if (key->size != node->ksize) {
    delta = key->size - node->ksize;
    if (delta > 0 && SIZELEFT(mp) < delta) {
      return BT_FAIL;
    }

    numkeys = NUMKEYS(mp);
    for (i = 0; i < numkeys; i++) {
      if (mp->page->ptrs[i] <= ptr) {
        mp->page->ptrs[i] -= delta;
      }
    }

    base = (char *)mp->page + mp->page->upper;
    len = ptr - mp->page->upper + NODESIZE;
    memmove(base - delta, base, len);
    mp->page->upper -= delta;

    node = NODEPTR(mp, indx);
    node->ksize = key->size;
  }

  memmove(NODEKEY(node), key->data, key->size);

  return BT_SUCCESS;
}

static int btree_adjust_prefix(struct btree *bt, struct mpage *src, int delta) {
  indx_t i;
  struct node *node;
  struct btkey tmpkey;
  struct btval key;

  for (i = 0; i < NUMKEYS(src); i++) {
    node = NODEPTR(src, i);
    tmpkey.len = node->ksize - delta;
    if (delta > 0) {
      memmove(tmpkey.str, (char *)NODEKEY(node) + delta, tmpkey.len);
    } else {
      memmove(tmpkey.str, src->prefix.str + src->prefix.len + delta, -delta);
      memmove(tmpkey.str - delta, NODEKEY(node), node->ksize);
    }
    key.size = tmpkey.len;
    key.data = tmpkey.str;
    if (btree_update_key(bt, src, i, &key) != BT_SUCCESS) {
      return BT_FAIL;
    }
  }

  return BT_SUCCESS;
}

/* Move a node from src to dst. */
static int btree_move_node(struct btree *bt, struct mpage *src, indx_t srcindx,
                           struct mpage *dst, indx_t dstindx) {
  int rc;
  unsigned int pfxlen, mp_pfxlen = 0;
  struct node *srcnode;
  struct mpage *mp = NULL;
  struct btkey tmpkey, srckey;
  struct btval key, data;

  srcnode = NODEPTR(src, srcindx);

  find_common_prefix(bt, src);

  if (IS_BRANCH(src)) {
    /* Need to check if the page the moved node points to
     * changes prefix.
     */
    if ((mp = btree_get_mpage(bt, NODEPGNO(srcnode))) == NULL) {
      return BT_FAIL;
    }
    mp->parent = src;
    mp->parent_index = srcindx;
    find_common_prefix(bt, mp);
    mp_pfxlen = mp->prefix.len;
  }

  /* Mark src and dst as dirty. */
  if ((src = mpage_touch(bt, src)) == NULL ||
      (dst = mpage_touch(bt, dst)) == NULL) {
    return BT_FAIL;
  }

  find_common_prefix(bt, dst);

  /* Check if src node has destination page prefix. Otherwise the
   * destination page must expand its prefix on all its nodes.
   */
  srckey.len = srcnode->ksize;
  memmove(srckey.str, NODEKEY(srcnode), srckey.len);
  common_prefix(bt, &srckey, &dst->prefix, &tmpkey);
  if (tmpkey.len != dst->prefix.len) {
    if (btree_adjust_prefix(bt, dst, tmpkey.len - dst->prefix.len) !=
        BT_SUCCESS) {
      return BT_FAIL;
    }
    memmove(&dst->prefix, &tmpkey, sizeof(tmpkey));
  }

  if (srcindx == 0 && IS_BRANCH(src)) {
    struct mpage *low;

    /* must find the lowest key below src. */
    btree_search_page_root(bt, src, NULL, NULL, 0, &low);
    expand_prefix(bt, low, 0, &srckey);
  } else {
    srckey.len = srcnode->ksize;
    memmove(srckey.str, NODEKEY(srcnode), srcnode->ksize);
  }
  find_common_prefix(bt, src);

  /* expand the prefix */
  tmpkey.len = sizeof(tmpkey.str);
  concat_prefix(bt, src->prefix.str, src->prefix.len, srckey.str, srckey.len,
                tmpkey.str, &tmpkey.len);

  /* Add the node to the destination page. Adjust prefix for
   * destination page.
   */
  key.size = tmpkey.len;
  key.data = tmpkey.str;
  remove_prefix(bt, &key, dst->prefix.len);
  data.size = NODEDSZ(srcnode);
  data.data = NODEDATA(srcnode);
  rc = btree_add_node(bt, dst, dstindx, &key, &data, NODEPGNO(srcnode),
                      srcnode->flags);
  if (rc != BT_SUCCESS) {
    return rc;
  }

  /* Delete the node from the source page. */
  btree_del_node(bt, src, srcindx);

  /* Update the parent separators. */
  if (srcindx == 0 && src->parent_index != 0) {
    expand_prefix(bt, src, 0, &tmpkey);
    key.size = tmpkey.len;
    key.data = tmpkey.str;
    remove_prefix(bt, &key, src->parent->prefix.len);

    if (btree_update_key(bt, src->parent, src->parent_index, &key) !=
        BT_SUCCESS) {
      return BT_FAIL;
    }
  }

  if (srcindx == 0 && IS_BRANCH(src)) {
    struct btval nullkey;
    nullkey.size = 0;
    btree_update_key(bt, src, 0, &nullkey);
  }

  if (dstindx == 0 && dst->parent_index != 0) {
    expand_prefix(bt, dst, 0, &tmpkey);
    key.size = tmpkey.len;
    key.data = tmpkey.str;
    remove_prefix(bt, &key, dst->parent->prefix.len);

    if (btree_update_key(bt, dst->parent, dst->parent_index, &key) !=
        BT_SUCCESS) {
      return BT_FAIL;
    }
  }

  if (dstindx == 0 && IS_BRANCH(dst)) {
    struct btval nullkey;
    nullkey.size = 0;
    btree_update_key(bt, dst, 0, &nullkey);
  }

  /* We can get a new page prefix here!
   * Must update keys in all nodes of this page!
   */
  pfxlen = src->prefix.len;
  find_common_prefix(bt, src);
  if (src->prefix.len != pfxlen) {
    if (btree_adjust_prefix(bt, src, src->prefix.len - pfxlen) != BT_SUCCESS) {
      return BT_FAIL;
    }
  }

  pfxlen = dst->prefix.len;
  find_common_prefix(bt, dst);
  if (dst->prefix.len != pfxlen) {
    if (btree_adjust_prefix(bt, dst, dst->prefix.len - pfxlen) != BT_SUCCESS) {
      return BT_FAIL;
    }
  }

  if (IS_BRANCH(dst)) {
    mp->parent = dst;
    mp->parent_index = dstindx;
    find_common_prefix(bt, mp);
    if (mp->prefix.len != mp_pfxlen) {
      if ((mp = mpage_touch(bt, mp)) == NULL) {
        return BT_FAIL;
      }
      if (btree_adjust_prefix(bt, mp, mp->prefix.len - mp_pfxlen) !=
          BT_SUCCESS) {
        return BT_FAIL;
      }
    }
  }

  return BT_SUCCESS;
}

static int btree_merge(struct btree *bt, struct mpage *src, struct mpage *dst) {
  int rc;
  indx_t i;
  unsigned int pfxlen;
  struct node *srcnode;
  struct btkey tmpkey, dstpfx;
  struct btval key, data;

  /* Mark src and dst as dirty. */
  if ((src = mpage_touch(bt, src)) == NULL ||
      (dst = mpage_touch(bt, dst)) == NULL) {
    return BT_FAIL;
  }

  find_common_prefix(bt, src);
  find_common_prefix(bt, dst);

  /* Check if source nodes has destination page prefix. Otherwise
   * the destination page must expand its prefix on all its nodes.
   */
  common_prefix(bt, &src->prefix, &dst->prefix, &dstpfx);
  if (dstpfx.len != dst->prefix.len) {
    if (btree_adjust_prefix(bt, dst, dstpfx.len - dst->prefix.len) !=
        BT_SUCCESS) {
      return BT_FAIL;
    }
    memmove(&dst->prefix, &dstpfx, sizeof(dstpfx));
  }

  /* Move all nodes from src to dst. */
  for (i = 0; i < NUMKEYS(src); i++) {
    srcnode = NODEPTR(src, i);

    /* If branch node 0 (implicit key), find the real key. */
    if (i == 0 && IS_BRANCH(src)) {
      struct mpage *low;

      /* must find the lowest key below src. */
      btree_search_page_root(bt, src, NULL, NULL, 0, &low);
      expand_prefix(bt, low, 0, &tmpkey);
    } else {
      expand_prefix(bt, src, i, &tmpkey);
    }

    key.size = tmpkey.len;
    key.data = tmpkey.str;

    remove_prefix(bt, &key, dst->prefix.len);
    data.size = NODEDSZ(srcnode);
    data.data = NODEDATA(srcnode);
    rc = btree_add_node(bt, dst, NUMKEYS(dst), &key, &data, NODEPGNO(srcnode),
                        srcnode->flags);
    if (rc != BT_SUCCESS) {
      return rc;
    }
  }

  /* Unlink the src page from parent. */
  btree_del_node(bt, src->parent, src->parent_index);
  if (src->parent_index == 0) {
    key.size = 0;
    if (btree_update_key(bt, src->parent, 0, &key) != BT_SUCCESS) {
      return BT_FAIL;
    }

    pfxlen = src->prefix.len;
    find_common_prefix(bt, src);
  }

  if (IS_LEAF(src)) {
    bt->meta.leaf_pages--;
  } else {
    bt->meta.branch_pages--;
  }

  return btree_rebalance(bt, src->parent);
}

#define FILL_THRESHOLD 250

static int btree_rebalance(struct btree *bt, struct mpage *mp) {
  struct node *node;
  struct mpage *parent;
  struct mpage *root;
  struct mpage *neighbor = NULL;
  indx_t si = 0, di = 0;

  if (PAGEFILL(bt, mp) >= FILL_THRESHOLD) {
    return BT_SUCCESS;
  }

  parent = mp->parent;

  if (parent == NULL) {
    if (NUMKEYS(mp) == 0) {
      bt->txn->root = P_INVALID;
      bt->meta.depth--;
      bt->meta.leaf_pages--;
    } else if (IS_BRANCH(mp) && NUMKEYS(mp) == 1) {
      bt->txn->root = NODEPGNO(NODEPTR(mp, 0));
      if ((root = btree_get_mpage(bt, bt->txn->root)) == NULL) {
        return BT_FAIL;
      }
      root->parent = NULL;
      bt->meta.depth--;
      bt->meta.branch_pages--;
    }

    return BT_SUCCESS;
  }

  /* Leaf page fill factor is below the threshold.
   * Try to move keys from left or right neighbor, or
   * merge with a neighbor page.
   */

  /* Find neighbors.*/
  if (mp->parent_index == 0) {
    /* We're the leftmost leaf in our parent. */
    node = NODEPTR(parent, mp->parent_index + 1);
    if ((neighbor = btree_get_mpage(bt, NODEPGNO(node))) == NULL) {
      return BT_FAIL;
    }
    neighbor->parent_index = mp->parent_index + 1;
    si = 0;
    di = NUMKEYS(mp);
  } else {
    /* There is at least one neighbor to the left. */
    node = NODEPTR(parent, mp->parent_index - 1);
    if ((neighbor = btree_get_mpage(bt, NODEPGNO(node))) == NULL) {
      return BT_FAIL;
    }
    neighbor->parent_index = mp->parent_index - 1;
    si = NUMKEYS(neighbor) - 1;
    di = 0;
  }
  neighbor->parent = parent;

  /* If the neighbor page is above threshold and has at least two
   * keys, move one key from it.
   *
   * Otherwise we should try to merge them, but that might not be
   * possible, even if both are below threshold, as prefix expansion
   * might make keys larger. FIXME: detect this
   */
  if (PAGEFILL(bt, neighbor) >= FILL_THRESHOLD && NUMKEYS(neighbor) >= 2) {
    return btree_move_node(bt, neighbor, si, mp, di);
  } else { /* FIXME: if (has_enough_room()) */
    if (mp->parent_index == 0) {
      return btree_merge(bt, neighbor, mp);
    } else {
      return btree_merge(bt, mp, neighbor);
    }
  }
}

int btree_txn_del(struct btree *bt, struct btree_txn *txn, struct btval *key,
                  struct btval *data) {
  int rc, exact, close_txn = 0;
  unsigned int ki;
  struct node *leaf;
  struct mpage *mp;

  if (bt != NULL && txn != NULL && bt != txn->bt) {
    errno = EINVAL;
    return BT_FAIL;
  }

  if (txn != NULL && F_ISSET(txn->flags, BT_TXN_RDONLY)) {
    errno = EINVAL;
    return BT_FAIL;
  }

  if (bt == NULL) {
    if (txn == NULL) {
      errno = EINVAL;
      return BT_FAIL;
    }
    bt = txn->bt;
  }

  if (key->size == 0 || key->size > MAXKEYSIZE) {
    errno = EINVAL;
    return BT_FAIL;
  }

  if (txn == NULL) {
    close_txn = 1;
    if ((txn = btree_txn_begin(bt, 0)) == NULL) {
      return BT_FAIL;
    }
  }

  if ((rc = btree_search_page(bt, txn, key, NULL, 1, &mp)) != BT_SUCCESS)
    goto done;

  leaf = btree_search_node(bt, mp, key, &exact, &ki);
  if (leaf == NULL || !exact) {
    errno = ENOENT;
    rc = BT_FAIL;
    goto done;
  }

  if (data && (rc = btree_read_data(bt, NULL, leaf, data)) != BT_SUCCESS) {
    goto done;
  }

  btree_del_node(bt, mp, ki);
  bt->meta.entries--;
  rc = btree_rebalance(bt, mp);
  if (rc != BT_SUCCESS) {
    txn->flags |= BT_TXN_ERROR;
  }

done:
  if (close_txn) {
    if (rc == BT_SUCCESS) {
      rc = btree_txn_commit(txn);
    } else {
      btree_txn_abort(txn);
    }
  }
  mpage_prune(bt);
  return rc;
}

/* Reduce the length of the prefix separator <sep> to the minimum length that
 * still makes it uniquely distinguishable from <min>.
 *
 * <min> is guaranteed to be sorted less than <sep>
 *
 * On return, <sep> is modified to the minimum length.
 */
static void bt_reduce_separator(struct btree *bt, struct node *min,
                                struct btval *sep) {
  size_t n = 0;
  char *p1;
  char *p2;

  p1 = (char *)NODEKEY(min);
  p2 = (char *)sep->data;

  while (*p1 == *p2) {
    p1++;
    p2++;
    n++;
    if (n == min->ksize || n == sep->size) {
      break;
    }
  }

  sep->size = n + 1;
}

/* Split page <*mpp>, and insert <key,(data|newpgno)> in either left or
 * right sibling, at index <*newindxp> (as if unsplit). Updates *mpp and
 * *newindxp with the actual values after split, ie if *mpp and *newindxp
 * refer to a node in the new right sibling page.
 */
static int btree_split(struct btree *bt, struct mpage **mpp,
                       unsigned int *newindxp, struct btval *newkey,
                       struct btval *newdata, pgno_t newpgno) {
  uint8_t flags;
  int rc = BT_SUCCESS, ins_new = 0;
  indx_t newindx;
  pgno_t pgno = 0;
  size_t orig_pfx_len, left_pfx_diff, right_pfx_diff, pfx_diff;
  unsigned int i, j, split_indx;
  struct node *node;
  struct mpage *pright, *p, *mp;
  struct btval sepkey, rkey, rdata;
  struct btkey tmpkey;
  struct page *copy;

  mp = *mpp;
  newindx = *newindxp;

  orig_pfx_len = mp->prefix.len;

  if (mp->parent == NULL) {
    if ((mp->parent = btree_new_page(bt, P_BRANCH)) == NULL) {
      return BT_FAIL;
    }
    mp->parent_index = 0;
    bt->txn->root = mp->parent->pgno;
    bt->meta.depth++;

    /* Add left (implicit) pointer. */
    if (btree_add_node(bt, mp->parent, 0, NULL, NULL, mp->pgno, 0) !=
        BT_SUCCESS) {
      return BT_FAIL;
    }
  }

  /* Create a right sibling. */
  if ((pright = btree_new_page(bt, mp->page->flags)) == NULL) {
    return BT_FAIL;
  }
  pright->parent = mp->parent;
  pright->parent_index = mp->parent_index + 1;

  /* Move half of the keys to the right sibling. */
  if ((copy = malloc(bt->head.psize)) == NULL) {
    return BT_FAIL;
  }
  memmove(copy, mp->page, bt->head.psize);
  memset(&mp->page->ptrs, 0, bt->head.psize - PAGEHDRSZ);
  mp->page->lower = PAGEHDRSZ;
  mp->page->upper = bt->head.psize;

  split_indx = NUMKEYSP(copy) / 2 + 1;

  /* First find the separating key between the split pages. */
  memset(&sepkey, 0, sizeof(sepkey));
  if (newindx == split_indx) {
    sepkey.size = newkey->size;
    sepkey.data = newkey->data;
    remove_prefix(bt, &sepkey, mp->prefix.len);
  } else {
    node = NODEPTRP(copy, split_indx);
    sepkey.size = node->ksize;
    sepkey.data = NODEKEY(node);
  }

  if (IS_LEAF(mp)) {
    /* Find the smallest separator. */
    /* Ref: Prefix B-trees, R. Bayer, K. Unterauer, 1977 */
    node = NODEPTRP(copy, split_indx - 1);
    bt_reduce_separator(bt, node, &sepkey);
  }

  /* Fix separator wrt parent prefix. */
  tmpkey.len = sizeof(tmpkey.str);
  concat_prefix(bt, mp->prefix.str, mp->prefix.len, sepkey.data, sepkey.size,
                tmpkey.str, &tmpkey.len);
  sepkey.data = tmpkey.str;
  sepkey.size = tmpkey.len;

  /* Copy separator key to the parent. */
  if (SIZELEFT(pright->parent) < bt_branch_size(bt, &sepkey)) {
    rc = btree_split(bt, &pright->parent, &pright->parent_index, &sepkey, NULL,
                     pright->pgno);

    /* Right page might now have changed parent.
     * Check if left page also changed parent.
     */
    if (pright->parent != mp->parent &&
        mp->parent_index >= NUMKEYS(mp->parent)) {
      mp->parent = pright->parent;
      mp->parent_index = pright->parent_index - 1;
    }
  } else {
    remove_prefix(bt, &sepkey, pright->parent->prefix.len);
    rc = btree_add_node(bt, pright->parent, pright->parent_index, &sepkey, NULL,
                        pright->pgno, 0);
  }
  if (rc != BT_SUCCESS) {
    free(copy);
    return BT_FAIL;
  }

  /* Update prefix for right and left page, if the parent was split. */
  find_common_prefix(bt, pright);
  right_pfx_diff = pright->prefix.len - orig_pfx_len;

  find_common_prefix(bt, mp);
  left_pfx_diff = mp->prefix.len - orig_pfx_len;

  for (i = j = 0; i <= NUMKEYSP(copy); j++) {
    if (i < split_indx) {
      /* Re-insert in left sibling. */
      p = mp;
      pfx_diff = left_pfx_diff;
    } else {
      /* Insert in right sibling. */
      if (i == split_indx)
        /* Reset insert index for right sibling. */
        j = (i == newindx && ins_new);
      p = pright;
      pfx_diff = right_pfx_diff;
    }

    if (i == newindx && !ins_new) {
      /* Insert the original entry that caused the split. */
      rkey.data = newkey->data;
      rkey.size = newkey->size;
      if (IS_LEAF(mp)) {
        rdata.data = newdata->data;
        rdata.size = newdata->size;
      } else {
        pgno = newpgno;
      }
      flags = 0;
      pfx_diff = p->prefix.len;

      ins_new = 1;

      /* Update page and index for the new key. */
      *newindxp = j;
      *mpp = p;
    } else if (i == NUMKEYSP(copy)) {
      break;
    } else {
      node = NODEPTRP(copy, i);
      rkey.data = NODEKEY(node);
      rkey.size = node->ksize;
      if (IS_LEAF(mp)) {
        rdata.data = NODEDATA(node);
        rdata.size = node->n_dsize;
      } else {
        pgno = node->n_pgno;
      }
      flags = node->flags;

      i++;
    }

    if (!IS_LEAF(mp) && j == 0) {
      /* First branch index doesn't need key data. */
      rkey.size = 0;
    } else {
      remove_prefix(bt, &rkey, pfx_diff);
    }

    rc = btree_add_node(bt, p, j, &rkey, &rdata, pgno, flags);
  }

  free(copy);
  return rc;
}

int btree_txn_put(struct btree *bt, struct btree_txn *txn, struct btval *key,
                  struct btval *data) {
  int rc = BT_SUCCESS, exact, close_txn = 0;
  unsigned int ki;
  struct node *leaf;
  struct mpage *mp;
  struct btval xkey;

  if (bt != NULL && txn != NULL && bt != txn->bt) {
    errno = EINVAL;
    return BT_FAIL;
  }

  if (txn != NULL && F_ISSET(txn->flags, BT_TXN_RDONLY)) {
    errno = EINVAL;
    return BT_FAIL;
  }

  if (bt == NULL) {
    if (txn == NULL) {
      errno = EINVAL;
      return BT_FAIL;
    }
    bt = txn->bt;
  }

  if (key->size == 0 || key->size > MAXKEYSIZE) {
    errno = EINVAL;
    return BT_FAIL;
  }

  if (txn == NULL) {
    close_txn = 1;
    if ((txn = btree_txn_begin(bt, 0)) == NULL) {
      return BT_FAIL;
    }
  }

  rc = btree_search_page(bt, txn, key, NULL, 1, &mp);
  if (rc == BT_SUCCESS) {
    leaf = btree_search_node(bt, mp, key, &exact, &ki);
    if (leaf && exact) {
      btree_del_node(bt, mp, ki);
    }
    if (leaf == NULL) { /* append if not found */
      ki = NUMKEYS(mp);
    }
  } else if (errno == ENOENT) {
    /* new file, just write a root leaf page */
    if ((mp = btree_new_page(bt, P_LEAF)) == NULL) {
      rc = BT_FAIL;
      goto done;
    }
    txn->root = mp->pgno;
    bt->meta.depth++;
    ki = 0;
  } else
    goto done;

  /* Copy the key pointer as it is modified by the prefix code. The
   * caller might have malloc'ed the data.
   */
  xkey.data = key->data;
  xkey.size = key->size;

  if (SIZELEFT(mp) < bt_leaf_size(bt, key, data)) {
    rc = btree_split(bt, &mp, &ki, &xkey, data, P_INVALID);
  } else {
    /* There is room already in this leaf page. */
    remove_prefix(bt, &xkey, mp->prefix.len);
    rc = btree_add_node(bt, mp, ki, &xkey, data, 0, 0);
  }

  if (rc != BT_SUCCESS) {
    txn->flags |= BT_TXN_ERROR;
  } else {
    bt->meta.entries++;
  }

done:
  if (close_txn) {
    if (rc == BT_SUCCESS) {
      rc = btree_txn_commit(txn);
    } else {
      btree_txn_abort(txn);
    }
  }
  mpage_prune(bt);
  return rc;
}

static pgno_t btree_compact_tree(struct btree *bt, pgno_t pgno,
                                 struct btree *btc) {
  ssize_t rc;
  indx_t i;
  pgno_t *pnext, next;
  struct node *node;
  struct page *p;
  struct mpage *mp;

  /* Get the page and make a copy of it. */
  if ((mp = btree_get_mpage(bt, pgno)) == NULL) {
    return P_INVALID;
  }
  if ((p = malloc(bt->head.psize)) == NULL) {
    return P_INVALID;
  }
  memmove(p, mp->page, bt->head.psize);

  /* Go through all nodes in the (copied) page and update the
   * page pointers.
   */
  if (F_ISSET(p->flags, P_BRANCH)) {
    for (i = 0; i < NUMKEYSP(p); i++) {
      node = NODEPTRP(p, i);
      node->n_pgno = btree_compact_tree(bt, node->n_pgno, btc);
      if (node->n_pgno == P_INVALID) {
        free(p);
        return P_INVALID;
      }
    }
  } else if (F_ISSET(p->flags, P_LEAF)) {
    for (i = 0; i < NUMKEYSP(p); i++) {
      node = NODEPTRP(p, i);
      if (F_ISSET(node->flags, F_BIGDATA)) {
        memmove(&next, NODEDATA(node), sizeof(next));
        next = btree_compact_tree(bt, next, btc);
        if (next == P_INVALID) {
          free(p);
          return P_INVALID;
        }
        memmove(NODEDATA(node), &next, sizeof(next));
      }
    }
  } else if (F_ISSET(p->flags, P_OVERFLOW)) {
    pnext = &p->p_next_pgno;
    if (*pnext > 0) {
      *pnext = btree_compact_tree(bt, *pnext, btc);
      if (*pnext == P_INVALID) {
        free(p);
        return P_INVALID;
      }
    }
  }

  pgno = p->pgno = btc->txn->next_pgno++;
  rc = write(btc->fd, p, bt->head.psize);
  free(p);
  if (rc != (ssize_t)bt->head.psize) {
    return P_INVALID;
  }
  mpage_prune(bt);
  return pgno;
}

int btree_compact(struct btree *bt) {
  char *compact_path = NULL;
  size_t compact_path_size;
  struct btree *btc;
  struct btree_txn *txn, *txnc = NULL;
  int fd;
  pgno_t root;

  if (bt->path == NULL) {
    errno = EINVAL;
    return BT_FAIL;
  }

  if ((txn = btree_txn_begin(bt, 0)) == NULL) {
    return BT_FAIL;
  }

  compact_path_size = strlen(bt->path) + strlen(".compact.XXXXXX") + 1;
  compact_path = malloc(compact_path_size);

  if (sprintf(compact_path, "%s.compact.XXXXXX", bt->path) == -1) {
    btree_txn_abort(txn);
    return BT_FAIL;
  }
  fd = mkstemp(compact_path);
  if (fd == -1) {
    free(compact_path);
    btree_txn_abort(txn);
    return BT_FAIL;
  }

  if ((btc = btree_open_fd(fd, 0)) == NULL) {
    goto failed;
  }
  memmove(&btc->meta, &bt->meta, sizeof(bt->meta));
  btc->meta.revisions = 0;

  if ((txnc = btree_txn_begin(btc, 0)) == NULL) {
    goto failed;
  }

  if (bt->meta.root != P_INVALID) {
    root = btree_compact_tree(bt, bt->meta.root, btc);
    if (root == P_INVALID) {
      goto failed;
    }
    if (btree_write_meta(btc, root, 0) != BT_SUCCESS) {
      goto failed;
    }
  }

  fsync(fd);

  if (rename(compact_path, bt->path) != 0) {
    goto failed;
  }

  /* Write a "tombstone" meta page so other processes can pick up
   * the change and re-open the file.
   */
  if (btree_write_meta(bt, P_INVALID, BT_TOMBSTONE) != BT_SUCCESS) {
    goto failed;
  }

  btree_txn_abort(txn);
  btree_txn_abort(txnc);
  free(compact_path);
  btree_close(btc);
  mpage_prune(bt);
  return 0;

failed:
  btree_txn_abort(txn);
  btree_txn_abort(txnc);
  unlink(compact_path);
  free(compact_path);
  btree_close(btc);
  mpage_prune(bt);
  return BT_FAIL;
}

void btree_set_cache_size(struct btree *bt, unsigned int cache_size) {
  bt->max_cache = cache_size;
}