#pragma once

#include <mutex>
#include <malloc.h>
#include <assert.h>
#include <string.h>

#define CL_SFT 6
#define CL_SIZE (1 << CL_SFT)

#define atomic_xadd(P, V) __sync_fetch_and_add((P), (V))
#define cmpxchg(P, O, N) __sync_bool_compare_and_swap((P), (O), (N))
#define atomic_inc(P) __sync_add_and_fetch((P), 1)
#define atomic_dec(P) __sync_add_and_fetch((P), -1)
#define atomic_add(P, V) __sync_add_and_fetch((P), (V))
#define atomic_set_bit(P, V) __sync_or_and_fetch((P), 1 << (V))
#define atomic_clear_bit(P, V) __sync_and_and_fetch((P), ~(1 << (V)))

#define ALIGN_UP(a, siz) (((a) + (siz)-1) & (~((siz)-1)))
#define ALIGN_DOWN(a, siz) ((a) & (~((siz)-1)))

namespace kv {

static inline bool likely(bool x) { return __builtin_expect((x), true); }

static inline bool unlikely(bool x) { return __builtin_expect((x), false); }

static inline void *safe_align(size_t siz, size_t ali, bool clear)
{
	void *mem;
	mem = memalign(ali, siz);
	assert(likely(mem != NULL));
	if (clear)
		memset(mem, 0, siz);
	return mem;
}

struct bitmap
{
	unsigned long cnt, free_cnt, siz;
	unsigned long data[0];
};

// bitmap大小不要超过100000,否则性能很差
static inline struct bitmap *create_bitmap(unsigned long cnt)
{
	struct bitmap *bp;
	unsigned long siz;
	siz = ALIGN_UP(cnt, 64);
	bp = (struct bitmap *)safe_align(sizeof(bitmap) + (siz / 64) * sizeof(unsigned long), CL_SIZE, true);
	bp->cnt = cnt;
	bp->free_cnt = cnt;
	for (unsigned long i = cnt; i < siz; i++)
		bp->data[i >> 6] |= 1UL << (i & 63);
	bp->siz = siz;
	return bp;
}

static inline bool bitmap_full(struct bitmap *bp)
{
	return bp->free_cnt == 0;
}

static inline int get_free(struct bitmap *bp)
{
	unsigned long tot, i, ii, j;
	unsigned long old_free_cnt, old_val;
	do
	{
		old_free_cnt = bp->free_cnt;
		if (unlikely(old_free_cnt == 0))
			return -1;
	} while (unlikely(!cmpxchg(&bp->free_cnt, old_free_cnt, old_free_cnt - 1)));

	tot = bp->siz / 64;
	for (i = 0; i < tot; i++)
	{
		for (;;)
		{
			old_val = bp->data[i];
			if (old_val == (unsigned long)-1)
				break;
			j = __builtin_ffsl(old_val + 1) - 1;
			if (cmpxchg(&bp->data[i], old_val, old_val | (1UL << j)))
				return (i << 6) | j;
		}
	}
	assert(false);
	return 0;
}

static inline void put_back(struct bitmap *bp, int bk)
{
	unsigned long old_val;
	assert((bp->data[bk >> 6] >> (bk & 63)) & 1);
	do
	{
		old_val = bp->data[bk >> 6];
	} while (unlikely(!cmpxchg(&bp->data[bk >> 6], old_val, old_val ^ (1UL << (bk & 63)))));
	atomic_inc(&bp->free_cnt);
}

}