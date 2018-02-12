#ifndef __FSENTRY_H__
#define __FSENTRY_H__

#include "filter.h"

struct fsentry;

struct fsentry_operations {
	bool (*exists)(struct fsentry *fsentry);
	void (*free)(struct fsentry *fsentry);
};

struct backend;

struct fsentry {
	const struct backend *backend;
	char *name;
	const struct fsentry_operations *op;
};

/** fsentry_exists - does the fsentry exist?
 * @param fsentry	the fsentry to test
 *
 * @return		true if the fsentry exists in its backend,
 *			false otherwise
 *
 * Note that this checks the existence of an fsentry _according to its backend_
 */
static inline bool
fsentry_exists(struct fsentry *fsentry)
{
	return fsentry->op->exists(fsentry);
}

/** fsentry_free - cleanup an fsentry
 * @param fsentry	the fsentry to free
 */
static inline void
fsentry_free(struct fsentry *fsentry)
{
	fsentry->op->free(fsentry);
}

struct fsentry_iterator {
	struct fsentry *(*next)(struct fsentry_iterator *iterator);
};

/** fsentry_next - return the next fsentry from the iterator
 * @param iterator	the iterator to traverse
 *
 * @return		the next fsentry, or NULL if the iterator is exhausted
 *
 * The iterator is automatically freed when it is exhausted, this means that
 * any call to fsentry_next() on an iterator that already returned NULL once is
 * undefined behaviour.
 */
static inline struct fsentry *
fsentry_next(struct fsentry_iterator *iterator)
{
	return iterator->next(iterator);
}

#endif
