#ifndef __FSENTRY_H__
#define __FSENTRY_H__

#include <errno.h>

#include <sys/stat.h>
#include <uuid/uuid.h>

#include "filter.h"

struct fsentry;

struct fsentry_operations {
    const char *(*get_name)(struct fsentry *fsentry);
    int (*stat)(struct fsentry *fsentry, struct stat *stat);
    int (*hydrate)(struct fsentry *fsentry);
    bool (*exists)(struct fsentry *fsentry);
    void (*free)(struct fsentry *fsentry);
};

struct fsentry {
    uuid_t uuid;
    const struct fsentry_operations *op;
};

/** fsentry_name - get the name of an fsentry
 * @param fsentry   the fsentry whose name to get
 *
 * @return          the name of the fsentry
 */
static inline const char *
fsentry_name(struct fsentry *fsentry)
{
    return fsentry->op->get_name(fsentry);
}

/** fsentry_stat - get a struct stat referring to the fsentry
 * @param fsentry   the fsentry whose struct stat to get
 * @param stat      a pointer to a struct stat to fill
 *
 * @return          < 0 on failure
 */
static inline int
fsentry_stat(struct fsentry *fsentry, struct stat *stat)
{

    return fsentry->op->stat(fsentry, stat);
}
/** fsentry_hydrate - update an fsentry's info from the backend
 * @param fsentry   the fsentry to hydrate
 *
 * @return          < 0 on failure
 */
static inline int
fsentry_hydrate(struct fsentry *fsentry)
{
    if (fsentry->op->hydrate == NULL)
        return -ENOTSUP;

    return fsentry->op->hydrate(fsentry);
}

/** fsentry_exists - does the fsentry exist?
 * @param fsentry   the fsentry to test
 *
 * @return          true if the fsentry exists in its backend, false otherwise
 *
 * Note that this checks the existence of an fsentry _according to its backend_
 */
static inline bool
fsentry_exists(struct fsentry *fsentry)
{
    return fsentry->op->exists(fsentry);
}

/** fsentry_free - cleanup an fsentry
 * @param fsentry   the fsentry to free
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
 * @param iterator  the iterator to traverse
 *
 * @return          the next fsentry, or NULL if the iterator is exhausted
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
