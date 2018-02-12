#ifndef __BACKEND_H__
#define __BACKEND_H__

#include <stdarg.h>

#include "filter.h"
#include "fsentry.h"

struct backend_operations {
    int (*init)(const struct backend *backend, int num, va_list args);
    void (*exit)(const struct backend *backend);
    struct fsentry_iterator *(*list_fsentries)(const struct backend *backend,
                                               const struct fsentry *root,
                                               const struct filter *filter);
};

struct backend {
    const char *name;
    const struct backend_operations *op;
};

/** backend_init - initialize a backend
 * @param backend   the struct backend to initialize
 * @param num       the number of arguments to follow
 * @param ...       cf. the backend's documentation
 *
 * @return          0 on success, < 0 otherwise
 */
static inline int
backend_init(const struct backend *backend, int num, ...)
{
    va_list args;
    int rc;

    va_start(args, num);
    rc = backend->op->init(backend, num, args);
    va_end(args);

    return rc;
}

/** backend_exit - cleanup a backend
 * @param backend   the struct backend to cleanup
 */
static inline void
backend_exit(const struct backend *backend)
{
    return backend->op->exit(backend);
}

/** list_fsentries - list fsentries that match certain criteria
 * @param backend   the backend on which to operate
 * @param root      the root path under which to look for
 * @param filter    the set of criteria that the fsentries must match
 *
 * @return          an iterator of struct fsentry
 */
static inline struct fsentry_iterator *
list_fsentries(const struct backend *backend, const struct fsentry *root,
               const struct filter *filter)
{
    return backend->op->list_fsentries(backend, root, filter);
}

extern const struct backend *mongodb;

#endif
