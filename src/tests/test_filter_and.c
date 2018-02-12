#include <stdlib.h>
#include <stdio.h>

#include <bson.h>
#include <mongoc.h>

#include "backend.h"
#include "filter.h"

static void
print_stat(struct stat stat)
{
    printf("stat(dev='%ld', ino='%ld', mode='%d', nlink='%ld', uid='%d', gid='%d', rdev='%ld', size='%ld', blksize='%ld', blocks='%ld', atime='%ld', mtime='%ld', ctime='%ld')\n",
           stat.st_dev, stat.st_ino, stat.st_mode, stat.st_nlink, stat.st_uid,
           stat.st_gid, stat.st_rdev, stat.st_size, stat.st_blksize,
           stat.st_blocks, stat.st_atime, stat.st_mtime, stat.st_ctime);
}

int main(int argc, char *argv[])
{
    struct filter *filters[4];
    struct filter *filter;
    struct fsentry_iterator *iterator;
    struct fsentry *fsentry;

    backend_init(mongodb, 2, "mongodb://localhost:27017", "scratch");

    filters[0] = filter_compare_regex(NAME, 0, "a");
    filters[1] = filter_compare_regex(NAME, 0, "b");
    filters[2] = filter_compare_regex(NAME, 0, "c");
    filters[3] = filter_compare_regex(NAME, 0, "d");
    filter = filter_and(4, filters);

    iterator = list_fsentries(mongodb, NULL, filter);

    while ((fsentry = fsentry_next(iterator))) {
        char uuid[37];
        struct stat stat;

        uuid_unparse(fsentry->uuid, uuid);
        printf("uuid='%s', name='%s'\n", uuid, fsentry_name(fsentry));
        fsentry_stat(fsentry, &stat);
        print_stat(stat);

        assert(fsentry_exists(fsentry));

        fsentry_free(fsentry);
    }

    filter_free(filter);

    backend_exit(mongodb);

    return EXIT_SUCCESS;
}
