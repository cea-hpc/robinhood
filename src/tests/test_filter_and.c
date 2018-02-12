#include <stdlib.h>
#include <stdio.h>

#include <bson.h>
#include <mongoc.h>

#include "backend.h"
#include "filter.h"

int main(int argc, char *argv[])
{
    struct filter *filters[4];
    struct filter *filter;
    struct fsentry_iterator *iterator;
    struct fsentry *fsentry;

    backend_init(mongodb, 2, "mongodb://localhost:27017", "scratch");

    filters[0] = filter_compare(REGEX, NAME, 0, "a");
    filters[1] = filter_compare(REGEX, NAME, 0, "b");
    filters[2] = filter_compare(REGEX, NAME, 0, "c");
    filters[3] = filter_compare(REGEX, NAME, 0, "d");
    filter = filter_and(4, filters);

    iterator = list_fsentries(mongodb, NULL, filter);

    while ((fsentry = fsentry_next(iterator))) {
        printf("%s\n", fsentry->name);
        fsentry_free(fsentry);
    }

    filter_free(filter);

    backend_exit(mongodb);

    return EXIT_SUCCESS;
}
