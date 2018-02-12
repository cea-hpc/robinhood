#include <stdlib.h>
#include <stdio.h>

#include <bson.h>
#include <mongoc.h>

#include "backend.h"
#include "filter.h"

int main(int argc, char *argv[])
{
	struct filter *filter;
	struct fsentry_iterator *iterator;
	struct fsentry *fsentry;

	backend_init(mongodb, 2, "mongodb://localhost:27017", "scratch");

	filter = filter_and(4,
			filter_compare(REGEX, NAME, 0, "a"),
			filter_compare(REGEX, NAME, 0, "b"),
			filter_compare(REGEX, NAME, 0, "c"),
			filter_compare(REGEX, NAME, 0, "d")
			);

	iterator = list_fsentries(mongodb, NULL, filter);

	while ((fsentry = fsentry_next(iterator))) {
		printf("%s\n", fsentry->name);
		fsentry_free(fsentry);
	}

	filter_free(filter);

	backend_exit(mongodb);

	return EXIT_SUCCESS;
}
