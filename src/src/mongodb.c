#include <assert.h>
#include <stdlib.h>
#include <stdbool.h>

#include <bson.h>
#include <mongoc.h>

#include "backend.h"

struct mongo_backend {
	const struct backend backend;
	mongoc_client_t *client;
	mongoc_database_t *db;
};

/** mongo_init - sets up a struct mongo_backend
 *
 * @param backend	the mongodb backend
 * @param num		the number of arguments to come
 * @param uri		(const char *) a uri pointing to a mongodb instance
 * @param database	(const char *) the name of the database to use
 */
static int
mongo_init(const struct backend *backend, int num, va_list args)
{
	struct mongo_backend *mongo = (struct mongo_backend *)backend;

	mongoc_init();

	assert(num == 2);

	mongo->client = mongoc_client_new(va_arg(args, const char *));
	mongo->db = mongoc_client_get_database(mongo->client,
					       va_arg(args, const char *));

	return 0;
}

static void
mongo_exit(const struct backend *backend)
{
	struct mongo_backend *mongo = (struct mongo_backend *)backend;

	mongoc_database_destroy(mongo->db);
	mongoc_client_destroy(mongo->client);
	mongoc_cleanup();
}

struct mongo_fsentry {
	struct fsentry fsentry;
};

static bool
mongo_fsentry_exists(struct fsentry *fsentry)
{
	// TODO
	return true;
}

static void
mongo_fsentry_free(struct fsentry *fsentry)
{
	struct mongo_fsentry *mongo_fsentry = (struct mongo_fsentry *)fsentry;

	free(mongo_fsentry->fsentry.name);
	free(mongo_fsentry);
}

static const struct fsentry_operations mongo_fsentry_operations = {
	.exists	= mongo_fsentry_exists,
	.free	= mongo_fsentry_free,
};

static struct mongo_fsentry *
mongo_fsentry_from_bson(const bson_t *doc)
{
	struct mongo_fsentry *fsentry;
	bson_iter_t iter;

	fsentry = malloc(sizeof(*fsentry));
	if (fsentry == NULL)
		goto out;

	fsentry->fsentry.op = &mongo_fsentry_operations;

	bson_iter_init(&iter, doc);
	while (bson_iter_next(&iter)) {
		const char *key = bson_iter_key(&iter);
		const bson_value_t *value = bson_iter_value(&iter);

		if (strcmp(key, "name") == 0) {
			size_t name_len = value->value.v_utf8.len;
			char *name;

			assert(BSON_ITER_HOLDS_UTF8(&iter));

			name = malloc(name_len + 1);
			if (name == NULL)
				goto out_free;

			strncpy(name, value->value.v_utf8.str, name_len);
			name[name_len] = 0;

			fsentry->fsentry.name = name;
		}
	}

	return fsentry;

out_free:
	free(fsentry);
out:
	return NULL;
}

struct mongo_iterator {
	struct fsentry_iterator iterator;
	mongoc_cursor_t *cursor;
};

static struct fsentry *
mongo_iterator_next(struct fsentry_iterator *iterator)
{
	struct mongo_iterator *iter = (struct mongo_iterator *)iterator;
	const bson_t *doc;

	while (mongoc_cursor_next(iter->cursor, &doc)) {
		struct mongo_fsentry *fsentry;

		fsentry = mongo_fsentry_from_bson(doc);
		if (fsentry == NULL)
			break;

		return &fsentry->fsentry;
	}

	mongoc_cursor_destroy(iter->cursor);
	free(iterator);
	return NULL;
}

static void
mongo_iterator_init(struct mongo_iterator *iterator)
{
	iterator->iterator.next = mongo_iterator_next;
}

static const char *
mongo_operator_to_str(enum filter_operator operator)
{
	switch(operator) {
	case EQUAL:
		return "$eq";
	case NOT_EQUAL:
		return "$ne";
	case LOWER_THAN:
		return "$lt";
	case LOWER_OR_EQUAL:
		return "$le";
	case GREATER_THAN:
		return "$gt";
	case GREATER_OR_EQUAL:
		return "$ge";
	case IN:
		return "$in";
	case NOT_IN:
		return "$nin";
	case REGEX:
		return "$regex";
	case NOT_REGEX:
		return "$regex"; /* This is not a mistake */
	case AND:
		return "$and";
	case OR:
		return "$or";
	}
	assert(false);
}

static bson_t *
bson_from_filter_list(const struct filter *filter)
{
	bson_t *bson = bson_new(), *array = bson_new();

	for (size_t i = 0; i < filter->compare.value.list.size; i++) {
		const char *key;
		char str[16];

		bson_uint32_to_string(i, &key, str, sizeof(str));
		BSON_APPEND_UTF8(array, key,
			         filter->compare.value.list.values[i]);
	}

	BSON_APPEND_ARRAY(bson, mongo_operator_to_str(filter->op), array);

	return bson;
}

static bson_t *
bson_from_filter_regex(const struct filter *filter)
{
	bson_t *bson = bson_new();
	unsigned int regex_options = filter->compare.value.regex.options;
	char mongo_regex_options[6] = { '\0' };
	size_t i = 0;

	if (regex_options & CASE_INSENSITIVE)
		mongo_regex_options[i++] = 'i';

	BSON_APPEND_REGEX(bson, mongo_operator_to_str(filter->op),
		          filter->compare.value.regex.value,
			  mongo_regex_options);

	/* NOT_REGEX is special cased as the operator does not
	 * exist per-se */
	if (filter->op == NOT_REGEX) {
		bson_t *tmp = bson_new();

		BSON_APPEND_DOCUMENT(tmp, "$not", bson);
		bson = tmp;
	}

	return bson;
}

static bson_t *
bson_from_filter_string(const struct filter *filter)
{
	bson_t *bson = bson_new();

	BSON_APPEND_UTF8(bson, mongo_operator_to_str(filter->op),
			 filter->compare.value.value);

	return bson;
}

static bson_t *
mongo_query_from_name_filter(const struct filter *filter)
{
	bson_t *query = bson_new(), *comparison;

	switch(filter->op) {
	case EQUAL: /* Intentional fallthrough */
	case NOT_EQUAL:
		comparison = bson_from_filter_string(filter);
		break;
	case REGEX: /* Intentional fallthrough */
	case NOT_REGEX:
		comparison = bson_from_filter_regex(filter);
		break;
	case IN: /* Intentional fallthrough */
	case NOT_IN:
		comparison = bson_from_filter_list(filter);
		break;
	default:
		assert(false);
	}

	BSON_APPEND_DOCUMENT(query, "name", comparison);

	return query;
}

static bson_t *
__mongo_query_from_filter(const struct filter *filter)
{
	switch(filter->compare.field) {
	case NAME:
		return mongo_query_from_name_filter(filter);
	case PATH:
		// TODO
	default:
		return bson_new();
	}
}

static bson_t *
mongo_query_from_filter(const struct filter *filter)
{
	bson_t *query, *array;

	if (is_comparison_operator(filter->op))
		return __mongo_query_from_filter(filter);

	array = bson_new();
	for(uint32_t i = 0; i < filter->logical.size; i++) {
		struct filter *_filter = filter->logical.filters[i];
		const char *key;
		char str[16];

		bson_uint32_to_string(i, &key, str, sizeof(*str));
		BSON_APPEND_DOCUMENT(array, key,
				     mongo_query_from_filter(_filter));
	}

	query = bson_new();
	BSON_APPEND_ARRAY(query, mongo_operator_to_str(filter->op), array);
	return query;
}

static struct fsentry_iterator *
mongo_list_fsentries(const struct backend *backend, const struct fsentry *root,
		     const struct filter *filter)
{
	struct mongo_backend *mongo = (struct mongo_backend *)backend;
	mongoc_collection_t *collection;
	struct mongo_iterator *iterator;
	bson_t *query;

	iterator = malloc(sizeof(*iterator));
	if (iterator == NULL)
		return NULL;

	mongo_iterator_init(iterator);

	collection = mongoc_database_get_collection(mongo->db, "entries");

	query = mongo_query_from_filter(filter);
	iterator->cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE,
						  0, 0, 0, query, NULL, NULL);

	bson_destroy(query);
	mongoc_collection_destroy(collection);

	return &iterator->iterator;
}

static struct mongo_backend mongo = {
	.backend = {
		.name		= "MongoDB",
		.init		= mongo_init,
		.exit		= mongo_exit,
		.list_fsentries	= mongo_list_fsentries,
	},
};

const struct backend *mongodb = &mongo.backend;
