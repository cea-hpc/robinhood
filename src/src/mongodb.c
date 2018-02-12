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
 * @param backend   the mongodb backend
 * @param size      the number of arguments to come
 * @param uri       (const char *) a uri pointing to a mongodb instance
 * @param database  (const char *) the name of the database to use
 *
 * @return          < 0 on failure
 */
static int
mongo_init(const struct backend *backend, size_t size, va_list args)
{
    struct mongo_backend *mongo = (struct mongo_backend *)backend;

    mongoc_init();

    assert(size == 2);

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

struct mongo_iterator {
    struct fsentry_iterator iterator;
    mongoc_cursor_t *cursor;
};

struct mongo_fsentry {
    struct fsentry fsentry;
    char *name;
    struct stat stat;
};

static struct mongo_fsentry *
mongo_fsentry_from_bson(const bson_t *doc);

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
    switch (operator) {
    case FOP_EQUAL:
        return "$eq";
    case FOP_NOT_EQUAL:
        return "$ne";
    case FOP_LOWER_THAN:
        return "$lt";
    case FOP_LOWER_OR_EQUAL:
        return "$le";
    case FOP_GREATER_THAN:
        return "$gt";
    case FOP_GREATER_OR_EQUAL:
        return "$ge";
    case FOP_IN:
        return "$in";
    case FOP_NOT_IN:
        return "$nin";
    case FOP_REGEX:
        return "$regex";
    case FOP_NOT_REGEX:
        return "$regex"; /* This is not a mistake */
    case FOP_AND:
        return "$and";
    case FOP_OR:
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
        BSON_APPEND_UTF8(array, key, filter->compare.value.list.values[i]);
    }

    BSON_APPEND_ARRAY(bson, mongo_operator_to_str(filter->op), array);
    bson_destroy(array);

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

    /* FOP_NOT_REGEX is special cased as the operator does not
     * exist per-se */
    if (filter->op == FOP_NOT_REGEX) {
        bson_t *tmp = bson_new();

        BSON_APPEND_DOCUMENT(tmp, "$not", bson);
        bson_destroy(bson);
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

    switch (filter->op) {
    case FOP_EQUAL: /* Intentional fallthrough */
    case FOP_NOT_EQUAL:
        comparison = bson_from_filter_string(filter);
        break;
    case FOP_REGEX: /* Intentional fallthrough */
    case FOP_NOT_REGEX:
        comparison = bson_from_filter_regex(filter);
        break;
    case FOP_IN: /* Intentional fallthrough */
    case FOP_NOT_IN:
        comparison = bson_from_filter_list(filter);
        break;
    default:
        assert(false);
    }

    BSON_APPEND_DOCUMENT(query, "name", comparison);
    bson_destroy(comparison);

    return query;
}

static bson_t *
__mongo_query_from_filter(const struct filter *filter)
{
    switch (filter->compare.field) {
    case NAME:
        return mongo_query_from_name_filter(filter);
    case PATH:
        /* TODO */
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
    for (uint32_t i = 0; i < filter->logical.size; i++) {
        struct filter *_filter = filter->logical.filters[i];
        const char *key;
        char str[16];
        bson_t *tmp;

        bson_uint32_to_string(i, &key, str, sizeof(*str));
        tmp = mongo_query_from_filter(_filter);
        BSON_APPEND_DOCUMENT(array, key, tmp);
        bson_destroy(tmp);
    }

    query = bson_new();
    BSON_APPEND_ARRAY(query, mongo_operator_to_str(filter->op), array);
    bson_destroy(array);
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

static struct backend_operations mongo_backend_operations = {
    .init           = mongo_init,
    .exit           = mongo_exit,
    .list_fsentries = mongo_list_fsentries,
};

static struct mongo_backend mongo = {
    .backend = {
        .name   = "MongoDB",
        .op     = &mongo_backend_operations,
    },
};

const struct backend *mongodb = &mongo.backend;

/* mongo_fsentry operations */

static const char *
mongo_fsentry_get_name(struct fsentry *fsentry)
{
    struct mongo_fsentry *mongo_fsentry = (struct mongo_fsentry *)fsentry;

    return mongo_fsentry->name;
}

static int
mongo_fsentry_stat(struct fsentry *fsentry, struct stat *stat)
{
    struct mongo_fsentry *mongo_fsentry = (struct mongo_fsentry *)fsentry;

    *stat = mongo_fsentry->stat;
    return 0;
}

static bool
mongo_fsentry_exists(struct fsentry *fsentry)
{
    mongoc_collection_t *collection;
    mongoc_cursor_t *cursor;
    bson_t *query = bson_new();
    const bson_t *doc;
    bool exists;

    collection = mongoc_database_get_collection(mongo.db, "entries");

    BSON_APPEND_BINARY(query, "_id", BSON_SUBTYPE_UUID, fsentry->uuid,
                       sizeof(fsentry->uuid));

    cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0,
                                    query, NULL, NULL);

    bson_destroy(query);
    mongoc_collection_destroy(collection);

    exists = mongoc_cursor_next(cursor, &doc);

    mongoc_cursor_destroy(cursor);

    return exists;
}

static void
mongo_fsentry_free(struct fsentry *fsentry)
{
    struct mongo_fsentry *mongo_fsentry = (struct mongo_fsentry *)fsentry;

    free(mongo_fsentry->name);
    free(mongo_fsentry);
}

static const struct fsentry_operations mongo_fsentry_operations = {
    .get_name   = mongo_fsentry_get_name,
    .stat       = mongo_fsentry_stat,
    .hydrate    = NULL,
    .exists     = mongo_fsentry_exists,
    .free       = mongo_fsentry_free,
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

        if (strcmp(key, "_id") == 0) {
            assert(value->value.v_binary.subtype == BSON_SUBTYPE_UUID);

            uuid_copy(fsentry->fsentry.uuid, value->value.v_binary.data);
        } else if (strcmp(key, "dev") == 0) {
            assert(bson_iter_type(&iter) == BSON_TYPE_INT32);

            fsentry->stat.st_dev = value->value.v_int32;
        } else if (strcmp(key, "ino") == 0) {
            assert(bson_iter_type(&iter) == BSON_TYPE_INT32);

            fsentry->stat.st_ino = value->value.v_int32;
        } else if (strcmp(key, "mode") == 0) {
            assert(bson_iter_type(&iter) == BSON_TYPE_INT32);

            fsentry->stat.st_mode = value->value.v_int32;
        } else if (strcmp(key, "nlink") == 0) {
            assert(bson_iter_type(&iter) == BSON_TYPE_INT32);

            fsentry->stat.st_nlink = value->value.v_int32;
        } else if (strcmp(key, "uid") == 0) {
            assert(bson_iter_type(&iter) == BSON_TYPE_INT32);

            fsentry->stat.st_uid = value->value.v_int32;
        } else if (strcmp(key, "gid") == 0) {
            assert(bson_iter_type(&iter) == BSON_TYPE_INT32);

            fsentry->stat.st_gid = value->value.v_int32;
        } else if (strcmp(key, "rdev") == 0) {
            assert(bson_iter_type(&iter) == BSON_TYPE_INT32);

            fsentry->stat.st_rdev = value->value.v_int32;
        } else if (strcmp(key, "size") == 0) {
            if (bson_iter_type(&iter) == BSON_TYPE_INT32)
                fsentry->stat.st_size = value->value.v_int32;
            else if (bson_iter_type(&iter) == BSON_TYPE_INT64)
                fsentry->stat.st_size = value->value.v_int64;
            else
                assert(false);
        } else if (strcmp(key, "blksize") == 0) {
            assert(bson_iter_type(&iter) == BSON_TYPE_INT32);

            fsentry->stat.st_blksize = value->value.v_int32;
        } else if (strcmp(key, "blocks") == 0) {
            assert(bson_iter_type(&iter) == BSON_TYPE_INT32);

            fsentry->stat.st_blocks = value->value.v_int32;
        } else if (strcmp(key, "atime") == 0) {
            switch (bson_iter_type(&iter)) {
            case BSON_TYPE_INT32:
                fsentry->stat.st_atime = value->value.v_int32;
                break;
            case BSON_TYPE_INT64:
                fsentry->stat.st_atime = value->value.v_int64;
                break;
            case BSON_TYPE_DOUBLE:
                fsentry->stat.st_atime = value->value.v_double;
                break;
            default:
                assert(false);
            }
        } else if (strcmp(key, "mtime") == 0) {
            switch (bson_iter_type(&iter)) {
            case BSON_TYPE_INT32:
                fsentry->stat.st_atime = value->value.v_int32;
                break;
            case BSON_TYPE_INT64:
                fsentry->stat.st_atime = value->value.v_int64;
                break;
            case BSON_TYPE_DOUBLE:
                fsentry->stat.st_atime = value->value.v_double;
                break;
            default:
                assert(false);
            }
        } else if (strcmp(key, "ctime") == 0) {
            switch (bson_iter_type(&iter)) {
            case BSON_TYPE_INT32:
                fsentry->stat.st_atime = value->value.v_int32;
                break;
            case BSON_TYPE_INT64:
                fsentry->stat.st_atime = value->value.v_int64;
                break;
            case BSON_TYPE_DOUBLE:
                fsentry->stat.st_atime = value->value.v_double;
                break;
            default:
                assert(false);
            }
        } else if (strcmp(key, "name") == 0) {
            size_t name_len = value->value.v_utf8.len;
            char *name;

            assert(BSON_ITER_HOLDS_UTF8(&iter));

            name = malloc(name_len + 1);
            if (name == NULL)
                goto out_free;

            strncpy(name, value->value.v_utf8.str, name_len);
            name[name_len] = 0;

            fsentry->name = name;
        }
    }

    return fsentry;

out_free:
    free(fsentry);
out:
    return NULL;
}
