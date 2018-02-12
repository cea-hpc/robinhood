#include <assert.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "filter.h"

struct filter *
filter_compare_value(enum filter_operator op, enum filter_field field,
                     const char *value)
{
    struct filter *filter;
    size_t len = strlen(value) + 1;

    assert(is_comparison_operator(op));
    assert(op != FOP_IN && op != FOP_NOT_IN);
    assert(op != FOP_REGEX && op != FOP_NOT_REGEX);

    filter = malloc(sizeof(*filter) + len);
    if (filter == NULL)
        return NULL;

    strncpy(filter->compare.value.value, value, len);

    filter->op = op;
    filter->compare.field = field;

    return filter;
}

struct filter *
filter_compare_list(enum filter_field field, size_t size, const char *values[])
{
    struct filter *filter;
    size_t i;

    filter = malloc(sizeof(*filter) + size * sizeof(char *));
    if (filter == NULL)
        return NULL;

    for (i = 0; i < size; i++) {
        char *value;

        value = strdup(values[i]);
        if (value == NULL)
            goto out_free_list;

        filter->compare.value.list.values[i] = value;
    }

    filter->op = FOP_IN;
    filter->compare.field = field;

    return filter;

out_free_list:
    for (i -= 1; i >= 0; i--)
        free(filter->compare.value.list.values[i]);

    free(filter);
    return NULL;
}

struct filter *
filter_compare_regex(enum filter_field field, unsigned int options,
                     const char *regex)
{
    struct filter *filter;
    size_t len = strlen(regex) + 1;

    filter = malloc(sizeof(*filter) + len);
    if (filter == NULL)
        return NULL;

    filter->compare.value.regex.options = options;
    strncpy(filter->compare.value.regex.value, regex, len);

    filter->op = FOP_REGEX;
    filter->compare.field = field;

    return filter;
}

static struct filter *
filter_compose(enum filter_operator op, size_t size, struct filter *filters[])
{
    struct filter *filter;

    assert(is_logical_operator(op));

    filter = calloc(size + 1, sizeof(*filter));
    if (filter == NULL)
        return NULL;

    filter->op = op;
    filter->logical.size = size;

    for (int i = 0; i < size; i++)
        filter->logical.filters[i] = filters[i];

    return filter;
}

struct filter *
filter_and(size_t size, struct filter *filters[])
{
    return filter_compose(FOP_AND, size, filters);
}

struct filter *
filter_or(size_t size, struct filter *filters[])
{
    return filter_compose(FOP_OR, size, filters);
}

void
__filter_negate(struct filter *filter)
{
    switch (filter->op) {
    case FOP_EQUAL:
        filter->op = FOP_NOT_EQUAL;
        break;
    case FOP_NOT_EQUAL:
        filter->op = FOP_EQUAL;
    case FOP_LOWER_THAN:
        filter->op = FOP_GREATER_OR_EQUAL;
        break;
    case FOP_LOWER_OR_EQUAL:
        filter->op = FOP_GREATER_THAN;
        break;
    case FOP_GREATER_THAN:
        filter->op = FOP_LOWER_OR_EQUAL;
        break;
    case FOP_GREATER_OR_EQUAL:
        filter->op = FOP_LOWER_THAN;
        break;
    case FOP_IN:
        filter->op = FOP_NOT_IN;
        break;
    case FOP_NOT_IN:
        filter->op = FOP_IN;
        break;
    case FOP_REGEX:
        filter->op = FOP_NOT_REGEX;
        break;
    case FOP_NOT_REGEX:
        filter->op = FOP_REGEX;
        break;
    case FOP_AND:
        filter->op = FOP_OR;
        break;
    case FOP_OR:
        filter->op = FOP_AND;
        break;
    }
}

struct filter *
filter_negate(struct filter *filter)
{
    __filter_negate(filter);
    if (is_comparison_operator(filter->op))
        return filter;

    for (unsigned int i = 0; i < filter->logical.size; i++)
        filter_negate(filter->logical.filters[i]);

    return filter;
}

void filter_free(struct filter *filter)
{
    switch (filter->op) {
    case FOP_IN: /* Intentional fallthrough */
    case FOP_NOT_IN:
        for (size_t i = 0; i < filter->compare.value.list.size; i++)
            free(filter->compare.value.list.values[i]);
        break;
    case FOP_AND: /* Intentional fallthrough */
    case FOP_OR:
        for (size_t i = 0; i < filter->logical.size; i++)
            filter_free(filter->logical.filters[i]);
        break;
    default:
        break;
    }
    free(filter);
}
