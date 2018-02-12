#include <assert.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "filter.h"

static struct filter *
filter_compare_value(const char *value)
{
    struct filter *filter;
    size_t len = strlen(value) + 1;

    filter = malloc(sizeof(*filter) + len);
    if (filter == NULL)
        return NULL;

    strncpy(filter->compare.value.value, value, len);

    return filter;
}

static struct filter *
filter_compare_list(size_t size, va_list args)
{
    struct filter *filter;
    size_t i;

    filter = malloc(sizeof(*filter) + size * sizeof(char *));
    if (filter == NULL)
        return NULL;

    for (i = 0; i < size; i++) {
        const char *value = va_arg(args, const char *);
        size_t len = strlen(value) + 1;
        char *_value;

        _value = malloc(len);
        if (_value == NULL)
            goto out_free_list;

        strncpy(_value, value, len);
        filter->compare.value.list.values[i] = _value;
    }

    return filter;

out_free_list:
    for (i -= 1; i >= 0; i--) {
        free(filter->compare.value.list.values[i]);
    }
    free(filter);
    return NULL;
}

static struct filter *
filter_compare_regex(unsigned int options, const char *regex)
{
    struct filter *filter;
    size_t len = strlen(regex) + 1;

    filter = malloc(sizeof(*filter) + len);
    if (filter == NULL)
        return NULL;

    filter->compare.value.regex.options = options;
    strncpy(filter->compare.value.regex.value, regex, len);

    return filter;
}

struct filter *
filter_compare(enum filter_operator op, enum filter_field field, ...)
{
    struct filter *filter;
    va_list args;

    assert(is_comparison_operator(op));

    va_start(args, field);

    switch(op) {
    case REGEX: /* Intentional fallthrough */
    case NOT_REGEX:
    {
        unsigned int options = va_arg(args, unsigned int);
        const char *regex = va_arg(args, const char *);
        filter = filter_compare_regex(options, regex);
        break;
    }
    case IN: /* Intentional fallthrough */
    case NOT_IN:
        filter = filter_compare_list(va_arg(args, size_t),
                args);
        break;
    default:
        filter = filter_compare_value(va_arg(args, const char *));
        break;
    }

    va_end(args);

    if (filter == NULL)
        return NULL;

    filter->op = op;
    filter->compare.field = field;

    return filter;
}

static struct filter *
filter_compose(enum filter_operator op, size_t num, struct filter *filters[])
{
    struct filter *filter;

    assert(is_logical_operator(op));

    filter = calloc(num + 1, sizeof(*filter));
    filter->op = op;
    filter->logical.size = num;

    for (int i = 0; i < num; i++)
        filter->logical.filters[i] = filters[i];

    return filter;
}

struct filter *
filter_and(size_t num, struct filter *filters[])
{
    return filter_compose(AND, num, filters);
}

struct filter *
filter_or(size_t num, struct filter *filters[])
{
    return filter_compose(OR, num, filters);
}

void
__filter_negate(struct filter *filter)
{
    switch(filter->op) {
    case EQUAL:
        filter->op = NOT_EQUAL;
        break;
    case NOT_EQUAL:
        filter->op = EQUAL;
    case LOWER_THAN:
        filter->op = GREATER_OR_EQUAL;
        break;
    case LOWER_OR_EQUAL:
        filter->op = GREATER_THAN;
        break;
    case GREATER_THAN:
        filter->op = LOWER_OR_EQUAL;
        break;
    case GREATER_OR_EQUAL:
        filter->op = LOWER_THAN;
        break;
    case IN:
        filter->op = NOT_IN;
        break;
    case NOT_IN:
        filter->op = IN;
        break;
    case REGEX:
        filter->op = NOT_REGEX;
        break;
    case NOT_REGEX:
        filter->op = REGEX;
        break;
    case AND:
        filter->op = OR;
        break;
    case OR:
        filter->op = AND;
        break;
    }
}

void filter_negate(struct filter *filter)
{
    __filter_negate(filter);
    if (is_comparison_operator(filter->op))
        return;

    for (unsigned int i = 0; i < filter->logical.size; i++)
        filter_negate(filter->logical.filters[i]);
}

void filter_free(struct filter *filter)
{
    switch (filter->op) {
    case IN: /* Intentional fallthrough */
    case NOT_IN:
        for (size_t i = 0; i < filter->compare.value.list.size; i++)
            free(filter->compare.value.list.values[i]);
        break;
    case AND: /* Intentional fallthrough */
    case OR:
        for (size_t i = 0; i < filter->logical.size; i++)
            filter_free(filter->logical.filters[i]);
        break;
    default:
        break;
    }
    free(filter);
}
