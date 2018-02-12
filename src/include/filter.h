#ifndef __FILTER_H__
#define __FILTER_H__

#include <stdbool.h>
#include <stddef.h>

enum filter_operator {
    /* Comparison */
    FOP_EQUAL = 0,
    FOP_NOT_EQUAL,
    FOP_LOWER_THAN,
    FOP_LOWER_OR_EQUAL,
    FOP_GREATER_THAN,
    FOP_GREATER_OR_EQUAL,
    FOP_IN,
    FOP_NOT_IN,
    FOP_REGEX,
    FOP_NOT_REGEX,

    FOP_COMPARISON_MIN = FOP_EQUAL,
    FOP_COMPARISON_MAX = FOP_NOT_REGEX,

    /* Logical */
    FOP_AND,
    FOP_OR,

    FOP_LOGICAL_MIN = FOP_AND,
    FOP_LOGICAL_MAX = FOP_OR,
};

/** is_comparison_operator - is the operator a comparison operator?
 * @param op    the operator to test
 *
 * @return      true if the operator is a comparison operator, false otherwise
 */
static inline bool
is_comparison_operator(enum filter_operator op)
{
    return FOP_COMPARISON_MIN <= op && op <= FOP_COMPARISON_MAX;
}

/** is_logical_operator - is the operator a logical operator?
 * @param op    the operator to test
 *
 * @return      true if the operator is a logical operator, false otherwise
 */
static inline bool
is_logical_operator(enum filter_operator op)
{
    return FOP_LOGICAL_MIN <= op && op <= FOP_LOGICAL_MAX;
}

enum filter_field {
    NAME = 0,
    PATH,
};

enum filter_regex_option {
    CASE_INSENSITIVE = 1 << 0,
};

struct filter {
    enum filter_operator op;
    union {
        /* op is a comparison operator */
        struct {
            enum filter_field field;
            union {
                /* op is FOP_REGEX or FOP_NOT_REGEX */
                struct {
                    unsigned int options;
                    char value[];
                } regex;

                /* op is FOP_IN or FOP_NOT_IN */
                struct {
                    size_t size;
                    char *values[];
                } list;

                /* op is anything else */
                char value[0];
            } value;
        } compare;

        /* op is a logical operator */
        struct {
            size_t size;
            struct filter *filters[];
        } logical;
    };
};

/** filter_compare - create a filter that compares a field to a value
 * @param op    the type of comparison to use
 * @param field the field to compare
 * @param value the value to compare the field to
 *
 * @return      a pointer to the resulting struct filter
 *
 * op may be any comparison operator except (NOT_)REGEX and (NOT_)IN
 */
struct filter *
filter_compare_value(enum filter_operator op, enum filter_field,
                     const char *value);

/** filter_compare_regex - create a filter that matches a field against a regex
 * @param field the field to compare
 * @param value the value to compare the field to
 *
 * @return      a pointer to the resulting struct filter
 */
struct filter *
filter_compare_regex(enum filter_field, unsigned int regex_options,
                     const char *regex);

/** filter_compare_list - create a filter that compares a field to a list of values
 * @param field     the field to compare
 * @param size      the number of elements in values
 * @param values    the list of values to compare field to
 *
 * @return          a pointer to the resulting struct filter
 */
struct filter *
filter_compare_list(enum filter_field field, size_t size, const char *values[]);

/** filter_negate - negates a filter in place
 * @param filter    the filter to negate
 *
 * @return          the negated filter
 */
struct filter *
filter_negate(struct filter *filter);

/** filter_and - compose multiple filters with the AND operator
 * @param num   the number of filter to compose
 * @param ...   a series of pointers to struct filter
 *
 * @return      a pointer to a the resulting struct filter
 */
struct filter *
filter_and(size_t size, struct filter *filters[]);

/** filter_and - compose multiple filters with the OR operator
 * @param num   the number of filter to compose
 * @param ...   a series of pointers to struct filter
 *
 * @return      a pointer to a the resulting struct filter
 */
struct filter *
filter_or(size_t size, struct filter *filters[]);

/** filter_free - recursively free a filter
 * @param filter    the filter to recursively free
 */
void
filter_free(struct filter *filter);

#endif
