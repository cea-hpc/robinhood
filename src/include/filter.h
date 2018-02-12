#ifndef __FILTER_H__
#define __FILTER_H__

#include <stdbool.h>
#include <stddef.h>

enum filter_operator {
	/* Comparison */
	EQUAL = 0,
	NOT_EQUAL,
	LOWER_THAN,
	LOWER_OR_EQUAL,
	GREATER_THAN,
	GREATER_OR_EQUAL,
	IN,
	NOT_IN,
	REGEX,
	NOT_REGEX,

	/* Logical */
	AND,
	OR,
};
#define FILTER_OPERATOR_MAX (OR + 1)

/** is_comparison_operator - is the operator a comparison operator?
 * @param op	the operator to test
 *
 * @return	true if the operator is a comparison operator, false otherwise
 */
static inline bool
is_comparison_operator(enum filter_operator op)
{
	return op <= NOT_REGEX;
}

/** is_logical_operator - is the operator a logical operator?
 * @param op	the operator to test
 *
 * @return	true if the operator is a logical operator, false otherwise
 */
static inline bool
is_logical_operator(enum filter_operator op)
{
	return op >= AND;
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
		struct {
			enum filter_field field;
			union {
				struct {
					unsigned int options;
					char value[];
				} regex;
				struct {
					size_t size;
					char *values[];
				} list;
				char value[0];
			} value;
		} compare;
		struct {
			size_t size;
			struct filter *filters[];
		} logical;
	};
};

/** filter_compare - create a filter that represents a certain condition
 * @param op	the type of comparison to use
 * @param field	the field to compare
 * TODO: document the variadic arguments (I don't know how to do it)
 *
 * @return	a pointer to the resulting struct filter
 */
struct filter *
filter_compare(enum filter_operator op, enum filter_field field, ...);

/** filter_negate - negates a filter in place
 * @param filter	the filter to negate
 */
void
filter_negate(struct filter *filter);

/** filter_and - compose multiple filters with the AND operator
 * @param num	the number of filter to compose
 * @param ...	a series of pointers to struct filter
 *
 * @return	a pointer to a the resulting struct filter
 */
struct filter *
filter_and(int num, ...);

/** filter_and - compose multiple filters with the OR operator
 * @param num	the number of filter to compose
 * @param ...	a series of pointers to struct filter
 *
 * @return	a pointer to a the resulting struct filter
 */
struct filter *
filter_or(int num, ...);

/** filter_free - recursively free a filter
 * @param filter	the filter to recursively free
 */
void
filter_free(struct filter *filter);

#endif
