/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "Memory.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "listmgr_common.h"
#include <stdlib.h>

#define FILTER_PREALLOC_INIT 2

int lmgr_simple_filter_init(lmgr_filter_t *p_filter)
{
    lmgr_simple_filter_t *sf = &p_filter->filter_simple;

    p_filter->filter_type = FILTER_SIMPLE;

    sf->filter_count = 0;
    sf->filter_flags = MemCalloc(FILTER_PREALLOC_INIT, sizeof(int));
    sf->filter_index = MemCalloc(FILTER_PREALLOC_INIT, sizeof(unsigned int));
    sf->filter_compar = MemCalloc(FILTER_PREALLOC_INIT,
                                  sizeof(filter_comparator_t));
    sf->filter_value = MemCalloc(FILTER_PREALLOC_INIT, sizeof(filter_value_t));

    if (sf->filter_flags == NULL || sf->filter_index == NULL
        || sf->filter_compar == NULL || sf->filter_value == NULL)
        return DB_NO_MEMORY;

    sf->prealloc = FILTER_PREALLOC_INIT;
    return 0;
}

static int convert_regexp(const char *in_string, char *db_string)
{
    char *p_wild;

    strcpy(db_string, in_string);

    /* replace classes [] with _ */

    while ((p_wild = strchr(db_string, '[')) != NULL) {
        char *p_end = strchr(p_wild, ']');

        if (p_end == NULL) {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Error unmatched '[' in regexp '%s'.", in_string);
            return DB_INVALID_ARG;
        }
        /* copy from character after ']' to the caracter after '['
         * and replace '[' with '_'
         */
        strcpy(p_wild + 1, p_end + 1);
        *p_wild = '_';
    }

    while ((p_wild = strchr(db_string, '*')) != NULL)
        *p_wild = '%';

    while ((p_wild = strchr(db_string, '?')) != NULL)
        *p_wild = '_';

    return 0;
}

static int lmgr_simple_filter_dup_buffers(lmgr_filter_t *p_filter,
                                          unsigned int index)
{
    lmgr_simple_filter_t *sf = &p_filter->filter_simple;
    filter_comparator_t comparator = sf->filter_compar[index];
    filter_value_t *p_value = &sf->filter_value[index];
    int flag = sf->filter_flags[index];

    /* @TODO support lists of strings (with both FILTER_FLAG_ALLOC_STR
     * and FILTER_FLAG_ALLOC_LIST */

    if ((comparator == LIKE) || (comparator == UNLIKE) || (comparator == RLIKE)
        || (comparator == ILIKE) || (comparator == IUNLIKE)) {
        int rc;
        char *newstr = MemAlloc(strlen(p_value->value.val_str) + 1);

        if (comparator != RLIKE) {
            /* value is a perl regexp, don't convert it */
            rc = convert_regexp(p_value->value.val_str, newstr);
            if (rc) {
                MemFree(newstr);
                return rc;
            }
        } else
            strcpy(newstr, p_value->value.val_str);

        /* free the previous string */
        if (flag & FILTER_FLAG_ALLOC_STR)
            MemFree((char *)p_value->value.val_str);

        /* mark the new string as releasable */
        sf->filter_flags[index] |= FILTER_FLAG_ALLOC_STR;
        sf->filter_value[index].value.val_str = newstr;
    } else if ((comparator == IN) || (comparator == NOTIN)) {
        /* allocate and copy the list */
        db_type_u *values =
            (db_type_u *) MemAlloc(p_value->list.count * sizeof(db_type_u));
        memcpy(values, p_value->list.values,
               p_value->list.count * sizeof(db_type_u));

        /* free the previous list */
        if (flag & FILTER_FLAG_ALLOC_LIST)
            MemFree((char *)p_value->list.values);

        sf->filter_flags[index] |= FILTER_FLAG_ALLOC_LIST;
        sf->filter_value[index].list.values = values;
    }

    return 0;
}

static void lmgr_simple_filter_free_buffers(lmgr_filter_t *p_filter,
                                            unsigned int index)
{
    lmgr_simple_filter_t *sf = &p_filter->filter_simple;

    /* @TODO support lists of strings (with both FILTER_FLAG_ALLOC_STR
       and FILTER_FLAG_ALLOC_LIST */

    /* check if previous value must be released */
    if ((sf->filter_flags[index] & FILTER_FLAG_ALLOC_STR)
        && (sf->filter_value[index].value.val_str != NULL)) {
        MemFree((char *)sf->filter_value[index].value.val_str);
    } else if ((sf->filter_flags[index] & FILTER_FLAG_ALLOC_LIST)
            && (sf->filter_value[index].list.values != NULL)) {
        MemFree((char *)sf->filter_value[index].list.values);
    }
}

int lmgr_simple_filter_add(lmgr_filter_t *p_filter, unsigned int attr_index,
                           filter_comparator_t comparator, filter_value_t value,
                           enum filter_flags flag)
{
    int rc;
    lmgr_simple_filter_t *sf = &p_filter->filter_simple;

    if (p_filter->filter_type != FILTER_SIMPLE)
        return DB_INVALID_ARG;

    if (sf->filter_count >= sf->prealloc) {
        /* double the size of the buffers */
        sf->prealloc *= 2;
        sf->filter_flags = MemRealloc(sf->filter_flags,
                                      sf->prealloc * sizeof(int));
        sf->filter_index = MemRealloc(sf->filter_index,
                                      sf->prealloc * sizeof(unsigned int));
        sf->filter_compar = MemRealloc(sf->filter_compar,
                                   sf->prealloc * sizeof(filter_comparator_t));
        sf->filter_value = MemRealloc(sf->filter_value,
                                   sf->prealloc * sizeof(filter_value_t));

        if (sf->filter_flags == NULL || sf->filter_index == NULL
            || sf->filter_compar == NULL || sf->filter_value == NULL)
            return DB_NO_MEMORY;
    }

    sf->filter_flags[sf->filter_count] = flag;
    sf->filter_index[sf->filter_count] = attr_index;
    sf->filter_compar[sf->filter_count] = comparator;
    sf->filter_value[sf->filter_count] = value;

    /* duplicate and copy buffers if needed */
    rc = lmgr_simple_filter_dup_buffers(p_filter, sf->filter_count);
    if (rc)
        return rc;

    sf->filter_count++;

    return 0;
}

/* check if the given attribute is part of a filter */
int lmgr_filter_check_field(const lmgr_filter_t *p_filter,
                            unsigned int attr_index)
{
    unsigned int i;

    if (p_filter->filter_type != FILTER_SIMPLE)
        return DB_INVALID_ARG;

    /* first check if there is already a filter on this argument */
    for (i = 0; i < p_filter->filter_simple.filter_count; i++) {
        if (p_filter->filter_simple.filter_index[i] == attr_index)
            return 1;
    }
    return 0;
}

int lmgr_simple_filter_add_or_replace(lmgr_filter_t *p_filter,
                                      unsigned int attr_index,
                                      filter_comparator_t comparator,
                                      filter_value_t value,
                                      enum filter_flags flag)
{
    unsigned int i;
    int rc;
    lmgr_simple_filter_t *sf;

    if (p_filter->filter_type != FILTER_SIMPLE)
        return DB_INVALID_ARG;
    sf = &p_filter->filter_simple;

    /* first check if there is already a filter on this argument */
    for (i = 0; i < sf->filter_count; i++) {
        if (sf->filter_index[i] != attr_index)
            continue;

        int syntax_flags = sf->filter_flags[i]
            & (FILTER_FLAG_BEGIN | FILTER_FLAG_END | FILTER_FLAG_OR);

        /* check if previous value must be released */
        lmgr_simple_filter_free_buffers(p_filter, i);

        /* ensure parenthesing and 'OR' keywords are conserved */
        sf->filter_flags[i] = flag | syntax_flags;
        sf->filter_compar[i] = comparator;
        sf->filter_value[i] = value;

        /* duplicate and copy buffers if needed */
        rc = lmgr_simple_filter_dup_buffers(p_filter, i);
        if (rc)
            return rc;

        return 0;
    }

    /* not found: add it */
    return lmgr_simple_filter_add(p_filter, attr_index, comparator, value,
                                  flag);
}

int lmgr_simple_filter_add_if_not_exist(lmgr_filter_t *p_filter,
                                        unsigned int attr_index,
                                        filter_comparator_t comparator,
                                        filter_value_t value,
                                        enum filter_flags flag)
{
    unsigned int i;

    if (p_filter->filter_type != FILTER_SIMPLE)
        return DB_INVALID_ARG;

    /* first check if there is already a filter on this argument */
    for (i = 0; i < p_filter->filter_simple.filter_count; i++) {
        if (p_filter->filter_simple.filter_index[i] == attr_index) {
            return DB_ALREADY_EXISTS;
        }
    }

    /* not found: add it */
    return lmgr_simple_filter_add(p_filter, attr_index, comparator, value,
                                  flag);

}

int lmgr_simple_filter_free(lmgr_filter_t *p_filter)
{
    int i;
    lmgr_simple_filter_t *sf;

    if (p_filter->filter_type != FILTER_SIMPLE)
        return DB_INVALID_ARG;
    sf = &p_filter->filter_simple;

    /* free the values that must be released */
    for (i = 0; i < sf->filter_count; i++)
        lmgr_simple_filter_free_buffers(p_filter, i);

    if (sf->filter_flags)
        MemFree(sf->filter_flags);
    if (sf->filter_index)
        MemFree(sf->filter_index);
    if (sf->filter_compar)
        MemFree(sf->filter_compar);
    if (sf->filter_value)
        MemFree(sf->filter_value);
    memset(p_filter, 0, sizeof(lmgr_filter_t));
    return 0;
}

/* Add begin or end block. */
int lmgr_simple_filter_add_block(lmgr_filter_t *p_filter,
                                  enum filter_flags flag)
{
    filter_value_t val;
    memset(&val, 0, sizeof(filter_value_t));

    return lmgr_simple_filter_add(p_filter, 0, 0, val, flag);
}


/* is it a simple 'AND' expression ? */
static bool is_simple_expr(bool_node_t *boolexpr, int depth, bool_op_t op_ctx)
{
    switch (boolexpr->node_type) {
    case NODE_UNARY_EXPR:
        if (boolexpr->content_u.bool_expr.bool_op != BOOL_NOT) {
            /* Error */
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Invalid unary operator %d in %s()",
                       boolexpr->content_u.bool_expr.bool_op, __FUNCTION__);
            return false;
        }
        /* only accept 'NOT condition', but reject 'NOT (cond AND cond)' */
        return (boolexpr->content_u.bool_expr.expr1->node_type ==
                NODE_CONDITION);

    case NODE_BINARY_EXPR:
        if (depth > 2) {
            DisplayLog(LVL_EVENT, LISTMGR_TAG,
                       "Too many levels of nested parenthesis in expression "
                       "(%d levels). Consider simplifying it.", depth);
            return false;
        }
        if (boolexpr->content_u.bool_expr.bool_op != BOOL_AND
                 && boolexpr->content_u.bool_expr.bool_op != BOOL_OR)
            return false;

        /* bool operation context unchanged? */
        if (boolexpr->content_u.bool_expr.bool_op == op_ctx)
            return (is_simple_expr
                    (boolexpr->content_u.bool_expr.expr1, depth, op_ctx)
                    && is_simple_expr(boolexpr->content_u.bool_expr.expr2,
                                      depth, op_ctx));
        else
            return (is_simple_expr
                    (boolexpr->content_u.bool_expr.expr1, depth + 1,
                     boolexpr->content_u.bool_expr.bool_op)
                    && is_simple_expr(boolexpr->content_u.bool_expr.expr2,
                                      depth + 1,
                                      boolexpr->content_u.bool_expr.bool_op));

    case NODE_CONDITION:
        /* If attribute is in DB, it can be filtered
         * If attribute is not in DB, we ignore it and get all entries
         *  (~ AND true)
         */
        return true;

    case NODE_CONSTANT:
        return true;

    default:
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Invalid boolean expression in %s()",
                   __FUNCTION__);
        return false;
    }
}

static bool allow_null(unsigned int attr_index,
                       const filter_comparator_t *comp,
                       const filter_value_t *val)
{
    /* don't add 'OR IS NULL' if NULL is explicitely matched */
    if (*comp == ISNULL || *comp == NOTNULL)
        return false;

    /* allow NULL for strings if matching is:
     * x != 'non empty val' (or x not like 'non empty')
     * x == '' (or x like '')
     * DON't allow NULL string if matching is:
     * x == 'non empty'  (or x like 'non empty')
     * x != '' (or x not like '')
     */
    if (field_type(attr_index) == DB_TEXT ||
        field_type(attr_index) == DB_ENUM_FTYPE) {
        if (*comp == EQUAL || *comp == LIKE || *comp == ILIKE)
            /* allow NULL if matching against empty string */
            return (val->value.val_str == NULL
                    || EMPTY_STRING(val->value.val_str));
        else if (*comp == NOTEQUAL || *comp == UNLIKE || *comp == IUNLIKE)
            /* allow NULL if matching != non-empty string */
            return !(val->value.val_str == NULL
                     || EMPTY_STRING(val->value.val_str));
        else {
            /* unexpected case */
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Warning: unhandled case in %s(), line %u", __func__,
                       __LINE__);
        }
    }
    return true;    /* allow, by default */
}

bool cond2sql_ok(bool_node_t *boolexpr,
                      const sm_instance_t *smi,
                      const time_modifier_t *time_mod)
{
    unsigned int index = ATTR_INDEX_FLG_UNSPEC;
    int rc;
    filter_comparator_t comp;
    filter_value_t val;
    bool must_free;
    attr_mask_t tmp;

    switch (boolexpr->node_type) {
    case NODE_UNARY_EXPR:
        return cond2sql_ok(boolexpr->content_u.bool_expr.expr1,
                                smi, time_mod);
        break;
    case NODE_CONDITION:
        rc = criteria2filter(boolexpr->content_u.condition,
                             &index, &comp, &val, &must_free, smi, time_mod);

        if (rc != 0 || (index & ATTR_INDEX_FLG_UNSPEC))
            /* do nothing (equivalent to 'AND TRUE') */
            return false;

        // free allocated memory
        if (must_free)
            MemFree((char *)val.value.val_str);

        /* test generated fields */
        tmp = null_mask;
        attr_mask_set_index(&tmp, index);

        if (generated_fields(tmp) || dirattr_fields(tmp)
            || funcattr_fields(tmp))
            return false;
        else
            return true;
        break;
    case NODE_BINARY_EXPR:
        return cond2sql_ok(boolexpr->content_u.bool_expr.expr1,
                                 smi, time_mod)
                || cond2sql_ok(boolexpr->content_u.bool_expr.expr2,
                                    smi, time_mod);
        break;
    case NODE_CONSTANT:
        return false;
        break;
    }

    // Should never arrive here
    RBH_BUG("DB condition verification, found unknown case");
    return false;
}

/* Extract simple pieces of expressions and append them to filter.
 * The resulting filter is expected to return a larger set than the actual
 * condition.
 * Ignore conflicting criteria.
 * \param expr_flag indicate if BEGIN/END parenthesing is needed
 * \param depth indicate the current parenthesing depth
 * \param op_ctx indicate the current operation context:
 *      e.g. AND for 'x and y and z'
 */
static int append_simple_expr(bool_node_t *boolexpr, lmgr_filter_t *filter,
                              const sm_instance_t *smi,
                              const time_modifier_t *time_mod, int expr_flag,
                              int depth, bool_op_t op_ctx)
{
    int rc, new_depth;
    unsigned int index = ATTR_INDEX_FLG_UNSPEC;
    int flag = 0;
    filter_comparator_t comp;
    filter_value_t val;
    bool must_free;
    /* keep original filter count to reset it on error */
    int count_orig = filter->filter_simple.filter_count;

    if (depth > 2) {
        DisplayLog(LVL_EVENT, LISTMGR_TAG,
                   "Too many levels of nested parenthesis in expression "
                   "(%d levels). Consider simplifying it.", depth);
        return DB_INVALID_ARG;
    }

    switch (boolexpr->node_type) {
    case NODE_UNARY_EXPR:
        if (boolexpr->content_u.bool_expr.bool_op != BOOL_NOT) {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Invalid unary operator %d in %s()",
                       boolexpr->content_u.bool_expr.bool_op, __FUNCTION__);
            return DB_INVALID_ARG;
        }

        return append_simple_expr(boolexpr->content_u.bool_expr.expr1,
                                  filter, smi, time_mod,
                                  expr_flag | FILTER_FLAG_NOT_BEGIN
                                  | FILTER_FLAG_NOT_END, depth + 1, op_ctx);

    case NODE_CONDITION:
        /* If attribute is in DB, it can be filtered
         * If attribute is not in DB, we ignore it and get all entries
         * (~ AND TRUE)
         */
        // Return if this condition can't be translated to SQL statement
        if (!cond2sql_ok(boolexpr, smi, time_mod))
            return 0;

        /* get info about condition */
        rc = criteria2filter(boolexpr->content_u.condition,
                             &index, &comp, &val, &must_free, smi, time_mod);

        if ((expr_flag & FILTER_FLAG_ALLOW_NULL)
            || (allow_null(index, &comp, &val)
            /* Don't filter null value if we are in a negated block */
                && !(expr_flag & (FILTER_FLAG_NOT_BEGIN|FILTER_FLAG_NOT_END))))
            flag |= FILTER_FLAG_ALLOW_NULL;

        if (must_free)
            flag |= FILTER_FLAG_ALLOC_STR;

        /* propagate parenthesing flag + OR (NOT?) */
        flag |= (expr_flag & (FILTER_FLAG_BEGIN | FILTER_FLAG_END
                              | FILTER_FLAG_NOT_BEGIN | FILTER_FLAG_OR
                              | FILTER_FLAG_NOT_END));

        /* @TODO support FILTER_FLAG_ALLOC_LIST */

        /* add condition to filter */
        DisplayLog(LVL_FULL, LISTMGR_TAG,
                   "Appending filter on \"%s\", flags=%#X", field_name(index),
                   flag);

        return lmgr_simple_filter_add(filter, index, comp, val, flag);

    case NODE_BINARY_EXPR:
        {
            int flag1, flag2;
            bool dbcond1, dbcond2;
            bool begin_end = false;

            /* only AND/OR binary operators supported */
            if (boolexpr->content_u.bool_expr.bool_op != BOOL_AND
                     && boolexpr->content_u.bool_expr.bool_op != BOOL_OR)
                return DB_INVALID_ARG;

            flag1 = (op_ctx == BOOL_OR) ? FILTER_FLAG_OR : 0;
            /* x OR y? */
            flag2 = (boolexpr->content_u.bool_expr.bool_op == BOOL_OR) ?
                FILTER_FLAG_OR : 0;
            if (boolexpr->content_u.bool_expr.bool_op == op_ctx) {
                new_depth = depth;
                /* propagate BEGIN/END flags */
                flag1 |= expr_flag & (FILTER_FLAG_BEGIN
                                      | FILTER_FLAG_NOT_BEGIN);
                flag2 |= expr_flag & (FILTER_FLAG_END | FILTER_FLAG_NOT_END);
            } else {
                new_depth = depth + 1;
                /* new level of parenthesing */
                /* propagate NOT_BEGIN/NOT_END flags */
                flag1 |=
                    FILTER_FLAG_BEGIN | (expr_flag & FILTER_FLAG_NOT_BEGIN);
                flag2 |= FILTER_FLAG_END | (expr_flag & FILTER_FLAG_NOT_END);

                // don't create a new block if parent already has one
                if (!(expr_flag
                      & (FILTER_FLAG_BEGIN_BLOCK | FILTER_FLAG_NOT_BEGIN))) {
                    // Append begin node
                    flag1 |= FILTER_FLAG_BEGIN_BLOCK;
                    flag1 &= ~(FILTER_FLAG_BEGIN | FILTER_FLAG_NOT_BEGIN);
                    flag2 &= ~(FILTER_FLAG_END | FILTER_FLAG_NOT_END);
                    begin_end = true;
                    switch (op_ctx) {
                        case BOOL_OR:
                            lmgr_simple_filter_add_block(filter, FILTER_FLAG_OR
                                                     | FILTER_FLAG_BEGIN_BLOCK);
                            break;
                        case BOOL_AND:
                            lmgr_simple_filter_add_block(filter,
                                                     FILTER_FLAG_BEGIN_BLOCK);
                            break;
                        default:
                             DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                                              "BOOLEXPR unhandled case");
                             break;
                    }
                }
            }

            // check if expr1 and expr2 have valid DB filter
            // if not, do something about it
            dbcond1 = cond2sql_ok(boolexpr->content_u.bool_expr.expr1,
                                       smi, time_mod);
            dbcond2 = cond2sql_ok(boolexpr->content_u.bool_expr.expr2,
                                       smi, time_mod);

            if (dbcond1 && dbcond2) {
                rc = append_simple_expr(boolexpr->content_u.bool_expr.expr1,
                                        filter, smi, time_mod, flag1, new_depth,
                                        boolexpr->content_u.bool_expr.bool_op);
                if (rc) {
                    filter->filter_simple.filter_count = count_orig;
                    return rc;
                }
                rc = append_simple_expr(boolexpr->content_u.bool_expr.expr2,
                                        filter, smi, time_mod, flag2, new_depth,
                                        boolexpr->content_u.bool_expr.bool_op);
                if (begin_end)
                    lmgr_simple_filter_add_block(filter, FILTER_FLAG_END_BLOCK);

            } else if (dbcond1 && !dbcond2)
                rc = append_simple_expr(boolexpr->content_u.bool_expr.expr1,
                                        filter, smi, time_mod,
                                        flag1 | flag2, new_depth,
                                        boolexpr->content_u.bool_expr.bool_op);
            else if (!dbcond1 && dbcond2)
                rc = append_simple_expr(boolexpr->content_u.bool_expr.expr2,
                                        filter, smi, time_mod,
                                        flag1 | flag2, new_depth,
                                        boolexpr->content_u.bool_expr.bool_op);
            else
                // No valid conditions in exp1 and exp2
                rc = 0;

            if (rc)
                filter->filter_simple.filter_count = count_orig;
            return rc;
        }

    case NODE_CONSTANT:
        if (boolexpr->content_u.constant) {
            return 0;   /* 'and true' */
        } else {
            /* no sense, abort the query */
            DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                       "Building DB request which is always false?!");
            return DB_INVALID_ARG;
        }

    default:
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Invalid boolean expression %#x in %s()",
                   boolexpr->node_type, __FUNCTION__);
        return DB_INVALID_ARG;
    }
}

/** Convert simple expressions to ListMgr filter (append filter) */
int convert_boolexpr_to_simple_filter(bool_node_t *boolexpr,
                                      lmgr_filter_t *filter,
                                      const sm_instance_t *smi,
                                      const time_modifier_t *time_mod,
                                      enum filter_flags flags,
                                      bool_op_t op_ctx)
{
    int rc;

    if (!is_simple_expr(boolexpr, 0, op_ctx))
        return DB_INVALID_ARG;

    /* create a boolexpr as 'NOT ( <expr> )' */
    if (flags & FILTER_FLAG_NOT) {
        bool_node_t notexpr;
        int prev_nb;

        notexpr.node_type = NODE_UNARY_EXPR;
        notexpr.content_u.bool_expr.bool_op = BOOL_NOT;
        notexpr.content_u.bool_expr.expr1 = boolexpr;
        notexpr.content_u.bool_expr.owner = 0;

        /* add all or nothing => save filter count before */
        prev_nb = filter->filter_simple.filter_count;

        /* default filter context is op_ctx */
        rc = append_simple_expr(&notexpr, filter, smi, time_mod,
                                flags & ~FILTER_FLAG_NOT, 0, op_ctx);
        if (rc)
            filter->filter_simple.filter_count = prev_nb;

        return rc;
    }

    /* default filter context is op_ctx */
    rc = append_simple_expr(boolexpr, filter, smi, time_mod, flags,
                              0, op_ctx);
    return rc;
}

/** Set a complex filter structure */
int lmgr_set_filter_expression(lmgr_filter_t *p_filter,
                               struct bool_node_t *boolexpr)
{
    p_filter->filter_type = FILTER_BOOLEXPR;
    p_filter->filter_u.boolean_expr = boolexpr;
    return 0;
}

/** Check that all fields in filter are in the given mask of supported
 *  attributes
 * @param index if not NULL, it is set to the index of the unsupported filter.
 *              and -1 for other errors.
 */
int lmgr_check_filter_fields(lmgr_filter_t *p_filter, attr_mask_t attr_mask,
                             int *index)
{
    int i;

    if (index)
        *index = -1;

    if (p_filter->filter_type != FILTER_SIMPLE)
        return DB_INVALID_ARG;

    for (i = 0; i < p_filter->filter_simple.filter_count; i++) {
        /* Skip begin/end blocks as they are not related to an
         * attribute */
        if (p_filter->filter_simple.filter_flags[i] &
            (FILTER_FLAG_BEGIN_BLOCK | FILTER_FLAG_END_BLOCK))
            continue;

        if (!attr_mask_test_index(&attr_mask,
                                  p_filter->filter_simple.filter_index[i])) {
            if (index)
                *index = i;
            return DB_NOT_SUPPORTED;
        }
    }

    return DB_SUCCESS;
}
