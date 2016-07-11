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

int lmgr_simple_filter_init( lmgr_filter_t * p_filter )
{
    p_filter->filter_type = FILTER_SIMPLE;

    p_filter->filter_simple.filter_count = 0;

    p_filter->filter_simple.filter_flags =
        MemCalloc( FILTER_PREALLOC_INIT, sizeof( int ) );
    p_filter->filter_simple.filter_index =
        MemCalloc( FILTER_PREALLOC_INIT, sizeof( unsigned int ) );
    p_filter->filter_simple.filter_compar =
        MemCalloc( FILTER_PREALLOC_INIT, sizeof( filter_comparator_t ) );
    p_filter->filter_simple.filter_value =
        MemCalloc( FILTER_PREALLOC_INIT, sizeof( filter_value_t ) );

    if ( p_filter->filter_simple.filter_flags == NULL
         || p_filter->filter_simple.filter_index == NULL
         || p_filter->filter_simple.filter_compar == NULL
         || p_filter->filter_simple.filter_value == NULL )
        return DB_NO_MEMORY;

    p_filter->filter_simple.prealloc = FILTER_PREALLOC_INIT;
    return 0;
}

static int convert_regexp( const char * in_string, char * db_string )
{
    char * p_wild;

   strcpy( db_string, in_string );

    /* replace classes [] with _ */

    while ( (p_wild = strchr(db_string,'[')) != NULL )
    {
        char * p_end = strchr(p_wild,']');

        if (p_end == NULL)
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "Error unmatched '[' in regexp '%s'.", in_string );
            return DB_INVALID_ARG;
        }
        /* copy from character after ']' to the caracter after '['
         * and replace '[' with '_'
         */
        strcpy( p_wild+1, p_end+1);
        *p_wild = '_';
    }


   while ( (p_wild = strchr(db_string,'*')) != NULL )
        *p_wild = '%';

    while ( (p_wild = strchr(db_string,'?')) != NULL )
        *p_wild = '_';


    return 0;
}

static int lmgr_simple_filter_dup_buffers(lmgr_filter_t * p_filter, unsigned int index)
{
    filter_comparator_t comparator = p_filter->filter_simple.filter_compar[index];
    filter_value_t *p_value = &p_filter->filter_simple.filter_value[index];
    int flag = p_filter->filter_simple.filter_flags[index];

    /* @TODO support lists of strings (with both FILTER_FLAG_ALLOC_STR and FILTER_FLAG_ALLOC_LIST */

    if ((comparator == LIKE) || (comparator == UNLIKE) || (comparator == RLIKE)
        || (comparator == ILIKE) || (comparator == IUNLIKE))
    {
        int rc;
        char * newstr = MemAlloc( strlen(p_value->value.val_str)+1 );

        if (comparator != RLIKE) /* value is a perl regexp, don't convert it */
        {
            rc = convert_regexp( p_value->value.val_str, newstr );
            if ( rc )
            {
                MemFree(newstr);
                return rc;
            }
        }
        else
            strcpy(newstr, p_value->value.val_str);

        /* free the previous string */
        if ( flag & FILTER_FLAG_ALLOC_STR )
            MemFree( (char*)p_value->value.val_str );

        /* mark the new string as releasable */
        p_filter->filter_simple.filter_flags[index] |= FILTER_FLAG_ALLOC_STR;
        p_filter->filter_simple.filter_value[index].value.val_str = newstr;
    }
    else if ((comparator == IN) || (comparator == NOTIN) )
    {
        /* allocate and copy the list */
        db_type_u *values = (db_type_u *)MemAlloc(p_value->list.count * sizeof(db_type_u));
        memcpy(values, p_value->list.values, p_value->list.count * sizeof(db_type_u));

        /* free the previous list */
        if ( flag & FILTER_FLAG_ALLOC_LIST )
            MemFree( (char*)p_value->list.values );

        p_filter->filter_simple.filter_flags[index] |= FILTER_FLAG_ALLOC_LIST;
        p_filter->filter_simple.filter_value[index].list.values = values;
    }

    return 0;
}

static void lmgr_simple_filter_free_buffers(lmgr_filter_t * p_filter, unsigned int index)
{
    /* @TODO support lists of strings (with both FILTER_FLAG_ALLOC_STR and FILTER_FLAG_ALLOC_LIST */

    /* check if previous value must be released */
    if ( ( p_filter->filter_simple.filter_flags[index] & FILTER_FLAG_ALLOC_STR )
         && ( p_filter->filter_simple.filter_value[index].value.val_str != NULL ) )
    {
        MemFree( (char*)p_filter->filter_simple.filter_value[index].value.val_str );
    }
    else if (( p_filter->filter_simple.filter_flags[index] & FILTER_FLAG_ALLOC_LIST)
            && ( p_filter->filter_simple.filter_value[index].list.values != NULL ))
    {
        MemFree( (char*)p_filter->filter_simple.filter_value[index].list.values );
    }
}



int lmgr_simple_filter_add( lmgr_filter_t * p_filter, unsigned int attr_index,
                            filter_comparator_t comparator, filter_value_t value,
                            int flag )
{
    int rc;

    if ( p_filter->filter_type != FILTER_SIMPLE )
        return DB_INVALID_ARG;

    if ( p_filter->filter_simple.filter_count >= p_filter->filter_simple.prealloc )
    {
        /* double the size of the buffers */
        p_filter->filter_simple.prealloc *= 2;

        p_filter->filter_simple.filter_flags =
            MemRealloc( p_filter->filter_simple.filter_flags,
                        p_filter->filter_simple.prealloc * sizeof( int ) );
        p_filter->filter_simple.filter_index =
            MemRealloc( p_filter->filter_simple.filter_index,
                        p_filter->filter_simple.prealloc * sizeof( unsigned int ) );
        p_filter->filter_simple.filter_compar =
            MemRealloc( p_filter->filter_simple.filter_compar,
                        p_filter->filter_simple.prealloc * sizeof( filter_comparator_t ) );
        p_filter->filter_simple.filter_value =
            MemRealloc( p_filter->filter_simple.filter_value,
                        p_filter->filter_simple.prealloc * sizeof( filter_value_t ) );

        if ( p_filter->filter_simple.filter_flags == NULL
             || p_filter->filter_simple.filter_index == NULL
             || p_filter->filter_simple.filter_compar == NULL
             || p_filter->filter_simple.filter_value == NULL )
            return DB_NO_MEMORY;
    }

    p_filter->filter_simple.filter_flags[p_filter->filter_simple.filter_count] = flag;
    p_filter->filter_simple.filter_index[p_filter->filter_simple.filter_count] = attr_index;
    p_filter->filter_simple.filter_compar[p_filter->filter_simple.filter_count] = comparator;
    p_filter->filter_simple.filter_value[p_filter->filter_simple.filter_count] = value;

    /* duplicate and copy buffers if needed */
    rc = lmgr_simple_filter_dup_buffers(p_filter, p_filter->filter_simple.filter_count);
    if (rc)
        return rc;

    p_filter->filter_simple.filter_count++;

    return 0;
}

/* check if the given attribute is part of a filter */
int lmgr_filter_check_field(const lmgr_filter_t *p_filter, unsigned int attr_index)
{
    unsigned int i;

    if (p_filter->filter_type != FILTER_SIMPLE)
        return DB_INVALID_ARG;

    /* first check if there is already a filter on this argument */
    for (i = 0; i < p_filter->filter_simple.filter_count; i++)
    {
        if (p_filter->filter_simple.filter_index[i] == attr_index)
            return 1;
    }
    return 0;
}

int lmgr_simple_filter_add_or_replace( lmgr_filter_t * p_filter,
                                       unsigned int attr_index,
                                       filter_comparator_t comparator, filter_value_t value,
                                       int flag )
{
    unsigned int  i;
    int rc;

    if ( p_filter->filter_type != FILTER_SIMPLE )
        return DB_INVALID_ARG;

    /* first check if there is already a filter on this argument */
    for ( i = 0; i < p_filter->filter_simple.filter_count; i++ )
    {
        if ( p_filter->filter_simple.filter_index[i] == attr_index )
        {
            int syntax_flags = p_filter->filter_simple.filter_flags[i]
                               & (FILTER_FLAG_BEGIN | FILTER_FLAG_END
                                  | FILTER_FLAG_OR);

            /* check if previous value must be released */
            lmgr_simple_filter_free_buffers(p_filter, i);

            /* ensure parenthesing and 'OR' keywords are conserved */
            p_filter->filter_simple.filter_flags[i] = flag | syntax_flags;
            p_filter->filter_simple.filter_compar[i] = comparator;
            p_filter->filter_simple.filter_value[i] = value;

            /* duplicate and copy buffers if needed */
            rc = lmgr_simple_filter_dup_buffers(p_filter, i);
            if (rc)
                return rc;

            return 0;
        }
    }

    /* not found: add it */
    return lmgr_simple_filter_add( p_filter, attr_index, comparator, value, flag );
}

int lmgr_simple_filter_add_if_not_exist( lmgr_filter_t * p_filter,
                                         unsigned int attr_index,
                                         filter_comparator_t comparator, filter_value_t value,
                                         int flag )
{
    unsigned int   i;

    if ( p_filter->filter_type != FILTER_SIMPLE )
        return DB_INVALID_ARG;

    /* first check if there is already a filter on this argument */
    for ( i = 0; i < p_filter->filter_simple.filter_count; i++ )
    {
        if ( p_filter->filter_simple.filter_index[i] == attr_index )
        {
            return DB_ALREADY_EXISTS;
        }
    }

    /* not found: add it */
    return lmgr_simple_filter_add( p_filter, attr_index, comparator, value, flag );

}

int lmgr_simple_filter_free( lmgr_filter_t * p_filter )
{
    int i;

    if ( p_filter->filter_type != FILTER_SIMPLE )
        return DB_INVALID_ARG;

    /* free the values that must be released */
    for ( i = 0; i < p_filter->filter_simple.filter_count; i++ )
        lmgr_simple_filter_free_buffers(p_filter, i);

    if ( p_filter->filter_simple.filter_flags )
        MemFree( p_filter->filter_simple.filter_flags );
    if ( p_filter->filter_simple.filter_index )
        MemFree( p_filter->filter_simple.filter_index );
    if ( p_filter->filter_simple.filter_compar )
        MemFree( p_filter->filter_simple.filter_compar );
    if ( p_filter->filter_simple.filter_value )
        MemFree( p_filter->filter_simple.filter_value );
    memset( p_filter, 0, sizeof( lmgr_filter_t ) );
    return 0;
}

/* is it a simple 'AND' expression ? */
static bool is_simple_expr(bool_node_t *boolexpr, int depth, bool_op_t op_ctx)
{
    switch (boolexpr->node_type)
    {
        case NODE_UNARY_EXPR:
            if (boolexpr->content_u.bool_expr.bool_op != BOOL_NOT)
            {
                /* Error */
                DisplayLog(LVL_CRIT, LISTMGR_TAG, "Invalid unary operator %d in %s()",
                            boolexpr->content_u.bool_expr.bool_op, __FUNCTION__);
                return false;
            }
            /* only accept 'NOT condition', but reject 'NOT (cond AND cond)' */
            return  (boolexpr->content_u.bool_expr.expr1->node_type == NODE_CONDITION);

        case NODE_BINARY_EXPR:
            if (depth > 1)
                return false;
            else if (boolexpr->content_u.bool_expr.bool_op != BOOL_AND
                && boolexpr->content_u.bool_expr.bool_op != BOOL_OR)
                return false;

            /* bool operation context unchanged? */
            if (boolexpr->content_u.bool_expr.bool_op == op_ctx)
                return (is_simple_expr(boolexpr->content_u.bool_expr.expr1, depth, op_ctx)
                        && is_simple_expr(boolexpr->content_u.bool_expr.expr2, depth, op_ctx));
            else
                return (is_simple_expr(boolexpr->content_u.bool_expr.expr1, depth + 1,
                            boolexpr->content_u.bool_expr.bool_op)
                        && is_simple_expr(boolexpr->content_u.bool_expr.expr2, depth + 1,
                            boolexpr->content_u.bool_expr.bool_op));

        case NODE_CONDITION:
            /* If attribute is in DB, it can be filtered
             * If attribute is not in DB, we ignore it and get all entries (~ AND true)
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

static bool allow_null(unsigned int attr_index, const filter_comparator_t *comp,
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
        field_type(attr_index) == DB_ENUM_FTYPE)
    {
        if (*comp == EQUAL || *comp == LIKE || *comp == ILIKE)
            /* allow NULL if matching against empty string */
            return (val->value.val_str == NULL || EMPTY_STRING(val->value.val_str));
        else if (*comp == NOTEQUAL || *comp == UNLIKE || *comp == IUNLIKE)
            /* allow NULL if matching != non-empty string */
            return !(val->value.val_str == NULL || EMPTY_STRING(val->value.val_str));
        else
        {
            /* unexpected case */
            DisplayLog(LVL_CRIT, LISTMGR_TAG, "Warning: unhandled case in %s(), line %u",
                       __func__, __LINE__);
        }
    }
    return true; /* allow, by default */
}

/* Extract simple pieces of expressions and append them to filter.
 * The resulting filter is expected to return a larger set than the actual condition.
 * Ignore conflicting criteria.
 * \param expr_flag indicate if BEGIN/END parenthesing is needed
 * \param depth indicate the current parenthesing depth
 * \param op_ctx indicate the current operation context: e.g. AND for 'x and y and z'
 */
static int append_simple_expr(bool_node_t *boolexpr, lmgr_filter_t *filter,
                              const sm_instance_t *smi,
                              const time_modifier_t *time_mod, int expr_flag,
                              int depth, bool_op_t op_ctx)
{
    int                 rc, new_depth;
    unsigned int        index = ATTR_INDEX_FLG_UNSPEC;
    int                 flag = 0;
    filter_comparator_t comp;
    filter_value_t      val;
    bool                must_free;
    attr_mask_t         tmp;

    switch (boolexpr->node_type)
    {
        case NODE_UNARY_EXPR:
            if (boolexpr->content_u.bool_expr.bool_op != BOOL_NOT)
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG, "Invalid unary operator %d in %s()",
                            boolexpr->content_u.bool_expr.bool_op, __FUNCTION__);
                return DB_INVALID_ARG;
            }

            /* XXX is it possible to append a binary expr with parenthesing? e.g. NOT (x AND y) */
            if (boolexpr->content_u.bool_expr.expr1->node_type != NODE_CONDITION)
                /* do nothing (equivalent to 'AND TRUE') */
                return 0;

            /* get info about condition */
            rc = criteria2filter(boolexpr->content_u.bool_expr.expr1->content_u.condition,
                                 &index, &comp, &val, &must_free, smi, time_mod);

            if (rc != 0 || (index & ATTR_INDEX_FLG_UNSPEC))
                /* do nothing (equivalent to 'AND TRUE') */
                return 0;

            flag = FILTER_FLAG_NOT;

            if ((expr_flag & FILTER_FLAG_ALLOW_NULL) || allow_null(index, &comp, &val))
                flag |= FILTER_FLAG_ALLOW_NULL;
            if (must_free)
                flag |= FILTER_FLAG_ALLOC_STR;

            /* propagate parenthesing flag + OR (NOT?) */
            flag |= (expr_flag & (FILTER_FLAG_BEGIN | FILTER_FLAG_END
                                  | FILTER_FLAG_OR));

            /* @TODO support FILTER_FLAG_ALLOC_LIST */

            /* add condition to filter */
            DisplayLog(LVL_FULL, LISTMGR_TAG, "Appending filter on \"%s\", flags=%#X",
                       field_name(index), flag);

            return lmgr_simple_filter_add(filter, index, comp, val, flag);

         case NODE_CONDITION:
            /* If attribute is in DB, it can be filtered
             * If attribute is not in DB, we ignore it and get all entries (~ AND TRUE)
             */
            /* get info about condition */
            rc = criteria2filter(boolexpr->content_u.condition,
                                 &index, &comp, &val, &must_free,
                                 smi, time_mod);

            if (rc != 0 || (index & ATTR_INDEX_FLG_UNSPEC))
                /* do nothing (equivalent to 'AND TRUE') */
                return 0;

            /* test readonly fields */
            tmp = null_mask;
            attr_mask_set_index(&tmp, index);
            if (readonly_fields(tmp))
                /* do nothing (equivalent to 'AND TRUE') */
                return 0;

            if ((expr_flag & FILTER_FLAG_ALLOW_NULL)
                || allow_null(index, &comp, &val))
                flag |= FILTER_FLAG_ALLOW_NULL;

            if (must_free)
                flag |= FILTER_FLAG_ALLOC_STR;

            /* propagate parenthesing flag + OR (NOT?) */
            flag |= (expr_flag & (FILTER_FLAG_BEGIN | FILTER_FLAG_END
                                  | FILTER_FLAG_OR));

            /* @TODO support FILTER_FLAG_ALLOC_LIST */

            /* add condition to filter */
            DisplayLog(LVL_FULL, LISTMGR_TAG, "Appending filter on \"%s\", flags=%#X",
                       field_name(index), flag);

            return lmgr_simple_filter_add(filter, index, comp, val, flag);

        case NODE_BINARY_EXPR:
        {
            int flag1, flag2;

            /* ensures there are no 2 levels of perenthesing */
            if (depth > 1)
                return DB_INVALID_ARG;
            else if (boolexpr->content_u.bool_expr.bool_op != BOOL_AND
                && boolexpr->content_u.bool_expr.bool_op != BOOL_OR)
                return DB_INVALID_ARG;

            flag1 = (op_ctx == BOOL_OR)?FILTER_FLAG_OR:0;
            /* x OR y? */
            flag2 = (boolexpr->content_u.bool_expr.bool_op == BOOL_OR)?
                        FILTER_FLAG_OR:0;
            if (boolexpr->content_u.bool_expr.bool_op == op_ctx)
            {
                new_depth = depth;
                /* propagate BEGIN/END flags */
                flag1 |= expr_flag & FILTER_FLAG_BEGIN;
                flag2 |= expr_flag & FILTER_FLAG_END;
            }
            else
            {
                new_depth = depth + 1;
                /* new level of parenthesing */
                flag1 |= FILTER_FLAG_BEGIN;
                flag2 |= FILTER_FLAG_END;
            }

            rc = append_simple_expr(boolexpr->content_u.bool_expr.expr1,
                                    filter, smi, time_mod, flag1, new_depth,
                                    boolexpr->content_u.bool_expr.bool_op);
            if (rc) return rc;
            rc = append_simple_expr(boolexpr->content_u.bool_expr.expr2,
                                    filter, smi, time_mod, flag2, new_depth,
                                    boolexpr->content_u.bool_expr.bool_op);
            return rc;
        }

        case NODE_CONSTANT:
            if (boolexpr->content_u.constant)
            {
                return 0; /* 'and true' */
            }
            else
            {
                /* no sense, abort the query */
                DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Building DB request which is always false?!");
                return DB_INVALID_ARG;
            }

        default:
            DisplayLog(LVL_CRIT, LISTMGR_TAG, "Invalid boolean expression %#x in %s()",
                        boolexpr->node_type, __FUNCTION__);
            return DB_INVALID_ARG;
    }
}

/** Convert simple expressions to ListMgr filter (append filter) */
int convert_boolexpr_to_simple_filter(bool_node_t *boolexpr, lmgr_filter_t *filter,
                                      const sm_instance_t *smi,
                                      const time_modifier_t *time_mod,
                                      int flags)
{
    if (!is_simple_expr(boolexpr, 0, BOOL_AND))
        return DB_INVALID_ARG;

    /* create a boolexpr as 'NOT ( <expr> )' */
    if (flags & FILTER_FLAG_NOT)
    {
        bool_node_t notexpr;

        notexpr.node_type = NODE_UNARY_EXPR;
        notexpr.content_u.bool_expr.bool_op = BOOL_NOT;
        notexpr.content_u.bool_expr.expr1 = boolexpr;
        notexpr.content_u.bool_expr.owner = 0;

        /* default filter context is AND */
        return append_simple_expr(&notexpr, filter, smi, time_mod,
                                  flags & ~FILTER_FLAG_NOT, 0, BOOL_AND);
    }

    /* default filter context is AND */
    return append_simple_expr(boolexpr, filter, smi, time_mod, flags,
                              0, BOOL_AND);
}


/** Set a complex filter structure */
int lmgr_set_filter_expression( lmgr_filter_t * p_filter, struct bool_node_t *boolexpr )
{
    p_filter->filter_type = FILTER_BOOLEXPR;
    p_filter->filter_u.boolean_expr = boolexpr;
    return 0;
}

/** Check that all fields in filter are in the given mask of supported attributes
 * @param index if not NULL, it is set to the index of the unsupported filter.
 *              and -1 for other errors.
 */
int lmgr_check_filter_fields(lmgr_filter_t *p_filter, attr_mask_t attr_mask, int *index)
{
    int i;

    if (index)
        *index = -1;

    if (p_filter->filter_type != FILTER_SIMPLE)
        return DB_INVALID_ARG;

    for (i = 0; i < p_filter->filter_simple.filter_count; i++)
    {
        if (!attr_mask_test_index(&attr_mask,
                                  p_filter->filter_simple.filter_index[i]))
        {
            if (index)
                *index = i;
            return DB_NOT_SUPPORTED;
        }
    }

    return DB_SUCCESS;
}
