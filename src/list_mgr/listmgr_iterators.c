/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009 CEA/DAM
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
#include "listmgr_common.h"
#include "listmgr_stripe.h"
#include "listmgr_internal.h"
#include "database.h"
#include "Memory.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include <stdio.h>
#include <stdlib.h>

/* generate a select query that defines the given dirattr with the given name.
 * (for FILTERDIR_OTHER types)
 * e.g. SELECT parent_id, COUNT(*) as dirattr... FROM ...
 * \return number of bytes written (-1 on error)
 */
static int append_dirattr_select(GString *str, unsigned int dirattr_index,
                                 const char *attrname)
{
    if (dirattr_index == ATTR_INDEX_dircount)
    {
        /* group parent and count their children */
        g_string_append_printf(str, "SELECT parent_id, %s as %s "
                               "FROM "DNAMES_TABLE" GROUP BY parent_id",
                               dirattr2str(ATTR_INDEX_dircount), attrname);
        return 0;
    }
    else if (dirattr_index == ATTR_INDEX_avgsize)
    {
        /* join all entries and avg their size when grouping by parent */
        g_string_append_printf(str, "SELECT parent_id, %s as %s from "DNAMES_TABLE" d,"
                               MAIN_TABLE" m WHERE d.id=m.id and m.type='file' GROUP BY parent_id",
                               dirattr2str(ATTR_INDEX_avgsize), attrname);
        return 0;
    }
    return -1;
}

#define append_dir_filter(_a1, _a2, _a3, _a4, _a5, _a6) append_dir_req(_a1, _a2, _a3, ATTR_INDEX_FLG_UNSPEC, _a4, _a5, _a6)


/**
 * Append a directory condition (sort or filter on dirattr) to an iterator request.
 */
static void append_dir_req(GString *from, GString *where, unsigned int sort_attr_index,
                           filter_dir_e filter_dir, /* type of dir filter */
                           unsigned int filter_dir_index, /* index of filter dirattr */
                           const char *filter_dir_str) /* looks like dirattr >= X */
{
    if (sort_attr_index & ATTR_INDEX_FLG_UNSPEC)
    {
        switch (filter_dir)
        {
            case FILTERDIR_NONE:
                break;

            case FILTERDIR_EMPTY:
                /* only empty dir filter is to be appended */
                g_string_append_printf(where, " AND %s", filter_dir_str);
                break;

            case FILTERDIR_OTHER:

                /* join dir entry attributes from main table with special dir attrs */
                g_string_append(from, " INNER JOIN (");
                append_dirattr_select(from, filter_dir_index, "dirattr");
                g_string_append_printf(from, ") AS da ON id=da.parent_id");
                g_string_append_printf(where, " AND %s", filter_dir_str);
                break;

            default:
                DisplayLog( LVL_CRIT, LISTMGR_TAG, "Unexpected filter on directory attribute in %s(): %#x",
                            __func__, filter_dir );
                /* ignore dir filter */
                break;
        }
    }
    else /* sorting on dirattr */
    {
        /* sort on 1 dirattr, possibly needs a second for filter */
        switch (filter_dir)
        {
            case FILTERDIR_NONE:
                /* implicit filter on 'type == dir' */
                /* @TODO optim: directly perform request on parent_id if no table_filter? */
                g_string_append(from, " INNER JOIN (");
                append_dirattr_select(from, sort_attr_index, "dirattr_sort");
                g_string_append(from, ") AS ds ON id=ds.parent_id");
                break;

            case FILTERDIR_EMPTY:
                /* join with empty dir filter + dirattr_sort */
                g_string_append(from, " INNER JOIN (");
                append_dirattr_select(from, sort_attr_index, "dirattr_sort");
                g_string_append(from ,") AS ds ON id=ds.parent_id");
                g_string_append(where, filter_dir_str); /* FIXME add AND? */
                break;

            case FILTERDIR_OTHER:
                /* left join with dirattr_sort + right join on filter */
                g_string_append(from, "  LEFT JOIN (");
                append_dirattr_select(from, sort_attr_index, "dirattr_sort");
                g_string_append(from, ") ds ON id=ds.parent_id RIGHT JOIN (");
                append_dirattr_select(from, filter_dir_index, "dirattr");
                g_string_append(from, ") da ON id=da.parent_id");
                g_string_append(where, filter_dir_str);  /* FIXME add AND? */
                break;

            default:
                DisplayLog( LVL_CRIT, LISTMGR_TAG, "Unexpected filter on directory attribute in %s(): %#x",
                            __func__, filter_dir );
                /* ignore dir filter */
                break;
        }
    }
}

/** Determine sort operation to be done.
 * Set t_sort or sort_dirattr depending on sort type.
 */
static inline void check_sort(const lmgr_sort_type_t *p_sort_type,
                              table_enum *t_sort, unsigned int *sort_dirattr,
                              bool *distinct)
{
    /* initialize outputs */
    *t_sort = T_NONE;
    *sort_dirattr = ATTR_INDEX_FLG_UNSPEC;

    /* is there a sort order ? */
    if (p_sort_type == NULL || p_sort_type->order == SORT_NONE)
        return;

    /* check sort order */
    if (is_main_field(p_sort_type->attr_index))
        *t_sort = T_MAIN;
    else if (is_annex_field(p_sort_type->attr_index))
        *t_sort = T_ANNEX;
    else if (field_infos[p_sort_type->attr_index].db_type == DB_STRIPE_INFO)
        *t_sort = T_STRIPE_INFO;
    else if (field_infos[p_sort_type->attr_index].db_type == DB_STRIPE_ITEMS)
    {
        *t_sort = T_STRIPE_ITEMS;
        *distinct = true;
    }
    else if (is_dirattr(p_sort_type->attr_index))
        *sort_dirattr = p_sort_type->attr_index;
    else
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Invalid field for sort order (index=%d)", p_sort_type->attr_index);
}

/** Indicate if a sort operation is to be done depending on the variables
 * set by check_sort(). */
static inline bool do_sort(table_enum t_sort, unsigned int sort_dirattr)
{
    return (t_sort != T_NONE) || ((sort_dirattr & ATTR_INDEX_FLG_UNSPEC) == 0);
}

static int select_all_request(lmgr_t *p_mgr, GString *req, table_enum sort_table,
                              unsigned int sort_dirattr, bool distinct)
{
    if (!do_sort(sort_table, sort_dirattr))
    {
        DisplayLog(LVL_FULL, LISTMGR_TAG, "Empty filter: all records will be selected");
        g_string_assign(req, "SELECT id FROM "MAIN_TABLE);
    }
    else if (sort_table != T_NONE)
    {
        g_string_printf(req, "SELECT %s FROM %s", distinct?"DISTINCT(id)":"id",
                        table2name(sort_table));
    }
    else if ((sort_dirattr & ATTR_INDEX_FLG_UNSPEC) == 0)
    {
        append_dirattr_select(req, sort_dirattr, "dirattr_sort");
    }
    else
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Unsupported sort table");
        return DB_NOT_SUPPORTED;
    }
    return DB_SUCCESS;
}

/** get an iterator on a list of entries */
struct lmgr_iterator_t *ListMgr_Iterator(lmgr_t *p_mgr,
                                         const lmgr_filter_t *p_filter,
                                         const lmgr_sort_type_t *p_sort_type,
                                         const lmgr_iter_opt_t *p_opt)
{
    int                 rc;
    lmgr_iterator_t    *it;
    filter_dir_e        filter_dir_type = FILTERDIR_NONE;
    unsigned int        filter_dir_index = 0;
    table_enum          sort_table = T_NONE;
    unsigned int        sort_dirattr = ATTR_INDEX_FLG_UNSPEC;
    struct field_count  fcnt = {0};
    bool                distinct = false;
    table_enum          query_tab = T_NONE;

    GString            *from = NULL;
    GString            *where = NULL;
    GString            *req = NULL;
    GString            *filter_dir = NULL;

    /* Iterator only select a sorted list of ids.
     * Entry attributes are retrieved afterward in ListMgr_GetNext() call.
     */

    /* is there a sort order? */
    check_sort(p_sort_type, &sort_table, &sort_dirattr, &distinct);

    /* initialize the request */
    req = g_string_new(NULL);

    if (no_filter(p_filter))
    {
        /* no filter is specified: build a select request with no criteria */
        rc = select_all_request(p_mgr, req, sort_table, sort_dirattr, distinct);
        if (rc)
            goto free_str;
    }
    else /* analyse filter contents */
    {
        unsigned int nbft;

        /* check condition on directory */
        filter_dir = g_string_new(NULL);
        filter_dir_type = dir_filter(p_mgr, filter_dir, p_filter, &filter_dir_index,
                                     (sort_table != T_NONE) ?
                                        table2name(sort_table) : MAIN_TABLE);
        /* XXX is sort dirattr the same as filter dirattr? */

        where = g_string_new(NULL);
        filter_where(p_mgr, p_filter, &fcnt, where, 0);

        nbft = nb_field_tables(&fcnt);

        /* finally, there was no filter */
        if (nbft == 0 && filter_dir_type == FILTERDIR_NONE)
        {
            rc = select_all_request(p_mgr, req, sort_table, sort_dirattr, distinct);
            if (rc)
                goto free_str;
        }
        else
        {
            /* build the FROM clause */
            from = g_string_new(NULL);
            filter_from(p_mgr, &fcnt, from, &query_tab, &distinct, 0);

            /* If there is a single table: use the filter as is.
             * Else, build the filter a more ordered way */
            if (nbft > 1)
            {
                /* rebuild the contents of "where", a more ordered way */
                g_string_assign(where, "");
                if (unlikely(filter2str(p_mgr, where, p_filter, T_NONE, AOF_PREFIX) <= 0))
                    RBH_BUG("Inconsistent case: more than 1 filter table, but no filter???");
            }

            append_dir_req(from, where, sort_dirattr, filter_dir_type,
                           filter_dir_index, filter_dir->str);

            /* build the whole request */
            if (distinct)
                g_string_printf(req, "SELECT DISTINCT(%s.id) AS id",
                                table2name(query_tab));
            else
                g_string_printf(req, "SELECT %s.id AS id",
                                table2name(query_tab));

            g_string_append_printf(req, " FROM %s WHERE %s", from->str, where->str);
        }
    }

#define SORT_ATTR_OPTIM (ATTR_INDEX_FLG_UNSPEC | 0x2)
#if 0 /** @TODO RBHv3 to be reimplemnted */
                        /* both filter and sort order */
                        /* @TODO optim if both filter and sort order are on the same field */
                        if (filter_dir_index == sort_dirattr)
                        {
                            query_end = query + append_dirattr_select(query, filter_dir_index, "dirattr");
                            query_end += sprintf(query_end, " HAVING %s", filter_dir_str);
                            /* special value for this optim */
                            sort_dirattr = SORT_ATTR_OPTIM;
                        }
                        else
                        {
                            query_end = query + sprintf(query, "SELECT da.parent_id FROM (");
                            query_end += append_dirattr_select(query_end, filter_dir_index, "dirattr");
                            query_end += sprintf(query_end, " HAVING %s) da LEFT JOIN (", filter_dir_str);
                            query_end += append_dirattr_select(query_end, sort_dirattr, "dirattr_sort");
                            query_end += sprintf(query_end, ") ds ON ds.parent_id = da.parent_id");
                        }
#endif

    /* sort order */
    if (do_sort(sort_table, sort_dirattr))
    {
        /* special cases: stripe info stands for pool_name, stripe items for ost_idx */
        if (sort_table == T_STRIPE_INFO)
            g_string_append(req, " ORDER BY "STRIPE_INFO_TABLE".pool_name ");
        else if (sort_table == T_STRIPE_ITEMS)
            g_string_append(req, " ORDER BY "STRIPE_ITEMS_TABLE".ostidx ");
        else if (sort_table != T_NONE)
            g_string_append_printf(req, " ORDER BY %s.%s ", table2name(sort_table),
                                   field_name(p_sort_type->attr_index));
        else if (sort_dirattr == SORT_ATTR_OPTIM)
            g_string_append(req, " ORDER BY dirattr ");
        else if ((sort_dirattr & ATTR_INDEX_FLG_UNSPEC) == 0)
            g_string_append(req, " ORDER BY dirattr_sort ");

        if (p_sort_type->order == SORT_ASC)
            g_string_append(req, "ASC");
        else
            g_string_append(req, "DESC");
    }

    /* iterator opt */
    if (p_opt && (p_opt->list_count_max > 0))
        g_string_append_printf(req, " LIMIT %u", p_opt->list_count_max);

    /* allocate a new iterator */
    it = (lmgr_iterator_t *) MemAlloc(sizeof(lmgr_iterator_t));
    it->p_mgr = p_mgr;
    if (p_opt)
    {
        it->opt = *p_opt;
        it->opt_is_set = 1;
    }
    else
    {
        it->opt_is_set = 0;
    }

    /* execute request */
    rc = db_exec_sql(&p_mgr->conn, req->str, &it->select_result);
    if (rc)
        goto free_it;

    if (filter_dir != NULL)
        g_string_free(filter_dir, TRUE);
    if (from != NULL)
        g_string_free(from, TRUE);
    if (where != NULL)
        g_string_free(where, TRUE);
    if (req != NULL)
        g_string_free(req, TRUE);

    return it;

free_it:
    if (it != NULL)
        MemFree(it);
free_str:
    if (filter_dir != NULL)
        g_string_free(filter_dir, TRUE);
    if (from != NULL)
        g_string_free(from, TRUE);
    if (where != NULL)
        g_string_free(where, TRUE);
    if (req != NULL)
        g_string_free(req, TRUE);
    return NULL;
}



int ListMgr_GetNext( struct lmgr_iterator_t *p_iter, entry_id_t * p_id, attr_set_t * p_info )
{
    int            rc = 0;
    /* can contain id+dirattr+dirattr_sort in case of directory listing */
    char          *idstr[3];
    DEF_PK(pk);

    bool           entry_disappeared = false;

    do
    {
        entry_disappeared = false;

        idstr[0] = idstr[1] = idstr[2] = NULL;
        rc = db_next_record( &p_iter->p_mgr->conn, &p_iter->select_result, idstr, 3 );

        if ( rc )
            return rc;
        if ( idstr[0] == NULL )
            return DB_REQUEST_FAILED;

        rc = parse_entry_id(p_iter->p_mgr, idstr[0], PTR_PK(pk), p_id);
        if (rc == DB_NOT_EXISTS)
             entry_disappeared = true;
        else if (rc)
            return rc;

        rc = listmgr_get_by_pk( p_iter->p_mgr, pk, p_info );

        if ( rc == DB_NOT_EXISTS )
        {
            if (p_iter->opt_is_set && p_iter->opt.allow_no_attr)
            {
                /* clear missing fields */
                p_info->attr_mask = attr_mask_and(&p_info->attr_mask, &dir_attr_set);
                /* special field dircount */
                if (dirattr_fields(p_info->attr_mask))
                {
                    if (listmgr_get_dirattrs(p_iter->p_mgr, pk, p_info))
                    {
                        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "listmgr_get_dirattr failed for "DPK, pk );
                        p_info->attr_mask = attr_mask_and_not(&p_info->attr_mask, &dir_attr_set);
                    }
                }

                /* compute generated fields if asked */
                generate_fields( p_info );

                rc = 0;
            }
            else
                entry_disappeared = true;
        }
    }
    while ( entry_disappeared );        /* goto next record if entry desappered */

    return rc;

}


void ListMgr_CloseIterator( struct lmgr_iterator_t *p_iter )
{
    db_result_free( &p_iter->p_mgr->conn, &p_iter->select_result );
    MemFree( p_iter );
}
