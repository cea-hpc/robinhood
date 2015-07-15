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
#include "database.h"
#include "listmgr_common.h"
#include "listmgr_stripe.h"
#include "RobinhoodLogs.h"
#include "Memory.h"
#include <stdio.h>
#include <stdlib.h>

/**
 * Create a (persitent) table to tag entries.
 * \param filter indicate this applies to a restricted set of entries.
 * \param reset indicate if the table is cleaned in case it already exists.
 */
int ListMgr_CreateTag(lmgr_t * p_mgr, const char *tag_name,
                      lmgr_filter_t * p_filter, int reset)
{
    char select[4096];
    char query[4096];
    int rc;
    char           filter_str_main[2048] = "";
    char           filter_str_annex[2048] = "";
    char           filter_str_name[2048] = "";
    char           filter_str_stripe_info[2048] = "";
    char           filter_str_stripe_items[2048] = "";
    int            filter_main = 0;
    int            filter_annex = 0;
    int            filter_name = 0;
    int            filter_stripe_info = 0;
    int            filter_stripe_items = 0;
    char           fields[2048];
    char           tables[2048];

    if ( !p_filter
         || ( ( p_filter->filter_type == FILTER_SIMPLE )
              && ( p_filter->filter_simple.filter_count == 0 ) )
         || ( ( p_filter->filter_type == FILTER_BOOLEXPR )
              && ( p_filter->filter_boolexpr == NULL ) ) )
    {
        /* no filter, create a table with all ids */
        strcpy(select, "SELECT id FROM "MAIN_TABLE );
    }
    else
    {
        int filters = 0;

        /* need to build filters to populate the table */

        /* on which table is the filter? */
        filter_main = filter2str( p_mgr, filter_str_main, p_filter, T_MAIN,
                                  FALSE, TRUE );
        filters += (filter_main > 0 ? 1 : 0);

        if ( annex_table )
            filter_annex = filter2str( p_mgr, filter_str_annex, p_filter,
                                     T_ANNEX, filters > 0, TRUE );
        else
            filter_annex = 0;
        filters += (filter_annex > 0 ? 1 : 0);

        filter_name = filter2str(p_mgr, filter_str_name, p_filter, T_DNAMES,
                                 filters > 0, TRUE);
        filters += (filter_name > 0 ? 1 : 0);

        filter_stripe_info =
            filter2str(p_mgr, filter_str_stripe_info, p_filter, T_STRIPE_INFO,
                       filters > 0, TRUE);
        filters += (filter_stripe_info > 0 ? 1 : 0);

        filter_stripe_items =
            filter2str( p_mgr, filter_str_stripe_items, p_filter, T_STRIPE_ITEMS,
                      ( filter_main > 0 ) || ( filter_annex > 0 )
                      || ( filter_stripe_info > 0 ), TRUE );
        filters += (filter_stripe_items > 0 ? 1 : 0);

        if (filter_main && (filters == 1))
        {
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Filter is only on " MAIN_TABLE " table" );
            sprintf(select, "SELECT id FROM " MAIN_TABLE " WHERE %s", filter_str_main);
        }
        else if (filter_annex && (filters == 1))
        {
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Filter is only on " ANNEX_TABLE " table" );
            sprintf(select, "SELECT id FROM " ANNEX_TABLE " WHERE %s", filter_str_annex);
        }
        else if (filter_name && (filters == 1))
        {
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Filter is only on " DNAMES_TABLE " table" );
            sprintf(select, "SELECT DISTINCT(id) FROM " DNAMES_TABLE " WHERE %s", filter_str_name);
        }
        else if (filter_stripe_info && !(filter_main || filter_annex || filter_stripe_items ))
        {
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Filter is only on " STRIPE_INFO_TABLE " table" );
            sprintf(select, "SELECT id FROM " STRIPE_INFO_TABLE " WHERE %s", filter_str_stripe_info);
        }
        else if ( filter_stripe_items && !( filter_main || filter_annex || filter_stripe_info ))
        {
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Filter is only on " STRIPE_ITEMS_TABLE " table" );
            sprintf(select, "SELECT DISTINCT(id) FROM " STRIPE_ITEMS_TABLE" WHERE %s", filter_str_stripe_items);
        }
        else /* filter on several tables */
        {
            char          *curr_fields = fields;
            char          *curr_tables = tables;
            char          *first_table = NULL;
            bool           distinct = 0;

            DisplayLog(LVL_FULL, LISTMGR_TAG,
                        "Filter on several tables: "
                        MAIN_TABLE ":%d, " ANNEX_TABLE ":%d, "
                        DNAMES_TABLE ":%d, "
                        STRIPE_INFO_TABLE ":%d, "
                        STRIPE_ITEMS_TABLE ":%d",
                        filter_main, filter_name, filter_annex,
                        filter_stripe_info, filter_stripe_items);

            if (filter_main > 0)
            {
                first_table = MAIN_TABLE;
                curr_fields += sprintf( curr_fields, "%s", filter_str_main );
                curr_tables += sprintf( curr_tables, "%s", MAIN_TABLE );
            }

            if (filter_annex > 0)
            {
                curr_fields += sprintf( curr_fields, "%s", filter_str_annex );

                if (first_table != NULL)
                {
                    *curr_tables = ',';
                    curr_tables++;

                    /* add junction condition */
                    curr_fields += sprintf(curr_fields, " AND %s.id=%s.id",
                                           MAIN_TABLE, ANNEX_TABLE );
                }
                else
                    first_table = ANNEX_TABLE;

                curr_tables += sprintf(curr_tables, "%s", ANNEX_TABLE);
            }

            if (filter_name > 0)
            {
                distinct = 1;
                if (filter_name > 0)
                    curr_fields += sprintf(curr_fields, "%s", filter_str_name);

                if (first_table != NULL)
                {
                    *curr_tables = ',';
                    curr_tables++;

                    /* also add junction condition */
                    curr_fields +=
                        sprintf(curr_fields, " AND %s.id=%s.id", first_table, DNAMES_TABLE);
                }
                else
                    first_table = DNAMES_TABLE;

                curr_tables += sprintf( curr_tables, "%s", DNAMES_TABLE );
            }

            if (filter_stripe_items > 0)
            {
                distinct = 1;
                curr_fields += sprintf(curr_fields, "%s", filter_str_stripe_items);

                if (first_table != NULL)
                {
                    *curr_tables = ',';
                    curr_tables++;

                    /* add junction condition */
                    if (filter_main > 0)
                        curr_fields += sprintf(curr_fields, " AND %s.id=%s.id",
                                               MAIN_TABLE, STRIPE_ITEMS_TABLE);
                    if (filter_annex > 0)
                        curr_fields += sprintf(curr_fields, " AND %s.id=%s.id",
                                               ANNEX_TABLE, STRIPE_ITEMS_TABLE);
                }
                else
                    first_table = STRIPE_ITEMS_TABLE;

                curr_tables += sprintf( curr_tables, "%s", STRIPE_ITEMS_TABLE );
            }

            if (filter_stripe_info > 0)
            {
                curr_fields += sprintf( curr_fields, "%s", filter_str_stripe_info );

                if (first_table != NULL)
                {
                    *curr_tables = ',';
                    curr_tables++;

                    /* add junction condition */
                    if (filter_main > 0)
                        curr_fields += sprintf(curr_fields, " AND %s.id=%s.id",
                                               MAIN_TABLE, STRIPE_INFO_TABLE);
                    if (filter_annex > 0)
                        curr_fields += sprintf(curr_fields, " AND %s.id=%s.id",
                                               ANNEX_TABLE, STRIPE_INFO_TABLE);
                    if (filter_stripe_items > 0)
                        curr_fields += sprintf(curr_fields, " AND %s.id=%s.id",
                                               STRIPE_ITEMS_TABLE, STRIPE_INFO_TABLE);
                }
                else
                    first_table = STRIPE_INFO_TABLE;

                curr_tables += sprintf( curr_tables, "%s", STRIPE_INFO_TABLE );
            }

            if (distinct)
                sprintf(select, "SELECT DISTINCT(%s.id) AS id FROM %s WHERE %s",
                        first_table, tables, fields);
            else
                sprintf(select, "SELECT %s.id as id FROM %s WHERE %s",
                        first_table, tables, fields);
        }
    }

    sprintf(query, "CREATE TABLE TAG_%s (id "PK_TYPE" PRIMARY KEY) AS %s",
            tag_name, select);

retry:
    rc = lmgr_begin( p_mgr );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    /* create the table */
    rc = db_exec_sql( &p_mgr->conn, query, NULL );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    /** TODO handle 'reset' option if table already exists */

    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    return rc;

rollback:
    lmgr_rollback( p_mgr );
    return rc;
}

/** destroy a tag */
int ListMgr_DestroyTag(lmgr_t * p_mgr, const char *tag_name)
{
    char tabname[1024];
    snprintf(tabname, 1024, "TAG_%s", tag_name);

    return db_drop_component( &p_mgr->conn, DBOBJ_TABLE, tabname );
}

/**
 * Tag an entry (in the set specified by CreateTag filter)
 */
int ListMgr_TagEntry(lmgr_t * p_mgr, const char *tag_name, const entry_id_t * p_id)
{
    char           request[1024];
    int            rc;
    DEF_PK(pk);

    /* We want the remove operation to be atomic */
retry:
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    entry_id2pk(p_id, PTR_PK(pk));

    /* Only keep untagged entries in the table, as the goal
     * is to list untagged entries at the end. */
    sprintf( request, "DELETE FROM TAG_%s WHERE id="DPK, tag_name, pk);
    rc = db_exec_sql( &p_mgr->conn, request, NULL );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    return rc;
}


/**
 * Return an iterator on non-tagged entries (in the set specified by CreateTag filter)
 */
struct lmgr_iterator_t *ListMgr_ListUntagged( lmgr_t * p_mgr,
                                          const char * tag_name,
                                          const lmgr_iter_opt_t * p_opt )
{
    char query[1024];
    char * query_end = query;
    struct lmgr_iterator_t * it;
    int rc;

    query_end += sprintf(query_end, "SELECT id FROM TAG_%s", tag_name);

    if (p_opt && (p_opt->list_count_max > 0))
        query_end += sprintf(query_end, " LIMIT %u", p_opt->list_count_max);

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
retry:
    rc = db_exec_sql( &p_mgr->conn, query, &it->select_result );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
    {
        MemFree( it );
        return NULL;
    }
    else
        return it;
}
