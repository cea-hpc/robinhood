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
#include "rbh_logs.h"
#include "Memory.h"
#include <stdio.h>
#include <stdlib.h>

/**
 * Create a (persitent) table to tag entries.
 * \param filter indicate this applies to a restricted set of entries.
 * \param reset indicate if the table is cleaned in case it already exists.
 */
int ListMgr_CreateTag(lmgr_t * p_mgr, const char *tag_name,
                      lmgr_filter_t * p_filter, bool reset)
{
    GString         *req = NULL;
    GString         *from = NULL;
    GString         *where = NULL;
    table_enum       query_tab = T_NONE;
    bool             distinct = false;
    int              rc;
    struct field_count fcnt = {0};

    /* create table statement */
    req = g_string_new("CREATE TABLE ");
    g_string_append_printf(req, "TAG_%s (id "PK_TYPE" PRIMARY KEY) AS ",
                           tag_name);

    /* now build the SELECT clause */
    if (no_filter(p_filter))
    {
        /* no filter, create a table with all ids */
        g_string_append(req, "SELECT id FROM "MAIN_TABLE);
    }
    else
    {
        where = g_string_new(NULL);
        filter_where(p_mgr, p_filter, &fcnt, where, 0);

        if (nb_field_tables(&fcnt) == 0)
        {
            /* finally, no filter */
            g_string_append(req, "SELECT id FROM "MAIN_TABLE);
        }
        else
        {
            /* build the FROM clause */
            from = g_string_new(NULL);
            filter_from(p_mgr, &fcnt, from, &query_tab, &distinct, 0);

            if (distinct)
                g_string_printf(req, "SELECT DISTINCT(%s.id) AS id",
                                table2name(query_tab));
            else
                g_string_printf(req, "SELECT %s.id AS id",
                                table2name(query_tab));

            g_string_append_printf(req, "FROM %s WHERE %s", from->str, where->str);
        }
    }

retry:
    rc = lmgr_begin( p_mgr );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto free_str;

    /* create the table */
    rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    /** TODO handle 'reset' option if table already exists */

    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;

    goto free_str;

rollback:
    lmgr_rollback(p_mgr);
free_str:
    if (req != NULL)
        g_string_free(req, TRUE);
    if (from != NULL)
        g_string_free(from, TRUE);
    if (where != NULL)
        g_string_free(where, TRUE);
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
