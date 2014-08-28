/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009, 2010 CEA/DAM
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
#include "database.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "Memory.h"
#include "var_str.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>


static int clean_names(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                       int *has_name_filter)
{
    char filter_str[RBH_PATH_MAX];
    char query[RBH_PATH_MAX+128];
    int rc;

    int filter_names = filter2str(p_mgr, filter_str, p_filter, T_DNAMES, false, false);
    *has_name_filter = filter_names?1:0;

    /* if there is a filter on names, clean them independantly, whatever the over filters */
    if (filter_names)
    {
        DisplayLog(LVL_DEBUG, LISTMGR_TAG, "Direct deletion in "DNAMES_TABLE" table");
        sprintf(query, "DELETE FROM "DNAMES_TABLE" WHERE %s", filter_str);

        rc = db_exec_sql(&p_mgr->conn, query, NULL);
        if (rc)
            return rc;
    }
    return DB_SUCCESS;
}


#define APPEND_TABLE_JOIN(_table_name, _alias) do { \
        curr_fields += sprintf(curr_fields, "%s%c.*", curr_fields == fields?"":",", _alias); \
        if (curr_tables == tables) { \
            first_table = _alias; \
            curr_tables += sprintf(curr_tables, "%s %c", _table_name, _alias); \
        } else \
            curr_tables += sprintf(curr_tables, " LEFT JOIN %s %c ON %c.id = %c.id", \
                                   _table_name, _alias, first_table, _alias); \
        if (EMPTY_STRING(where)) \
            sprintf(where, "%c.id="DPK, _alias, pk); \
    } while (0)


static int listmgr_remove_single(lmgr_t *p_mgr, PK_ARG_T pk, table_enum exclude_tab)
{
    char fields[1024];
    char tables[1024];
    char first_table = '\0';
    char where[1024] = "";
    char request[4096];
    char *curr_fields = fields;
    char *curr_tables = tables;

    if (exclude_tab != T_MAIN)
        APPEND_TABLE_JOIN(MAIN_TABLE, 'M');
    if (exclude_tab != T_ANNEX)
        APPEND_TABLE_JOIN(ANNEX_TABLE, 'A');
    if (exclude_tab != T_DNAMES)
        APPEND_TABLE_JOIN(DNAMES_TABLE, 'N');
#ifdef _LUSTRE
    if (exclude_tab != T_STRIPE_INFO)
        APPEND_TABLE_JOIN(STRIPE_INFO_TABLE, 'I');
    if (exclude_tab != T_STRIPE_ITEMS)
        APPEND_TABLE_JOIN(STRIPE_ITEMS_TABLE, 'S');
#endif

    /* doing this in a single request instead of 1 DELETE per table
     * results in a huge speed up (246sec -> 59s).
     */
    sprintf(request, "DELETE %s FROM %s WHERE %s", fields, tables, where);

    return db_exec_sql(&p_mgr->conn, request, NULL);
}


int listmgr_remove_no_tx(lmgr_t *p_mgr, const entry_id_t *p_id,
                         const attr_set_t *p_attr_set, bool last)
{
    char           request[4096];
    int            rc;
    DEF_PK(pk);
    DEF_PK(ppk);

    entry_id2pk(p_id, PTR_PK(pk));

    if (last)
    {
        /* remove from all tables except from NAMES (handled at the end of this function) */
        rc = listmgr_remove_single(p_mgr, pk, T_DNAMES);
        if (rc)
            return rc;
    } else {
        /* XXX else update attributes according to attributes contents? */

#ifdef ATTR_INDEX_nlink
        /* Since we're removing one entry but not the file, decrement nlink. */
        sprintf( request, "UPDATE " MAIN_TABLE " set nlink=nlink-1 where id="DPK, pk);
        rc = db_exec_sql( &p_mgr->conn, request, NULL );
        if ( rc )
            return rc;
#endif
    }

    /* Allow removing entry from MAIN_TABLE without removing it from NAMES */
    if (p_attr_set && ATTR_MASK_TEST(p_attr_set, parent_id) && ATTR_MASK_TEST(p_attr_set, name))
    {
        char escaped[RBH_NAME_MAX*2];

        entry_id2pk(&ATTR( p_attr_set, parent_id ), PTR_PK(ppk));

        db_escape_string(&p_mgr->conn, escaped, RBH_NAME_MAX*2, ATTR(p_attr_set, name));

        sprintf(request, "DELETE FROM " DNAMES_TABLE " WHERE pkn="HNAME_FMT" AND id="DPK,
                ppk, escaped, pk);
        rc = db_exec_sql( &p_mgr->conn, request, NULL );
        if ( rc )
            return rc;
    }
    else if (!p_attr_set || !ATTR_MASK_TEST(p_attr_set, parent_id) || !ATTR_MASK_TEST(p_attr_set, name))
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "WARNING: missing attribute to delete entry from "DNAMES_TABLE);

    return rc;
}


int ListMgr_Remove(lmgr_t *p_mgr, const entry_id_t *p_id,
                   const attr_set_t *p_attr_set, bool last)
{
    int rc;

    /* We want the remove operation to be atomic */
retry:
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    rc = listmgr_remove_no_tx( p_mgr, p_id, p_attr_set, last );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
    {
        lmgr_rollback( p_mgr );
        return rc;
    }

    rc = lmgr_commit( p_mgr );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    if (!rc)
         p_mgr->nbop[OPIDX_RM]++;
    return rc;
}

/**
 * Insert all entries to soft rm table.
 * @TODO check if it works with millions/billion entries
 */
static int listmgr_softrm_all( lmgr_t * p_mgr, time_t rm_time )
{
    char softrm_fields[1024];
    char main_fields[1024];
    char annex_fields[1024];
    char query[2048];
    int nbmain = 0;
    int nbannex = 0;

    /* insert those entries to soft rm table */

    /* manage fullpath independantly to make sure it is the first attribute,
     * as we will set it to one_path(id). */
    attrmask2fieldlist(softrm_fields, SOFTRM_MASK & ~ATTR_MASK_fullpath, T_SOFTRM,
                       true,  false, "","");

    if (annex_table)
        nbannex = attrmask2fieldlist(annex_fields, SOFTRM_MASK, T_ANNEX, true,  false,
                                     ANNEX_TABLE".", "");

    nbmain = attrmask2fieldlist(main_fields, SOFTRM_MASK, T_MAIN, true,  false,
                                nbannex > 0 ? MAIN_TABLE".":"", "");

    if (nbannex > 0)
    {
        sprintf(query, "INSERT IGNORE INTO " SOFT_RM_TABLE " (id,fullpath%s) "
                "(SELECT "MAIN_TABLE".id,"ONE_PATH_FUNC"("MAIN_TABLE".id)%s%s,%u"
                " FROM "MAIN_TABLE " LEFT JOIN "ANNEX_TABLE
                " ON " MAIN_TABLE ".id = " ANNEX_TABLE ".id)",
                softrm_fields, main_fields, annex_fields, (unsigned int)rm_time);
    }
    else
    {
       sprintf(query, "INSERT IGNORE INTO " SOFT_RM_TABLE " (id,fullpath%s) "
               "(SELECT id,"ONE_PATH_FUNC"(id)%s, %u FROM "MAIN_TABLE")",
               softrm_fields, main_fields, (unsigned int)rm_time);
    }

    return db_exec_sql( &p_mgr->conn, query, NULL );
}

/**
 * delete all entries from all tables (except softrm
 */
static int listmgr_rm_all(lmgr_t * p_mgr)
{
    int rc;

    /* stripes are only managed for lustre filesystems */
#ifdef _LUSTRE
    rc = db_exec_sql(&p_mgr->conn, "DELETE FROM " STRIPE_ITEMS_TABLE, NULL);
    if (rc)
        return rc;

    rc = db_exec_sql(&p_mgr->conn, "DELETE FROM " STRIPE_INFO_TABLE, NULL);
    if (rc)
        return rc;
#endif

    if (annex_table)
    {
        rc = db_exec_sql(&p_mgr->conn, "DELETE FROM " ANNEX_TABLE, NULL);
        if (rc)
            return rc;
    }

    rc = db_exec_sql(&p_mgr->conn, "DELETE FROM " MAIN_TABLE, NULL);
    if (rc)
        return rc;

    rc = db_exec_sql(&p_mgr->conn, "DELETE FROM " DNAMES_TABLE, NULL);
    if (rc)
        return rc;

    return DB_SUCCESS;
}

/**
 * Insert a single entry to soft rm table.
 * p_old_attrs must include rm_time
 */
static int listmgr_softrm_single(lmgr_t *p_mgr, const entry_id_t *p_id,
                                 attr_set_t *p_old_attrs)
{
    char buf[4096];
    DEF_PK(pk);
    var_str query = VAR_STR_NULL;
    int rc, nb;

    if (!ATTR_MASK_TEST(p_old_attrs, rm_time))
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Error: rm_time attr is supposed to be set in %s()",
                   __func__);
    }

    nb = attrmask2fieldlist(buf, SOFTRM_MASK & p_old_attrs->attr_mask,
                            T_SOFTRM, true,  false, "","");

    var_str_append(&query, "INSERT IGNORE INTO " SOFT_RM_TABLE "(id");
    if (nb > 0)
        var_str_append(&query, buf);
    var_str_append(&query, ") VALUES (");

    entry_id2pk(p_id, PTR_PK(pk));
    sprintf(buf, DPK, pk);
    var_str_append(&query, buf);

    attrset2valuelist(p_mgr, buf, p_old_attrs, T_SOFTRM, true);
    var_str_append(&query, buf);
    var_str_append(&query, ")");

    rc = db_exec_sql(&p_mgr->conn, VAR_STR_START(query), NULL);

    if (rc)
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "DB query failed in %s line %d: query=\"%s\", code=%d: %s",
                   __FUNCTION__, __LINE__, VAR_STR_START(query), rc,
                   db_errmsg(&p_mgr->conn, buf, 1024));
    var_str_free(&query);
    return rc;
}


static int listmgr_mass_remove(lmgr_t *p_mgr, const lmgr_filter_t *p_filter, int soft_rm,
                               time_t rm_time, rm_cb_func_t cb_func)
{
    int            rc;
    char           query[4096];
    char           filter_str[2048];
    char           filter_str_names[2048];
    char           *curr_filter;
    int            filter_main = 0;
    int            filter_annex = 0;
    int            filter_names = 0;
    int            filter_stripe_info = 0;
    int            filter_stripe_items = 0;
    char           from[1024];
    char           *curr_from;
    const char* first_table = NULL;
    table_enum     query_tab = T_MAIN;
    const char* direct_del_table = NULL;
    char           tmp_table_name[256];
    result_handle_t result;
    char          *field_tab[ATTR_COUNT+1]; /* id + attributes */
    DEF_PK(pk);
    unsigned int   rmcount = 0;
    int nb;

    /* This is needed for creating big temporary table.
     * Set READ COMMITTED isolation level for the next transaction
     * so locks can be released immediatly after the record is read.
     * - This can only be done if we are outside a transaction
     * - If the mode is autocommit, do it just before the create tmp table
     *   statement.
     */
    if ((p_mgr->last_commit == 0) && (lmgr_config.commit_behavior != 0))
    {
        rc = db_transaction_level(&p_mgr->conn, TRANS_NEXT, TXL_READ_COMMITTED);
        if ( rc )
        {
            char errmsg[1024];
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to set READ_COMMITTED isolation level: Error: %s", db_errmsg( &p_mgr->conn, errmsg, 1024 ) );
            /* continue anyway */
        }
    }

    /* We want the remove operation to be atomic */
retry:
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    if ( !p_filter
         || ( ( p_filter->filter_type == FILTER_SIMPLE )
              && ( p_filter->filter_simple.filter_count == 0 ) )
         || ( ( p_filter->filter_type == FILTER_BOOLEXPR )
              && ( p_filter->filter_boolexpr == NULL ) ) )
    {

        if (soft_rm)
        {
            rc = listmgr_softrm_all(p_mgr, rm_time);
            if (lmgr_delayed_retry(p_mgr, rc))
                goto retry;
            else if (rc)
                goto rollback;
        }

        /* Remove all !!! */
        DisplayLog( LVL_EVENT, LISTMGR_TAG,
                    "No filter is specified: removing entries from all tables." );
        rc = listmgr_rm_all(p_mgr);
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;

        /* FIXME how many entries removed? */
        rc = lmgr_commit(p_mgr);
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else
            return rc;
    }

    if (!soft_rm)
    {
        /* no soft_rm:
         * 1) clean names if there is a filter on them.
         * 2) clean related entries in other tables if there is no remaining path.
         */
        rc = clean_names(p_mgr, p_filter, &filter_names);
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;
    }
    else
    {
        /* soft rm: just build the name filter for the later request */
        filter_names = filter2str(p_mgr, filter_str_names, p_filter, T_DNAMES, false, false);
    }

    /* on which table are the filters ?  */
    curr_filter = filter_str;
    filter_main = filter2str(p_mgr, curr_filter, p_filter, T_MAIN, false /* no leading AND */, true);
    curr_filter += strlen(curr_filter);

    if (annex_table)
    {
        filter_annex = filter2str(p_mgr, curr_filter, p_filter, T_ANNEX,
                                  (curr_filter != filter_str), true);
        curr_filter += strlen(curr_filter);
    }
    else
        filter_annex = 0;

    /* stripes are only managed for Lustre filesystems */
#ifdef _LUSTRE
    filter_stripe_info =
        filter2str(p_mgr, curr_filter, p_filter, T_STRIPE_INFO,
                   (curr_filter != filter_str), true);
    curr_filter += strlen(curr_filter);
    filter_stripe_items =
        filter2str(p_mgr, curr_filter, p_filter, T_STRIPE_ITEMS,
                   (curr_filter != filter_str), true);
    curr_filter += strlen(curr_filter);
#endif

    /* sanity check */
    if (filter_main + filter_annex + filter_stripe_info + filter_stripe_items
        + filter_names == 0)
    {
        /* should have been detected in the beginning of this function ! */
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "How come empty filter has not been detected" );
        rc = DB_REQUEST_FAILED;
        goto rollback;
    }
    else if (filter_main + filter_annex + filter_stripe_info + filter_stripe_items == 0)
    {
        /* it is about names only (done by clean_names) */
        goto names_only;
    }

    DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Creating temporary table" );

    curr_from = from;
    if (filter_main)
    {
        strcpy(curr_from, MAIN_TABLE);
        curr_from = curr_from + strlen(curr_from);
        first_table = MAIN_TABLE;
        query_tab = T_MAIN;
    }
    if (filter_annex)
    {
        if (first_table)
            curr_from += sprintf(curr_from, " LEFT JOIN "ANNEX_TABLE" ON %s.id="ANNEX_TABLE".id",
                                 first_table);
        else
        {
            strcpy(from, ANNEX_TABLE);
            curr_from = from + strlen(from);
            first_table = ANNEX_TABLE;
            query_tab = T_ANNEX;
        }
    }
    if (filter_stripe_info)
    {
        if (first_table)
            curr_from += sprintf(curr_from, " LEFT JOIN "STRIPE_INFO_TABLE" ON %s.id="STRIPE_INFO_TABLE".id",
                                 first_table);
        else
        {
            strcpy(from, STRIPE_INFO_TABLE);
            curr_from = from + strlen(from);
            first_table = STRIPE_INFO_TABLE;
            query_tab = T_STRIPE_INFO;
        }
    }
    if ( filter_stripe_items )
    {
        if (first_table)
            /* this may create duplicate records for multiple stripes, but it
             * doesn't matter as the request gets DISTINCT(id) */
            curr_from += sprintf(curr_from, " LEFT JOIN "STRIPE_ITEMS_TABLE" ON %s.id="STRIPE_ITEMS_TABLE".id",
                                 first_table);
        else
        {
            strcpy(from, STRIPE_ITEMS_TABLE);
            curr_from = from + strlen(from);
            first_table = STRIPE_ITEMS_TABLE;
            query_tab = T_STRIPE_ITEMS;
        }
    }

    if (first_table == NULL)
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Error: unexpected case: filters= "
                        MAIN_TABLE ":%d, " ANNEX_TABLE ":%d, "
                        STRIPE_ITEMS_TABLE ":%d, " STRIPE_INFO_TABLE ":%d",
                        filter_main, filter_annex, filter_stripe_items, filter_stripe_info );
        rc = DB_REQUEST_FAILED;
        goto rollback;
    }

    sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
       ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );

    if (soft_rm)
    {
        char fields[4096];
        char *curr_fields = fields;

        /* always an annex table with softrm modes */
        if (!annex_table)
            RBH_BUG("no annex table with softrm mode");

        /* case A: full scan (no filter on fullpath), all non-updated entries are to be removed + all unseen names must be cleaned.
         *          => filter_names + soft_rm
         * case B: partial scan, we don't remove objects from ENTRIES (only from NAMES).
         */
        if (lmgr_filter_check_field(p_filter, ATTR_INDEX_fullpath)) /* partial scan with condition on NAMES + ENTRIES */
        {
                /* 1) get the entries to be removed (those with nb paths = removed paths)
                 * 2) then we will clean the names
                 */

                /* To determine names whose we remove the last reference, we avoid huge JOIN like this one:
                            (select id,COUNT(*) as rmd from NAMES LINKS WHERE <condition> GROUP BY id) rmname
                            JOIN
                            (select id, COUNT(*) as all FROM NAMES GROUP BY id) allname
                            ON rmname.id=allname.id
                            WHERE rmname.rmcount=paths.pathcount
                 *
                 * Instead we do:
                 *  SELECT id,sum(this_path(parent_id,name) LIKE '%/foo' AND path_update < x) as rmcnt, count(*) as tot FROM NAMES GROUP BY id HAVING rmcnt=tot;
                 *
                 * BUT we must also get ENTRIES with no remaining name (no matching entry in NAMES)...
                 * Finally we do:
                 *  SELECT ENTRIES.id, this_path(parent_id, name) as fullpath, ...
                    sum(path_update < 1377176998 and this_path(parent_id, name) like 'dir1/%') as rm, count(*) as tot
                    FROM ENTRIES LEFT JOIN NAMES ON ENTRIES.id=NAMES.id GROUP BY ENTRIES.id HAVING s=tot or fullpath is NULL;
                 */

                attrmask2fieldlist(curr_fields, SOFTRM_MASK, T_MAIN, true,  false,
                                   MAIN_TABLE".", "");
                curr_fields += strlen(curr_fields);
                attrmask2fieldlist(curr_fields, SOFTRM_MASK, T_ANNEX, true,  false,
                                   ANNEX_TABLE".", "");
                curr_fields += strlen(curr_fields);
                attrmask2fieldlist(curr_fields, SOFTRM_MASK, T_DNAMES, true,  false,
                                   DNAMES_TABLE".", "");


                sprintf(query, "CREATE TEMPORARY TABLE %s AS "
                        "SELECT "MAIN_TABLE".id%s,SUM(%s) AS rmcnt,COUNT(*) AS tot"
                        " FROM "MAIN_TABLE" LEFT JOIN "DNAMES_TABLE" ON "MAIN_TABLE".id="DNAMES_TABLE".id"
                                " LEFT JOIN "ANNEX_TABLE" ON "MAIN_TABLE".id="ANNEX_TABLE".id"
                        " WHERE %s GROUP BY "MAIN_TABLE".id HAVING rmcnt=tot OR fullpath is NULL",
                        tmp_table_name, fields, filter_str_names, filter_str);
        }
        else /* full scan */
        {
            attrmask2fieldlist(curr_fields, SOFTRM_MASK, T_MAIN, true,  false,
                               MAIN_TABLE".", "");
            curr_fields += strlen(curr_fields);
            attrmask2fieldlist(curr_fields, SOFTRM_MASK, T_ANNEX, true,  false,
                               ANNEX_TABLE".", "");

            sprintf(query,
                 "CREATE TEMPORARY TABLE %s AS SELECT DISTINCT("MAIN_TABLE".id),"
                 ONE_PATH_FUNC"("MAIN_TABLE".id) AS fullpath%s"
                 " FROM "MAIN_TABLE " LEFT JOIN "ANNEX_TABLE" ON "MAIN_TABLE".id = "ANNEX_TABLE".id"
                 " WHERE %s", tmp_table_name, fields,  filter_str);
        }
    }
    else
    {
        if (filter_names)
        {
            /* Only delete entries with no remaining name */
            /* 2 requests were tested here, with a significant performance difference: use the fastest.
             * (request time for 2.6M entries)
             *  mysql> select * from ENTRIES WHERE id not in (select id from NAMES);
             *  Empty set (7.06 sec)
             *  mysql> select * from ENTRIES LEFT JOIN NAMES on ENTRIES.id=NAMES.id WHERE NAMES.id IS NULL;
             *  Empty set (16.09 sec)
             */
            sprintf(curr_filter, " AND %s.id NOT IN (SELECT DISTINCT(id) FROM "
                                                     DNAMES_TABLE")", first_table);
        }
        sprintf( query,
                 "CREATE TEMPORARY TABLE %s AS SELECT DISTINCT(%s.id) FROM %s"
                 " WHERE %s", tmp_table_name, first_table, from, filter_str );
    }

    /* in autocommit mode, set the transaction level, just before the needed statement */
    if (lmgr_config.commit_behavior == 0)
    {
        /* set READ COMMITTED isolation level for the next (big!) request
         * so locks can be released immediatly after the record is read */
        rc = db_transaction_level(&p_mgr->conn, TRANS_NEXT, TXL_READ_COMMITTED);
        if ( rc )
        {
            char errmsg[1024];
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to set READ_COMMITTED isolation level: Error: %s", db_errmsg( &p_mgr->conn, errmsg, 1024 ) );
            /* continue anyway */
        }
    }

    /* create the temporary table */
    rc = db_exec_sql( &p_mgr->conn, query, NULL );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    /* if the filter is only a single table, entries can be directly deleted in it */
    if (((filter_main?1:0) + (filter_annex?1:0) + (filter_stripe_info?1:0) +
        (filter_stripe_items?1:0) + (filter_names?1:0)== 1)
        && (query_tab != T_STRIPE_ITEMS)) /* can't delete directly in stripe_items with the select criteria */
    {
        DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Direct deletion in %s table", first_table );

        /* if filter is on annex, we can directly use filter in WHERE clause */
        sprintf( query, "DELETE FROM %s WHERE %s", first_table, filter_str );

        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;
        else
            direct_del_table = first_table;
    }

    /* do the cleaning in other tables */
    DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Starting indirect removal" );

    nb = 1; /* at least 1 for id */
    if (soft_rm)
    {
        char fields[1024];
        nb += attrmask2fieldlist(fields, SOFTRM_MASK & ~ATTR_MASK_rm_time,
                                 T_SOFTRM, true, false, "","");
        sprintf(query, "SELECT id%s FROM %s", fields, tmp_table_name);
    }
    else
        sprintf(query, "SELECT id FROM %s", tmp_table_name);

    rc = db_exec_sql( &p_mgr->conn, query, &result );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    DisplayLog( LVL_DEBUG, LISTMGR_TAG,
                "%d identifiers to be removed from all tables",
                db_result_nb_records( &p_mgr->conn, &result ) );

    rmcount = 0;

    while ((rc = db_next_record(&p_mgr->conn, &result, field_tab, nb))
                == DB_SUCCESS
            && (field_tab[0] != NULL))
    {
        entry_id_t id;

        if (sscanf(field_tab[0], SPK, PTR_PK(pk)) != 1)
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Unexpected format for database key: '%s'",
                        field_tab[0] );
            rc = DB_INVALID_ARG;
            goto free_res;
        }

        rc = pk2entry_id(p_mgr, pk, &id);
        if (rc)
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Unexpected format for database key: "DPK, pk);
            goto free_res;
        }

        if (soft_rm)
        {
            attr_set_t old_attrs;

            old_attrs.attr_mask = SOFTRM_MASK & ~ATTR_MASK_rm_time;

            /* parse result attributes + set rm_time for listmgr_softrm_single */
            rc = result2attrset(T_SOFTRM, field_tab + 1,  nb - 1, &old_attrs);
            if (rc)
                goto free_res;

            ATTR_MASK_SET(&old_attrs, rm_time);
            ATTR(&old_attrs, rm_time) = rm_time;

            /* insert into softrm table */
            rc = listmgr_softrm_single(p_mgr, &id, &old_attrs);

            if (lmgr_delayed_retry(p_mgr, rc))
                goto retry;
            else if (rc)
                goto free_res;
        }

        /* delete all entries related to this id (except from query table if we did
         * a direct deletion in it) */
        rc = listmgr_remove_single(p_mgr, pk, direct_del_table ? query_tab : T_NONE);
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto free_res;

        if (cb_func)
            cb_func(&id);

        rmcount++;
    }

    db_result_free( &p_mgr->conn, &result );

    if ( ( rc != 0 ) && ( rc != DB_END_OF_LIST ) )
        return rc;

    DisplayLog( LVL_DEBUG, LISTMGR_TAG,
                "End of indirect removal: %u identifiers removed", rmcount );

    /* drop tmp table */
    rc = db_drop_component( &p_mgr->conn, DBOBJ_TABLE, tmp_table_name);
    if ( rc )
        goto rollback;

names_only:
    /* condition on names only (partial scan that clean unseen names).
     * If no soft_rm, it was do at the beginning of the function.
     */
    if (soft_rm && filter_names)
    {
        rc = clean_names(p_mgr, p_filter, &filter_names);
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;
    }

    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    if (!rc)
        p_mgr->nbop[OPIDX_RM]+=rmcount;
    return rc;

free_res:
    db_result_free( &p_mgr->conn, &result );
rollback:
    lmgr_rollback( p_mgr );
    return rc;
}

int ListMgr_MassRemove( lmgr_t * p_mgr, const lmgr_filter_t * p_filter,
                        rm_cb_func_t cb_func )
{
    /* not a soft rm */
    return listmgr_mass_remove(p_mgr, p_filter, false, 0, cb_func);
}

int ListMgr_MassSoftRemove(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                           time_t rm_time, rm_cb_func_t cb_func)
{
    /* soft rm */
    return listmgr_mass_remove(p_mgr, p_filter, true, rm_time, cb_func);
}

/**
 * Remove an entry from the main database, and insert it to secondary table
 * for delayed removal.
 * \param real_remove_time time when the entry was removed.
 * \param p_old_attrs contains old attributes, parent+name and backendpath must be set.
 *        rm_time must be set too.
 *        If NULL, it is retrieved from the database.
 */
int            ListMgr_SoftRemove(lmgr_t *p_mgr, const entry_id_t *p_id,
                                  attr_set_t *p_old_attrs)
{
    int        rc;
    attr_set_t all_attrs = {0};

    ATTR_MASK_INIT(&all_attrs);

    /* get missing attributes for SOFT_RM table from DB */
    all_attrs.attr_mask = SOFTRM_MASK &~ ATTR_MASK_rm_time
                          &~ p_old_attrs->attr_mask;

    /* these are needed for remove function */
    if (!ATTR_MASK_TEST(&all_attrs, parent_id)
        || !ATTR_MASK_TEST(&all_attrs, name))
    {
        ATTR_MASK_SET(&all_attrs, parent_id);
        ATTR_MASK_SET(&all_attrs, name);
    }

    if (all_attrs.attr_mask && (ListMgr_Get(p_mgr, p_id, &all_attrs) != DB_SUCCESS))
        ATTR_MASK_INIT(&all_attrs);

    if (p_old_attrs != NULL)
        ListMgr_MergeAttrSets(&all_attrs, p_old_attrs, true);

    /* if fullpath is not determined, try to build it */
    if (!ATTR_MASK_TEST(&all_attrs, fullpath)
        && ATTR_MASK_TEST(&all_attrs, parent_id)
        && ATTR_MASK_TEST(&all_attrs, name))
    {
        attr_set_t dir_attrs = {0};

        /* try to get parent path, so we can build <parent_path>/<name> */
        ATTR_MASK_SET(&dir_attrs, fullpath);
        if ((ListMgr_Get(p_mgr, &ATTR(&all_attrs, parent_id), &dir_attrs) == DB_SUCCESS)
            && ATTR_MASK_TEST(&dir_attrs, fullpath))
        {
            snprintf(ATTR(&all_attrs, fullpath), RBH_PATH_MAX, "%s/%s",
                     ATTR(&dir_attrs, fullpath), ATTR(&all_attrs, name));
            ATTR_MASK_SET(&all_attrs, fullpath);
        }
        else /* display fullpath as <parent_id>/<name>*/
        {
            char tmp[RBH_PATH_MAX];
            DEF_PK(parent_pk);

            /* prefix with parent id */
            entry_id2pk(&ATTR(&all_attrs, parent_id), PTR_PK(parent_pk));
            sprintf(tmp, "%s/%s", parent_pk, ATTR(&all_attrs, name));
            fullpath_db2attr(tmp, ATTR(&all_attrs, fullpath));
            ATTR_MASK_SET(&all_attrs, fullpath);
        }
    }

    if (!ATTR_MASK_TEST(&all_attrs, rm_time))
    {
        ATTR_MASK_SET(&all_attrs, rm_time);
        ATTR(&all_attrs, rm_time) = time(NULL);
    }

    /* We want the removal sequence to be atomic */
retry:
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    rc = listmgr_softrm_single(p_mgr, p_id, &all_attrs);

    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
    {
        lmgr_rollback(p_mgr);
        return rc;
    }

    /* remove the entry from main tables, if it exists */
    rc = listmgr_remove_no_tx(p_mgr, p_id, &all_attrs, true);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc != DB_SUCCESS && rc != DB_NOT_EXISTS)
    {
        lmgr_rollback(p_mgr);
        return rc;
    }

    /* commit */
    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    if (!rc)
         p_mgr->nbop[OPIDX_RM]++;
    return rc;
}

typedef struct lmgr_rm_list_t
{
    lmgr_t        *p_mgr;
    result_handle_t select_result;
    unsigned int  result_len;
} lmgr_rm_list_t;

/* XXX selecting 'expired' entries is done using a rm_time criteria in p_filter */
struct lmgr_rm_list_t *ListMgr_RmList(lmgr_t *p_mgr, lmgr_filter_t *p_filter)
{
    int rc, nb;
    lmgr_rm_list_t * p_list = MemAlloc( sizeof(lmgr_rm_list_t) );
    char query[4096];
    char filter_str[1024] = "WHERE ";
    char fields[1024];
    char *curr_filter = filter_str + strlen(filter_str);

    if ( !p_list )
        return NULL;

    if (p_filter)
    {
        if (p_filter->filter_type != FILTER_SIMPLE )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                       "Unsupported filter in %s(): simple filter expected",
                       __FUNCTION__ );
            return NULL;
        }
        /* are there unsuported fields in this filter? */
        if (lmgr_check_filter_fields(p_filter, SOFTRM_MASK))
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "Unsupported field in filter (in %s())",
                         __FUNCTION__);
            return NULL;
        }
        if (filter2str(p_mgr, curr_filter, p_filter, T_SOFTRM, false, false) < 0)
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "Error converting filter to SQL request" );
            return NULL;
        }
    }

    p_list->p_mgr = p_mgr;

    nb = attrmask2fieldlist(fields, SOFTRM_MASK, T_SOFTRM, true, false, "", "");
    snprintf(query, 4096, "SELECT id%s FROM "SOFT_RM_TABLE" %s "
             "ORDER BY rm_time ASC", fields, p_filter ?filter_str:"");

    p_list->result_len = nb + 1; /* id + attrs */

    /* execute request (retry on connexion error or deadlock) */
retry:
    rc = db_exec_sql(&p_mgr->conn, query, &p_list->select_result);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;

    if ( rc )
    {
        char msg_buff[1024];
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "DB query failed in %s line %d: query=\"%s\",code=%d, %s",
                    __FUNCTION__, __LINE__, query, rc, db_errmsg( &p_mgr->conn, msg_buff, 1024 ) );
        MemFree(p_list);
        return NULL;
    }

    return p_list;
}

int            ListMgr_GetNextRmEntry(struct lmgr_rm_list_t *p_iter,
                                      entry_id_t *p_id, attr_set_t *p_attrs)
{
    int            rc = 0;
    int i;
#define MAX_SOFTRM_FIELDS (ATTR_COUNT+1) /* id + attributes */
    char *record[MAX_SOFTRM_FIELDS];

    if (p_iter->result_len > MAX_SOFTRM_FIELDS)
        RBH_BUG("unexpected result length > MAX_SOFTRM_FIELDS");

    if (!p_id || !p_attrs)
        return DB_INVALID_ARG;

    for ( i=0; i < MAX_SOFTRM_FIELDS; i++)
        record[i] = NULL;

    rc = db_next_record(&p_iter->p_mgr->conn, &p_iter->select_result, record,
                        p_iter->result_len);
    /* what to do on connexion error? */

    if ( rc )
        return rc;
    if ( record[0] == NULL )
        return DB_REQUEST_FAILED;

    if ( sscanf( record[0], SFID, RFID(p_id) ) <= 0 )
        return DB_REQUEST_FAILED;

    /* force fields of SOFTRM table */
    p_attrs->attr_mask = SOFTRM_MASK;
    rc = result2attrset(T_SOFTRM, record + 1, p_iter->result_len - 1, p_attrs);

    return rc;
}


void           ListMgr_CloseRmList( struct lmgr_rm_list_t *p_iter )
{
    db_result_free( &p_iter->p_mgr->conn, &p_iter->select_result );
    MemFree( p_iter );
}

/**
 * Get entry to be removed from its fid.
 */
int     ListMgr_GetRmEntry(lmgr_t * p_mgr,
                           const entry_id_t *p_id,
                           attr_set_t *p_attrs)
{
    char query[4096];
    char fields[1024];
    result_handle_t result;
    int rc, i, nb;

    char          *record[MAX_SOFTRM_FIELDS];

    if ( !p_id || !p_attrs)
        return DB_INVALID_ARG;

    /* remove fields that are not in SOFTRM table */
    p_attrs->attr_mask &= SOFTRM_MASK;

    nb = attrmask2fieldlist(fields, p_attrs->attr_mask, T_SOFTRM, 0, 0, 0, 0);
    snprintf(query, 4096, "SELECT %s FROM "SOFT_RM_TABLE" WHERE id='"DFID_NOBRACE"'",
             fields, PFID(p_id));

    /* execute request (retry on connexion error or timeout) */
retry:
    rc = db_exec_sql( &p_mgr->conn, query, &result );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;

    if (rc)
    {
        char msg_buff[1024];
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "DB query failed in %s line %d: query=\"%s\",code=%d, %s",
                    __FUNCTION__, __LINE__, query, rc, db_errmsg( &p_mgr->conn, msg_buff, 1024 ) );
        return rc;
    }

    for ( i=0; i < MAX_SOFTRM_FIELDS; i++)
        record[i] = NULL;

    rc = db_next_record(&p_mgr->conn, &result, record, nb);
    if ( rc == DB_END_OF_LIST )
    {
        rc = DB_NOT_EXISTS;
        goto free_res;
    } else if ( rc )
         return rc;

    if (record[0] == NULL)
        return DB_REQUEST_FAILED;

    rc = result2attrset(T_SOFTRM, record, nb, p_attrs);

free_res:
    db_result_free( &p_mgr->conn, &result );
    return rc;
}

int ListMgr_SoftRemove_Discard( lmgr_t * p_mgr, const entry_id_t * p_id )
{
    char query[1024];
    int rc;

    snprintf(query, 1024,
             "DELETE FROM "SOFT_RM_TABLE" WHERE id='"DFID_NOBRACE"'",
             PFID(p_id) );

retry:
    rc = db_exec_sql(&p_mgr->conn, query, NULL);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    return rc;
}

