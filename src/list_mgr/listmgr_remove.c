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
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>


static int clean_names(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                       unsigned int *nb_filter_names)
{
    int      rc = DB_SUCCESS;
    GString *req;

    req = g_string_new("DELETE FROM "DNAMES_TABLE" WHERE ");
    *nb_filter_names = filter2str(p_mgr, req, p_filter, T_DNAMES, 0);

    if (*nb_filter_names == 0)
        goto out;

    DisplayLog(LVL_DEBUG, LISTMGR_TAG, "Direct deletion in "DNAMES_TABLE" table");
    rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
out:
    g_string_free(req, TRUE);
    return rc;
}

/** helper for listmgr_remove_single */
static inline void append_table_join(GString *fields, GString *tables, GString *where,
                                     const char *tname, const char *talias,
                                     PK_ARG_T pk, const char **first_table)
{
    g_string_append_printf(fields, "%s%s.*", *first_table == NULL?"":",", talias);

    if (GSTRING_EMPTY(tables))
    {
        *first_table = talias;
        g_string_printf(tables, "%s %s", tname, talias);
    }
    else
        g_string_append_printf(tables, " LEFT JOIN %s %s ON %s.id = %s.id",
                               tname, talias, *first_table, talias);

    if (GSTRING_EMPTY(where))
        g_string_printf(where, "%s.id="DPK, talias, pk);
}

/** removal of a single entry (no transaction management) */
static int listmgr_remove_single(lmgr_t *p_mgr, PK_ARG_T pk, table_enum exclude_tab)
{
    const char *first_table = NULL;
    GString *req, *tables, *where;
    int rc;

    req = g_string_new("DELETE ");
    tables = g_string_new(NULL);
    where = g_string_new(NULL);

    if (exclude_tab != T_MAIN)
        append_table_join(req, tables, where, MAIN_TABLE, "M", pk, &first_table);
    if (exclude_tab != T_ANNEX)
        append_table_join(req, tables, where, ANNEX_TABLE, "A", pk, &first_table);
    if (exclude_tab != T_DNAMES)
        append_table_join(req, tables, where, DNAMES_TABLE, "N", pk, &first_table);
#ifdef _LUSTRE
    if (exclude_tab != T_STRIPE_INFO)
        append_table_join(req, tables, where, STRIPE_INFO_TABLE, "I", pk, &first_table);
    if (exclude_tab != T_STRIPE_ITEMS)
        append_table_join(req, tables, where, STRIPE_ITEMS_TABLE, "S", pk, &first_table);
#endif

    /* Doing this in a single request instead of 1 DELETE per table
     * results in a huge speed up (246sec -> 59s).  */
    /* - req already contains "DELETE filed_list"
     * - using GSTRING_SAFE in case where or tables is still NULL */
    g_string_append_printf(req, " FROM %s WHERE %s", GSTRING_SAFE(tables),
                           GSTRING_SAFE(where));
    rc = db_exec_sql(&p_mgr->conn, req->str, NULL);

    g_string_free(req, TRUE);
    g_string_free(tables, TRUE);
    g_string_free(where, TRUE);

    return rc;
}


int listmgr_remove_no_tx(lmgr_t *p_mgr, const entry_id_t *p_id,
                         const attr_set_t *p_attr_set, bool last)
{
    GString *req;
    int      rc = DB_SUCCESS;
    DEF_PK(pk);
    DEF_PK(ppk);

    entry_id2pk(p_id, PTR_PK(pk));

    req = g_string_new(NULL);

    if (last)
    {
        /* remove from all tables except from NAMES (handled at the end of this function) */
        rc = listmgr_remove_single(p_mgr, pk, T_DNAMES);
        if (rc)
            goto out;
    }
    else
    {
        /* XXX else update attributes according to attributes contents? */

        /* Since we're removing one entry but not the file, decrement nlink. */
        g_string_printf(req, "UPDATE "MAIN_TABLE" SET nlink=nlink-1 WHERE "
                        "id="DPK" AND nlink>0", pk);
        rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
        if (rc)
            goto out;
    }

    /* Allow removing entry from MAIN_TABLE without removing it from NAMES */
    if (p_attr_set && ATTR_MASK_TEST(p_attr_set, parent_id) && ATTR_MASK_TEST(p_attr_set, name))
    {
        char *escaped;
        int   len;

        entry_id2pk(&ATTR(p_attr_set, parent_id), PTR_PK(ppk));

        /* according to MySQL documentation, escaped string can be up to 2*orig_len+1 */
        len = 2 * strlen(ATTR(p_attr_set, name)) + 1;
        escaped = MemAlloc(len);
        if (escaped == NULL)
        {
            rc = DB_NO_MEMORY;
            goto out;
        }
        db_escape_string(&p_mgr->conn, escaped, len, ATTR(p_attr_set, name));

        g_string_printf(req, "DELETE FROM "DNAMES_TABLE" WHERE pkn="HNAME_FMT" AND id="DPK,
                        ppk, escaped, pk);
        MemFree(escaped);

        rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
        if (rc)
            return rc;
    }
    else if (!p_attr_set || !ATTR_MASK_TEST(p_attr_set, parent_id) || !ATTR_MASK_TEST(p_attr_set, name))
    {
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "WARNING: missing attribute(s) to "
                    "delete entry from "DNAMES_TABLE":%s%s%s",
                    !p_attr_set ? " attrs=NULL" : "",
                    p_attr_set && !ATTR_MASK_TEST(p_attr_set, parent_id) ? " parent" : "",
                    p_attr_set && !ATTR_MASK_TEST(p_attr_set, name) ? " name" : "");
    }

out:
    g_string_free(req, TRUE);
    return rc;
}


int ListMgr_Remove(lmgr_t *p_mgr, const entry_id_t *p_id,
                   const attr_set_t *p_attr_set, bool last)
{
    int rc;
    int retry_status;

    /* We want the remove operation to be atomic */
retry:
    rc = lmgr_begin(p_mgr);
    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    else if (retry_status == 2)
        return DB_RBH_SIG_SHUTDOWN;
    else if (rc)
        return rc;

    rc = listmgr_remove_no_tx(p_mgr, p_id, p_attr_set, last);
    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    else if (rc || retry_status == 2)
    {
        lmgr_rollback(p_mgr);
        return (retry_status == 2) ? DB_RBH_SIG_SHUTDOWN : rc;
    }

    rc = lmgr_commit(p_mgr);
    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    if (!rc)
         p_mgr->nbop[OPIDX_RM]++;
    return rc;
}

/**
 * Insert all entries to soft rm table.
 * @TODO check how it behaves with millions/billion entries.
 */
static int listmgr_softrm_all(lmgr_t *p_mgr, time_t rm_time)
{
    int rc;
    GString *req, *annex_fields;
    attr_mask_t mask_tmp = softrm_attr_set;

    /* manage fullpath independently to make sure it is the first attribute
     * as we will set it to "one_path(id)". */
    attr_mask_unset_index(&mask_tmp, ATTR_INDEX_fullpath);

    req = g_string_new("INSERT IGNORE INTO " SOFT_RM_TABLE "(id,fullpath");
    attrmask2fieldlist(req, mask_tmp, T_SOFTRM, "", "", AOF_LEADING_SEP);

    annex_fields = g_string_new(NULL);
    attrmask2fieldlist(annex_fields, softrm_attr_set, T_ANNEX,
                       ANNEX_TABLE".", "", AOF_LEADING_SEP);
    rc = db_exec_sql(&p_mgr->conn, req->str, NULL);

    g_string_free(annex_fields, TRUE);
    g_string_free(req, TRUE);
    return rc;
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

    rc = db_exec_sql(&p_mgr->conn, "DELETE FROM " ANNEX_TABLE, NULL);
    if (rc)
        return rc;

    rc = db_exec_sql(&p_mgr->conn, "DELETE FROM " MAIN_TABLE, NULL);
    if (rc)
        return rc;

    rc = db_exec_sql(&p_mgr->conn, "DELETE FROM " DNAMES_TABLE, NULL);
    if (rc)
        return rc;

    return DB_SUCCESS;
}

/** try to build the full path, if it is missing */
static void set_fullpath(lmgr_t *p_mgr, attr_set_t *attrs)
{
    int rc;

    /* if fullpath is not determined, try to build it */
    if (!ATTR_MASK_TEST(attrs, fullpath)
        && ATTR_MASK_TEST(attrs, parent_id)
        && ATTR_MASK_TEST(attrs, name))
    {
        attr_set_t dir_attrs = ATTR_SET_INIT;

        /* try to get parent path, so we can build <parent_path>/<name> */
        ATTR_MASK_SET(&dir_attrs, fullpath);
        if ((ListMgr_Get(p_mgr, &ATTR(attrs, parent_id), &dir_attrs) == DB_SUCCESS)
            && ATTR_MASK_TEST(&dir_attrs, fullpath))
        {
            rc = snprintf(ATTR(attrs, fullpath), RBH_PATH_MAX, "%s/%s",
                          ATTR(&dir_attrs, fullpath), ATTR(attrs, name));
            if (rc > RBH_PATH_MAX) {
                DisplayLog(LVL_EVENT, LISTMGR_TAG, "path truncated: %s/%s",
                           ATTR(&dir_attrs, fullpath), ATTR(attrs, name));
            }
            ATTR_MASK_SET(attrs, fullpath);
        }
        else /* display fullpath as <parent_id>/<name>*/
        {
            char tmp[RBH_PATH_MAX];
            DEF_PK(parent_pk);

            /* prefix with parent id */
            entry_id2pk(&ATTR(attrs, parent_id), PTR_PK(parent_pk));
            snprintf(tmp, RBH_PATH_MAX, "%s/%s", parent_pk, ATTR(attrs, name));
            fullpath_db2attr(tmp, ATTR(attrs, fullpath));
            ATTR_MASK_SET(attrs, fullpath);
        }
    }
}


/**
 * Insert a single entry to soft rm table.
 * p_old_attrs must include rm_time
 */
static int listmgr_softrm_single(lmgr_t *p_mgr, const entry_id_t *p_id,
                                 attr_set_t *p_old_attrs)
{
    DEF_PK(pk);
    int  rc;
    char err_buf[1024];
    GString *req;
    attr_mask_t tmp_mask;

    if (!ATTR_MASK_TEST(p_old_attrs, rm_time))
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Error: rm_time attr is supposed to be set in %s()",
                   __func__);
    }

    set_fullpath(p_mgr, p_old_attrs);

    /* if fullpath is set, update it */
    if (ATTR_MASK_TEST(p_old_attrs, fullpath))
        req = g_string_new("INSERT INTO " SOFT_RM_TABLE "(id");
    else /* else, don't update */
        req = g_string_new("INSERT IGNORE INTO " SOFT_RM_TABLE "(id");

    tmp_mask = attr_mask_and(&softrm_attr_set, &p_old_attrs->attr_mask);
    attrmask2fieldlist(req, tmp_mask, T_SOFTRM, "", "", AOF_LEADING_SEP);
    g_string_append(req, ") VALUES (");

    entry_id2pk(p_id, PTR_PK(pk));
    g_string_append_printf(req, DPK, pk);

    attrset2valuelist(p_mgr, req, p_old_attrs, T_SOFTRM, AOF_LEADING_SEP);
    g_string_append(req, ")");

    if (ATTR_MASK_TEST(p_old_attrs, fullpath))
        g_string_append(req, " ON DUPLICATE KEY UPDATE fullpath=VALUES(fullpath)");

    rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
    if (rc)
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "DB query failed in %s line %d: query=\"%s\", code=%d: %s",
                   __FUNCTION__, __LINE__, req->str, rc,
                   db_errmsg(&p_mgr->conn, err_buf, sizeof(err_buf)));
    g_string_free(req, TRUE);
    return rc;
}

/** create a temporary table with all entries to be deleted */
static int create_tmp_table_rm(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                               const struct field_count *counts,
                               const char *tmpname, bool soft_rm,
                               table_enum query_tab, GString *from,
                               GString *filter_names, GString *where,
                               bool distinct)
{
    GString *req = NULL;
    int      rc;

    DisplayLog(LVL_DEBUG, LISTMGR_TAG, "Creating temporary table");

    req = g_string_new("CREATE TEMPORARY TABLE");
    g_string_append_printf(req, " %s AS ", tmpname);

    if (soft_rm)
    {
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

                g_string_append(req,"SELECT "MAIN_TABLE".id");
                attrmask2fieldlist(req, softrm_attr_set, T_MAIN,
                                   MAIN_TABLE".", "", AOF_LEADING_SEP);
                attrmask2fieldlist(req, softrm_attr_set, T_ANNEX,
                                   ANNEX_TABLE".", "", AOF_LEADING_SEP);
                attrmask2fieldlist(req, softrm_attr_set, T_DNAMES,
                                   DNAMES_TABLE".", "", AOF_LEADING_SEP);
                g_string_append_printf(req, ",SUM(%s) AS rmcnt,COUNT(*) AS tot FROM "MAIN_TABLE
                                       " LEFT JOIN "DNAMES_TABLE" ON "MAIN_TABLE".id="DNAMES_TABLE".id"
                                       " LEFT JOIN "ANNEX_TABLE" ON "MAIN_TABLE".id="ANNEX_TABLE".id"
                                       " WHERE %s GROUP BY "MAIN_TABLE".id"
                                       " HAVING rmcnt=tot OR fullpath is NULL",
                                       GSTRING_SAFE(filter_names), GSTRING_SAFE(where));
        }
        else /* full scan */
        {
            g_string_append(req,"SELECT "MAIN_TABLE".id," ONE_PATH_FUNC"("MAIN_TABLE".id) AS fullpath");
            attrmask2fieldlist(req, softrm_attr_set, T_MAIN,
                               MAIN_TABLE".", "", AOF_LEADING_SEP);
            attrmask2fieldlist(req, softrm_attr_set, T_ANNEX,
                               ANNEX_TABLE".", "", AOF_LEADING_SEP);

            g_string_append_printf(req, " FROM "MAIN_TABLE
                                   " LEFT JOIN "ANNEX_TABLE" ON "MAIN_TABLE".id="ANNEX_TABLE".id"
                                   " WHERE %s", GSTRING_SAFE(where));
        }
    }
    else
    {
        if (counts->nb_names > 0)
        {
            /* Only delete entries with no remaining name */
            /* 2 requests were tested here, with a significant performance difference: use the fastest.
             * (request time for 2.6M entries)
             *  mysql> select * from ENTRIES WHERE id not in (select id from NAMES);
             *  Empty set (7.06 sec)
             *  mysql> select * from ENTRIES LEFT JOIN NAMES on ENTRIES.id=NAMES.id WHERE NAMES.id IS NULL;
             *  Empty set (16.09 sec)
             */
            g_string_append_printf(where, " AND %s.id NOT IN "
                                   "(SELECT DISTINCT(id) FROM "DNAMES_TABLE")",
                                   table2name(query_tab));
        }
        if (distinct)
            g_string_append_printf(req, "SELECT DISTINCT(%s.id) FROM %s"
                                   " WHERE %s", table2name(query_tab),
                                   GSTRING_SAFE(from), GSTRING_SAFE(where));
        else
            g_string_append_printf(req, "SELECT %s.id FROM %s" " WHERE %s",
                                   table2name(query_tab), GSTRING_SAFE(from),
                                   GSTRING_SAFE(where));
    }

    /* create the temporary table */
    rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
    g_string_free(req, TRUE);
    return rc;
}

#define MAX_SOFTRM_FIELDS 128 /* id + std attributes + status + sminfo */

/** Perform removal or soft removal for all entries matching a filter
 * (no transaction management).
 */
static int listmgr_mass_remove_no_tx(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                                     bool soft_rm, time_t rm_time, rm_cb_func_t cb_func,
                                     unsigned int *rm_count)
{
    struct field_count counts = {0};
    table_enum          query_tab;
    bool                distinct = false;
    bool                direct_del = false;
    char                tmp_table_name[256];
    char           *field_tab[MAX_SOFTRM_FIELDS];
    result_handle_t result;
    DEF_PK(pk);
    GString        *filter_names = NULL;
    GString        *from = NULL;
    GString        *where = NULL;
    GString        *req = NULL;
    int             rc;
    unsigned int    nb;
    attr_mask_t mask_no_rmtime = softrm_attr_set;

    attr_mask_unset_index(&mask_no_rmtime, ATTR_INDEX_rm_time);

    if (no_filter(p_filter))
    {
        if (soft_rm)
        {
            rc = listmgr_softrm_all(p_mgr, rm_time);
            if (rc)
                return rc;
        }

        /* Remove all !!! */
        DisplayLog(LVL_EVENT, LISTMGR_TAG,
                    "No filter is specified: removing entries from all tables.");
        return listmgr_rm_all(p_mgr);
    }

    if (!soft_rm)
    {
        /* no soft_rm:
         * 1) clean names if there is a filter on them.
         * 2) clean related entries in other tables if there is no remaining path.
         */
        rc = clean_names(p_mgr, p_filter, &counts.nb_names);
        if (rc)
            return rc;
    }
    else
    {
        filter_names = g_string_new(NULL);
        /* soft rm: just build the name filter for the later request */
        counts.nb_names = filter2str(p_mgr, filter_names, p_filter, T_DNAMES, 0);
    }

    from = g_string_new(NULL);
    where = g_string_new(NULL);

    /* build the where clause */
    if (filter_where(p_mgr, p_filter, &counts, where, AOF_SKIP_NAME) == 0)
    {
        if (unlikely(counts.nb_names == 0))
        {
            /* empty filter should have been detected earlier */
            DisplayLog(LVL_CRIT, LISTMGR_TAG, "How come empty filter has not been detected?!");
            rc = DB_REQUEST_FAILED;
            goto free_str;
        }

        /* filter is only on names table */
        if (soft_rm)
            rc = clean_names(p_mgr, p_filter, &counts.nb_names);
        /* else (no softrm): name cleaning has been done at the beginning of the function */
        else
            rc = 0;

        goto free_str;
    }

    /* build the from clause */
    filter_from(p_mgr, &counts, from, &query_tab, &distinct, AOF_SKIP_NAME);

    /* sanity check */
    if (unlikely(query_tab == T_NONE || GSTRING_EMPTY(from)))
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Error: unexpected case: filters= "
                   MAIN_TABLE ":%u, " ANNEX_TABLE ":%u, "DNAMES_TABLE": %u,"
                   STRIPE_ITEMS_TABLE ":%u, " STRIPE_INFO_TABLE ":%u",
                   counts.nb_main, counts.nb_annex, counts.nb_names,
                   counts.nb_stripe_items, counts.nb_stripe_info);
        rc = DB_REQUEST_FAILED;
        goto free_str;
    }

    snprintf(tmp_table_name, sizeof(tmp_table_name), "TMP_TABLE_%u_%u",
        (unsigned int)getpid(), (unsigned int)pthread_self());

    rc = create_tmp_table_rm(p_mgr, p_filter, &counts, tmp_table_name, soft_rm,
                             query_tab, from, filter_names, where, distinct);
    if (rc)
        goto free_str;

    req = g_string_new(NULL);

    /* If the filter is only a single table, entries can be directly deleted in it. */
    /* NOTE: can't delete directly in stripe_items with the select criteria. */
    if ((nb_field_tables(&counts) == 1) && (query_tab != T_STRIPE_ITEMS))
    {
        DisplayLog(LVL_DEBUG, LISTMGR_TAG, "Direct deletion in %s table", table2name(query_tab));
        direct_del = true;

        /* if filter is on a single table, we can directly use filter in WHERE clause */
        g_string_printf(req, "DELETE FROM %s WHERE %s", table2name(query_tab),
                        GSTRING_SAFE(where));

        rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
        if (rc)
            goto free_str;
    }

    /* do the cleaning in other tables */
    DisplayLog(LVL_DEBUG, LISTMGR_TAG, "Starting indirect removal (soft_rm=%d)",
               soft_rm);

    /* get all records from the tmp table to clean them from other tables */
    nb = 1; /* at least 1 field for id */
    if (soft_rm)
    {
        g_string_assign(req, "SELECT id");
        nb += attrmask2fieldlist(req, mask_no_rmtime, T_TMP_SOFTRM,
                                 "", "", AOF_LEADING_SEP);
        g_string_append_printf(req, " FROM %s", tmp_table_name);
        g_string_append_printf(req, " ORDER BY CHAR_LENGTH(fullpath) DESC");
    }
    else
        g_string_printf(req, "SELECT id FROM %s", tmp_table_name);

    rc = db_exec_sql(&p_mgr->conn, req->str, &result);
    if (rc)
        goto free_str;

    DisplayLog(LVL_DEBUG, LISTMGR_TAG,
               "%d identifiers to be removed from all tables",
               db_result_nb_records(&p_mgr->conn, &result));

    *rm_count = 0;

    /* for each returned record from tmp table */
    while ((rc = db_next_record(&p_mgr->conn, &result, field_tab, nb))
                == DB_SUCCESS
            && (field_tab[0] != NULL))
    {
        entry_id_t id;

        rc = parse_entry_id(p_mgr, field_tab[0], PTR_PK(pk), &id);
        if (rc)
            goto free_res;

        if (soft_rm)
        {
            attr_set_t old_attrs = ATTR_SET_INIT;

            old_attrs.attr_mask = mask_no_rmtime;

            /* parse result attributes + set rm_time for listmgr_softrm_single */
            rc = result2attrset(T_TMP_SOFTRM, field_tab + 1,  nb - 1, &old_attrs);
            if (rc)
                goto free_res;

            ATTR_MASK_SET(&old_attrs, rm_time);
            ATTR(&old_attrs, rm_time) = rm_time;

            /* insert into softrm table */
            rc = listmgr_softrm_single(p_mgr, &id, &old_attrs);
            ListMgr_FreeAttrs(&old_attrs);
            if (rc)
                goto free_res;
        }

        /* delete all entries related to this id (except from query table if we did
         * a direct deletion in it) */
        rc = listmgr_remove_single(p_mgr, pk, direct_del ? query_tab : T_NONE);
        if (rc)
            goto free_res;

        if (cb_func)
            cb_func(&id);

        (*rm_count)++;
    }

    db_result_free(&p_mgr->conn, &result);

    if ((rc != 0) && (rc != DB_END_OF_LIST))
        goto free_str;

    DisplayLog(LVL_DEBUG, LISTMGR_TAG,
               "End of indirect removal: %u identifiers removed", *rm_count);

    /* drop tmp table */
    rc = db_drop_component(&p_mgr->conn, DBOBJ_TABLE, tmp_table_name);
    if (rc)
        return rc;

    /* Condition on names only (partial scan cleans not found names). */
    if (soft_rm && filter_names)
        rc = clean_names(p_mgr, p_filter, &counts.nb_names);
    /* else, it has been done at the beginning of the function */

    goto free_str;

free_res:
    db_result_free(&p_mgr->conn, &result);

free_str:
    if (from != NULL)
        g_string_free(from, TRUE);
    if (filter_names != NULL)
        g_string_free(filter_names, TRUE);
    if (where != NULL)
        g_string_free(where, TRUE);
    if (req != NULL)
        g_string_free(req, TRUE);

    return rc;
}


/** handles a mass_remove transaction */
static int listmgr_mass_remove(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                               bool soft_rm, time_t rm_time, rm_cb_func_t cb_func)
{
    int             rc;
    unsigned int    rmcount = 0;

    /* We want the remove operation to be atomic */
retry:
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    rc = listmgr_mass_remove_no_tx(p_mgr, p_filter, soft_rm, rm_time, cb_func, &rmcount);

    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;

    if (rc == DB_SUCCESS)
        p_mgr->nbop[OPIDX_RM] += rmcount;

    return rc;

rollback:
    lmgr_rollback(p_mgr);
    return rc;
}

int ListMgr_MassRemove(lmgr_t * p_mgr, const lmgr_filter_t * p_filter,
                        rm_cb_func_t cb_func)
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
    attr_set_t all_attrs = ATTR_SET_INIT;

    /* get missing attributes for SOFT_RM table from DB */
    all_attrs.attr_mask = softrm_attr_set;
    /* ...except rm_time */
    attr_mask_unset_index(&all_attrs.attr_mask, ATTR_INDEX_rm_time);
    /* ...except attributes already in p_old_attrs */
    all_attrs.attr_mask = attr_mask_and_not(&all_attrs.attr_mask, &p_old_attrs->attr_mask);

    /* these are needed for remove function */
    if (!ATTR_MASK_TEST(&all_attrs, parent_id)
        || !ATTR_MASK_TEST(&all_attrs, name))
    {
        ATTR_MASK_SET(&all_attrs, parent_id);
        ATTR_MASK_SET(&all_attrs, name);
    }

    if (!attr_mask_is_null(all_attrs.attr_mask)
        && (ListMgr_Get(p_mgr, p_id, &all_attrs) != DB_SUCCESS))
        ATTR_MASK_INIT(&all_attrs);

    if (p_old_attrs != NULL)
        ListMgr_MergeAttrSets(&all_attrs, p_old_attrs, true);

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
        goto out;

    rc = listmgr_softrm_single(p_mgr, p_id, &all_attrs);

    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
    {
        lmgr_rollback(p_mgr);
        goto out;
    }

    /* remove the entry from main tables, if it exists */
    rc = listmgr_remove_no_tx(p_mgr, p_id, &all_attrs, true);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc != DB_SUCCESS && rc != DB_NOT_EXISTS)
    {
        lmgr_rollback(p_mgr);
        goto out;
    }

    /* commit */
    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    if (!rc)
         p_mgr->nbop[OPIDX_RM]++;

out:
    ListMgr_FreeAttrs(&all_attrs);
    return rc;
}

typedef struct lmgr_rm_list_t
{
    lmgr_t        *p_mgr;
    result_handle_t select_result;
    unsigned int  result_len;
} lmgr_rm_list_t;

/* XXX selecting 'expired' entries is done using a rm_time criteria in p_filter */
struct lmgr_rm_list_t *ListMgr_RmList(lmgr_t *p_mgr, lmgr_filter_t *p_filter,
                                      const lmgr_sort_type_t *p_sort_type)
{
    int             rc, nb;
    lmgr_rm_list_t *p_list = MemAlloc(sizeof(lmgr_rm_list_t));
    GString        *req;

    if (!p_list)
        return NULL;

    req = g_string_new("SELECT id");
    nb = attrmask2fieldlist(req, softrm_attr_set, T_SOFTRM,
                            "", "", AOF_LEADING_SEP);
    g_string_append(req, " FROM "SOFT_RM_TABLE);

    if (p_filter)
    {
        if (p_filter->filter_type != FILTER_SIMPLE)
        {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Unsupported filter in %s(): simple filter expected",
                       __FUNCTION__);
            goto free_err;
        }
        /* are there unsuported fields in this filter? */
        if (lmgr_check_filter_fields(p_filter, softrm_attr_set, &rc))
        {
            DisplayLog(LVL_CRIT, LISTMGR_TAG, "Unsupported field in filter: %s (in %s())",
                       rc == -1 ? "supported filter type" :
                       field_name(p_filter->filter_simple.filter_index[rc]), __func__);
            goto free_err;
        }
        g_string_append(req, " WHERE ");
        if (filter2str(p_mgr, req, p_filter, T_SOFTRM, 0) <= 0)
        {
            DisplayLog(LVL_CRIT, LISTMGR_TAG, "Error converting filter to SQL request");
            goto free_err;
        }
    }

    /*
     * Is there a sort order ? add default order only if not specified,
     * do not add ORDER BY if lru_sort_attr is none
     */

    if (p_sort_type == NULL)
    {
        /* default is rm_time */
        g_string_append(req, " ORDER BY rm_time ASC");
    }
    else if (p_sort_type->order == SORT_NONE) {
        // do nothing
        // required to avoid assert on next else if, as NONE is not a DB field
    }
    else if (!is_softrm_field(p_sort_type->attr_index))
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "ERROR: attribute '%s' is not part of %s table",
                   field_name(p_sort_type->attr_index), SOFT_RM_TABLE);
        goto free_err;
    }
    else
    {
        g_string_append_printf(req, " ORDER BY %s %s",
                               field_name(p_sort_type->attr_index),
                               p_sort_type->order == SORT_ASC ? "ASC" : "DESC");
    }

    p_list->p_mgr = p_mgr;
    p_list->result_len = nb + 1; /* id + attrs */

    /* execute request (retry on connexion error or deadlock) */
    do {
        rc = db_exec_sql(&p_mgr->conn, req->str, &p_list->select_result);
    } while (lmgr_delayed_retry(p_mgr, rc));

    if (rc)
    {
        char msg_buff[1024];

        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "DB query failed in %s line %d: query=\"%s\",code=%d, %s",
                   __FUNCTION__, __LINE__, req->str, rc,
                   db_errmsg(&p_mgr->conn, msg_buff, sizeof(1024)));
        goto free_err;
    }

    /* success */
    g_string_free(req, TRUE);
    return p_list;

free_err: /* error */
    g_string_free(req, TRUE);
    MemFree(p_list);
    return NULL;
}

int            ListMgr_GetNextRmEntry(struct lmgr_rm_list_t *p_iter,
                                      entry_id_t *p_id, attr_set_t *p_attrs)
{
    int            rc = 0;
    int i;
    char *record[MAX_SOFTRM_FIELDS];

    if (p_iter->result_len > MAX_SOFTRM_FIELDS)
        RBH_BUG("unexpected result length > MAX_SOFTRM_FIELDS");

    if (!p_id || !p_attrs)
        return DB_INVALID_ARG;

    for (i=0; i < MAX_SOFTRM_FIELDS; i++)
        record[i] = NULL;

    rc = db_next_record(&p_iter->p_mgr->conn, &p_iter->select_result, record,
                        p_iter->result_len);
    /* what to do on connexion error? */

    if (rc)
        return rc;
    if (record[0] == NULL)
        return DB_REQUEST_FAILED;

    if (sscanf(record[0], SFID, RFID(p_id)) <= 0)
        return DB_REQUEST_FAILED;

    /* force fields of SOFTRM table */
    p_attrs->attr_mask = softrm_attr_set;
    rc = result2attrset(T_SOFTRM, record + 1, p_iter->result_len - 1, p_attrs);

    return rc;
}


void           ListMgr_CloseRmList(struct lmgr_rm_list_t *p_iter)
{
    db_result_free(&p_iter->p_mgr->conn, &p_iter->select_result);
    MemFree(p_iter);
}

/**
 * Get entry to be removed from its fid.
 */
int     ListMgr_GetRmEntry(lmgr_t * p_mgr,
                           const entry_id_t *p_id,
                           attr_set_t *p_attrs)
{
    result_handle_t result;
    int             rc, i, nb;
    GString        *req;
    char           *record[MAX_SOFTRM_FIELDS];

    if (!p_id || !p_attrs)
        return DB_INVALID_ARG;

    /* only keep fields in SOFTRM table */
    p_attrs->attr_mask = attr_mask_and(&p_attrs->attr_mask, &softrm_attr_set);

    req = g_string_new("SELECT ");
    nb = attrmask2fieldlist(req, p_attrs->attr_mask, T_SOFTRM, "", "", 0);
    if (nb == 0)
        g_string_append(req, "id");

    g_string_append_printf(req, " FROM "SOFT_RM_TABLE" WHERE id='"DFID_NOBRACE"'",
                           PFID(p_id));

    /* execute request (retry on connexion error or timeout) */
    do {
        rc = db_exec_sql(&p_mgr->conn, req->str, &result);
    } while (lmgr_delayed_retry(p_mgr, rc));

    if (rc)
    {
        char msg_buff[1024];

        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "DB query failed in %s line %d: query=\"%s\",code=%d, %s",
                   __FUNCTION__, __LINE__, req->str, rc,
                   db_errmsg(&p_mgr->conn, msg_buff, sizeof(msg_buff)));
        goto free_str;
    }

    for (i=0; i < MAX_SOFTRM_FIELDS; i++)
        record[i] = NULL;

    rc = db_next_record(&p_mgr->conn, &result, record, nb);
    if (rc == DB_END_OF_LIST)
    {
        rc = DB_NOT_EXISTS;
        goto free_res;
    } else if (rc)
        goto free_str;

    if (record[0] == NULL)
        return DB_REQUEST_FAILED;

    rc = result2attrset(T_SOFTRM, record, nb, p_attrs);

free_res:
    db_result_free(&p_mgr->conn, &result);
free_str:
    g_string_free(req, TRUE);
    return rc;
}

int ListMgr_SoftRemove_Discard(lmgr_t * p_mgr, const entry_id_t * p_id)
{
    int      rc;
    GString *req;

    req = g_string_new("DELETE FROM "SOFT_RM_TABLE" WHERE id=");
    g_string_append_printf(req, "'"DFID_NOBRACE"'", PFID(p_id));

    do {
        rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
    } while(lmgr_delayed_retry(p_mgr, rc));

    g_string_free(req, TRUE);
    return rc;
}
