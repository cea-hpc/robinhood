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
#include "RobinhoodLogs.h"
#include "Memory.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>


static int listmgr_remove_no_transaction( lmgr_t * p_mgr, const entry_id_t * p_id,
                                          const attr_set_t * p_attr_set, int last )
{
    char           request[4096];
    int            rc;
    DEF_PK(pk);
    DEF_PK(ppk);

    rc = entry_id2pk( p_mgr, p_id, FALSE, PTR_PK(pk) );
    if (rc)
        return rc;

    if (last)
    {
        /* stripes are only managed for Lustre filesystems */
    #ifdef _LUSTRE

        /* First remove stripe info */
        sprintf( request, "DELETE FROM " STRIPE_ITEMS_TABLE " WHERE id="DPK, pk );
        rc = db_exec_sql( &p_mgr->conn, request, NULL );
        if ( rc )
            return rc;

        sprintf( request, "DELETE FROM " STRIPE_INFO_TABLE " WHERE id="DPK, pk );
        rc = db_exec_sql( &p_mgr->conn, request, NULL );
        if ( rc )
            return rc;

    #endif

        /* then remove in other tables */
        sprintf( request, "DELETE FROM " MAIN_TABLE " WHERE id="DPK, pk );
        rc = db_exec_sql( &p_mgr->conn, request, NULL );
        if ( rc )
            return rc;

        if ( annex_table )
        {
            sprintf( request, "DELETE FROM " ANNEX_TABLE " WHERE id="DPK, pk );
            rc = db_exec_sql( &p_mgr->conn, request, NULL );
            if ( rc )
                return rc;
        }
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
        rc = entry_id2pk( p_mgr, &ATTR( p_attr_set, parent_id ), FALSE, PTR_PK(ppk) );
        if (rc)
            return rc;

        sprintf( request, "DELETE FROM " DNAMES_TABLE " WHERE parent_id="DPK" AND hname=sha1('%s') AND id="DPK, ppk, ATTR( p_attr_set, name ), pk );
        rc = db_exec_sql( &p_mgr->conn, request, NULL );
        if ( rc )
            return rc;
    }
    else if (!p_attr_set || !ATTR_MASK_TEST(p_attr_set, parent_id) || !ATTR_MASK_TEST(p_attr_set, name))
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "WARNING: missing attribute to delete entry from "DNAMES_TABLE);

    return rc;
}


int ListMgr_Remove( lmgr_t * p_mgr, const entry_id_t * p_id,
                    const attr_set_t * p_attr_set, int last )
{
    int rc;

    /* We want the remove operation to be atomic */
    rc = lmgr_begin( p_mgr );
    if ( rc )
        return rc;

    rc = listmgr_remove_no_transaction( p_mgr, p_id, p_attr_set, last );
    if (rc)
    {
        lmgr_rollback( p_mgr );
        return rc;
    }

    rc = lmgr_commit( p_mgr );
    if (!rc)
         p_mgr->nbop[OPIDX_RM]++;
    return rc;
}

/* macro for clarifying the code */
#ifdef HAVE_RM_POLICY
/* path is retrieved only for information: just get one of them */
#ifdef _HSM_LITE
#define BUILD_SOFTRM_FIELDS "one_path(%s.id) as fullpath, backendpath"
#define GET_SOFTRM_FIELDS "fullpath, backendpath"
#else
#define BUILD_SOFTRM_FIELDS "one_path(%s.id) as fullpath"
#define GET_SOFTRM_FIELDS "fullpath"
#endif

/**
 * Insert all entries to soft rm table.
 * @TODO check if it works with millions/billion entries
 */
static int listmgr_softrm_all( lmgr_t * p_mgr, time_t due_time )
{
    char query[2048];
    /* insert those entries to soft rm table */

    if ( annex_table )
    {
        sprintf( query, "INSERT IGNORE INTO " SOFT_RM_TABLE " (fid, "
                 GET_SOFTRM_FIELDS ", soft_rm_time, real_rm_time) "
                "(SELECT "MAIN_TABLE".id, "BUILD_SOFTRM_FIELDS", "
                 "%u, %u FROM "MAIN_TABLE " LEFT JOIN "ANNEX_TABLE
                 " ON " MAIN_TABLE ".id = " ANNEX_TABLE ".id)",
                 MAIN_TABLE,
                 (unsigned int)time(NULL),
                 (unsigned int)due_time );
    }
    else
    {
       sprintf( query, "INSERT IGNORE INTO " SOFT_RM_TABLE " (fid, "
                 GET_SOFTRM_FIELDS ", soft_rm_time, real_rm_time) "
                 "(SELECT id, "BUILD_SOFTRM_FIELDS", %u, %u FROM "MAIN_TABLE")",
                 MAIN_TABLE,
                 (unsigned int)time(NULL),
                 (unsigned int)due_time );
    }

    return db_exec_sql( &p_mgr->conn, query, NULL );
}

/**
 * Insert a single entry to soft rm table.
 */
static int listmgr_softrm_single( lmgr_t * p_mgr, const entry_id_t * p_id,
                                   const char * entry_path,
#ifdef _HSM_LITE
                                   const char * backend_path,
#endif
                                   time_t due_time )
{
    char query[4096];
    char escaped[RBH_PATH_MAX];
    char * curr = query;
    int rc;

    curr += sprintf( query,
                     "INSERT IGNORE INTO " SOFT_RM_TABLE
                     "(fid, " );

    if ( entry_path )
        curr += sprintf(curr, "fullpath, " );
#ifdef _HSM_LITE
    if ( backend_path )
        curr += sprintf(curr, "backendpath, " );
#endif

    curr += sprintf(curr, "soft_rm_time, real_rm_time) "
                  "VALUES ('"DFID_NOBRACE"', ", PFID(p_id) );

    if ( entry_path )
    {
        db_escape_string( &p_mgr->conn, escaped, RBH_PATH_MAX, entry_path );
        curr += sprintf(curr, "'%s', ", escaped );
    }
#ifdef _HSM_LITE
    if ( backend_path )
    {
        db_escape_string( &p_mgr->conn, escaped, RBH_PATH_MAX, backend_path );
        curr += sprintf(curr, "'%s', ", escaped );
    }
#endif

    curr += sprintf( curr, " %u, %u ) ",
                    (unsigned int)time(NULL),
                    (unsigned int)due_time );

    rc = db_exec_sql( &p_mgr->conn, query, NULL );

    if (rc)
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "DB query failed in %s line %d: query=\"%s\", code=%d: %s",
                    __FUNCTION__, __LINE__, query, rc, db_errmsg( &p_mgr->conn, query, 1024 ) );
    return rc;
}

#endif


/* /!\ cross table conditions cannot be used */
/* /!\ the table on which the filter apply must be removed at last */

static int listmgr_mass_remove( lmgr_t * p_mgr, const lmgr_filter_t * p_filter, int soft_rm,
                                time_t real_remove_time, rm_cb_func_t cb_func )
{
    int            rc;
    char           query[4096];
    char           filter_str[2048];
    char           filter_str_names[1024];
    char           *curr_filter;
    int            filter_main = 0;
    int            filter_annex = 0;
    int            filter_names = 0;
    int            filter_stripe_info = 0;
    int            filter_stripe_items = 0;
    char           from[1024];
    char           *curr_from;
    const char* first_table = NULL;
    table_enum     query_tab;
    const char* direct_del_table = NULL;
    char           tmp_table_name[256];
    result_handle_t result;
    char          *field_tab[3]; /* 3 max: id, fullpath, backendpath */
    DEF_PK(pk);
    unsigned int   rmcount = 0;

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
    rc = lmgr_begin( p_mgr );
    if ( rc )
        return rc;

    if ( !p_filter
         || ( ( p_filter->filter_type == FILTER_SIMPLE )
              && ( p_filter->filter_simple.filter_count == 0 ) )
         || ( ( p_filter->filter_type == FILTER_BOOLEXPR )
              && ( p_filter->filter_boolexpr == NULL ) ) )
    {

#ifdef HAVE_RM_POLICY
        if (soft_rm)
        {
            rc = listmgr_softrm_all( p_mgr, real_remove_time );
            if ( rc )
                goto rollback;
        }
#endif

        /* Remove all !!! */
        DisplayLog( LVL_EVENT, LISTMGR_TAG,
                    "No filter is specified: removing entries from all tables." );

        /* stripes are only managed for lustre filesystems */
#ifdef _LUSTRE
        rc = db_exec_sql( &p_mgr->conn, "DELETE FROM " STRIPE_ITEMS_TABLE, NULL );
        if ( rc )
            goto rollback;

        rc = db_exec_sql( &p_mgr->conn, "DELETE FROM " STRIPE_INFO_TABLE, NULL );
        if ( rc )
            goto rollback;
#endif

        if ( annex_table )
        {
            rc = db_exec_sql( &p_mgr->conn, "DELETE FROM " ANNEX_TABLE, NULL );
            if ( rc )
                goto rollback;
        }

        rc = db_exec_sql( &p_mgr->conn, "DELETE FROM " MAIN_TABLE, NULL );
        if ( rc )
            goto rollback;

        rc = db_exec_sql( &p_mgr->conn, "DELETE FROM " DNAMES_TABLE, NULL );
        if ( rc )
            goto rollback;

        /* FIXME how many entries removed? */
        return lmgr_commit( p_mgr );
    }

    /* on which table are the filters ?  */
    curr_filter = filter_str;
    filter_main = filter2str( p_mgr, curr_filter, p_filter, T_MAIN, FALSE /* no leading AND */, TRUE );
    curr_filter += strlen(curr_filter);

    filter_names = filter2str( p_mgr, filter_str_names, p_filter, T_DNAMES, FALSE, FALSE );

    if ( annex_table )
    {
        filter_annex = filter2str( p_mgr, curr_filter, p_filter, T_ANNEX, (curr_filter != filter_str), TRUE );
        curr_filter += strlen(curr_filter);
    }
    else
        filter_annex = 0;

    /* stripes are only managed for Lustre filesystems */
#ifdef _LUSTRE
    filter_stripe_info =
        filter2str( p_mgr, curr_filter, p_filter, T_STRIPE_INFO, (curr_filter != filter_str), TRUE );
    curr_filter += strlen(curr_filter);
    filter_stripe_items =
        filter2str( p_mgr, curr_filter, p_filter, T_STRIPE_ITEMS, (curr_filter != filter_str), TRUE );
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
        /* its about names only */
        goto clean_names;

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

#ifdef HAVE_RM_POLICY
    if ( soft_rm )
    {
        if (annex_table)
            sprintf( query,
                 "CREATE TEMPORARY TABLE %s AS SELECT DISTINCT(%s.id), "BUILD_SOFTRM_FIELDS
                 " FROM "MAIN_TABLE " LEFT JOIN "ANNEX_TABLE" ON "MAIN_TABLE".id = "ANNEX_TABLE".id"
                 " WHERE %s", tmp_table_name, first_table, first_table, filter_str );
        else
            sprintf( query,
                 "CREATE TEMPORARY TABLE %s AS SELECT DISTINCT(%s.id), "BUILD_SOFTRM_FIELDS" FROM %s"
                 " WHERE %s", tmp_table_name, first_table, first_table, from, filter_str );
    }
    else
#endif
        sprintf( query,
                 "CREATE TEMPORARY TABLE %s AS SELECT DISTINCT(%s.id) FROM %s"
                 " WHERE %s", tmp_table_name, first_table, from, filter_str );

    /* apply filters on function return val */
    /* FIXME it must also match entries whose path is null (no path) */
    func_filter(p_mgr, query + strlen(query), p_filter, query_tab, TRUE, TRUE);

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
    if ( rc )
        goto rollback;

    /* if the filter is only a single table, entries can be directly deleted */
    if ((filter_main?1:0) + (filter_annex?1:0) + (filter_stripe_info?1:0) + (filter_stripe_items?1:0) == 1)
    {
        DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Direct deletion in %s table", first_table );

        /* if filter is on annex, we can directly use filter in WHERE clause */
        sprintf( query, "DELETE FROM %s WHERE %s", first_table, filter_str );

        /* apply filters on function return val */
        func_filter(p_mgr, query + strlen(query), p_filter, query_tab, TRUE, FALSE);

        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if ( rc )
            goto rollback;
        else
            direct_del_table = first_table;
    }

    /* do the cleaning in other tables */
    DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Starting indirect removal" );

#ifdef HAVE_RM_POLICY
    if ( soft_rm )
        sprintf( query, "SELECT id, "GET_SOFTRM_FIELDS" FROM %s", tmp_table_name );
    else
#endif
        sprintf( query, "SELECT id FROM %s", tmp_table_name );

    rc = db_exec_sql( &p_mgr->conn, query, &result );
    if ( rc )
        goto rollback;

    DisplayLog( LVL_DEBUG, LISTMGR_TAG,
                "%d identifiers to be removed from all tables",
                db_result_nb_records( &p_mgr->conn, &result ) );

    rmcount = 0;

    while ((rc = db_next_record( &p_mgr->conn, &result, field_tab, 3 ))
                == DB_SUCCESS
            && ( field_tab[0] != NULL ) )
    {
        entry_id_t id;

        if ( sscanf( field_tab[0], SPK, PTR_PK(pk) ) != 1 )
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Unexpected format for database key: '%s'",
                        field_tab[0] );
            rc = DB_INVALID_ARG;
            goto free_res;
        }

        rc = pk2entry_id( p_mgr, pk, &id );
        if (rc)
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Unexpected format for database key: "DPK, pk);
            goto free_res;
        }

#ifdef HAVE_RM_POLICY
        if ( soft_rm )
        {
            /* insert into softrm table */
            rc = listmgr_softrm_single( p_mgr, &id,
                                        field_tab[1],
#ifdef _HSM_LITE
                                        field_tab[2],
#endif
                                        real_remove_time );
            if (rc)
                goto free_res;
        }
#endif

        /* delete all entries related to this id */

        /* remove all paths to an entry if it has no longer info in ENTRIES */
        sprintf( query, "DELETE FROM " DNAMES_TABLE " WHERE id="DPK, pk );
        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if ( rc )
            goto free_res;

        /* stripes are only managed for Lustre filesystems */
#ifdef _LUSTRE
        if (!direct_del_table || strcmp(direct_del_table, STRIPE_ITEMS_TABLE))
        {
            sprintf( query, "DELETE FROM " STRIPE_ITEMS_TABLE " WHERE id="DPK, pk );
            rc = db_exec_sql( &p_mgr->conn, query, NULL );
            if ( rc )
                goto free_res;
        }

        if (!direct_del_table || strcmp(direct_del_table, STRIPE_INFO_TABLE))
        {
            sprintf( query, "DELETE FROM " STRIPE_INFO_TABLE " WHERE id="DPK, pk );
            rc = db_exec_sql( &p_mgr->conn, query, NULL );
            if ( rc )
                goto free_res;
        }
#endif

        if (!direct_del_table || strcmp(direct_del_table, MAIN_TABLE))
        {
            sprintf( query, "DELETE FROM " MAIN_TABLE " WHERE id="DPK, pk );
            rc = db_exec_sql( &p_mgr->conn, query, NULL );
            if ( rc )
                goto free_res;
        }

        if (!direct_del_table || strcmp(direct_del_table, ANNEX_TABLE))
        {
            sprintf( query, "DELETE FROM " ANNEX_TABLE " WHERE id="DPK, pk );
            rc = db_exec_sql( &p_mgr->conn, query, NULL );
            if ( rc )
                goto free_res;
        }

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

clean_names:
    /* if there is a filter on names, clean them independantly, whatever the over filters */
    if (filter_names)
    {
        DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Direct deletion in " DNAMES_TABLE " table" );
        sprintf( query, "DELETE FROM " DNAMES_TABLE " WHERE %s", filter_str_names );

        /* apply filters on function return val */
        func_filter(p_mgr, query + strlen(query), p_filter, T_DNAMES, TRUE, FALSE);

        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if ( rc )
            goto rollback;
    }

    rc = lmgr_commit( p_mgr );
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
    return listmgr_mass_remove( p_mgr, p_filter, FALSE, 0, cb_func );
}

#ifdef HAVE_RM_POLICY

int ListMgr_MassSoftRemove( lmgr_t * p_mgr, const lmgr_filter_t * p_filter,
                            time_t real_remove_time, rm_cb_func_t cb_func )
{
    /* soft rm */
    return listmgr_mass_remove( p_mgr, p_filter, TRUE, real_remove_time, cb_func );
}

/**
 * Remove an entry from the main database, and insert it to secondary table
 * for delayed removal.
 * \param real_remove_time time when the entry must be really removed.
 * \param p_old_attrs contains old attributes, parent+name and backendpath must be set.
 *        If NULL, it is retrieved from the database.
 */
int            ListMgr_SoftRemove( lmgr_t * p_mgr, const entry_id_t * p_id,
                                   attr_set_t * p_old_attrs,
                                   time_t real_remove_time )
{
    int rc;
    const char * entry_path = NULL;
#ifdef _HSM_LITE
    const char * backendpath = NULL;
#endif
    attr_set_t missing_attrs;
    ATTR_MASK_INIT( &missing_attrs );

    /* last known path is needed for insertion in SOFT_RM table */
    if (ATTR_MASK_TEST( p_old_attrs, fullpath ))
        entry_path = ATTR(p_old_attrs, fullpath);
    else
        ATTR_MASK_SET( &missing_attrs, fullpath );

    /* this is needed for remove function */
    if (!ATTR_MASK_TEST( p_old_attrs, parent_id)
        || !ATTR_MASK_TEST( p_old_attrs, name))
    {
        ATTR_MASK_SET( &missing_attrs, parent_id );
        ATTR_MASK_SET( &missing_attrs, name );
    }

#ifdef _HSM_LITE
    if (ATTR_MASK_TEST( p_old_attrs, backendpath ))
        backendpath = ATTR(p_old_attrs, backendpath);
    else
        /* check if the previous entry had a path in backend */
        ATTR_MASK_SET( &missing_attrs, backendpath );
#endif

    if ( missing_attrs.attr_mask )
    {
        if ( ListMgr_Get( p_mgr, p_id, &missing_attrs ) == DB_SUCCESS )
        {
            if ((entry_path == NULL) && ATTR_MASK_TEST(&missing_attrs, fullpath))
                entry_path = ATTR(&missing_attrs, fullpath);
#ifdef _HSM_LITE
            if ((backendpath == NULL) && ATTR_MASK_TEST( &missing_attrs, backendpath ))
                backendpath = ATTR(&missing_attrs, backendpath);
#endif
        }
    }

    /* We want the removal sequence to be atomic */
    rc = lmgr_begin( p_mgr );
    if ( rc )
        return rc;

    rc = listmgr_softrm_single( p_mgr, p_id, entry_path,
#ifdef _HSM_LITE
    backendpath,
#endif
                                real_remove_time );
    if ( rc )
    {
        lmgr_rollback( p_mgr );
        return rc;
    }

    /* merge old attrs to missing attrs (can overwrite) */
    ListMgr_MergeAttrSets(&missing_attrs, p_old_attrs, TRUE);

    /* remove the entry from main tables, if it exists */
    rc = listmgr_remove_no_transaction( p_mgr, p_id, &missing_attrs, TRUE );
    if (rc != DB_SUCCESS && rc != DB_NOT_EXISTS )
    {
        lmgr_rollback( p_mgr );
        return rc;
    }

    /* commit */
    rc = lmgr_commit( p_mgr );
    if (!rc)
         p_mgr->nbop[OPIDX_RM]++;
    return rc;
}

typedef struct lmgr_rm_list_t
{
    lmgr_t        *p_mgr;
    result_handle_t select_result;
} lmgr_rm_list_t;

struct lmgr_rm_list_t * ListMgr_RmList( lmgr_t * p_mgr, int expired_only, lmgr_filter_t * p_filter )
{
    int rc;
    lmgr_rm_list_t * p_list = MemAlloc( sizeof(lmgr_rm_list_t) );
    char query[4096];
    char filter_str[1024] = "WHERE ";
    char * curr_filter = filter_str + strlen(filter_str);

    if ( !p_list )
        return NULL;

    if ( expired_only )
    {
        curr_filter += sprintf( curr_filter, "real_rm_time <= %u ",
                                (unsigned int)time(NULL) );
    }

    if ( p_filter )
    {
        if (p_filter->filter_type != FILTER_SIMPLE )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                       "Unsupported filter in %s(): simple filter expected",
                       __FUNCTION__ );
            return NULL;
        }
        /* are there unsuported fields in this filter? */
#ifdef _HSM_LITE
        if ( lmgr_check_filter_fields( p_filter, ATTR_MASK_fullpath | ATTR_MASK_backendpath ) )
#else
        if ( lmgr_check_filter_fields( p_filter, ATTR_MASK_fullpath ) )
#endif
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "Unsupported field in filter (in %s())",
                         __FUNCTION__);
            return NULL;
        }
        if ( filter2str( p_mgr, curr_filter, p_filter, T_SOFTRM, expired_only, FALSE ) < 0)
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "Error converting filter to SQL request" );
            return NULL;
        }
    }

    p_list->p_mgr = p_mgr;

#ifdef _HSM_LITE
    snprintf( query, 4096, "SELECT fid, fullpath, backendpath, soft_rm_time, real_rm_time "
#else
    snprintf( query, 4096, "SELECT fid, fullpath, soft_rm_time, real_rm_time "
#endif
                "FROM "SOFT_RM_TABLE" "
                "%s "
                "ORDER BY real_rm_time ASC",
                (expired_only || p_filter) ? filter_str:"" );

    /* execute request */
    rc = db_exec_sql( &p_mgr->conn, query, &p_list->select_result );

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

int            ListMgr_GetNextRmEntry( struct lmgr_rm_list_t *p_iter,
                                       entry_id_t * p_id,
                                       char * last_known_path,
#ifdef _HSM_LITE
                                       char * bkpath,
#endif
                                       time_t * soft_rm_time,
                                       time_t * expiration_time )
{
    int            rc = 0;
    int i;

#ifdef _HSM_LITE
    #define SHIFT 1
#else
    #define SHIFT 0
#endif

    /* 0=fid, 1=path, 2=soft_rm_time, 3=real_rm_time */
    char          * record[4+SHIFT];

    if ( !p_id )
        return DB_INVALID_ARG;

    for ( i=0; i < 4+SHIFT; i++)
        record[i] = NULL;

    rc = db_next_record( &p_iter->p_mgr->conn, &p_iter->select_result, record, 4+SHIFT);

    if ( rc )
        return rc;
    if ( record[0] == NULL )
        return DB_REQUEST_FAILED;

    if ( sscanf( record[0], SFID, RFID(p_id) ) <= 0 )
        return DB_REQUEST_FAILED;

    if ( last_known_path )
    {
        if (record[1])
            strcpy( last_known_path, record[1] );
        else
            last_known_path[0] = '\0';
    }

#ifdef _HSM_LITE
    if ( bkpath )
    {
        if (record[2])
            strcpy( bkpath, record[2] );
        else
            bkpath[0] = '\0';
    }
#endif

    if ( soft_rm_time )
    {
        if ( sscanf( record[2+SHIFT], "%lu", soft_rm_time ) <= 0 )
            return DB_REQUEST_FAILED;
    }

    if ( expiration_time )
    {
        if ( sscanf( record[3+SHIFT], "%lu", expiration_time ) <= 0 )
            return DB_REQUEST_FAILED;
    }

    return DB_SUCCESS;
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
                           const entry_id_t * p_id,
                           char * last_known_path,
#ifdef _HSM_LITE
                           char * bkpath,
#endif
                           time_t * soft_rm_time,
                           time_t * expiration_time)
{
    char query[4096];
    result_handle_t result;
    int rc, i;

    /* 0=path, 1=soft_rm_time, 2=real_rm_time */
    char          * record[3+SHIFT];

    if ( !p_id )
        return DB_INVALID_ARG;

#ifdef _HSM_LITE
    snprintf( query, 4096, "SELECT fullpath, backendpath, soft_rm_time, real_rm_time "
#else
    snprintf( query, 4096, "SELECT fullpath, soft_rm_time, real_rm_time "
#endif
              "FROM "SOFT_RM_TABLE" WHERE fid='"DFID_NOBRACE"'",
              PFID(p_id) );

    /* execute request */
    rc = db_exec_sql( &p_mgr->conn, query, &result );

    if ( rc )
    {
        char msg_buff[1024];
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "DB query failed in %s line %d: query=\"%s\",code=%d, %s",
                    __FUNCTION__, __LINE__, query, rc, db_errmsg( &p_mgr->conn, msg_buff, 1024 ) );
        return rc;
    }

    for ( i=0; i < 3+SHIFT; i++)
        record[i] = NULL;

    rc = db_next_record( &p_mgr->conn, &result, record, 3+SHIFT);
    if ( rc == DB_END_OF_LIST )
    {
        rc = DB_NOT_EXISTS;
        goto free_res;
    } else if ( rc )
         return rc;

    if ( record[0] == NULL )
        return DB_REQUEST_FAILED;

    if ( last_known_path )
    {
        if (record[0])
            strcpy( last_known_path, record[0] );
        else
            last_known_path[0] = '\0';
    }

#ifdef _HSM_LITE
    if ( bkpath )
    {
        if (record[1])
            strcpy( bkpath, record[1] );
        else
            bkpath[1] = '\0';
    }
#endif

    if ( soft_rm_time )
    {
        if ( sscanf( record[1+SHIFT], "%lu", soft_rm_time ) <= 0 )
            return DB_REQUEST_FAILED;
    }

    if ( expiration_time )
    {
        if ( sscanf( record[2+SHIFT], "%lu", expiration_time ) <= 0 )
            return DB_REQUEST_FAILED;
    }

free_res:
    db_result_free( &p_mgr->conn, &result );
    return rc;
}

int ListMgr_SoftRemove_Discard( lmgr_t * p_mgr, const entry_id_t * p_id )
{
    char query[1024];

    snprintf(query, 1024,
             "DELETE FROM "SOFT_RM_TABLE" WHERE fid='"DFID_NOBRACE"'",
             PFID(p_id) );

    return db_exec_sql( &p_mgr->conn, query, NULL );
}

#endif



