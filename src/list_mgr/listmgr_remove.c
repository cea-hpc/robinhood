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


static int ListMgr_Remove_NoTransaction( lmgr_t * p_mgr, const entry_id_t * p_id )
{
    char           request[4096];
    int            rc;
    DEF_PK(pk);

    rc = entry_id2pk( p_mgr, p_id, FALSE, PTR_PK(pk) );
    if (rc)
        return rc;

    /* First remove stripe info */
    sprintf( request, "DELETE FROM " STRIPE_ITEMS_TABLE " WHERE id="DPK, pk );
    rc = db_exec_sql( &p_mgr->conn, request, NULL );
    if ( rc )
        return rc;

    sprintf( request, "DELETE FROM " STRIPE_INFO_TABLE " WHERE id="DPK, pk );
    rc = db_exec_sql( &p_mgr->conn, request, NULL );
    if ( rc )
        return rc;

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

    return rc;
}


int ListMgr_Remove( lmgr_t * p_mgr, const entry_id_t * p_id )
{
    int rc;

    /* We want the remove operation to be atomic */
    rc = lmgr_begin( p_mgr );
    if ( rc )
        return rc;

    rc = ListMgr_Remove_NoTransaction( p_mgr, p_id );
    if (rc)
    {
        lmgr_rollback( p_mgr );
        return rc;
    }
        
    return lmgr_commit( p_mgr );
}


/* /!\ cross table conditions cannot be used */
/* /!\ the table on which the filter apply must be removed at last */

int ListMgr_MassRemove( lmgr_t * p_mgr, const lmgr_filter_t * p_filter )
{
    int            rc;
    char           query[2048];
    char           filter_str_main[1024];
    char           filter_str_annex[1024];
    char           filter_str_stripe_info[1024];
    char           filter_str_stripe_items[1024];
    int            filter_main = 0;
    int            filter_annex = 0;
    int            filter_stripe_info = 0;
    int            filter_stripe_items = 0;
    char           tmp_table_name[256];
    int            indirect_del_main = FALSE;
    int            indirect_del_annex = FALSE;
    result_handle_t result;
    char          *idstr;
    DEF_PK(pk);
    unsigned int   rmcount;

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
        /* Remove all !!! */
        DisplayLog( LVL_MAJOR, LISTMGR_TAG,
                    "No filter is specified: remove entries from all tables !!!" );

        rc = db_exec_sql( &p_mgr->conn, "DELETE FROM " STRIPE_ITEMS_TABLE, NULL );
        if ( rc )
            goto rollback;

        rc = db_exec_sql( &p_mgr->conn, "DELETE FROM " STRIPE_INFO_TABLE, NULL );
        if ( rc )
            goto rollback;

        if ( annex_table )
        {
            rc = db_exec_sql( &p_mgr->conn, "DELETE FROM " ANNEX_TABLE, NULL );
            if ( rc )
                goto rollback;
        }

        rc = db_exec_sql( &p_mgr->conn, "DELETE FROM " MAIN_TABLE, NULL );
        if ( rc )
            goto rollback;

        return lmgr_commit( p_mgr );
    }

    /* on which table is the filter ? */
    filter_main = filter2str( p_mgr, filter_str_main, p_filter, T_MAIN, FALSE, FALSE );

    if ( annex_table )
        filter_annex = filter2str( p_mgr, filter_str_annex, p_filter, T_ANNEX, FALSE, FALSE );
    else
        filter_annex = 0;

    filter_stripe_info =
        filter2str( p_mgr, filter_str_stripe_info, p_filter, T_STRIPE_INFO, FALSE, FALSE );
    filter_stripe_items =
        filter2str( p_mgr, filter_str_stripe_items, p_filter, T_STRIPE_ITEMS, FALSE, FALSE );

    if ( filter_main + filter_annex + filter_stripe_info + filter_stripe_items == 0 )
    {
        /* should have been detected in the beginning of this function ! */
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "How come empty filter has not been detected ?" );
        rc = DB_REQUEST_FAILED;
        goto rollback;
    }

    DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Creating temporary table" );

    /* create id table depending on filter */
    if ( filter_main && !( filter_annex || filter_stripe_info || filter_stripe_items ) )
    {
        sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
                 ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );
        sprintf( query,
                 "CREATE TEMPORARY TABLE %s AS SELECT id FROM " MAIN_TABLE
                 " WHERE %s", tmp_table_name, filter_str_main );
    }
    else if ( filter_annex && !( filter_main || filter_stripe_info || filter_stripe_items ) )
    {
        sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
                 ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );
        sprintf( query,
                 "CREATE TEMPORARY TABLE %s AS SELECT id FROM " ANNEX_TABLE
                 " WHERE %s", tmp_table_name, filter_str_annex );
    }
    else if ( filter_stripe_items && !( filter_main || filter_annex || filter_stripe_info ) )
    {
        sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
                 ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );
        sprintf( query,
                 "CREATE TEMPORARY TABLE %s AS SELECT id FROM "
                 STRIPE_ITEMS_TABLE " WHERE %s", tmp_table_name, filter_str_stripe_items );
    }
    else if ( filter_stripe_info && !( filter_main || filter_annex || filter_stripe_items ) )
    {
        sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
                 ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );
        sprintf( query,
                 "CREATE TEMPORARY TABLE %s AS SELECT id FROM "
                 STRIPE_INFO_TABLE " WHERE %s", tmp_table_name, filter_str_stripe_info );
    }
    else
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Error: Filter on several tables not supported: "
                    MAIN_TABLE ":%d, " ANNEX_TABLE ":%d, "
                    STRIPE_ITEMS_TABLE ":%d, " STRIPE_INFO_TABLE ":%d",
                    filter_main, filter_annex, filter_stripe_items, filter_stripe_info );
        rc = DB_NOT_SUPPORTED;
        goto rollback;
    }


    /* create the temporary table */
    rc = db_exec_sql( &p_mgr->conn, query, NULL );
    if ( rc )
        goto rollback;

    /* delete what can be directly deleted */

    if ( annex_table )
    {
        if ( filter_annex )
        {
            DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Direct deletion in " ANNEX_TABLE " table" );

            /* if filter is on annex, we can directly use filter in WHERE clause */
            sprintf( query, "DELETE FROM " ANNEX_TABLE " WHERE %s", filter_str_annex );
            rc = db_exec_sql( &p_mgr->conn, query, NULL );
            if ( rc )
                goto rollback;
        }
        else
        {
            indirect_del_annex = TRUE;
        }
    }

    if ( filter_main )
    {
        DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Direct deletion in " MAIN_TABLE " table" );

        /* if filter is on main table, we can directly use filter in WHERE clause */
        sprintf( query, "DELETE FROM " MAIN_TABLE " WHERE %s", filter_str_main );
        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if ( rc )
            goto rollback;
    }
    else
    {
        indirect_del_main = TRUE;
    }

    DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Starting indirect removal" );

    /* Now, make indirect deletion */

    sprintf( query, "SELECT id FROM %s", tmp_table_name );
    rc = db_exec_sql( &p_mgr->conn, query, &result );
    if ( rc )
        goto rollback;

    DisplayLog( LVL_DEBUG, LISTMGR_TAG,
                "%d identifiers to be removed from all tables",
                db_result_nb_records( &p_mgr->conn, &result ) );

    rmcount = 0;

    while ( ( rc =
              db_next_record( &p_mgr->conn, &result, &idstr,
                              1 ) ) == DB_SUCCESS && ( idstr != NULL ) )
    {
        if ( sscanf( idstr, SPK, PTR_PK(pk) ) != 1 )
            goto free_res;

        /* delete all entries related to this id */

        sprintf( query, "DELETE FROM " STRIPE_ITEMS_TABLE " WHERE id="DPK, pk );
        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if ( rc )
            goto free_res;

        sprintf( query, "DELETE FROM " STRIPE_INFO_TABLE " WHERE id="DPK, pk );
        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if ( rc )
            goto free_res;

        if ( indirect_del_main )
        {
            sprintf( query, "DELETE FROM " MAIN_TABLE " WHERE id="DPK, pk );
            rc = db_exec_sql( &p_mgr->conn, query, NULL );
            if ( rc )
                goto free_res;
        }

        if ( indirect_del_annex )
        {
            sprintf( query, "DELETE FROM " ANNEX_TABLE " WHERE id="DPK, pk );
            rc = db_exec_sql( &p_mgr->conn, query, NULL );
            if ( rc )
                goto free_res;
        }

        rmcount++;

    }

    db_result_free( &p_mgr->conn, &result );

    if ( ( rc != 0 ) && ( rc != DB_END_OF_LIST ) )
        return rc;

    DisplayLog( LVL_DEBUG, LISTMGR_TAG,
                "End of indirect removal: %u identifiers removed", rmcount );

    /* drop tmp table */
    sprintf( query, "DROP TABLE %s", tmp_table_name );
    rc = db_exec_sql( &p_mgr->conn, query, NULL );
    if ( rc )
        goto rollback;


    return lmgr_commit( p_mgr );

  free_res:
    db_result_free( &p_mgr->conn, &result );
  rollback:
    lmgr_rollback( p_mgr );
    return rc;
}


#ifdef HAVE_RM_POLICY
/**
 * Remove an entry from the main database, and insert it to secondary table
 * for delayed removal.
 * \param real_remove_time time when the entry must be really removed.
 * \param last_known_path last known path for this entry.
 *        If NULL, it is retrieved from the database.
 */
int            ListMgr_SoftRemove( lmgr_t * p_mgr, const entry_id_t * p_id,
                                   const char * last_known_path,
#ifdef _BACKUP_FS
                                   const char * bkpath,
#endif
                                   time_t real_remove_time )
{
    int rc;
    char query[4096];
    char * curr = query;
    const char * entry_path = NULL;
#ifdef _BACKUP_FS
    const char * backendpath = NULL;
#endif
    attr_set_t attrs;

    /* check if the previous entry had a path */
    if ( last_known_path )
        entry_path = last_known_path;
    else
    {
        ATTR_MASK_INIT( &attrs );
        ATTR_MASK_SET( &attrs, fullpath );

        if ( (ListMgr_Get( p_mgr, p_id, &attrs ) == DB_SUCCESS )
            && ATTR_MASK_TEST( &attrs, fullpath ) )
        {
            entry_path = ATTR(&attrs, fullpath);
        }
    }

#ifdef _BACKUP_FS
    /* check if the previous entry had a path in backend */
    if ( bkpath )
        backendpath = bkpath;
    else
    {
        ATTR_MASK_INIT( &attrs );
        ATTR_MASK_SET( &attrs, backendpath );

        if ( (ListMgr_Get( p_mgr, p_id, &attrs ) == DB_SUCCESS )
            && ATTR_MASK_TEST( &attrs, fullpath ) )
        {
            backendpath = ATTR(&attrs, backendpath);
        }
    }
#endif

    /* We want the removal sequence to be atomic */
    rc = lmgr_begin( p_mgr );
    if ( rc )
        return rc;

    curr += sprintf( query,
                     "INSERT IGNORE INTO " SOFT_RM_TABLE
                     "(fid, " );

    if ( entry_path )
        curr += sprintf(curr, "last_known_path, " );
#ifdef _BACKUP_FS
    if ( backendpath )
        curr += sprintf(curr, "bkpath, " );
#endif

    curr += sprintf(curr, "soft_rm_time, real_rm_time) "
                  "VALUES ('"DFID_NOBRACE"', ", PFID(p_id) );

    /* @TODO escape strings in these requests */

    if ( entry_path )
        curr += sprintf(curr, "'%s', ", entry_path );
#ifdef _BACKUP_FS
    if ( backendpath )
        curr += sprintf(curr, "'%s', ", backendpath );
#endif

    curr += sprintf( curr, " %u, %u ) ",
                    (unsigned int)time(NULL),
                    (unsigned int)real_remove_time );

    rc = db_exec_sql( &p_mgr->conn, query, NULL );

    if ( rc )
    {
        lmgr_rollback( p_mgr );
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "DB query failed in %s line %d: query=\"%s\", code=%d: %s",
                    __FUNCTION__, __LINE__, query, rc, db_errmsg( &p_mgr->conn, query, 1024 ) );
        return rc;
    }

    /* remove the entry from main tables, if it exists */
    rc = ListMgr_Remove_NoTransaction( p_mgr, p_id );
    if (rc != DB_SUCCESS && rc != DB_NOT_EXISTS )
    {
        lmgr_rollback( p_mgr );
        return rc;
    }

    /* commit */
    return lmgr_commit( p_mgr );
}

typedef struct lmgr_rm_list_t
{
    lmgr_t        *p_mgr;
    result_handle_t select_result;
} lmgr_rm_list_t;

struct lmgr_rm_list_t * ListMgr_RmList( lmgr_t * p_mgr, int expired_only )
{
    int rc;
    lmgr_rm_list_t * p_list = MemAlloc( sizeof(lmgr_rm_list_t) );
    char query[1024];

    if ( !p_list )
        return NULL;

    p_list->p_mgr = p_mgr;

    if ( expired_only )
        snprintf( query, 1024, "SELECT fid, last_known_path, soft_rm_time, real_rm_time "
                "FROM "SOFT_RM_TABLE" "
                "WHERE real_rm_time <= %u "
                "ORDER BY real_rm_time ASC",
                (unsigned int)time(NULL) );
    else
        strcpy( query, "SELECT fid, last_known_path, soft_rm_time, real_rm_time "
                "FROM "SOFT_RM_TABLE" "
                "ORDER BY real_rm_time ASC" );

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
                                       time_t * soft_rm_time,
                                       time_t * expiration_time )
{
    int            rc = 0;
    int i;

    /* 0=fid, 1=path, 2=soft_rm_time, 3=real_rm_time */
    char          * record[4];

    if ( !p_id )
        return DB_INVALID_ARG;

    for ( i=0; i < 4; i++)
        record[i] = NULL;

    rc = db_next_record( &p_iter->p_mgr->conn, &p_iter->select_result, record, 4 );
        
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

    if ( soft_rm_time )
    {
        if ( sscanf( record[2], "%lu", soft_rm_time ) <= 0 )
            return DB_REQUEST_FAILED;
    }

    if ( expiration_time )
    {
        if ( sscanf( record[3], "%lu", expiration_time ) <= 0 )
            return DB_REQUEST_FAILED;
    }

    return DB_SUCCESS;
}


void           ListMgr_CloseRmList( struct lmgr_rm_list_t *p_iter )
{
    db_result_free( &p_iter->p_mgr->conn, &p_iter->select_result );
    MemFree( p_iter );
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



