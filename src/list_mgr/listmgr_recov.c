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
/**
 * Special database function for disaster revocery (backup flavor)
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "database.h"
#include "listmgr_common.h"
#include "listmgr_prep_stmt.h"
#include "Memory.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include <stdio.h>
#include <stdlib.h>

#define MAX_DB_FIELDS 64

#define RECOV_ATTR_MASK ( ATTR_MASK_fullpath | ATTR_MASK_size | ATTR_MASK_owner | \
                          ATTR_MASK_gr_name | ATTR_MASK_last_mod | ATTR_MASK_backendpath | \
                          ATTR_MASK_status | ATTR_MASK_stripe_info )
/* table: id+... */
/* TODO: generate this list automatically */
#define RECOV_LIST_FIELDS "fullpath,size,owner,gr_name,last_mod,backendpath,status,stripe_count,stripe_size,pool_name"
#define RECOV_FIELD_COUNT 10


/**
 * \retval DB_NOT_EXISTS if the recovery table does not exist
 */
static int expected_recov_status( lmgr_t * p_mgr, lmgr_recov_stat_t * p_stats )
{
    int  rc, i;
    result_handle_t result;
    char * status[3];

    /* test if a RECOVERY table already exist, and contains entries */
    rc = db_exec_sql( &p_mgr->conn, "SELECT status,COUNT(*),SUM(size) FROM "RECOV_TABLE
                      " GROUP BY status", &result );
    if (rc)
        return rc;

    p_stats->total = 0;
    for (i = 0; i < RS_COUNT; i++ )
    {
        p_stats->status_count[i] = 0;
        p_stats->status_size[i] = 0;
    }

    while ( (rc = db_next_record( &p_mgr->conn, &result, status, 3 ))
            != DB_END_OF_LIST )
    {
        int cnt;
        uint64_t sz;
        if (rc)
            return rc;

        cnt = str2int( status[1] );
        if ( cnt == -1)
            return DB_INVALID_ARG;

        sz = str2size(  status[2] );
        if ( sz == -1LL)
            return DB_INVALID_ARG;

        p_stats->total += cnt;

        if ( status[0] != NULL )
        {
            int st = str2int( status[0] );

            switch (st)
            {
                case STATUS_NEW:
                    p_stats->status_count[RS_NOBACKUP] += cnt;
                    p_stats->status_size[RS_NOBACKUP] += sz;
                    break;
                case STATUS_MODIFIED:
                case STATUS_ARCHIVE_RUNNING:
                    p_stats->status_count[RS_DELTA] += cnt;
                    p_stats->status_size[RS_DELTA] += sz;
                    break;
                case STATUS_SYNCHRO:
                    p_stats->status_count[RS_OK] += cnt;
                    p_stats->status_size[RS_OK] += sz;
                    break;
            }
        }
    }

    db_result_free( &p_mgr->conn, &result );
    return 0;
}


/**
 * \retval DB_NOT_EXISTS if the recovery table does not exist
 */
int ListMgr_RecovStatus( lmgr_t * p_mgr, lmgr_recov_stat_t * p_stats )
{
    int  rc, i;
    result_handle_t result;
    char * status[3];

    /* test if a RECOVERY table already exist, and contains entries */
    rc = db_exec_sql( &p_mgr->conn, "SELECT recov_status,COUNT(*),SUM(size) FROM "RECOV_TABLE
                      " GROUP BY recov_status", &result );
    if (rc)
        return rc;

    /* table exists, fill status tab */
    p_stats->total = 0;
    for (i = 0; i < RS_COUNT; i++ )
    {
        p_stats->status_count[i] = 0;
        p_stats->status_size[i] = 0;
    }

    while ( (rc = db_next_record( &p_mgr->conn, &result, status, 3 ))
            != DB_END_OF_LIST )
    {
        int cnt;
        uint64_t sz;
        if (rc)
            return rc;

        cnt = str2int( status[1] );
        if ( cnt == -1)
            return DB_INVALID_ARG;

        sz = str2size(  status[2] );
        if ( sz == -1LL)
            return DB_INVALID_ARG;

        p_stats->total += cnt;

        if ( status[0] != NULL )
        {
            int idx = str2int( status[0] );
            if ((idx >= RS_COUNT) || (idx == -1) )
                return DB_REQUEST_FAILED;
            p_stats->status_count[idx] = cnt;
            p_stats->status_count[idx] = sz;
        }
    }

    db_result_free( &p_mgr->conn, &result );
    return 0;
}



/**
 *  Initialize a recovery process.
 *  \retval DB_SUCCESS the recovery process successfully started;
 *          the stats indicate the recovery states we can expect.
 *  \retval DB_ALREADY_EXISTS a recovery process already started
 *          and was not properly completed. stats indicate the current status.
 *  \retval error   another error occured.
 */
int ListMgr_RecovInit( lmgr_t * p_mgr, lmgr_recov_stat_t * p_stats )
{
    int  rc;

    rc = ListMgr_RecovStatus( p_mgr, p_stats );
    if (rc == 0)
    {
        if (p_stats->total != 0) /* RECOVERY table exists and is not empty */
            return DB_ALREADY_EXISTS;
    }
    else if ( rc != DB_NOT_EXISTS ) /* other error */
        return rc;

    if ( rc == 0 )
    {
        DisplayLog( LVL_EVENT, LISTMGR_TAG, "Dropping any previous "RECOV_TABLE" table" );
        /* start from clean state (no table, no indexes, no addl field) */
        rc = db_exec_sql( &p_mgr->conn, "DROP TABLE IF EXISTS "RECOV_TABLE, NULL );
        if ( rc )
            return rc;
    }

    DisplayLog( LVL_EVENT, LISTMGR_TAG, "Populating "RECOV_TABLE" table (this can take a few minutes)..." );
    /* create the recovery table */
    rc = db_exec_sql( &p_mgr->conn, "CREATE TABLE "RECOV_TABLE
        " SELECT ENTRIES.id," RECOV_LIST_FIELDS
        " FROM ENTRIES LEFT JOIN (ANNEX_INFO,STRIPE_INFO) ON "
        "( ENTRIES.id = ANNEX_INFO.id AND ENTRIES.id = STRIPE_INFO.id )",
                      NULL );
    if ( rc )
        return rc;

    DisplayLog( LVL_EVENT, LISTMGR_TAG, "Building indexes on "RECOV_TABLE" table..." );

    /* create pk */
    rc = db_exec_sql( &p_mgr->conn, "ALTER TABLE "RECOV_TABLE" ADD PRIMARY KEY (id)", NULL );
    if ( rc )
        return rc;

    /* add recov_status column */
    rc = db_exec_sql( &p_mgr->conn, "ALTER TABLE "RECOV_TABLE" ADD COLUMN recov_status INTEGER", NULL );
    if ( rc )
        return rc;

    /* add index on status */
    rc = db_exec_sql( &p_mgr->conn,  "CREATE INDEX recov_st_index ON "RECOV_TABLE"(recov_status)", NULL );
    if ( rc )
        return rc;

    /* count entries of each status */
    expected_recov_status( p_mgr, p_stats );

    /* delete all entries from tables (double check entry count) */

    return 0;
}

/**
 * Clear the recovery table.
 * /!\ all previously unrecovered entry will be lost
 */
int ListMgr_RecovReset( lmgr_t * p_mgr )
{
   DisplayLog( LVL_EVENT, LISTMGR_TAG, "Dropping any previous "RECOV_TABLE" table" );
   return db_exec_sql( &p_mgr->conn, "DROP TABLE IF EXISTS "RECOV_TABLE, NULL );
}

/**
 *  Continue a recovery process (returns an iterator on entry list),
 *  possibly using the specified filter.
 *  \retval iterator must be release using ListMgr_CloseIterator()
 */
struct lmgr_iterator_t * ListMgr_RecovResume( lmgr_t * p_mgr,
                                              const char * dir_path,
                                              int retry, /* also retry previously errorneous entries */
                                              const lmgr_iter_opt_t * p_opt )
{
    char query[4096];
    char * curr;
    lmgr_iterator_t * it;
    int rc;

    strcpy( query, "SELECT id,"RECOV_LIST_FIELDS" FROM "RECOV_TABLE" WHERE " );
    curr = query + strlen(query);
    if ( retry )
        curr += sprintf( curr, "(recov_status IS NULL OR recov_status == %u)",
                         RS_ERROR );
    else
        curr += sprintf( curr, "recov_status IS NULL" );

    if ( dir_path )
        curr += sprintf( curr, " AND fullpath LIKE '%s/%%'", dir_path );

    /* allocate a new iterator */
    it = ( lmgr_iterator_t * ) MemAlloc( sizeof( lmgr_iterator_t ) );
    it->p_mgr = p_mgr;

    /* execute request */
    rc = db_exec_sql( &p_mgr->conn, query, &it->select_result );

    if ( rc )
    {
        MemFree( it );
        return NULL;
    }
    else
        return it;
}

int ListMgr_RecovGetNext( struct lmgr_iterator_t *p_iter,
                          entry_id_t * p_id,
                          attr_set_t * p_info )
{
    int            rc = 0;
    char          *result_tab[1+RECOV_FIELD_COUNT]; /* +1 for id */
    DEF_PK(pk);
    int entry_disappeared = FALSE;

    do
    {
        entry_disappeared = FALSE;

        rc = db_next_record( &p_iter->p_mgr->conn, &p_iter->select_result,
                             result_tab, RECOV_FIELD_COUNT+1 );

        if ( rc )
            return rc;
        if ( result_tab[0] == NULL ) /* no id? */
            return DB_REQUEST_FAILED;

        if ( sscanf( result_tab[0], SPK, PTR_PK(pk) ) != 1 )
            return DB_REQUEST_FAILED;

        /* retrieve entry id (except validator) */
        rc = pk2entry_id( p_iter->p_mgr, pk, p_id );

        /* /!\ If the entry disappeared from DB, we must go to next record */
        if ( rc == DB_NOT_EXISTS )
            entry_disappeared = TRUE;
        else if ( rc )
            return rc;

    }
    while ( entry_disappeared );        /* goto next record if entry desappered */

    return result2attrset( T_RECOV, result_tab + 1, RECOV_FIELD_COUNT, p_info );
}

int ListMgr_RecovComplete( lmgr_t * p_mgr, lmgr_recov_stat_t * p_stats );

int ListMgr_RecovSetState( lmgr_t * p_mgr, const entry_id_t * p_id,
                           recov_status_t status );


