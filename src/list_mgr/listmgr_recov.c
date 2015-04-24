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
#include "Memory.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include <stdio.h>
#include <stdlib.h>

#define MAX_DB_FIELDS 64

/* table: id+... */
/* TODO: generate this list automatically */
/* /!\ it must be in the same order as in MAIN, ANNEX, ... */
#define BUILD_RECOV_LIST_FIELDS_NAMES THIS_PATH_FUNC"(NAMES.parent_id, NAMES.name) as relpath,owner,gr_name,size,last_mod,type,mode,status,stripe_count,stripe_size,pool_name,backendpath,link"
#define BUILD_RECOV_LIST_FIELDS ONE_PATH_FUNC"("MAIN_TABLE".id) as relpath,owner,gr_name,size,last_mod,type,mode,status,stripe_count,stripe_size,pool_name,backendpath,link"
#define GET_RECOV_LIST_FIELDS "relpath,owner,gr_name,size,last_mod,type,mode,status,stripe_count,stripe_size,pool_name,backendpath,link"
#define RECOV_FIELD_COUNT 13


/**
 * \retval DB_NOT_EXISTS if the recovery table does not exist
 */
static int expected_recov_status( lmgr_t * p_mgr, lmgr_recov_stat_t * p_stats )
{
    int  rc, i;
    result_handle_t result;
    char * status[5];

    /* test if a RECOVERY table already exist, and contains entries */
    rc = db_exec_sql_quiet( &p_mgr->conn, "SELECT status,type,COUNT(*),(size=0) as empty,SUM(size) FROM "RECOV_TABLE
                            " GROUP BY status,type,empty", &result );
    if (rc)
        return rc;

    /* @TODO manage dirs and symlinks differently */

    p_stats->total = 0;
    for (i = 0; i < RS_COUNT; i++ )
    {
        p_stats->status_count[i] = 0;
        p_stats->status_size[i] = 0;
    }

    while ( (rc = db_next_record( &p_mgr->conn, &result, status, 5 ))
            != DB_END_OF_LIST )
    {
        long long cnt;
        uint64_t sz;
        int isempty;

        if (rc)
            return rc;

        cnt = str2bigint( status[2] );
        if ( cnt == -1LL)
            return DB_INVALID_ARG;

        isempty = str2int(  status[3] );
        if ( isempty == -1)
            return DB_INVALID_ARG;

        sz = str2size(  status[4] );
        if ( sz == -1LL)
            return DB_INVALID_ARG;

        p_stats->total += cnt;

        if ( status[0] != NULL )
        {
            int st = str2int( status[0] );

            /* archived entries: file and (optionally) symlinks  */
            if (!strcasecmp(status[1], STR_TYPE_FILE))
            {
                if (isempty)
                {
                     p_stats->status_count[RS_FILE_EMPTY] += cnt;
                     p_stats->status_size[RS_FILE_EMPTY] += sz;
                }
                else
                {
                    switch (st)
                    {
                        case STATUS_NEW:
                            p_stats->status_count[RS_NOBACKUP] += cnt;
                            p_stats->status_size[RS_NOBACKUP] += sz;
                            break;
                        case STATUS_MODIFIED:
                        case STATUS_ARCHIVE_RUNNING:
                            p_stats->status_count[RS_FILE_DELTA] += cnt;
                            p_stats->status_size[RS_FILE_DELTA] += sz;
                            break;
                        case STATUS_SYNCHRO:
                        case STATUS_RELEASED:
                            p_stats->status_count[RS_FILE_OK] += cnt;
                            p_stats->status_size[RS_FILE_OK] += sz;
                            break;
                    }
                }
            }
            else if (!strcasecmp(status[1], STR_TYPE_LINK)
                     || !strcasecmp(status[1], STR_TYPE_DIR))
            {
                /* symlinks and dirs always recoverable from DB */
                p_stats->status_count[RS_NON_FILE] += cnt;
                p_stats->status_size[RS_NON_FILE] += sz;
            }
            else
            {
                /* non recoverable : special entry like fifo, blk, ... */
                p_stats->status_count[RS_NOBACKUP] += cnt;
                p_stats->status_size[RS_NOBACKUP] += sz;
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
    rc = db_exec_sql_quiet( &p_mgr->conn, "SELECT recov_status,COUNT(*),SUM(size) FROM "RECOV_TABLE
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
        long long cnt;
        uint64_t sz;
        if (rc)
            return rc;

        cnt = str2bigint( status[1] );
        if ( cnt == -1LL)
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
            p_stats->status_size[idx] = sz;
        }
    }

    db_result_free( &p_mgr->conn, &result );
    return 0;
}



/**
 *  Initialize a recovery process.
 *  \param p_filter[in] (optional) filter partial filesystem recovery
 *  \retval DB_SUCCESS the recovery process successfully started;
 *          the stats indicate the recovery states we can expect.
 *  \retval DB_ALREADY_EXISTS a recovery process already started
 *          and was not properly completed. stats indicate the current status.
 *  \retval error   another error occured.
 */
int ListMgr_RecovInit( lmgr_t * p_mgr, const lmgr_filter_t * p_filter, lmgr_recov_stat_t * p_stats )
{
    int  rc;
    db_value_t report_val;
    unsigned int nb;
    struct lmgr_report_t * report;
    report_field_descr_t report_count = {-1, REPORT_COUNT, SORT_NONE, FALSE, 0, FV_NULL};

    char           query[4096];
    char           filter_str[4096] = "";
    char          *filter_curr = filter_str;
    #define has_filters (filter_curr != filter_str)
    int            distinct = 0;

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
        rc = db_drop_component(&p_mgr->conn, DBOBJ_TABLE, RECOV_TABLE);
        if ( rc )
            return rc;
    }

    if ( p_filter )
    {
	 /* dummy vars */
        char           filter_dir_str[512] = "";
        unsigned int   filter_dir_index = 0;
        if (dir_filter(p_mgr, filter_dir_str, p_filter, &filter_dir_index, NULL) != FILTERDIR_NONE)
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "Directory filter not supported for recovery");
            return DB_NOT_SUPPORTED;
        }

        if (filter2str(p_mgr, filter_curr, p_filter, T_MAIN, FALSE, TRUE) > 0)
            filter_curr += strlen(filter_curr);

        if (annex_table && (filter2str(p_mgr, filter_curr, p_filter,
                                       T_ANNEX, has_filters, TRUE) > 0))
            filter_curr += strlen(filter_curr);

        if (filter2str( p_mgr, filter_curr, p_filter, T_DNAMES, has_filters, TRUE) > 0)
        {
            filter_curr += strlen(filter_curr);
            distinct = 1;
        }

        if (filter2str(p_mgr, filter_curr, p_filter, T_STRIPE_INFO, has_filters, TRUE) > 0)
            filter_curr += strlen(filter_curr);

        if (filter2str(p_mgr, filter_curr, p_filter, T_STRIPE_ITEMS, has_filters, TRUE) > 0)
        {
            filter_curr += strlen(filter_curr);
            distinct = 1;
        }
    }


    DisplayLog( LVL_EVENT, LISTMGR_TAG, "Populating "RECOV_TABLE" table (this can take a few minutes)..." );

    /* create the recovery table */
    if (distinct)
    {
        /* need to select only 1 instance of each object when joining with STRIPE_ITEMS or NAMES */
        strcpy(query, "CREATE TABLE "RECOV_TABLE
            " SELECT DISTINCT("MAIN_TABLE".id)," BUILD_RECOV_LIST_FIELDS_NAMES
            " FROM "MAIN_TABLE" LEFT JOIN "ANNEX_TABLE" ON "
            "("MAIN_TABLE".id = "ANNEX_TABLE".id)"
            " LEFT JOIN "DNAMES_TABLE" ON "
            "("MAIN_TABLE".id = "DNAMES_TABLE".id)"
            " LEFT JOIN "STRIPE_INFO_TABLE" ON "
            "("MAIN_TABLE".id = "STRIPE_INFO_TABLE".id)"
            " LEFT JOIN "STRIPE_ITEMS_TABLE" ON "
            "("MAIN_TABLE".id = "STRIPE_ITEMS_TABLE".id)");
    }
    else
    {
        strcpy(query, "CREATE TABLE "RECOV_TABLE
            " SELECT "MAIN_TABLE".id," BUILD_RECOV_LIST_FIELDS
            " FROM "MAIN_TABLE" LEFT JOIN "ANNEX_TABLE" ON "
            "("MAIN_TABLE".id = "ANNEX_TABLE".id)"
            " LEFT JOIN "STRIPE_INFO_TABLE" ON "
            "("MAIN_TABLE".id = "STRIPE_INFO_TABLE".id)");
    }

    if (has_filters)
    {
        strcat(query, " WHERE ");
        strcat(query, filter_str);
    }

    /* the whole function is not atomic as we try to preserve the progress
     * in case of DB engine failure. So we retry each step independently.
     */
retry1:
    rc = db_exec_sql(&p_mgr->conn, query, NULL);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry1;
    else if (rc)
        return rc;

    DisplayLog( LVL_EVENT, LISTMGR_TAG, "Building indexes on "RECOV_TABLE" table..." );

    /* create pk */
retry2:
    rc = db_exec_sql( &p_mgr->conn, "ALTER TABLE "RECOV_TABLE" ADD PRIMARY KEY (id)", NULL );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry2;
    else if (rc)
        return rc;

    /* add recov_status column */
retry3:
    rc = db_exec_sql( &p_mgr->conn, "ALTER TABLE "RECOV_TABLE" ADD COLUMN recov_status INTEGER", NULL );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry3;
    else if (rc)
        return rc;

    /* add index on status */
retry4:
    rc = db_exec_sql( &p_mgr->conn,
                      "CREATE INDEX recov_st_index ON "RECOV_TABLE"(recov_status)",
                      NULL );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry4;
    else if (rc)
        return rc;

    /* count entries of each status */
    expected_recov_status( p_mgr, p_stats );

    /* if there is a filter on OSTs, report distinct ids */
    if (distinct)
        report_count.report_type = REPORT_COUNT_DISTINCT;

    /* double check entry count before deleting entries */
    report = ListMgr_Report(p_mgr, &report_count, 1, NULL, p_filter, NULL);
    if (report == NULL)
        return DB_REQUEST_FAILED;

    nb = 1;
    rc = ListMgr_GetNextReportItem(report, &report_val, &nb, NULL);
    ListMgr_CloseReport(report);

    if (rc)
        return rc;


    if ( nb == 0 )
        return DB_REQUEST_FAILED;

    if ( report_val.value_u.val_biguint != p_stats->total )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "ERROR: recovery count (%llu) is different from entry count in main table (%lld): preserving entries",
                    p_stats->total,  report_val.value_u.val_biguint );
        return DB_REQUEST_FAILED;
    }

    /* clean previous DB content */

    return ListMgr_MassRemove( p_mgr, p_filter, NULL );
}

/**
 * Clear the recovery table.
 * /!\ all previously unrecovered entries will be lost
 */
int ListMgr_RecovReset( lmgr_t * p_mgr )
{
   DisplayLog( LVL_EVENT, LISTMGR_TAG, "Dropping any previous "RECOV_TABLE" table" );
   return db_drop_component(&p_mgr->conn, DBOBJ_TABLE, RECOV_TABLE);
}

/**
 *  Continue a recovery process (returns an iterator on entry list),
 *  possibly using the specified filter.
 *  \retval iterator must be release using ListMgr_CloseIterator()
 */
struct lmgr_iterator_t * ListMgr_RecovResume( lmgr_t * p_mgr,
                                              const char * dir_path,
                                              int retry, /* also retry previously erroneous entries */
                                              const lmgr_iter_opt_t * p_opt )
{
    char query[4096];
    char * curr;
    lmgr_iterator_t * it;
    int rc;

    strcpy( query, "SELECT id,recov_status,"GET_RECOV_LIST_FIELDS" FROM "RECOV_TABLE" WHERE " );
    curr = query + strlen(query);
    if ( retry )
        curr += sprintf( curr, "(recov_status IS NULL OR recov_status=%u)",
                         RS_ERROR );
    else
        curr += sprintf( curr, "recov_status IS NULL" );

    if ( dir_path )
    {
        char rel[RBH_PATH_MAX] = "";
        /* Recovery table contains path from DB (<root_id>/<rel_path>),
         * and dirpath is absolute. So convert it. */
        if (fullpath_attr2db(dir_path, rel))
            return NULL;
#ifdef _MYSQL
        /* MySQL is case insensitive.
         * To force case-sensitivity, use BINARY keyword. */
        curr += sprintf( curr, " AND relpath LIKE BINARY '%s/%%'", rel );
#else
        curr += sprintf( curr, " AND relpath LIKE '%s/%%'", rel );
#endif
    }

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

/**
 *  List entries by recovery status.
 *  \param st type of entries to be listed
 *  (done, failed, to be done, all)
 */
struct lmgr_iterator_t * ListMgr_RecovList( lmgr_t * p_mgr,recov_type_e st )
{
    char query[4096];
    char * curr;
    lmgr_iterator_t * it;
    int rc;

    strcpy( query, "SELECT id,recov_status,"GET_RECOV_LIST_FIELDS" FROM "RECOV_TABLE );
    curr = query + strlen(query);
    switch(st)
    {
        case RT_ALL:
            /* add no filter */
            break;
        case RT_TODO:
            strcpy(curr, " WHERE recov_status is NULL");
            break;
        case RT_DONE:
            sprintf(curr, " WHERE recov_status in (%u, %u, %u, %u, %u)",
                    RS_FILE_OK, RS_FILE_DELTA, RS_FILE_EMPTY, RS_NON_FILE, RS_NOBACKUP );
            break;
        case RT_FAILED:
            sprintf(curr, " WHERE recov_status=%u", RS_ERROR);
            break;
    }

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
                          attr_set_t * p_info,
                          recov_status_t * last_status )
{
    int            rc = 0;
    char          *result_tab[2+RECOV_FIELD_COUNT]; /* +2 for id and recov_status */
    DEF_PK(pk);
    int entry_disappeared = FALSE;

    do
    {
        entry_disappeared = FALSE;

        rc = db_next_record( &p_iter->p_mgr->conn, &p_iter->select_result,
                             result_tab, RECOV_FIELD_COUNT+2 );

        if ( rc )
            return rc;
        if ( result_tab[0] == NULL ) /* no id? */
            return DB_REQUEST_FAILED;

        if ( sscanf( result_tab[0], SPK, PTR_PK(pk) ) != 1 )
            return DB_REQUEST_FAILED;

        if ( result_tab[1] == NULL ) { /* no status */
            if (last_status)
                *last_status = -1;
        }
        else if (last_status)
            *last_status = str2int(result_tab[1]);

        /* retrieve entry id (except validator) */
        rc = pk2entry_id( p_iter->p_mgr, pk, p_id );

        /* /!\ If the entry disappeared from DB, we must go to next record */
        if ( rc == DB_NOT_EXISTS )
            entry_disappeared = TRUE;
        else if ( rc )
            return rc;

    }
    while ( entry_disappeared );        /* goto next record if entry desappered */

    return result2attrset( T_RECOV, result_tab + 2, RECOV_FIELD_COUNT, p_info );
}

int ListMgr_RecovComplete( lmgr_t * p_mgr, lmgr_recov_stat_t * p_stats )
{
    long long int diff;
    int rc;

    /* Check there is no more unprocessed entries */
    rc = ListMgr_RecovStatus( p_mgr, p_stats );
    if (rc)
        return rc;

    diff = p_stats->total - p_stats->status_count[RS_FILE_OK] - p_stats->status_count[RS_FILE_DELTA]
            - p_stats->status_count[RS_FILE_EMPTY] - p_stats->status_count[RS_NON_FILE]
           - p_stats->status_count[RS_NOBACKUP] - p_stats->status_count[RS_ERROR];
    if (diff > 0)
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Cannot complete recovery: there are still %lld unprocessed files",
                    diff );
        return DB_NOT_ALLOWED;
    }
    /* clear all */
    return ListMgr_RecovReset( p_mgr );
}

int ListMgr_RecovSetState( lmgr_t * p_mgr, const entry_id_t * p_id,
                           recov_status_t status )
{
    char query[4096];
    DEF_PK(pk);

    entry_id2pk(p_id, PTR_PK(pk));

    sprintf( query, "UPDATE "RECOV_TABLE" SET recov_status=%u WHERE id="DPK,
             status, pk );

    /* execute request */
    return db_exec_sql( &p_mgr->conn, query, NULL );
}


