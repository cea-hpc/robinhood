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
#include "database.h"
#include "RobinhoodLogs.h"
#include <stdio.h>
#include <unistd.h>

static int sqlite_error_convert( int err )
{
    switch ( err )
    {
    case SQLITE_OK:
        return DB_SUCCESS;
    case SQLITE_NOTFOUND:
        return DB_NOT_EXISTS;
    case SQLITE_CONSTRAINT: /* unique constraint violation */
        return DB_ALREADY_EXISTS;
    default:
        DisplayLog( LVL_MAJOR, LISTMGR_TAG,
                    "Unhandled error %d: default conversion to DB_REQUEST_FAILED", err );
        return DB_REQUEST_FAILED;
    }
}

static int db_is_busy_err( int rc )
{
    /* sometimes, SQLITE_CANTOPEN meens the db is busy (locked)... */
    return ( rc == SQLITE_BUSY ) || ( rc == SQLITE_CANTOPEN );
}

static int set_cache_size( sqlite3 * conn )
{
    int            rc;
    char          *errmsg;

    rc = sqlite3_exec( conn, "PRAGMA cache_size=1000000", NULL, NULL, &errmsg );
    if ( rc != SQLITE_OK )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "SQL error: %s", errmsg );
        sqlite3_free( errmsg );
        return DB_REQUEST_FAILED;
    }

    return DB_SUCCESS;
}


/* create client connection */
int db_connect( db_conn_t * conn )
{
    int            rc;

    /* Connect to database */
    rc = sqlite3_open( lmgr_config.db_config.filepath, conn );
    if ( rc != 0 )
    {
        if ( *conn )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to connect to SQLite DB (file %s): Error: %s",
                        lmgr_config.db_config.filepath, sqlite3_errmsg( *conn ) );
        }
        else
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to connect to SQLite DB (file %s): Error: %d",
                        lmgr_config.db_config.filepath, rc );
        }
        return DB_CONNECT_FAILED;
    }

    DisplayLog( LVL_FULL, LISTMGR_TAG, "Logged on to database successfully" );

    set_cache_size( *conn );

    return DB_SUCCESS;
}

/* retrieve error message */
char          *db_errmsg( db_conn_t * conn, char *errmsg, unsigned int buflen )
{
    if ( *conn == NULL )
    {
        strcpy( errmsg, "Connection not initialized" );
        return errmsg;
    }

    if ( strlen( sqlite3_errmsg( *conn ) ) + 1 > buflen )
        rh_strncpy(errmsg, sqlite3_errmsg(*conn), buflen);
    else
        strcpy( errmsg, sqlite3_errmsg( *conn ) );

    return errmsg;
}

int db_exec_sql( db_conn_t * conn, const char *query, result_handle_t * p_result )
{
    int            rc;
    char          *errmsg = NULL;

#ifdef _DEBUG_DB
    DisplayLog( LVL_FULL, LISTMGR_TAG, "SQL query: %s", query );
#endif

    if ( !p_result )
    {
        do
        {
            rc = sqlite3_exec( *conn, query, NULL, NULL, &errmsg );

            if ( db_is_busy_err( rc ) )
                usleep( lmgr_config.db_config.retry_delay_microsec );

        }
        while ( db_is_busy_err( rc ) );

        if ( rc != SQLITE_OK )
        {
            DisplayLog(  LVL_DEBUG, LISTMGR_TAG,
                        "SQLite command failed (%d): %s: %s", rc,
                        errmsg ? errmsg : sqlite3_errmsg( *conn ), query );
            if ( errmsg )
                sqlite3_free( errmsg );
            return sqlite_error_convert( rc );
        }
    }
    else
    {
        p_result->curr_row = 0;
        p_result->result_array = 0;

        do
        {
            rc = sqlite3_get_table( *conn, query, &p_result->result_array,
                                    &p_result->nb_rows, &p_result->nb_cols, &errmsg );

            if ( db_is_busy_err( rc ) )
                usleep( lmgr_config.db_config.retry_delay_microsec );

        }
        while ( db_is_busy_err( rc ) );


        if ( rc != SQLITE_OK )
        {
            DisplayLog( LVL_DEBUG, LISTMGR_TAG,
                        "SQLite command failed (%d): %s: %s", rc,
                        errmsg ? errmsg : sqlite3_errmsg( *conn ), query );
            if ( errmsg )
                sqlite3_free( errmsg );
            if ( p_result->result_array )
                sqlite3_free_table( p_result->result_array );
            return sqlite_error_convert( rc );
        }
    }

    return DB_SUCCESS;
}

int db_exec_sql_quiet( db_conn_t * conn, const char *query, result_handle_t * p_result )
{
    return db_exec_sql( conn, query, p_result);
}

/* get the next record from result */
int db_next_record( db_conn_t * conn,
                    result_handle_t * p_result, char *outtab[], unsigned int outtabsize )
{
    int            i;

    if ( p_result->curr_row >= p_result->nb_rows )
        return DB_END_OF_LIST;

    if ( p_result->nb_cols > outtabsize )
        return DB_BUFFER_TOO_SMALL;

    for ( i = 0; i < p_result->nb_cols; i++ )
    {
        /* /!\ in sqlite, the request retuns columns header as row 0 !!! */
        outtab[i] = p_result->result_array[( p_result->curr_row + 1 ) * p_result->nb_cols + i];
    }
    p_result->curr_row++;

    return DB_SUCCESS;
}


int db_result_free( db_conn_t * conn, result_handle_t * p_result )
{
    if ( p_result->result_array )
        sqlite3_free_table( p_result->result_array );
    memset( p_result, 0, sizeof( result_handle_t ) );

    return DB_SUCCESS;
}




/* retrieve number of records in result */
int db_result_nb_records( db_conn_t * conn, result_handle_t * p_result )
{
    return p_result->nb_rows;
}

int db_close_conn( db_conn_t * conn )
{
    /* XXX Ensure there is no pending transactions? */
    sqlite3_close( *conn );
    return DB_SUCCESS;
}


int db_list_table_fields( db_conn_t * conn, const char *table,
                          char **outtab,
                          unsigned int outtabsize, char *inbuffer, unsigned int inbuffersize )
{
    char           request[4096];
    char         **result = NULL;
    int            rows, cols;
    char          *errmsg = NULL;
    int            i, rc, curr_output;
    char          *curr_ptr = inbuffer;

    sprintf( request, "PRAGMA table_info(%s)", table );

    rc = sqlite3_get_table( *conn, request, &result, &rows, &cols, &errmsg );

    if ( rc != SQLITE_OK )
    {
        DisplayLog( LVL_DEBUG, LISTMGR_TAG, "SQLite command failed (%d):  "
                    "%s: %s", rc, errmsg ? errmsg : sqlite3_errmsg( *conn ),
                    request );
        if ( errmsg )
            sqlite3_free( errmsg );
        if ( result )
            sqlite3_free_table( result );
        return sqlite_error_convert( rc );
    }
    else if ( rows == 0 )
    {
        if ( errmsg )
            sqlite3_free( errmsg );
        if ( result )
            sqlite3_free_table( result );
        return DB_NOT_EXISTS;
    }

    /* init ouput tab */
    for ( i = 0; i < outtabsize; i++ )
        outtab[i] = NULL;

    curr_output = 0;

    /* starting at 1 because first raw contains headers */
    for ( i = 1; i < rows + 1; i++ )
    {
        strcpy( curr_ptr, result[1 + i * cols] );
        outtab[curr_output] = curr_ptr;
        curr_ptr += strlen( curr_ptr ) + 1;
        curr_output++;
    }

    sqlite3_free_table( result );

    return DB_SUCCESS;

}

unsigned long long db_last_id( db_conn_t * conn )
{
    return sqlite3_last_insert_rowid( *conn );;
}

/* escape a string in a SQL request */
void db_escape_string( db_conn_t * conn, char * str_out, size_t out_size, const char * str_in )
{
    /* using slqite3_snprintf with "%q" format, to escape strings */
    sqlite3_snprintf( out_size, str_out, str_in );
}
