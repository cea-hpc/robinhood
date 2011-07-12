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
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "Memory.h"
#include <stdio.h>
#include <unistd.h>
/* mysql includes */
#include <mysqld_error.h>
#include <errmsg.h>

#define _DEBUG_DB

static int mysql_error_convert( int err )
{
    switch ( err )
    {
    case 0:
        return DB_SUCCESS;
    case ER_NO_SUCH_TABLE:
        return DB_NOT_EXISTS;
    case ER_DUP_ENTRY:
        return DB_ALREADY_EXISTS;
    case ER_TRG_DOES_NOT_EXIST:
        return DB_TRG_NOT_EXISTS;
    case ER_BAD_FIELD_ERROR:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Invalid DB field" );
        return DB_INVALID_ARG;
    case ER_PARSE_ERROR:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "SQL request parse error" );
        return DB_REQUEST_FAILED;

        /* connection relative errors */

        /* In case of a deconnection, mysql_stmt_fetch returns this error CR_COMMANDS_OUT_OF_SYNC.
         * which is actually not very appropriate... */
    case CR_COMMANDS_OUT_OF_SYNC:
        /* when connection is lost, statements are no more valid */
    case ER_UNKNOWN_STMT_HANDLER:

        /* It also returns ER_UNKNOWN_ERROR... In this case, we treat it as a disconnection anyway,
         * to give a chance to the client to clean its internal state.
         */
    case ER_UNKNOWN_ERROR:

        /* These are really connection errors: */
    case ER_SERVER_SHUTDOWN:
    case CR_CONNECTION_ERROR:
    case CR_SERVER_GONE_ERROR:
    case CR_SERVER_LOST:
    case CR_CONN_HOST_ERROR:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "DB connection error %d", err );
        return DB_CONNECT_FAILED;

    default:
        DisplayLog( LVL_MAJOR, LISTMGR_TAG,
                    "Unhandled error %d: default conversion to DB_REQUEST_FAILED", err );
        return DB_REQUEST_FAILED;
    }
}

static int is_retryable( int sql_err )
{
    return ( mysql_error_convert( sql_err ) == DB_CONNECT_FAILED );
}


/* create client connection */
int db_connect( db_conn_t * conn )
{
    my_bool        reconnect = 1;

    /* Connect to database */
    if ( mysql_init( conn ) == NULL )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "ERROR: failed to create MySQL client struct" );
        return DB_CONNECT_FAILED;
    }

#if ( MYSQL_VERSION_ID >= 50013 )
    /* set auto-reconnect option */
    mysql_options( conn, MYSQL_OPT_RECONNECT, &reconnect );
#else
    /* older version */
    conn->reconnect = 1;
#endif

    /* connect to server */
    if ( !mysql_real_connect
         ( conn, lmgr_config.db_config.server, lmgr_config.db_config.user,
           lmgr_config.db_config.password, lmgr_config.db_config.db, 
           lmgr_config.db_config.port,
           EMPTY_STRING(lmgr_config.db_config.socket)?
                        NULL:lmgr_config.db_config.socket,
           0 ) )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Failed to connect to MySQL: Error: %s", mysql_error( conn ) );
        return DB_CONNECT_FAILED;
    }

    /* Note [MySQL reference guide]: mysql_real_connect()  incorrectly reset
     * the MYSQL_OPT_RECONNECT  option to its default value before MySQL 5.1.6.
     * Therefore, prior to that version, if you want reconnect to be enabled for
     * each connection, you must call mysql_options() with the MYSQL_OPT_RECONNECT
     * option after each call to mysql_real_connect().
     */
#if (MYSQL_VERSION_ID >= 50013) && (MYSQL_VERSION_ID < 50106)
    /* reset auto-reconnect option */
    mysql_options( conn, MYSQL_OPT_RECONNECT, &reconnect );
#endif

    DisplayLog( LVL_FULL, LISTMGR_TAG, "Logged on to database '%s' sucessfully", lmgr_config.db_config.db );
    return DB_SUCCESS;
}

int db_close_conn( db_conn_t * conn )
{
    /* XXX Ensure there is no pending transactions? */
    mysql_close( conn );

    DisplayLog( LVL_FULL, LISTMGR_TAG, "Database connection closed" );

    return DB_SUCCESS;
}


/* retrieve error message */
char          *db_errmsg( db_conn_t * conn, char *errmsg, unsigned int buflen )
{
    if ( strlen( mysql_error( conn ) ) + 1 > buflen )
        strncpy( errmsg, mysql_error( conn ), buflen );
    else
        strcpy( errmsg, mysql_error( conn ) );

    return errmsg;
}

static int _db_exec_sql( db_conn_t * conn, const char *query,
                         result_handle_t * p_result,
                         int quiet )
{
    int            rc;
    unsigned int   retry = lmgr_config.connect_retry_min;
#ifdef _DEBUG_DB
    DisplayLog( LVL_FULL, LISTMGR_TAG, "SQL query: %s", query );
#endif

    do
    {
        rc = mysql_real_query( conn, query, strlen( query ) );

        if ( rc && is_retryable( mysql_errno( conn ) ) )
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG,
                        "Connection to database lost in %s()... Retrying in %u sec.", __FUNCTION__,
                        retry );
            rh_sleep( retry );
            retry *= 2;
            if ( retry > lmgr_config.connect_retry_max )
                retry = lmgr_config.connect_retry_max;
        }
        else
            break;

    }
    while ( 1 );

    if ( rc )
    {
        if (mysql_errno( conn ) == ER_DUP_ENTRY)
        {
            DisplayLog( quiet?LVL_DEBUG:LVL_EVENT, LISTMGR_TAG,
                        "A database record already exists for this entry: '%s' (%s)",
                        query, mysql_error(conn) );
        }
        else if (mysql_errno( conn ) == ER_TRG_DOES_NOT_EXIST)
        {
            DisplayLog( quiet?LVL_DEBUG:LVL_EVENT, LISTMGR_TAG,
                        "Trigger does not exist: '%s' (%s)",
                        query, mysql_error(conn) );
        }
        else if (mysql_errno( conn ) == ER_NO_SUCH_TABLE)
            DisplayLog( quiet?LVL_DEBUG:LVL_EVENT, LISTMGR_TAG,
                        "Table does not exist: '%s' (%s)",
                        query, mysql_error(conn) );
        else
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d executing query '%s': %s",
                        rc, query, mysql_error(conn) );

        return mysql_error_convert( mysql_errno( conn ) );
    }
    else
    {
        /* fetch results to the client */
        if ( p_result )
            *p_result = mysql_store_result( conn );

        return DB_SUCCESS;
    }
}

int db_exec_sql_quiet( db_conn_t * conn, const char *query, result_handle_t * p_result )
{
    return _db_exec_sql(conn, query, p_result, TRUE );
}

int db_exec_sql( db_conn_t * conn, const char *query, result_handle_t * p_result )
{
    return _db_exec_sql(conn, query, p_result, FALSE );
}


/* free result resources */
int db_result_free( db_conn_t * conn, result_handle_t * p_result )
{
    if ( *p_result )
        mysql_free_result( *p_result );
    return DB_SUCCESS;
}

/* get the next record from result */
int db_next_record( db_conn_t * conn, result_handle_t * p_result,
                    char *outtab[], unsigned int outtabsize )
{
    int            i;
    MYSQL_ROW      row;
    unsigned int   nb_fields;

    /* init ouput tab */
    for ( i = 0; i < outtabsize; i++ )
        outtab[i] = NULL;

    if ( !( row = mysql_fetch_row( *p_result ) ) )
        return DB_END_OF_LIST;

    nb_fields = mysql_num_fields( *p_result );

    for ( i = 0; ( i < outtabsize ) && ( i < nb_fields ); i++ )
        outtab[i] = row[i];

    if ( nb_fields > outtabsize )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Output array too small: size = %u, num_fields = %u", outtabsize, nb_fields );
        return DB_BUFFER_TOO_SMALL;
    }

    return DB_SUCCESS;

}


/* retrieve number of records in result */
int db_result_nb_records( db_conn_t * conn, result_handle_t * p_result )
{
    return mysql_num_rows( *p_result );
}



int db_list_table_fields( db_conn_t * conn, const char *table,
                          char **outtab,
                          unsigned int outtabsize, char *inbuffer, unsigned int inbuffersize )
{
    char           request[4096];
    MYSQL_RES     *result;
    MYSQL_ROW      row;
    int            i, rc, curr_output;
    char          *curr_ptr = inbuffer;

    sprintf( request, "SHOW COLUMNS FROM %s", table );
    rc = db_exec_sql_quiet( conn, request, &result );

    if ( rc )
        return rc;

    if ( !result )
    {
        DisplayLog( LVL_DEBUG, LISTMGR_TAG, "%s does not exist", table );
        return DB_NOT_EXISTS;
    }

    /* init ouput tab */
    for ( i = 0; i < outtabsize; i++ )
        outtab[i] = NULL;

    curr_output = 0;
    while ( ( row = mysql_fetch_row( result ) ) )
    {
        strcpy( curr_ptr, row[0] );
        outtab[curr_output] = curr_ptr;
        curr_ptr += strlen( curr_ptr ) + 1;
        curr_output++;
    }
    mysql_free_result( result );

    return DB_SUCCESS;

}

unsigned long long db_last_id( db_conn_t * conn )
{
    return mysql_insert_id( conn );
}


/* escape a string in a SQL request */
void db_escape_string( db_conn_t * conn, char * str_out, size_t out_size, const char * str_in )
{
        /* escape special characters in value */
        mysql_real_escape_string( conn, str_out, str_in, strlen( str_in ) );
}

/* remove a trigger */
int db_drop_trigger( db_conn_t * conn, const char *name )
{
#ifdef _MYSQL5
    
    char query[1024];
    if( mysql_get_server_version(conn) < 50032 )
    {
        sprintf( query, "DROP TRIGGER %s ", name );
        return _db_exec_sql( conn, query, NULL, TRUE );
    }
    else
    {
        sprintf( query, "DROP TRIGGER IF EXISTS %s ", name );
        return _db_exec_sql( conn, query, NULL, FALSE );
    }

#else

    DisplayLog( LVL_CRIT, LISTMGR_TAG, "Trigger %s was not dropped: "
                "you should upgrade to MYSQL 5 to use triggers", name  );
    return DB_REQUEST_FAILED;

#endif
}

/* create a trigger */
int db_create_trigger( db_conn_t * conn, const char *name, const char *event,
                               const char *table, const char *body )
{
#ifdef _MYSQL5

    char query[4096];
    sprintf( query, "CREATE TRIGGER %s %s ON %s FOR EACH ROW "
                    "BEGIN %s END", name, event, table, body );
    return _db_exec_sql( conn, query, NULL, FALSE );

#else

    DisplayLog( LVL_CRIT, LISTMGR_TAG, "Trigger %s was not created: "
                "you should upgrade to MYSQL 5 to use triggers", name  );
    return DB_REQUEST_FAILED;

#endif
}
