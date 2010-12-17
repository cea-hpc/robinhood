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
           lmgr_config.db_config.password, lmgr_config.db_config.db, 0, NULL, 0 ) )
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

    DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Logged on to database sucessfully" );
    return DB_SUCCESS;
}

int db_close_conn( db_conn_t * conn )
{
    /* XXX Ensure there is no pending transactions? */
    mysql_close( conn );

    DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Database connection closed" );

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

int db_exec_sql( db_conn_t * conn, const char *query, result_handle_t * p_result )
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
            DisplayLog( LVL_EVENT, LISTMGR_TAG, "A database record already exists for this entry: '%s' (%s)",
                         query, mysql_error(conn) );
        else if (mysql_errno( conn ) == ER_NO_SUCH_TABLE)
            DisplayLog( LVL_EVENT, LISTMGR_TAG, "Table does not exist: '%s' (%s)",
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
    rc = db_exec_sql( conn, request, &result );

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


/* -------------------- prepared statements management ---------------- */

/* create a prepared statement */
prep_stmt_t db_create_prepared( db_conn_t * conn, const char *query )
{
    int            rc;
    unsigned int   retry;
    prep_stmt_t    prep = ( prep_stmt_t ) MemAlloc( sizeof( struct prep_stmt_data_t ) );

    if ( prep == NULL )
        return NULL;

    /* init the structure */
    prep->input = NULL;
    prep->nb_in = 0;
    prep->output = NULL;
    prep->nb_out = 0;
    prep->strlen_array_in = NULL;
    prep->strlen_array_out = NULL;
    prep->isnull_array_out = NULL;
    prep->error_array_out = NULL;

    /* retry loop */
    retry = lmgr_config.connect_retry_min;
    do
    {
        /* First create the prepared statement */
        prep->stmt = mysql_stmt_init( conn );

        /* retry if connection to server failed */
        if ( ( prep->stmt == NULL ) && is_retryable( mysql_errno( conn ) ) )
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

    if ( !prep->stmt )
        goto free_struct;

    /* retry loop */
    retry = lmgr_config.connect_retry_min;

    do
    {
        /* prepare the request */
        rc = mysql_stmt_prepare( prep->stmt, query, strlen( query ) );

        if ( rc && is_retryable( mysql_stmt_errno( prep->stmt ) ) )
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
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Failed to create prepared statement: Error: %s (query='%s')",
                    mysql_stmt_error( prep->stmt ), query );
        goto free_stmt;
    }

    /* retrieve the number of input/output fields */
    prep->nb_in = mysql_stmt_param_count( prep->stmt );
    prep->nb_out = mysql_stmt_field_count( prep->stmt );

    /* allocate INPUT bindings */
    if ( prep->nb_in > 0 )
    {
        prep->input = ( MYSQL_BIND * ) MemCalloc( prep->nb_in, sizeof( MYSQL_BIND ) );
        if ( prep->input == NULL )
            goto free_stmt;

        prep->strlen_array_in =
            ( unsigned long * ) MemCalloc( prep->nb_in, sizeof( unsigned long ) );
        if ( prep->strlen_array_in == NULL )
            goto free_bind_in;

    }

    /* allocate OUTPUT bindings */
    if ( prep->nb_out > 0 )
    {
        prep->output = ( MYSQL_BIND * ) MemCalloc( prep->nb_out, sizeof( MYSQL_BIND ) );
        if ( prep->output == NULL )
            goto free_strlen_in;

        prep->strlen_array_out =
            ( unsigned long * ) MemCalloc( prep->nb_out, sizeof( unsigned long ) );
        if ( prep->strlen_array_out == NULL )
            goto free_bind_out;

        prep->isnull_array_out = ( my_bool * ) MemCalloc( prep->nb_out, sizeof( my_bool ) );
        if ( prep->isnull_array_out == NULL )
            goto free_strlen_out;

        prep->error_array_out = ( my_bool * ) MemCalloc( prep->nb_out, sizeof( my_bool ) );
        if ( prep->error_array_out == NULL )
            goto free_isnull_out;

    }

    /* done */
    return prep;


/* Error handlers */
  free_isnull_out:
    if ( prep->isnull_array_out != NULL )
        MemFree( prep->isnull_array_out );

  free_strlen_out:
    if ( prep->strlen_array_out != NULL )
        MemFree( prep->strlen_array_out );

  free_bind_out:
    if ( prep->output != NULL )
        MemFree( prep->output );

  free_strlen_in:
    if ( prep->strlen_array_in != NULL )
        MemFree( prep->strlen_array_in );

  free_bind_in:
    if ( prep->input != NULL )
        MemFree( prep->input );

  free_stmt:
    mysql_stmt_close( prep->stmt );

  free_struct:
    MemFree( prep );
    return NULL;
}

/* bind a param to a prepared statement */
int db_bind_param( prep_stmt_t prep, unsigned int arg_index, db_type_t type,
                   void *p_value, size_t size )
{
    switch ( type )
    {
    case DB_TEXT:
        prep->input[arg_index].buffer_type = MYSQL_TYPE_STRING;
        prep->input[arg_index].buffer = ( char * ) p_value;
        prep->input[arg_index].buffer_length = size;
        prep->input[arg_index].is_null = NULL;  /* never */
        prep->strlen_array_in[arg_index] = strlen( ( char * ) p_value );
        prep->input[arg_index].length = &( prep->strlen_array_in[arg_index] );
        break;
    case DB_INT:
        prep->input[arg_index].buffer_type = MYSQL_TYPE_LONG;
        prep->input[arg_index].buffer = ( char * ) p_value;
        prep->input[arg_index].is_null = ( my_bool * ) 0;       /* never */
        prep->input[arg_index].is_unsigned = 0;
        prep->input[arg_index].length = NULL;
        break;
    case DB_UINT:
        prep->input[arg_index].buffer_type = MYSQL_TYPE_LONG;
        prep->input[arg_index].buffer = ( char * ) p_value;
        prep->input[arg_index].is_null = ( my_bool * ) 0;       /* never */
        prep->input[arg_index].is_unsigned = 1;
        prep->input[arg_index].length = NULL;
        break;
    case DB_BIGINT:
        prep->input[arg_index].buffer_type = MYSQL_TYPE_LONGLONG;
        prep->input[arg_index].buffer = ( char * ) p_value;
        prep->input[arg_index].is_null = ( my_bool * ) 0;       /* never */
        prep->input[arg_index].is_unsigned = 0;
        prep->input[arg_index].length = NULL;
        break;
    case DB_BIGUINT:
        prep->input[arg_index].buffer_type = MYSQL_TYPE_LONGLONG;
        prep->input[arg_index].buffer = ( char * ) p_value;
        prep->input[arg_index].is_null = ( my_bool * ) 0;       /* never */
        prep->input[arg_index].is_unsigned = 1;
        prep->input[arg_index].length = NULL;
        break;
    case DB_BOOL:
        prep->input[arg_index].buffer_type = MYSQL_TYPE_LONG;
        prep->input[arg_index].buffer = ( char * ) p_value;
        prep->input[arg_index].is_null = ( my_bool * ) 0;       /* never */
        prep->input[arg_index].is_unsigned = 0;
        prep->input[arg_index].length = NULL;
        break;
    case DB_STRIPE_INFO:
    case DB_STRIPE_ITEMS:
    default:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Unexpected type in %s: %d", __FUNCTION__, type );
        return DB_INVALID_ARG;
    }

    return DB_SUCCESS;
}

/* bind a param to a prepared statement */
int db_bind_params( prep_stmt_t stmt, db_type_t * type, /* array of expected types */
                    void **p_value,              /* array of target buffers */
                    size_t * size,               /* array of buffer sizes (for strings) */
                    unsigned int intabsize,      /* size of input array */
                    int buff_changed )           /* did output buffer changed ? */
{
    int            i, rc;
    for ( i = 0; i < intabsize; i++ )
    {
        if ( buff_changed )
        {
            rc = db_bind_param( stmt, i, type[i], p_value[i], ( size ? size[i] : 0 ) );
            if ( rc )
                return rc;
        }
        else if ( type[i] == DB_TEXT )
            /* only update string length */
            stmt->strlen_array_in[i] = strlen( ( char * ) p_value[i] );
    }

    if ( buff_changed )
    {
        if ( mysql_stmt_bind_param( stmt->stmt, stmt->input ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "mysql_stmt_bind_param() failed: %s",
                        mysql_stmt_error( stmt->stmt ) );
            return mysql_error_convert( mysql_stmt_errno( stmt->stmt ) );
        }
    }

    return DB_SUCCESS;
}


/**
 * Execute a prepared statement.
 * @param fetch_all indicates if all results are to be fetched once on client.
 */
int db_exec_prepared( prep_stmt_t stmt, int fetch_all )
{
    int            rc;

    rc = mysql_stmt_execute( stmt->stmt );

    if ( rc )
        return mysql_error_convert( mysql_stmt_errno( stmt->stmt ) );

    /* all results are to be fetched once on client? */
    if ( fetch_all )
        mysql_stmt_store_result( stmt->stmt );

    /* OK done */
    return DB_SUCCESS;

}

/* retrieve next record for prepared statement.
 */
int db_next_prepared_record( prep_stmt_t stmt, db_type_t * type,        /* array of expected types */
                             void **p_value,     /* array of target buffers */
                             size_t * size,      /* array of buffer sizes (for strings) */
                             unsigned int outtabsize,   /* size of output array */
                             int buff_changed )  /* did output buffer changed ? */
{
    unsigned int   i;
    int            rc;

    if ( outtabsize < stmt->nb_out )
        return DB_BUFFER_TOO_SMALL;

    if ( buff_changed )
    {
        /* bind output values if they changed */
        for ( i = 0; i < outtabsize; i++ )
        {
            switch ( type[i] )
            {
            case DB_TEXT:
                stmt->output[i].buffer_type = MYSQL_TYPE_STRING;
                stmt->output[i].buffer = ( char * ) p_value[i];
                stmt->output[i].buffer_length = size[i];
                stmt->output[i].is_null = &( stmt->isnull_array_out[i] );
                stmt->output[i].length = &( stmt->strlen_array_out[i] );
#ifdef _MYSQL5
                stmt->output[i].error = &( stmt->error_array_out[i] );
#endif
                break;
            case DB_INT:
                stmt->output[i].buffer_type = MYSQL_TYPE_LONG;
                stmt->output[i].buffer = ( char * ) p_value[i];
                stmt->output[i].is_unsigned = 0;
                stmt->output[i].is_null = &( stmt->isnull_array_out[i] );
                stmt->output[i].length = &( stmt->strlen_array_out[i] );
#ifdef _MYSQL5
                stmt->output[i].error = &( stmt->error_array_out[i] );
#endif
                break;
            case DB_UINT:
                stmt->output[i].buffer_type = MYSQL_TYPE_LONG;
                stmt->output[i].buffer = ( char * ) p_value[i];
                stmt->output[i].is_unsigned = 1;
                stmt->output[i].is_null = &( stmt->isnull_array_out[i] );
                stmt->output[i].length = &( stmt->strlen_array_out[i] );
#ifdef _MYSQL5
                stmt->output[i].error = &( stmt->error_array_out[i] );
#endif
                break;
            case DB_BIGINT:
                stmt->output[i].buffer_type = MYSQL_TYPE_LONGLONG;
                stmt->output[i].buffer = ( char * ) p_value[i];
                stmt->output[i].is_unsigned = 0;
                stmt->output[i].is_null = &( stmt->isnull_array_out[i] );
                stmt->output[i].length = &( stmt->strlen_array_out[i] );
#ifdef _MYSQL5
                stmt->output[i].error = &( stmt->error_array_out[i] );
#endif
                break;
            case DB_BIGUINT:
                stmt->output[i].buffer_type = MYSQL_TYPE_LONGLONG;
                stmt->output[i].buffer = ( char * ) p_value[i];
                stmt->output[i].is_unsigned = 1;
                stmt->output[i].is_null = &( stmt->isnull_array_out[i] );
                stmt->output[i].length = &( stmt->strlen_array_out[i] );
#ifdef _MYSQL5
                stmt->output[i].error = &( stmt->error_array_out[i] );
#endif
                break;
            case DB_BOOL:
                stmt->output[i].buffer_type = MYSQL_TYPE_LONG;
                stmt->output[i].buffer = ( char * ) p_value[i];
                stmt->output[i].is_null = &( stmt->isnull_array_out[i] );
                stmt->output[i].length = &( stmt->strlen_array_out[i] );
#ifdef _MYSQL5
                stmt->output[i].error = &( stmt->error_array_out[i] );
#endif
                break;
            case DB_STRIPE_INFO:
            case DB_STRIPE_ITEMS:
            default:
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Unexpected type in %s: %d", __FUNCTION__, type[i] );
                return DB_INVALID_ARG;
            }
        }                       /* for */

        /* Bind the result buffers */
        if ( mysql_stmt_bind_result( stmt->stmt, stmt->output ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "mysql_stmt_bind_result() failed: %s",
                        mysql_stmt_error( stmt->stmt ) );
            return mysql_error_convert( mysql_stmt_errno( stmt->stmt ) );
        }
    }                           /* buffer changed */

    /* Fetch results */
    rc = mysql_stmt_fetch( stmt->stmt );

    if ( rc == MYSQL_NO_DATA )
        return DB_END_OF_LIST;
    else if ( rc )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "mysql_stmt_fetch() failed: %s",
                    mysql_stmt_error( stmt->stmt ) );
        return mysql_error_convert( mysql_stmt_errno( stmt->stmt ) );
    }

    return DB_SUCCESS;
}

/* check if a result is null
 */
int db_is_null_result( prep_stmt_t stmt, unsigned int index )
{
    return stmt->isnull_array_out[index];
}


int db_clean_prepared( prep_stmt_t stmt )
{
    /* clean buffers */
    mysql_stmt_free_result( stmt->stmt );
    if ( stmt->input )
        memset( stmt->input, 0, sizeof( MYSQL_BIND ) * stmt->nb_in );
    if ( stmt->output )
        memset( stmt->output, 0, sizeof( MYSQL_BIND ) * stmt->nb_out );
    if ( stmt->strlen_array_in )
        memset( stmt->strlen_array_in, 0, sizeof( unsigned long ) * stmt->nb_in );
    if ( stmt->strlen_array_out )
        memset( stmt->strlen_array_out, 0, sizeof( unsigned long ) * stmt->nb_out );
    if ( stmt->isnull_array_out )
        memset( stmt->isnull_array_out, 0, sizeof( my_bool ) * stmt->nb_out );
    if ( stmt->error_array_out )
        memset( stmt->error_array_out, 0, sizeof( my_bool ) * stmt->nb_out );

    return DB_SUCCESS;
}

/* destroy a prepared statement */
void db_destroy_prepared( prep_stmt_t prep )
{
    if ( prep )
    {
        /* init the structure */
        if ( prep->input )
            MemFree( prep->input );
        if ( prep->output )
            MemFree( prep->output );
        if ( prep->strlen_array_in )
            MemFree( prep->strlen_array_in );
        if ( prep->strlen_array_out )
            MemFree( prep->strlen_array_out );
        if ( prep->isnull_array_out )
            MemFree( prep->isnull_array_out );
        if ( prep->error_array_out )
            MemFree( prep->error_array_out );

        mysql_stmt_close( prep->stmt );

        MemFree( prep );
    }
}
