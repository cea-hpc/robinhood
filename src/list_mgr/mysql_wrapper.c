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
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "Memory.h"
#include <stdio.h>
#include <unistd.h>
#include <glib.h>
#include <time.h>
/* mysql includes */
#include <mysqld_error.h>
#include <errmsg.h>

#define _DEBUG_DB

static int mysql_error_convert(int err, bool verb)
{
    switch ( err )
    {
    case 0:
        return DB_SUCCESS;
    case ER_NO_SUCH_TABLE:
        return DB_NOT_EXISTS;
    case ER_DUP_ENTRY:
        return DB_ALREADY_EXISTS;
#ifdef _MYSQL5
    case ER_TRG_DOES_NOT_EXIST:
        return DB_TRG_NOT_EXISTS;
#endif
    case ER_BAD_FIELD_ERROR:
        if (verb)
            DisplayLog(LVL_CRIT, LISTMGR_TAG, "Invalid DB field");
        return DB_INVALID_ARG;
    case ER_PARSE_ERROR:
        if (verb)
            DisplayLog(LVL_CRIT, LISTMGR_TAG, "SQL request parse error");
        return DB_REQUEST_FAILED;
    case ER_LOCK_DEADLOCK:
        if (verb)
            DisplayLog(LVL_EVENT, LISTMGR_TAG, "DB deadlock detected");
        return DB_DEADLOCK;
    case ER_LOCK_WAIT_TIMEOUT:
        if (verb)
        {
            if (!strcasecmp(lmgr_config.db_config.engine, "InnoDB"))
                DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Lock timeout detected. "
                           "Consider increasing \"innodb_lock_wait_timeout\" in MySQL config.");
            else
                DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Lock timeout detected. "
                           "Consider increasing \"lock_wait_timeout\" in MySQL config.");
        }
        return DB_DEADLOCK;

        /* connection relative errors */

        /* In case of a deconnection, mysql_stmt_fetch returns this error CR_COMMANDS_OUT_OF_SYNC.
         * which is actually not very appropriate... */
    case CR_COMMANDS_OUT_OF_SYNC:
        /* when connection is lost, statements are no longer valid */
    case ER_UNKNOWN_STMT_HANDLER:

        /* It also returns ER_UNKNOWN_ERROR... In this case, we treat it as a disconnection anyway,
         * to give a chance to the client to clean its internal state.
         */
    case ER_UNKNOWN_ERROR:

    /* query may be interrupted for a connexion shutdown */
    case ER_QUERY_INTERRUPTED:

        /* These are really connection errors: */
    case ER_SERVER_SHUTDOWN:
    case CR_CONNECTION_ERROR:
    case CR_SERVER_GONE_ERROR:
    case CR_SERVER_LOST:
    case CR_CONN_HOST_ERROR:
        if (verb)
            DisplayLog(LVL_CRIT, LISTMGR_TAG, "DB connection error %d", err);
        return DB_CONNECT_FAILED;

    default:
        DisplayLog(verb?LVL_MAJOR:LVL_DEBUG, LISTMGR_TAG,
                   "Unhandled error %d: default conversion to DB_REQUEST_FAILED", err);
        return DB_REQUEST_FAILED;
    }
}

bool db_is_retryable(int db_err)
{
    switch (db_err)
    {
        case DB_CONNECT_FAILED:
        case DB_DEADLOCK: /* Note: the whole transaction must be retryed */
            return true;
        default:
            return false;
    }
}


/* create client connection */
int db_connect( db_conn_t * conn )
{
    my_bool        reconnect = 1;
    unsigned int retry = 0;

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

    while(1) {
        /* connect to server */
        if ( !mysql_real_connect
             ( conn, lmgr_config.db_config.server, lmgr_config.db_config.user,
               lmgr_config.db_config.password, lmgr_config.db_config.db,
               lmgr_config.db_config.port,
               EMPTY_STRING(lmgr_config.db_config.socket)?
                            NULL:lmgr_config.db_config.socket,
               0 ) )
        {
            /* connection error is retried at DB level */
            if ((retry < 3) && db_is_retryable(mysql_error_convert(mysql_errno(conn),0))) {
                DisplayLog( LVL_MAJOR, LISTMGR_TAG,
                            "Failed to connect to MySQL: Error: %s. Retrying...", mysql_error( conn ) );
                retry ++;
                sleep(1);
            }
            else
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to connect to MySQL after %u retries: Error: %s. Aborting.",
                            retry, mysql_error( conn ) );
                return DB_CONNECT_FAILED;
            }
        }
        else
        {
            if (retry)
                DisplayLog( LVL_CRIT, LISTMGR_TAG, "Connection to MySQL server successful after %u retries.",
                            retry );
            /* OK */
            break;
        }
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

    DisplayLog( LVL_FULL, LISTMGR_TAG, "Logged on to database '%s' successfully", lmgr_config.db_config.db );
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
        rh_strncpy(errmsg, mysql_error(conn), buflen);
    else
        strcpy( errmsg, mysql_error( conn ) );

    return errmsg;
}

static int _db_exec_sql(db_conn_t *conn, const char *query,
                        result_handle_t *p_result, bool quiet)
{
    int            rc;
    int            dberr;
#ifdef _DEBUG_DB
    DisplayLog( LVL_FULL, LISTMGR_TAG, "SQL query: %s", query );
#endif

    rc = mysql_real_query(conn, query, strlen(query));
    dberr = mysql_errno(conn);
    if (rc)
    {
        rc = mysql_error_convert(dberr, quiet?0:1);
        if (dberr == ER_DUP_ENTRY)
        {
            DisplayLog(quiet?LVL_DEBUG:LVL_EVENT, LISTMGR_TAG,
                       "A database record already exists for this entry: '%s' (%s)",
                       query, mysql_error(conn));
        }
#ifdef _MYSQL5
        else if (dberr == ER_TRG_DOES_NOT_EXIST)
        {
            DisplayLog(quiet?LVL_DEBUG:LVL_EVENT, LISTMGR_TAG,
                       "Trigger does not exist: '%s' (%s)",
                       query, mysql_error(conn));
        }
#endif
        else if (dberr == ER_NO_SUCH_TABLE)
            DisplayLog(quiet?LVL_DEBUG:LVL_EVENT, LISTMGR_TAG,
                       "Table does not exist: '%s' (%s)",
                       query, mysql_error(conn));
        else if (!db_is_retryable(rc))
            DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Error %d executing query '%s': %s",
                       rc, query, mysql_error(conn));

        return rc;
    }
    else
    {
        /* fetch results to the client */
        if (p_result)
        {
            *p_result = mysql_store_result(conn);
            if (*p_result == NULL)
                return DB_NOT_EXISTS;
        }

        return DB_SUCCESS;
    }
}

int db_exec_sql_quiet( db_conn_t * conn, const char *query, result_handle_t * p_result )
{
    return _db_exec_sql(conn, query, p_result, true);
}

int db_exec_sql( db_conn_t * conn, const char *query, result_handle_t * p_result )
{
    return _db_exec_sql(conn, query, p_result, false);
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

int db_list_table_info(db_conn_t * conn, const char *table,
                       char **field_tab, char **type_tab, char **default_tab,
                       unsigned int outtabsize,
                       char *inbuffer, unsigned int inbuffersize)
{
    char           request[4096];
    MYSQL_RES     *result;
    MYSQL_ROW      row;
    int            i, rc, curr_output;
    char          *curr_ptr = inbuffer;

    snprintf(request, sizeof(request), "SHOW COLUMNS FROM %s", table);
    rc = db_exec_sql_quiet(conn, request, &result);

    if (rc)
        return rc;

    if (!result)
    {
        DisplayLog(LVL_DEBUG, LISTMGR_TAG, "%s does not exist", table);
        return DB_NOT_EXISTS;
    }

    /* init ouput tabs */
    for (i = 0; i < outtabsize; i++) {
        field_tab[i] = NULL;
        if (type_tab)
            type_tab[i] = NULL;
        if (default_tab)
            default_tab[i] = NULL;
    }

    curr_output = 0;
    while ((row = mysql_fetch_row(result)))
    {
        strcpy(curr_ptr, row[0]);
        field_tab[curr_output] = curr_ptr;
        curr_ptr += strlen(curr_ptr) + 1;

        if (type_tab)
        {
            strcpy(curr_ptr, row[1]);
            type_tab[curr_output] = curr_ptr;
            curr_ptr += strlen(curr_ptr) + 1;
        }

        if (default_tab && row[4] != NULL)
        {
            strcpy(curr_ptr, row[4]);
            default_tab[curr_output] = curr_ptr;
            curr_ptr += strlen(curr_ptr) + 1;
        }

        curr_output++;
    }
    mysql_free_result(result);

    return DB_SUCCESS;
}

unsigned long long db_last_id( db_conn_t * conn )
{
    return mysql_insert_id( conn );
}


/* escape a string in a SQL request */
int db_escape_string( db_conn_t * conn, char * str_out, size_t out_size, const char * str_in )
{
    int len_in = strlen(str_in);

    /* output size must be at least 2 x instrlen + 1 for the worst case */
   if (out_size < 2 * len_in + 1)
        return DB_BUFFER_TOO_SMALL;

    /* escape special characters in value */
    mysql_real_escape_string(conn, str_out, str_in, len_in);
    return DB_SUCCESS;
}


/* remove a database component (table, trigger, function, ...) */
int            db_drop_component( db_conn_t * conn, db_object_e obj_type, const char *name )
{
    const char * tname="";
    char query[1024];

    switch (obj_type)
    {
        case DBOBJ_TABLE: tname = "TABLE"; break;
        case DBOBJ_FUNCTION: tname = "FUNCTION"; break;
        case DBOBJ_PROC: tname = "PROCEDURE"; break;
        case DBOBJ_TRIGGER: tname = "TRIGGER"; break;
        default:
             DisplayLog( LVL_CRIT, LISTMGR_TAG, "Object type not supported in %s", __func__);
            return DB_NOT_SUPPORTED;
    }

#ifndef _MYSQL5 /* only tables are supported before MySQL 5 */
    if (obj_type != DBOBJ_TABLE)
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "You should upgrade to MYSQL 5 or + to use %s", tname  );
        return DB_NOT_SUPPORTED;
    }
#endif

    if( mysql_get_server_version(conn) < 50032 )
    {
        sprintf(query, "DROP %s %s ", tname, name);
        return _db_exec_sql(conn, query, NULL, true);
    }
    else
    {
        sprintf(query, "DROP %s IF EXISTS %s ", tname, name);
        return _db_exec_sql(conn, query, NULL, false);
    }
}

/**
 * check a component exists in the database
 * \param arg depends on the object type: src table for triggers, NULL for others.
 */
int db_check_component(db_conn_t *conn, db_object_e obj_type, const char *name, const char *arg)
{
    char       query[1024];
    MYSQL_RES *result;
    MYSQL_ROW  row;
    int rc;

    if (obj_type == DBOBJ_TRIGGER)
    {
        sprintf(query, "SELECT EVENT_OBJECT_TABLE FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA='%s'"
                "AND TRIGGER_NAME='%s'", lmgr_config.db_config.db, name);

        rc = _db_exec_sql(conn, query, &result, false);
        if ( rc )
            return rc;

        if (!result)
        {
            DisplayLog(LVL_DEBUG, LISTMGR_TAG, "%s does not exist", name);
            return DB_NOT_EXISTS;
        }

        row = mysql_fetch_row(result);
        if (row)
        {
            DisplayLog(LVL_FULL, LISTMGR_TAG, "Trigger %s exists and is defined on %s",
                       name, row[0] ? row[0] : "<null>"); 
            if (!arg)
            {
                /* just check the row is set */
                if (row[0] == NULL || row[0][0] == '\0')
                    rc = DB_BAD_SCHEMA;
                else
                    rc = DB_SUCCESS;
            }
            else if (strcmp(arg, row[0]))
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG, "Trigger %s is on wrong table: expected %s, got %s",
                           name, arg, row[0]);
                rc = DB_BAD_SCHEMA;
            }
            else
                rc = DB_SUCCESS;

            if (mysql_fetch_row(result))
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG, "Unexpected multiple definition of %s on %s",
                           name, row[0]);
                rc = DB_BAD_SCHEMA;
            }
        }
        else
            rc = DB_NOT_EXISTS;

        mysql_free_result(result);
        return rc;
    }
    else if (obj_type == DBOBJ_FUNCTION)
    {
        sprintf(query, "SHOW FUNCTION STATUS WHERE DB='%s' AND NAME='%s'",
                lmgr_config.db_config.db, name);

        rc = _db_exec_sql(conn, query, &result, false);
        if ( rc )
            return rc;

        if (!result)
        {
            DisplayLog(LVL_DEBUG, LISTMGR_TAG, "%s does not exist", name);
            return DB_NOT_EXISTS;
        }

        row = mysql_fetch_row(result);
        if (row)
        {
            DisplayLog(LVL_FULL, LISTMGR_TAG, "Function %s exists", name);
            rc = DB_SUCCESS;

            if (mysql_fetch_row(result))
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG, "Unexpected multiple definition of %s",
                           name);
                rc = DB_BAD_SCHEMA;
            }
        }
        else
            rc = DB_NOT_EXISTS;

        mysql_free_result(result);
        return rc;
    }
    else
    {
        RBH_BUG("Only triggers are supported for now");
    }
}

/* create a trigger */
int db_create_trigger( db_conn_t * conn, const char *name, const char *event,
                               const char *table, const char *body )
{
#ifdef _MYSQL5
    int rc;
    GString *request = g_string_new("CREATE TRIGGER ");

    g_string_append_printf(request, "%s %s ON %s FOR EACH ROW "
                           "BEGIN %s END", name, event, table, body);
    rc = _db_exec_sql(conn, request->str, NULL, false);
    g_string_free(request, TRUE);
    return rc;
#else

    DisplayLog( LVL_CRIT, LISTMGR_TAG, "Trigger %s was not created: "
                "you should upgrade to MYSQL 5 to use triggers", name  );
    return DB_NOT_SUPPORTED;
#endif
}

static inline const char * txlvl_str(tx_level_e lvl)
{
    switch(lvl)
    {
        case TXL_SERIALIZABLE: return "SERIALIZABLE";
        case TXL_REPEATABLE_RD: return "REPEATABLE READ";
        case TXL_READ_COMMITTED: return "READ COMMITTED";
        case TXL_READ_UNCOMMITTED: return "READ UNCOMMITTED";
        default: return "";
    }
}

/** set transaction level (optimize performance or locking) */
int db_transaction_level(db_conn_t * conn, what_trans_e what_tx, tx_level_e tx_level)
{
    char query[1024];
    if (what_tx == TRANS_NEXT)
        sprintf(query, "SET TRANSACTION ISOLATION LEVEL %s",
                txlvl_str(tx_level));
    else
        sprintf(query, "SET SESSION TRANSACTION ISOLATION LEVEL %s",
                txlvl_str(tx_level));

    return _db_exec_sql(conn, query, NULL, false);
}

