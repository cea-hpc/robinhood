/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * Persistant variables management
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "database.h"
#include "listmgr_common.h"
#include "RobinhoodLogs.h"
#include <stdio.h>

/**
 *  Get variable value.
 *  @param value must be of size 1024.
 */
int ListMgr_GetVar( lmgr_t * p_mgr, const char *varname, char *value )
{
    char           request[4096];
    int            rc;
    result_handle_t result;
    char          *str_val = NULL;

    if ( !varname || !value )
        return DB_INVALID_ARG;

    /* verify it exists in main table */

    sprintf( request, "SELECT value FROM " VAR_TABLE " WHERE varname='%s'", varname );

    /* execute the request */
    rc = db_exec_sql( &p_mgr->conn, request, &result );
    if ( rc )
        return rc;

    rc = db_next_record( &p_mgr->conn, &result, &str_val, 1 );

    if ( rc == DB_END_OF_LIST )
    {
        db_result_free( &p_mgr->conn, &result );
        return DB_NOT_EXISTS;
    }
    else if ( rc )
    {
        db_result_free( &p_mgr->conn, &result );
        return rc;
    }
    if ( str_val == NULL )
    {
        db_result_free( &p_mgr->conn, &result );
        return DB_REQUEST_FAILED;
    }

    /* result */
    strcpy( value, str_val );
    db_result_free( &p_mgr->conn, &result );

    return 0;
}


/**
 *  Set variable value.
 *  @param value size must not exceed 1024.
 */
int ListMgr_SetVar( lmgr_t * p_mgr, const char *varname, const char *value )
{
    char           query[4096];
    int            rc;

#ifdef _MYSQL
    char           escaped[1024];

    /* escape special characters in value */
    mysql_real_escape_string( &p_mgr->conn, escaped, value, strlen( value ) );

    sprintf( query,
             "INSERT INTO " VAR_TABLE " (varname, value) VALUES ('%s', '%s') "
             "ON DUPLICATE KEY UPDATE value = '%s'", varname, escaped, escaped );
#elif defined( _SQLITE )
    /* using slqite3_snprintf with "%q" format, to escape strings */
    sqlite3_snprintf( 4096, query,
             "INSERT INTO " VAR_TABLE " (varname, value) VALUES ('%s', '%q')",
             varname, value, value );
#else
#error "ListMgr_SetVar() not yet implemented for this database engine"
#endif

    rc = db_exec_sql( &p_mgr->conn, query, NULL );

    if (rc == DB_ALREADY_EXISTS)
    {
        DisplayLog( LVL_DEBUG, LISTMGR_TAG,
                    "Entry already exists in %s(). Updating it.",
                    __FUNCTION__ );
        /* insert failed, try to update */
#ifdef _MYSQL
        sprintf( query,
                 "UPDATE " VAR_TABLE " SET value = '%s' WHERE varname = '%s'",
                 escaped, varname );
#elif defined( _SQLITE )
        /* using slqite3_snprintf with "%q" format, to escape strings */
        sqlite3_snprintf( 4096, query,
                          "UPDATE " VAR_TABLE " SET value = '%q' WHERE varname = '%s'",
                          value, varname );
#endif

        rc = db_exec_sql( &p_mgr->conn, query, NULL );
    }

    return rc;
}
