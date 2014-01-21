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

int lmgr_get_var(db_conn_t *pconn, const char *varname, char *value)
{
    char           request[4096];
    int            rc;
    result_handle_t result;
    char          *str_val = NULL;

    if (!varname || !value)
        return DB_INVALID_ARG;

    sprintf(request, "SELECT value FROM "VAR_TABLE" WHERE varname='%s'",
            varname);

    /* execute the request */
    rc = db_exec_sql(pconn, request, &result);
    if (rc)
        return rc;

    rc = db_next_record(pconn, &result, &str_val, 1);

    if (rc == DB_END_OF_LIST)
    {
        db_result_free(pconn, &result);
        return DB_NOT_EXISTS;
    }
    else if (rc)
    {
        db_result_free(pconn, &result);
        return rc;
    }
    if (str_val == NULL)
    {
        db_result_free(pconn, &result);
        return DB_REQUEST_FAILED;
    }

    /* result */
    strcpy(value, str_val);
    db_result_free(pconn, &result);

    return DB_SUCCESS;
}

int lmgr_set_var(db_conn_t *pconn, const char *varname, const char *value)
{
    char           query[4096];
    int            rc;
#ifdef _MYSQL
    char           escaped[1024];

    /* delete var if value is NULL */
    if (value == NULL)
    {
        sprintf(query, "DELETE FROM "VAR_TABLE" WHERE varname = '%s'",
                 varname);
        return db_exec_sql(pconn, query, NULL);
    }

    /* escape special characters in value */
    db_escape_string(pconn, escaped, 1024, value);

    sprintf(query,
             "INSERT INTO "VAR_TABLE" (varname, value) VALUES ('%s', '%s') "
             "ON DUPLICATE KEY UPDATE value = '%s'", varname, escaped, escaped);
#elif defined(_SQLITE)
    /* using slqite3_snprintf with "%q" format, to escape strings */
    sqlite3_snprintf(4096, query,
             "INSERT INTO "VAR_TABLE" (varname, value) VALUES ('%s', '%q')",
             varname, value, value);
#else
#error "ListMgr_SetVar() not yet implemented for this database engine"
#endif

    rc = db_exec_sql(pconn, query, NULL);

    if (rc == DB_ALREADY_EXISTS)
    {
        DisplayLog(LVL_DEBUG, LISTMGR_TAG,
                    "Entry already exists in %s(). Updating it.",
                    __FUNCTION__);
        /* insert failed, try to update */
#ifdef _MYSQL
        sprintf(query,
                 "UPDATE " VAR_TABLE " SET value = '%s' WHERE varname = '%s'",
                 escaped, varname);
#elif defined(_SQLITE)
        /* using slqite3_snprintf with "%q" format, to escape strings */
        sqlite3_snprintf(4096, query,
                          "UPDATE " VAR_TABLE " SET value = '%q' WHERE varname = '%s'",
                          value, varname);
#endif

        rc = db_exec_sql(pconn, query, NULL);
    }
    return rc;
}

/**
 *  Get variable value.
 *  @param value must be of size 1024.
 */
int ListMgr_GetVar(lmgr_t *p_mgr, const char *varname, char *value)
{
    int rc;
retry:
    rc = lmgr_get_var(&p_mgr->conn, varname, value);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;

    return rc;
}


/**
 *  Set variable value.
 *  @param value size must not exceed 1024.
 */
int ListMgr_SetVar(lmgr_t *p_mgr, const char *varname, const char *value)
{
    int rc;
retry:
    rc = lmgr_set_var(&p_mgr->conn, varname, value);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    return rc;
}
