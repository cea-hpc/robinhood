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
 * Persistent variables management
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "database.h"
#include "listmgr_common.h"
#include "rbh_logs.h"
#include <stdio.h>

int lmgr_get_var(db_conn_t *pconn, const char *varname, char *value,
                 int bufsize)
{
    int rc;
    result_handle_t result;
    char *str_val = NULL;
    GString *req = NULL;

    if (!varname || !value)
        return DB_INVALID_ARG;

    req = g_string_new("SELECT value FROM " VAR_TABLE " WHERE varname=");
    g_string_append_printf(req, "'%s'", varname);

    /* execute the request */
    rc = db_exec_sql(pconn, req->str, &result);
    if (rc)
        goto free_str;

    rc = db_next_record(pconn, &result, &str_val, 1);

    if (rc == DB_END_OF_LIST)
        rc = DB_NOT_EXISTS;

    if (rc)
        goto free_res;

    if (str_val == NULL) {
        rc = DB_REQUEST_FAILED;
        goto free_res;
    }

    /* copy the result */
    if (strlen(str_val) >= bufsize) {
        rc = DB_BUFFER_TOO_SMALL;
    } else {
        strcpy(value, str_val);
        rc = DB_SUCCESS;
    }

 free_res:
    db_result_free(pconn, &result);
 free_str:
    g_string_free(req, TRUE);
    return rc;
}

int lmgr_set_var(db_conn_t *pconn, const char *varname, const char *value)
{
    GString *query;
    int rc;
    char escaped[1024];

    /* delete var if value is NULL */
    if (value == NULL) {
        query = g_string_new("DELETE FROM " VAR_TABLE " WHERE varname =");
        g_string_append_printf(query, "'%s'", varname);

        rc = db_exec_sql(pconn, query->str, NULL);
        goto out;
    } else
        query = g_string_new(NULL);

    /* escape special characters in value */
    rc = db_escape_string(pconn, escaped, sizeof(escaped), value);
    if (rc != DB_SUCCESS)
        goto out;

    g_string_printf(query,
                    "INSERT INTO " VAR_TABLE
                    " (varname,value) VALUES ('%s','%s') "
                    "ON DUPLICATE KEY UPDATE value='%s'", varname, escaped,
                    escaped);

    rc = db_exec_sql(pconn, query->str, NULL);
 out:
    g_string_free(query, TRUE);
    return rc;
}

/**
 *  Get variable value.
 */
int ListMgr_GetVar(lmgr_t *p_mgr, const char *varname, char *value,
                   int bufsize)
{
    int rc;
 retry:
    rc = lmgr_get_var(&p_mgr->conn, varname, value, bufsize);
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
