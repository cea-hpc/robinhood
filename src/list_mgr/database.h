/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
#ifndef _GENERIC_DB_H
#define _GENERIC_DB_H

#include "list_mgr.h"

#define MAIN_TABLE	        "ENTRIES"
#define DNAMES_TABLE        "NAMES"
#define ANNEX_TABLE	        "ANNEX_INFO"
#define STRIPE_INFO_TABLE	"STRIPE_INFO"
#define STRIPE_ITEMS_TABLE	"STRIPE_ITEMS"
#define SOFT_RM_TABLE       "SOFT_RM"
#define VAR_TABLE           "VARS"
#define ACCT_TABLE          "ACCT_STAT"
#define ACCT_TRIGGER_INSERT "ACCT_ENTRY_INSERT"
#define ACCT_TRIGGER_UPDATE "ACCT_ENTRY_UPDATE"
#define ACCT_TRIGGER_DELETE "ACCT_ENTRY_DELETE"
#define ACCT_FIELD_COUNT    "count"
#define ACCT_DEFAULT_OWNER  "unknown"
#define ACCT_DEFAULT_GROUP  "unknown"
#define SZRANGE_FUNC        "sz_range"
#define ONE_PATH_FUNC       "one_path"
#define THIS_PATH_FUNC      "this_path"

/* for HSM flavors only */
#define  RECOV_TABLE     "RECOVERY"

/* name of sz fields */
#define ACCT_SIZE_PREFIX "sz"
static const __attribute__ ((__unused__))
char * sz_field[SZ_PROFIL_COUNT] =
{
    ACCT_SIZE_PREFIX"0",
    ACCT_SIZE_PREFIX"1",
    ACCT_SIZE_PREFIX"32",
    ACCT_SIZE_PREFIX"1K",
    ACCT_SIZE_PREFIX"32K",
    ACCT_SIZE_PREFIX"1M",
    ACCT_SIZE_PREFIX"32M",
    ACCT_SIZE_PREFIX"1G",
    ACCT_SIZE_PREFIX"32G",
    ACCT_SIZE_PREFIX"1T"
};

extern lmgr_config_t lmgr_config;

/* -------------------- Connexion management ---------------- */

/* create client connection */
int            db_connect( db_conn_t * conn );

/* close connection */
int            db_close_conn( db_conn_t * conn );


/* -------------------- SQL queries/result management ---------------- */

/* execute sql directive (optionnaly with returned result) */
int            db_exec_sql( db_conn_t * conn, const char *query, result_handle_t * p_result );

/* like db_exec_sql, but expects duplicate key or no such table errors */
int            db_exec_sql_quiet( db_conn_t * conn, const char *query,
                                  result_handle_t * p_result );

/* get the next record from result */
int            db_next_record( db_conn_t * conn,
                               result_handle_t * p_result,
                               char *outtab[], unsigned int outtabsize );

/* retrieve number of records in result */
int            db_result_nb_records( db_conn_t * conn, result_handle_t * p_result );

/* free result resources */
int            db_result_free( db_conn_t * conn, result_handle_t * p_result );

/* indicate if the error is retryable (transaction must be restarted) */
bool db_is_retryable(int db_err);

typedef enum {DBOBJ_TABLE, DBOBJ_TRIGGER, DBOBJ_FUNCTION, DBOBJ_PROC, DBOBJ_INDEX} db_object_e;

static inline const char *dbobj2str(db_object_e ot)
{
    switch(ot)
    {
        case DBOBJ_TABLE:    return "table";
        case DBOBJ_TRIGGER:  return "trigger";
        case DBOBJ_FUNCTION: return "function";
        case DBOBJ_PROC:     return "procedure";
        case DBOBJ_INDEX:    return "index";
    }
    return NULL;
}


/** remove a database component (table, trigger, function, ...) */
int            db_drop_component( db_conn_t * conn, db_object_e obj_type, const char *name );

/**
 * check a component exists in the database
 * \param arg depends on the object type: src table for triggers, NULL for others.
 */
int db_check_component(db_conn_t *conn, db_object_e obj_type, const char *name, const char *arg);


/* create a trigger */
int            db_create_trigger( db_conn_t * conn, const char *name, const char *event,
                               const char *table, const char *body );

/* -------------------- miscellaneous routines ---------------- */

/* escape a string in a SQL request */
int db_escape_string(db_conn_t *conn, char *str_out, size_t out_size, const char *str_in);

/* retrieve error message */
char          *db_errmsg( db_conn_t * conn, char *errmsg, unsigned int buflen );

/** list table fields, their type, and default value */
int db_list_table_info(db_conn_t * conn, const char *table,
                       char **field_tab, char **type_tab, char **default_tab,
                       unsigned int outtabsize,
                       char *inbuffer, unsigned int inbuffersize);

/* id of the last inserted row */
unsigned long long db_last_id( db_conn_t * conn );

typedef enum { TRANS_NEXT, TRANS_SESSION } what_trans_e;
typedef enum { TXL_SERIALIZABLE,
               TXL_REPEATABLE_RD,
               TXL_READ_COMMITTED,
               TXL_READ_UNCOMMITTED } tx_level_e;

/** set transaction level (optimize performance or locking) */
int db_transaction_level(db_conn_t * conn, what_trans_e what_tx, tx_level_e tx_level);


#endif
