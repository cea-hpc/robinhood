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
#define ANNEX_TABLE	        "ANNEX_INFO"
#define STRIPE_INFO_TABLE	"STRIPE_INFO"
#define STRIPE_ITEMS_TABLE	"STRIPE_ITEMS"
#define VAR_TABLE           "VARS"
#define ACCT_TABLE          "ACCT_STAT"

#ifdef HAVE_RM_POLICY
#   define SOFT_RM_TABLE    "SOFT_RM"
#endif

#ifdef _BACKUP_FS
#   define  RECOV_TABLE     "RECOVERY"
#endif

extern lmgr_config_t lmgr_config;
extern int     annex_table;                      /* are we using an annex table */


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


/* -------------------- prepared statements management ---------------- */

/* create a prepared statement */
prep_stmt_t    db_create_prepared( db_conn_t * conn, const char *query );

/* individually bind a param to a prepared statement */
int            db_bind_param( prep_stmt_t prep, unsigned int arg_index, db_type_t type,
                              void *p_value, size_t size );

/* bind a set of params to a prepared statement.
 * If params have been individually bound, call this
 * with intabsize = 0 and buff_changed = TRUE
 */
int            db_bind_params( prep_stmt_t stmt, db_type_t * type,      /* array of expected types */
                               void **p_value,   /* array of target buffers */
                               size_t * size,    /* array of buffer sizes (for strings) */
                               unsigned int intabsize,  /* size of input array */
                               int buff_changed );      /* did input buffer changed ? */


/**
 * Execute a prepared statement.
 * @param fetch_all indicates if all results are to be fetched once on client.
 */
int            db_exec_prepared( prep_stmt_t stmt, int fetch_all );

/* retrieve next record for prepared statement */
int            db_next_prepared_record( prep_stmt_t stmt, db_type_t * type,     /* array of expected types */
                                        void **p_value, /* array of target buffers */
                                        size_t * size,  /* array of buffer sizes (for strings) */
                                        unsigned int outtabsize,        /* size of output array */
                                        int buff_changed );     /* did output buffer changed ? */


/* check if a result is null
 */
int            db_is_null_result( prep_stmt_t stmt, unsigned int index );



/* clean prepared statement result */
int            db_clean_prepared( prep_stmt_t stmt );

/* destroy a prepared statement */
void           db_destroy_prepared( prep_stmt_t stmt );

/* -------------------- miscellaneous routines ---------------- */

/* retrieve error message */
char          *db_errmsg( db_conn_t * conn, char *errmsg, unsigned int buflen );

/* check table fields */
int            db_list_table_fields( db_conn_t * conn, const char *table,
                                     char **outtab,
                                     unsigned int outtabsize,
                                     char *inbuffer, unsigned int inbuffersize );

/* id of the last inserted row */
unsigned long long db_last_id( db_conn_t * conn );



#endif
