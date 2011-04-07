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


/* -------------------- miscellaneous routines ---------------- */

/* escape a string in a SQL request */
void db_escape_string( db_conn_t * conn, char * str_out, size_t out_size, const char * str_in );

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
