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

/**
 * \file   db_types.h
 * \author Th. Leibovici
 * \brief  This file defines database specific types.
 */
/**
 * \addtogroup LIST_MANAGER
 * @{
 */
#ifndef _DB_TYPES_H
#define _DB_TYPES_H

#ifdef _MYSQL

#define DB_ENGINE_NAME    "MySQL"

#include <mysql/mysql.h>

typedef MYSQL  db_conn_t;
typedef MYSQL_RES *result_handle_t;

/** data related to a prepared statement */
struct prep_stmt_data_t
{
    MYSQL_STMT    *stmt;

    MYSQL_BIND    *input;
    unsigned int   nb_in;

    /* bind attributes arrays for input */
    unsigned long *strlen_array_in;

    MYSQL_BIND    *output;
    unsigned int   nb_out;

    /* bind attributes arrays for output */
    unsigned long *strlen_array_out;
    my_bool       *isnull_array_out;
    my_bool       *error_array_out;
};

typedef struct prep_stmt_data_t *prep_stmt_t;

/** specific database configuration */
typedef struct db_config_t
{
    char           server[256];
    char           db[256];
    char           user[256];
    char           password[256];
} db_config_t;

#elif defined(_SQLITE)

#define DB_ENGINE_NAME    "SQLite"

#include <sqlite3.h>

typedef sqlite3 *db_conn_t;
typedef sqlite3_stmt *prep_stmt_t;
typedef struct result_handle_t
{
    char         **result_array;
    unsigned int   curr_row;
    int            nb_rows;
    int            nb_cols;
} result_handle_t;

typedef struct db_config_t
{
    char           filepath[1024];
    unsigned int   retry_delay_microsec;         /* retry time when busy */
} db_config_t;


#else
#   error "No database type was specified"
#endif

#endif

/**
 * @}
 */
