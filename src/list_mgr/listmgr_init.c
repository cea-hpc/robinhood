/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009, 2010 CEA/DAM
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
#include "listmgr_internal.h"
#include "listmgr_prep_stmt.h"
#include "database.h"
#include "listmgr_common.h"
#include "RobinhoodLogs.h"
#include <stdio.h>

#define MAX_DB_FIELDS 64

static inline int append_field_def( int i, char *next, int is_first )
{
    switch ( field_infos[i].db_type )
    {
    case DB_STRIPE_INFO:   /* never in main table (ignored) */
    case DB_STRIPE_ITEMS:
        break;
    case DB_TEXT:
        if ( field_infos[i].db_type_size < 256 )
            return sprintf( next, "%s %s VARCHAR(%u)",is_first ? "" : ",",
                field_infos[i].field_name, field_infos[i].db_type_size );
        else
            return sprintf( next, "%s %s TEXT", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_INT:
        return sprintf( next, "%s %s INT", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_UINT:
        return sprintf( next, "%s %s INT UNSIGNED", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_BIGINT:
        return sprintf( next, "%s %s BIGINT", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_BIGUINT:
        return sprintf( next, "%s %s BIGINT UNSIGNED", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_BOOL:
        return sprintf( next, "%s %s BOOLEAN", is_first ? "" : ",", field_infos[i].field_name );
        break;
    }
    return 0;
}

/**
 * Check table fields.
 * @param i 
 * @param curr_field_index (in,out) filed index in currently checked schema
 * @retval 0 on success
 * @retval -1 on error
 */
static inline int check_field( int i, int * curr_field_index, char *table, char **fieldtab )
{
    if ( ( *curr_field_index >= MAX_DB_FIELDS )
        || ( fieldtab[*curr_field_index] == NULL ) )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Incompatible database schema (missing field '%s' in table %s):" 
                    " you should drop the database and start a new FS scan.",
                    field_infos[i].field_name, table );
        return -1;
    }
    /* check that this is the expected field */
    if ( !strcmp( field_infos[i].field_name, fieldtab[*curr_field_index] ) )
    {
        DisplayLog( LVL_DEBUG, LISTMGR_TAG, "%s OK", field_infos[i].field_name );
        (*curr_field_index)++;
        return 0;
    }
    else
    {
        DisplayLog( LVL_DEBUG, LISTMGR_TAG, "%s != %s",
                    field_infos[i].field_name, fieldtab[*curr_field_index] );
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Incompatible database schema (unexpected field '%s' in table %s):"
                    " you should drop the database and start a new FS scan.",
                    fieldtab[*curr_field_index], table );
        return -1;
    }
}

/* Return 0 if there is no extra field else return 1 */
static inline int has_extra_field( int curr_field_index, char *table, char **fieldtab )
{
        if ( ( curr_field_index < MAX_DB_FIELDS ) && ( fieldtab[curr_field_index] != NULL ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema (unexpected field '%s' in table %s):"
                        " you should drop the database and start a new FS scan.",
                        fieldtab[curr_field_index], table );
            return 1;
        }
        else
            return 0; 
}

/* Generate the list of accounting fields. Each field can be precede and/or followed by a string. 
If scope = 0 -> generate all the fields
If scope = 1 -> generate the list of primary key fields 
If scope = 2 -> generate the list of fields except primary keys*/

/*typedef enum {
    SCOPE_ALL,
    SCOPE_PK,
    SCOPE_FIELDS
} generate_scope;

static inline char* generate_acct_fields( char *next, const char *before, const char *after, generate_scope scope )
{ 
    int i;
    int is_first_field = 1;
    if ( scope == 0 || scope == 1 )
    {
        for ( i = 0; i < ATTR_COUNT; i++ )
        {
            if ( is_acct_pk( i ) )
            {
                if ( !is_first_field )
                    next += sprintf( next, ", %s%s%s", before,  field_infos[i].field_name, after );
                else
                {
                    next += sprintf( next, "%s%s%s", before, field_infos[i].field_name, after );
                    is_first_field = 0; 
                }
            }
        }
    }
    if ( scope == SCOPE_ALL || scope == 2 )
    {
        for ( i = 0; i < ATTR_COUNT; i++ )
        {
            if ( is_acct_field( i ) )
            {
                if ( !is_first_field )
                    next += sprintf( next, ",%s%s%s ", before,  field_infos[i].field_name, after );
                else
                {
                    next += sprintf( next, "%s%s%s ", before, field_infos[i].field_name, after );
                    is_first_field = 0;
                }
            }
        }
    }
    return next;
}*/

/* Generate an operation for the list of accounting fields (except primary keys)
For instance: block = block + NEW.block*/
/*static inline char* generate_trigger_operation( char *next, const char *prefix, const char operator )
{
    int i;
    int is_first_field = 1;
    for ( i = 0; i < ATTR_COUNT; i++ )
    {
        if ( is_acct_field( i) )
        {
            if ( !is_first_field )
                next += sprintf( next, ", %s=%s%c%s.%s ", field_infos[i].field_name, field_infos[i].field_name,  
                                operator, prefix, field_infos[i].field_name );
            else
            {
                next += sprintf( next, "%s=%s%c%s.%s ", field_infos[i].field_name, field_infos[i].field_name, 
                                operator, prefix, field_infos[i].field_name );
                is_first_field = 0;
            }
        }
    }
    return next;
}*/

int            annex_table = FALSE;              /* indicates if an annex table is used */

int ListMgr_Init( const lmgr_config_t * p_conf )
{
    int            i, rc;
    db_conn_t      conn;
    char           errmsg_buf[1024];
    char           strbuf[4096];
    char          *next;
    char          *fieldtab[MAX_DB_FIELDS];

#ifdef FID_PK
#   define PK_TYPE   "VARCHAR(" TOSTRING(FID_LEN) ")"
#else
#   define PK_TYPE   "VARCHAR(" TOSTRING(PK_LEN) ")"
#endif

    /* store the configuration */
    lmgr_config = *p_conf;

    /* determine if an annex table is used */
    annex_table = FALSE;
    for ( i = 0; i < ATTR_COUNT; i++ )
    {
        if ( field_infos[i].flags & ANNEX_INFO )
        {
            annex_table = TRUE;
            break;
        }
    }

    /* initilize attr masks for each table */
    init_attrset_masks( &lmgr_config );

    /* create a database access */
    rc = db_connect( &conn );

    if ( rc )
        return rc;

    /* check if tables exist, and check their schema */

    DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Checking database schema" );

    /*
     *  ===== Check table of vars =====
     */
    rc = db_list_table_fields( &conn, VAR_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );
    if ( rc == DB_SUCCESS )
    {
        if ( ( fieldtab[0] == NULL ) || ( fieldtab[1] == NULL )
             || strcmp( fieldtab[0], "varname" ) || strcmp( fieldtab[1], "value" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Invalid fields (%s, %s) for table "
                        VAR_TABLE,
                        ( fieldtab[0] ? fieldtab[0] : "(null)" ),
                        ( fieldtab[1] ? fieldtab[1] : "(null)" ) );
            return -1;
        }
        else
        {
            DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Fields (%s, %s) OK", fieldtab[0], fieldtab[1] );
        }
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        DisplayLog( LVL_EVENT, LISTMGR_TAG, VAR_TABLE " does not exist: creating it." );

        /* table does not exist */
        strcpy( strbuf, "CREATE TABLE " VAR_TABLE " ( "
                "varname VARCHAR(255) PRIMARY KEY, "
                "value TEXT )" );

        DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

        rc = db_exec_sql( &conn, strbuf, NULL );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }

        DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " VAR_TABLE " created sucessfully" );
    }
    else
    {
        /* error */
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Error checking database schema: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }

    /*
     * ====== CHECKING MAIN TABLE ==========
     */
    rc = db_list_table_fields( &conn, MAIN_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );

    if ( rc == DB_SUCCESS )
    {
        int            curr_field_index = 0;

        /* check primary key */
        if ( ( fieldtab[0] == NULL ) || strcmp( fieldtab[0], "id" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Invalid primary key (%s) for table " MAIN_TABLE,
                        ( fieldtab[0] ? fieldtab[0] : "(null)" ) );
            return -1;
        }
        else
        {
            DisplayLog( LVL_DEBUG, LISTMGR_TAG, "primary key (%s) OK", fieldtab[0] );
            curr_field_index += 1;
        }

        for ( i = 0; i < ATTR_COUNT; i++ )
        {
            if ( is_main_field( i ) )
            {
                if ( check_field( i, &curr_field_index, MAIN_TABLE, fieldtab ) )
                    return -1;
            }
        }

        /* is there any extra field ? */ 
        if ( has_extra_field( curr_field_index, MAIN_TABLE, fieldtab ) )
            return -1;

    }
    else if ( rc == DB_NOT_EXISTS )
    {
        DisplayLog( LVL_EVENT, LISTMGR_TAG, MAIN_TABLE " does not exist: creating it." );

        /* table does not exist */
        strcpy( strbuf, "CREATE TABLE " MAIN_TABLE " ( id "PK_TYPE" PRIMARY KEY" );
        next = strbuf + strlen( strbuf );

        for ( i = 0; i < ATTR_COUNT; i++ )
        {
            if ( is_main_field( i ) )
            {
                next += append_field_def( i, next, 0);
            }
        }

        strcpy( next, ")" );

        DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

        rc = db_exec_sql( &conn, strbuf, NULL );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }

        DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " MAIN_TABLE " created sucessfully" );

    }
    else
    {
        /* error */
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Error checking database schema: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }

    /*
     * ====== CHECKING STRIPE TABLES ==========
     */

    /* stripe info tables */
    /* T1 STRIPE_INFO: fid (pk) => stripesize, stripecount, poolname */
    /* T2 STRIPE_ITEMS: fid (indexed for get/del) => stripe entries */

    rc = db_list_table_fields( &conn, STRIPE_INFO_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );

    if ( rc == DB_SUCCESS )
    {
        int            curr_field_index = 0;

        /* check primary key */
        if ( ( fieldtab[0] == NULL ) || strcmp( fieldtab[0], "id" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Invalid primary key (%s) for table "
                        STRIPE_INFO_TABLE, ( fieldtab[0] ? fieldtab[0] : "(null)" ) );
            return -1;
        }
        else
        {
            DisplayLog( LVL_DEBUG, LISTMGR_TAG, "primary key (%s) OK", fieldtab[0] );
            curr_field_index += 1;
        }

        if ( ( fieldtab[curr_field_index] == NULL )
             || strcmp( fieldtab[curr_field_index], "validator" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Missing field 'validator' in table " STRIPE_INFO_TABLE );
            return -1;
        }
        else
        {
            DisplayLog( LVL_DEBUG, LISTMGR_TAG, "validator OK" );
            curr_field_index += 1;
        }

        if ( ( fieldtab[curr_field_index] == NULL )
             || strcmp( fieldtab[curr_field_index], "stripe_count" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema for table "
                        STRIPE_INFO_TABLE
                        " (missing field 'stripe_count'): you should drop the database and start a new FS scan." );
            return -1;
        }

        curr_field_index++;

        if ( ( fieldtab[curr_field_index] == NULL )
             || strcmp( fieldtab[curr_field_index], "stripe_size" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema for table "
                        STRIPE_INFO_TABLE
                        " (missing field 'stripesize'): you should drop the database and start a new FS scan." );
            return -1;
        }

        curr_field_index++;

        if ( ( fieldtab[curr_field_index] == NULL )
             || strcmp( fieldtab[curr_field_index], "pool_name" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema for table "
                        STRIPE_INFO_TABLE
                        " (missing field 'pool_name'): you should drop the database and start a new FS scan." );
            return -1;
        }

        curr_field_index++;

        /* is there any extra field ? */
        if ( has_extra_field( curr_field_index, STRIPE_INFO_TABLE, fieldtab ) )
            return -1;

    }
    else if ( rc == DB_NOT_EXISTS )
    {
        DisplayLog( LVL_EVENT, LISTMGR_TAG,
                    STRIPE_INFO_TABLE " table does not exist: creating it." );

        sprintf( strbuf,
                 "CREATE TABLE " STRIPE_INFO_TABLE
                 " (id "PK_TYPE" PRIMARY KEY, validator INT UNSIGNED, "
                 "stripe_count INT UNSIGNED, stripe_size INT UNSIGNED, pool_name VARCHAR(%u) )",
                 MAX_POOL_LEN - 1 );

        DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

        rc = db_exec_sql( &conn, strbuf, NULL );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }

        DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " STRIPE_INFO_TABLE " created sucessfully" );

    }
    else
    {
        /* error */
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Error checking database schema: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }


    rc = db_list_table_fields( &conn, STRIPE_ITEMS_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );

    if ( rc == DB_SUCCESS )
    {
        int            curr_field_index = 0;

        /* check index */
        if ( ( fieldtab[0] == NULL ) || strcmp( fieldtab[0], "id" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Invalid index (%s) for table "
                        STRIPE_ITEMS_TABLE, ( fieldtab[0] ? fieldtab[0] : "(null)" ) );
            return -1;
        }
        else
        {
            DisplayLog( LVL_DEBUG, LISTMGR_TAG, "index (%s) OK", fieldtab[0] );
            curr_field_index += 1;
        }

        if ( ( fieldtab[curr_field_index] == NULL )
             || strcmp( fieldtab[curr_field_index], "storage_item" )
             || fieldtab[curr_field_index + 1] != NULL )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema for table "
                        STRIPE_ITEMS_TABLE
                        ": you should drop the database and start a new FS scan." );
            return -1;
        }
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        DisplayLog( LVL_EVENT, LISTMGR_TAG, STRIPE_ITEMS_TABLE
                    " table does not exist: creating it." );

        strcpy( strbuf,
                "CREATE TABLE " STRIPE_ITEMS_TABLE
                " ( id "PK_TYPE", storage_item INT UNSIGNED ) " );

        DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

        rc = db_exec_sql( &conn, strbuf, NULL );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }

        DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " STRIPE_ITEMS_TABLE " created sucessfully" );

        strcpy( strbuf, "CREATE INDEX id_index ON " STRIPE_ITEMS_TABLE "(id)" );

        DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );

        rc = db_exec_sql( &conn, strbuf, NULL );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to create index: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }
        DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on " STRIPE_ITEMS_TABLE " created sucessfully" );

    }
    else
    {
        /* error */
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Error checking database schema: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }


    /*
     * ====== CHECKING ANNEX TABLE ==========
     */

    if ( annex_table )
    {
        rc = db_list_table_fields( &conn, ANNEX_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );

        if ( rc == DB_SUCCESS )
        {
            int            curr_field_index = 0;

            /* check primary key */
            if ( ( fieldtab[0] == NULL ) || strcmp( fieldtab[0], "id" ) )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Invalid primary key (%s) for table "
                            ANNEX_TABLE, ( fieldtab[0] ? fieldtab[0] : "(null)" ) );
                return -1;
            }
            else
            {
                DisplayLog( LVL_DEBUG, LISTMGR_TAG, "primary key (%s) OK", fieldtab[0] );
                curr_field_index += 1;
            }


            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_annex_field( i ) )
                {
                    if ( check_field( i, &curr_field_index, ANNEX_TABLE, fieldtab ) )
                        return -1;
                }
            }

            /* is there any extra field ? */
            if ( has_extra_field( curr_field_index, ANNEX_TABLE, fieldtab ) )
                return -1;

        }
        else if ( rc == DB_NOT_EXISTS )
        {
            DisplayLog( LVL_EVENT, LISTMGR_TAG, ANNEX_TABLE " does not exist: creating it." );

            /* table does not exist */
            strcpy( strbuf, "CREATE TABLE " ANNEX_TABLE " ( id "PK_TYPE" PRIMARY KEY" );
            next = strbuf + strlen( strbuf );

            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_annex_field( i ) )
                {
                    next += append_field_def( i, next, 0);
                }
            }
            strcpy( next, " )" );

            DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create table: Error: %s",
                            db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " ANNEX_TABLE " created sucessfully" );

        }
        else
        {
            /* error */
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Error checking database schema: %s",
                        db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }

    }                           /* if annex_table */

#ifdef _LUSTRE_HSM
    /*
     *  ===== Checking SOFT_RM table ====
     * SCHEMA: fid (string), last_known_path, soft_rm_time, real_rm_time
     */
    rc = db_list_table_fields( &conn, SOFT_RM_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );
    if ( rc == DB_SUCCESS )
    {
        if ( ( fieldtab[0] == NULL ) || ( fieldtab[1] == NULL ) ||
             ( fieldtab[2] == NULL ) || ( fieldtab[3] == NULL ) ||
                strcmp( fieldtab[0], "fid" ) ||
                strcmp( fieldtab[1], "fullpath" ) ||
                strcmp( fieldtab[2], "soft_rm_time" ) ||
                strcmp( fieldtab[3], "real_rm_time" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Invalid fields (%s, %s, %s, %s) for table "
                        SOFT_RM_TABLE,
                        ( fieldtab[0] ? fieldtab[0] : "(null)" ),
                        ( fieldtab[1] ? fieldtab[1] : "(null)" ),
                        ( fieldtab[2] ? fieldtab[2] : "(null)" ),
                        ( fieldtab[3] ? fieldtab[3] : "(null)" ) );
            return -1;
        }
        else
        {
            DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Fields (%s, %s, %s, %s) OK",
                        fieldtab[0], fieldtab[1], fieldtab[2], fieldtab[3] );
        }
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        DisplayLog( LVL_EVENT, LISTMGR_TAG, SOFT_RM_TABLE " does not exist: creating it." );

        /* table does not exist */
        strcpy( strbuf, "CREATE TABLE " SOFT_RM_TABLE " ( "
                "fid VARCHAR(" TOSTRING(FID_LEN) ") PRIMARY KEY, "
                "fullpath VARCHAR(1023), "
                "soft_rm_time INT UNSIGNED, "
                "real_rm_time INT UNSIGNED  )" );

        DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

        rc = db_exec_sql( &conn, strbuf, NULL );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }

        DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " SOFT_RM_TABLE " created sucessfully" );

        strcpy( strbuf, "CREATE INDEX rm_time ON " SOFT_RM_TABLE "(real_rm_time)" );

        DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );

        rc = db_exec_sql( &conn, strbuf, NULL );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to create index: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }
        DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on " SOFT_RM_TABLE " created sucessfully" );

    }
    else
    {
        /* error */
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Error checking database schema: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }
#elif defined(_BACKUP_FS)
    /*
     *  ===== Checking SOFT_RM table ====
     * SCHEMA: fid (string), last_known_path, bkpath, soft_rm_time, real_rm_time
     */
    rc = db_list_table_fields( &conn, SOFT_RM_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );
    if ( rc == DB_SUCCESS )
    {
        if ( ( fieldtab[0] == NULL ) || ( fieldtab[1] == NULL ) ||
             ( fieldtab[2] == NULL ) || ( fieldtab[3] == NULL ) ||
             ( fieldtab[4] == NULL ) ||
                strcmp( fieldtab[0], "fid" ) ||
                strcmp( fieldtab[1], "fullpath" ) ||
                strcmp( fieldtab[2], "backendpath" ) ||
                strcmp( fieldtab[3], "soft_rm_time" ) ||
                strcmp( fieldtab[4], "real_rm_time" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Invalid fields (%s, %s, %s, %s, %s) for table "
                        SOFT_RM_TABLE,
                        ( fieldtab[0] ? fieldtab[0] : "(null)" ),
                        ( fieldtab[1] ? fieldtab[1] : "(null)" ),
                        ( fieldtab[2] ? fieldtab[2] : "(null)" ),
                        ( fieldtab[3] ? fieldtab[3] : "(null)" ),
                        ( fieldtab[4] ? fieldtab[4] : "(null)" ) );
            return -1;
        }
        else
        {
            DisplayLog( LVL_DEBUG, LISTMGR_TAG, "Fields (%s, %s, %s, %s, %s) OK",
                        fieldtab[0], fieldtab[1], fieldtab[2], fieldtab[3], fieldtab[4] );
        }
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        DisplayLog( LVL_EVENT, LISTMGR_TAG, SOFT_RM_TABLE " does not exist: creating it." );

        /* table does not exist */
        strcpy( strbuf, "CREATE TABLE " SOFT_RM_TABLE " ( "
                "fid VARCHAR(" TOSTRING(FID_LEN) ") PRIMARY KEY, "
                "fullpath VARCHAR(1023), "
                "backendpath VARCHAR(1023), "
                "soft_rm_time INT UNSIGNED, "
                "real_rm_time INT UNSIGNED  )" );

        DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

        rc = db_exec_sql( &conn, strbuf, NULL );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }

        DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " SOFT_RM_TABLE " created sucessfully" );

        strcpy( strbuf, "CREATE INDEX rm_time ON " SOFT_RM_TABLE "(real_rm_time)" );

        DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );

        rc = db_exec_sql( &conn, strbuf, NULL );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to create index: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }
        DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on " SOFT_RM_TABLE " created sucessfully" );

    }
    else
    {
        /* error */
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Error checking database schema: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }
#endif

    /*
     * ====== CHECKING STAT TABLE ==========
     */
    if ( lmgr_config.user_acct || lmgr_config.group_acct )
    {
        rc = db_list_table_fields( &conn, ACCT_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );

        if ( rc == DB_SUCCESS )
        {
            int curr_field_index = 0;

            /* check primary key */
            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_acct_pk( i ) )
                {
                    if ( check_field( i, &curr_field_index, ACCT_TABLE, fieldtab ) )
                        return -1;
                }
            }

            /* check other fields */
            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_acct_field( i ) )
                {
                    if ( check_field( i, &curr_field_index, ACCT_TABLE, fieldtab ) )
                        return -1;
                }
            }

            /* check count field*/
            if ( ( fieldtab[curr_field_index] == NULL ) || strcmp( fieldtab[curr_field_index], "count" ) )
            { 
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Incompatible database schema (missing field 'count' in table "
                            ACCT_TABLE
                            "): you should drop the database and start a new FS scan." );
                return -1;
            }
            else
            {
                DisplayLog( LVL_DEBUG, LISTMGR_TAG, "%s OK", fieldtab[curr_field_index] );
                curr_field_index += 1;
            }

            if ( has_extra_field( curr_field_index, ACCT_TABLE, fieldtab ) )
                return -1;

        }
        else if ( rc == DB_NOT_EXISTS )
        {
            int first_acct_pk = 1;
            int is_first_acct_field = 1;

            DisplayLog( LVL_EVENT, LISTMGR_TAG, ACCT_TABLE " does not exist: creating it." );

            /* table does not exist */
            strcpy( strbuf, "CREATE TABLE " ACCT_TABLE "(" );
            next = strbuf + strlen( strbuf );
            
            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_acct_pk( i ) )
                {
                    next += append_field_def( i, next, is_first_acct_field);
                    is_first_acct_field = 0;
                }                
            }

            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_acct_field( i ) )
                { 
                    next += append_field_def( i, next, is_first_acct_field);
                } 
            }

            strcpy ( next, ", count BIGINT UNSIGNED, PRIMARY KEY ( " );
            next = next + strlen( next );

            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_acct_pk( i ) )
                {
                    if ( !first_acct_pk )
                        next += sprintf( next, ", %s", field_infos[i].field_name );
                    else
                    {
                        next += sprintf( next, "%s", field_infos[i].field_name );
                        first_acct_pk = 0;
                    }
                }
            } 

            strcpy( next, " ) )" );

            DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " ACCT_TABLE " created sucessfully" );

            /* Initial table population for already existing entries */
            strcpy( strbuf, "INSERT INTO " ACCT_TABLE "( " );
            next = strbuf + strlen( strbuf );
            attrmask2fieldlist( next, acct_pk_attr_set , T_ACCT, FALSE, FALSE, "", "" );
            next = next + strlen( next );
            attrmask2fieldlist( next, acct_attr_set, T_ACCT, TRUE, FALSE, "", "" );
            next = next + strlen( next );
            next += sprintf( next, ", count ) SELECT " );
            attrmask2fieldlist( next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "" );
            next = next + strlen( next );
            attrmask2fieldlist( next, acct_attr_set, T_ACCT, TRUE, FALSE, "SUM( ", " )" );
            next = next + strlen( next );
            next += sprintf( next, " ,COUNT( id ) FROM " MAIN_TABLE " GROUP BY " );
            attrmask2fieldlist( next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "" ); 
            next = next + strlen( next );
            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to insert accounting field: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
        }
    }
    else
    {
        rc = db_list_table_fields( &conn, ACCT_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );
        if ( rc != DB_NOT_EXISTS )
        {
            /* /##\ Deactivate triggers /##\ */
            strcpy( strbuf, "DROP TABLE " ACCT_TABLE );
            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to drop table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " ACCT_TABLE " dropped sucessfully" );

        }
    }


    /*
     * ====== CREATE TRIGGERS ==========
     */

    //Trigger on insert
    strcpy( strbuf, "INSERT INTO " ACCT_TABLE "( " );
    next = strbuf + strlen( strbuf );
    attrmask2fieldlist( next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "" );
    next = next + strlen( next );
    attrmask2fieldlist( next, acct_attr_set, T_ACCT, TRUE, FALSE, "", "" );
    next = next + strlen( next );
    strcpy( next, ", count ) VALUES ( " );
    next = next + strlen( next );
    attrmask2fieldlist( next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "NEW.", "" );
    next = next + strlen( next );
    attrmask2fieldlist( next, acct_attr_set, T_ACCT, TRUE, FALSE, "NEW.", "" );
    next = next + strlen( next );
    strcpy( next, ", 1 ) ON DUPLICATE KEY UPDATE " );
    next = next + strlen( next );
    attrmask2fieldoperation( next, acct_attr_set, T_ACCT, "NEW.", ADD );
    next = next + strlen( next );
    strcpy( next,", count=count+1;" );
    next = next + strlen( next );
    
    rc = db_drop_trigger( &conn, "ENTRY_INSERT" );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Failed to drop trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }
    
    rc = db_create_trigger( &conn, "ENTRY_INSERT", "AFTER INSERT", MAIN_TABLE, strbuf );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Failed to create trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }
  
    //Trigger on delete
    strcpy( strbuf, "UPDATE " ACCT_TABLE " SET " );
    next = strbuf + strlen( strbuf );
    attrmask2fieldoperation( next, acct_attr_set, T_ACCT, "OLD.", SUBSTRACT );
    next = next + strlen( next );
    strcpy( next,", count=count-1 WHERE " );
    next = next + strlen( next );
    attrmask2fieldoperation( next, acct_pk_attr_set, T_ACCT, "OLD.", COMPARE );
    next = next + strlen( next );
    strcpy( next,";" );

    rc = db_drop_trigger( &conn, "ENTRY_DELETE" );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Failed to drop trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }

    rc = db_create_trigger( &conn, "ENTRY_DELETE", "BEFORE DELETE", MAIN_TABLE, strbuf );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Failed to create trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }
 
    /* database is not ready, we can close db_connection */
    db_close_conn( &conn );

    return 0;

}                               /* ListMgr_Init */



int ListMgr_InitAccess( lmgr_t * p_mgr )
{
    int            rc;

    rc = db_connect( &p_mgr->conn );

    if ( rc )
        return rc;

    p_mgr->last_commit = 0;
    p_mgr->force_commit = FALSE;

#ifdef _ENABLE_PREP_STMT
    /* initialize prepared statement cache */
    init_prep_stmt_cache( p_mgr );
#endif

    return 0;
}


int ListMgr_CloseAccess( lmgr_t * p_mgr )
{
    int            rc;

    /* force to commit queued requests */
    rc = lmgr_flush_commit( p_mgr );

#ifdef _ENABLE_PREP_STMT
    /* free prepared requests */
    destroy_statements( p_mgr );
#endif

    /* close connexion */
    db_close_conn( &p_mgr->conn );

    return rc;
}
