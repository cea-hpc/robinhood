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
#include "database.h"
#include "listmgr_common.h"
#include "RobinhoodLogs.h"
#include <stdio.h>

#define MAX_DB_FIELDS 64

#define APPEND_TXT( _n, _str ) do { strcpy( _n, _str );  _n = _n + strlen( _n ); } while (0)
#define INCR_NEXT( _n ) do { _n = _n + strlen( _n ); } while (0)

static inline int append_field_def( int i, char *next, int is_first, db_type_u *default_value )
{
    switch ( field_infos[i].db_type )
    {
    case DB_STRIPE_INFO:   /* never in main table (ignored) */
    case DB_STRIPE_ITEMS:
        break;
    case DB_TEXT:
        if ( field_infos[i].db_type_size < 256 )
        {
            if ( default_value )
                return sprintf( next, "%s %s VARCHAR(%u) DEFAULT '%s'",is_first ? "" : ",",
                    field_infos[i].field_name, field_infos[i].db_type_size, default_value->val_str );
            else
                return sprintf( next, "%s %s VARCHAR(%u)",is_first ? "" : ",",
                    field_infos[i].field_name, field_infos[i].db_type_size );
        }
        else
        {
            if ( default_value )
                return sprintf( next, "%s %s TEXT DEFAULT '%s'", is_first ? "" : ",", field_infos[i].field_name,
                    default_value->val_str );
            else
                return sprintf( next, "%s %s TEXT", is_first ? "" : ",", field_infos[i].field_name );
        }
        break;
    case DB_INT:
        if ( default_value )
            return sprintf( next, "%s %s INT DEFAULT %d", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_int );
        else
            return sprintf( next, "%s %s INT", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_UINT:
        if ( default_value )
            return sprintf( next, "%s %s INT UNSIGNED DEFAULT %u", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_uint );
        else
            return sprintf( next, "%s %s INT UNSIGNED", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_SHORT:
        if ( default_value )
            return sprintf( next, "%s %s SMALLINT DEFAULT %hd", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_short );
        else
            return sprintf( next, "%s %s SMALLINT", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_USHORT:
        if ( default_value )
            return sprintf( next, "%s %s SMALLINT UNSIGNED DEFAULT %hu", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_ushort );
        else
            return sprintf( next, "%s %s SMALLINT UNSIGNED", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_BIGINT:
        if ( default_value )
            return sprintf( next, "%s %s BIGINT DEFAULT %lld", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_bigint );
        else
            return sprintf( next, "%s %s BIGINT", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_BIGUINT:
        if ( default_value )
            return sprintf( next, "%s %s BIGINT UNSIGNED DEFAULT %llu", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_biguint );
        else
            return sprintf( next, "%s %s BIGINT UNSIGNED", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_BOOL:
        if ( default_value )
            return sprintf( next, "%s %s BOOLEAN DEFAULT %d", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_bool );
        else
            return sprintf( next, "%s %s BOOLEAN", is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_ID:
        return sprintf( next, "%s %s "PK_TYPE, is_first ? "" : ",", field_infos[i].field_name );
        break;
    case DB_ENUM_FTYPE:
        return sprintf( next, "%s %s ENUM('%s', '%s', '%s', '%s', '%s', '%s', '%s')", is_first ? "" : ",", field_infos[i].field_name, STR_TYPE_LINK, STR_TYPE_DIR, STR_TYPE_FILE, STR_TYPE_CHR, STR_TYPE_BLK, STR_TYPE_FIFO, STR_TYPE_SOCK );
        break;
    }
    return 0;
}

#define DROP_MESSAGE "\nyou should: 1)backup current DB using 'rbh-config backup_db' 2)empty the DB using 'rbh-config empty_db' 3)start a new FS scan."
#define DROP_ACCT_MSG "\nyou should: 1)stop robinhood commands 2)drop '"ACCT_TABLE"' table 3)restart robinhood."

/**
 * Check table fields.
 * @param i 
 * @param curr_field_index [in,out] filed index in currently checked schema
 * @return 0 on success
 * @return -1 on error
 */
static inline int check_field( int i, int * curr_field_index, char *table, char **fieldtab )
{
    if ( ( *curr_field_index >= MAX_DB_FIELDS )
        || ( fieldtab[*curr_field_index] == NULL ) )
    {
        if (!strcmp(table, ACCT_TABLE))
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema (missing field '%s' in table %s):" 
                        " "DROP_ACCT_MSG, field_infos[i].field_name, table );
        else
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema (missing field '%s' in table %s):" 
                        " "DROP_MESSAGE, field_infos[i].field_name, table );
        return -1;
    }
    /* check that this is the expected field */
    if ( !strcmp( field_infos[i].field_name, fieldtab[*curr_field_index] ) )
    {
        DisplayLog( LVL_FULL, LISTMGR_TAG, "%s OK", field_infos[i].field_name );
        (*curr_field_index)++;
        return 0;
    }
    else
    {
        DisplayLog( LVL_DEBUG, LISTMGR_TAG, "%s != %s",
                    field_infos[i].field_name, fieldtab[*curr_field_index] );
        if (!strcmp(table, ACCT_TABLE))
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema (unexpected field '%s' in table %s): "DROP_ACCT_MSG,
                        fieldtab[*curr_field_index], table );
        else
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema (unexpected field '%s' in table %s): "DROP_MESSAGE,
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
                        "Incompatible database schema (unexpected field '%s' in table %s): " DROP_MESSAGE,
                        fieldtab[curr_field_index], table );
            return 1;
        }
        else
            return 0; 
}

/**
 * @param op_subs replacement for 'FLOOR(LOG2(<prefix>.size)/5)' (eg. local variable)
 */
static unsigned int append_size_range_val(char * str, int leading_comma, char *prefix, const char * op_subs)
{
    unsigned int i, l;
    char value[128];
    l=0;

    if (op_subs && op_subs[0])
        strcpy(value, op_subs);
    else
        sprintf(value, ACCT_SZ_VAL("%ssize"), prefix);

    l = sprintf( str, "%s %ssize=0", leading_comma?",":"", prefix );
    for (i = 1; i < SZ_PROFIL_COUNT-1; i++) /* 2nd to before the last */
    {
        l += sprintf( str+l, ", IFNULL(%s=%u,0)", value, i-1 );
    }
    /* last */
    l += sprintf( str+l, ", IFNULL(%s>=%u,0)", value, i-1 );
    return l;
}

/**
 * @param op_subs replacement for 'FLOOR(LOG2(<prefix>.size)/5)' (eg. local variable)
 */
static unsigned int append_size_range_op(char * str, int leading_comma, char *prefix, 
                                         const char * op_subs, operation_type optype )
{
    unsigned int i, l;
    char value[128];
    const char * op = (optype == ADD)?"+":"-";
    l=0;

    if (op_subs && op_subs[0])
        strcpy(value, op_subs);
    else
        sprintf(value,  ACCT_SZ_VAL("%ssize"), prefix);

    l = sprintf( str, "%s %s=CAST(%s as SIGNED)%sCAST((%ssize=0) as SIGNED)", leading_comma?",":"",
                 sz_field[0], sz_field[0], op, prefix);

    for (i = 1; i < SZ_PROFIL_COUNT-1; i++) /* 2nd to before the last */
    {
        l += sprintf( str+l, ", %s=CAST(%s as SIGNED)%sCAST(IFNULL(%s=%u,0) as SIGNED)", sz_field[i], sz_field[i], op,
                      value, i-1 );
    }
    /* last */
    l += sprintf( str+l, ", %s=CAST(%s as SIGNED)%sCAST(IFNULL(%s>=%u,0) as SIGNED)", sz_field[i], sz_field[i],
                         op, value, i-1 );
    return l;
}



int            annex_table = FALSE;              /* indicates if an annex table is used */
int            acct_on_annex = FALSE;            /* indicates if acct info are is on annex table */
int            acct_on_main = FALSE;             /* indicates if acct info is on main table */

int ListMgr_Init( const lmgr_config_t * p_conf, int report_only )
{
    int            i, rc, is_first_field;
    db_conn_t      conn;
    char           errmsg_buf[1024];
    char           strbuf[4096];
    char          *next;
    char          *fieldtab[MAX_DB_FIELDS];
    char          *acct_info_table;
    db_type_u      default_val;

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

    if ( annex_table && ( lmgr_config.user_acct || lmgr_config.group_acct ) )
    {
        acct_on_annex = FALSE ;
        acct_on_main = FALSE ;
        acct_info_table = NULL;

        for ( i = 0; i < ATTR_COUNT; i++ )
        {
            if ( is_acct_field( i ) || is_acct_pk( i ) )
            {
                if ( is_annex_field( i ) )
                    acct_on_annex = TRUE;
                else if ( is_main_field( i ) )
                    acct_on_main = TRUE;
                else
                {
                    DisplayLog( LVL_CRIT, LISTMGR_TAG, "ERROR: Accounting field not in "MAIN_TABLE" or "ANNEX_TABLE" table" );
                    return -1;
                }
            } 
        }
        if ( acct_on_annex && acct_on_main )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "ERROR: Accounting info is on several tables" );
            return -1;
        }
        else if (acct_on_annex )
            acct_info_table = ANNEX_TABLE;
        else if (acct_on_main )
            acct_info_table = MAIN_TABLE;
        else
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "ERROR: Accounting info is not in "MAIN_TABLE" or "ANNEX_TABLE" table" );
            return -1;
        }
    }
    else
        acct_info_table = MAIN_TABLE;



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
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Fields (%s, %s) OK", fieldtab[0], fieldtab[1] );
        }
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        /* Note: allow creating this table in report_only mode because the command can set some values in this table */

        DisplayLog( LVL_EVENT, LISTMGR_TAG, VAR_TABLE " does not exist: creating it." );

        /* table does not exist */
        strcpy( strbuf, "CREATE TABLE " VAR_TABLE " ( "
                "varname VARCHAR(255) PRIMARY KEY, "
                "value TEXT )" );
#ifdef _MYSQL
        if (lmgr_config.db_config.innodb)
            strcat(strbuf, " ENGINE=InnoDB");
#endif

        DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

        rc = db_exec_sql( &conn, strbuf, NULL );
        if ( rc )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }

        DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " VAR_TABLE " created successfully" );
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
            DisplayLog( LVL_FULL, LISTMGR_TAG, "primary key (%s) OK", fieldtab[0] );
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
        if ( report_only )
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: "MAIN_TABLE" table does not exist" );
        }
        else
        {
            DisplayLog( LVL_EVENT, LISTMGR_TAG, MAIN_TABLE " does not exist: creating it." );

            /* table does not exist */
            strcpy( strbuf, "CREATE TABLE " MAIN_TABLE " ( id "PK_TYPE" PRIMARY KEY" );
            next = strbuf + strlen( strbuf );

            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_main_field( i ) )
                {
#ifdef ATTR_INDEX_status
                    if ( i == ATTR_INDEX_status )
                    {
                        default_val.val_int = 0;
                        next += append_field_def( i, next, 0, &default_val );
                    }
                    else
#endif
                    if ( i == ATTR_INDEX_owner )
                    {
                        default_val.val_str = ACCT_DEFAULT_OWNER;
                        next += append_field_def( i, next, 0, &default_val );
                    }
                    else if ( i == ATTR_INDEX_gr_name )
                    {
                        default_val.val_str = ACCT_DEFAULT_GROUP;
                        next += append_field_def( i, next, 0, &default_val );
                    }
                    else
                         next += append_field_def( i, next, 0, NULL );
                }
            }

            strcpy( next, ")" );

#ifdef _MYSQL
        if (lmgr_config.db_config.innodb)
            strcat(strbuf, " ENGINE=InnoDB");
#endif
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " MAIN_TABLE " created successfully" );

            /* create indexes on this table */
            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_main_field( i ) && is_indexed_field( i ) )
                {
                    sprintf( strbuf, "CREATE INDEX %s_index ON " MAIN_TABLE "(%s)",
                             field_infos[i].field_name, field_infos[i].field_name );

                    DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );

                    rc = db_exec_sql( &conn, strbuf, NULL );
                    if ( rc )
                    {
                        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                    "Failed to create index: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                        return rc;
                    }
                    DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on " MAIN_TABLE "(%s) created successfully",
                                field_infos[i].field_name );
                }
            }
        }
    }
    else
    {
        /* error */
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Error checking database schema: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }


    /*
     * ====== CHECKING NAMES TABLE ==========
     */

    rc = db_list_table_fields( &conn, DNAMES_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );

    if ( rc == DB_SUCCESS )
    {
        int            curr_field_index = 0;

        /* check primary key */
        if ( ( fieldtab[0] == NULL ) || strcmp( fieldtab[0], "id" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Invalid primary key (%s) for table "
                        DNAMES_TABLE, ( fieldtab[0] ? fieldtab[0] : "(null)" ) );
            return -1;
        }
        else
        {
            DisplayLog( LVL_FULL, LISTMGR_TAG, "primary key (%s) OK", fieldtab[0] );
            curr_field_index += 1;
        }


        for ( i = 0; i < ATTR_COUNT; i++ )
        {
            if ( is_names_field( i ) )
            {
                if ( check_field( i, &curr_field_index, DNAMES_TABLE, fieldtab ) )
                    return -1;
            }
        }

        /* is there any extra field ? */
        if ( has_extra_field( curr_field_index, DNAMES_TABLE, fieldtab ) )
            return -1;

    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( report_only )
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: "DNAMES_TABLE" table does not exist" );
        }
        else
        {
            DisplayLog( LVL_EVENT, LISTMGR_TAG, DNAMES_TABLE " does not exist: creating it." );

            /* table does not exist */
            //TODO wrong key - idem when checking above
            strcpy( strbuf, "CREATE TABLE " DNAMES_TABLE " ( id "PK_TYPE );
            next = strbuf + strlen( strbuf );

            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_names_field( i ) )
                {
                    next += append_field_def( i, next, 0, NULL );
                }
            }
            next += sprintf(next, ", PRIMARY KEY (parent_id, hname) )");

#ifdef _MYSQL
            if (lmgr_config.db_config.innodb)
                strcat(strbuf, " ENGINE=InnoDB");
#endif
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create table: Error: %s",
                            db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " DNAMES_TABLE " created successfully" );

            /* create indexes on this table */
            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_names_field( i ) && is_indexed_field( i ) )
                {
                    sprintf( strbuf, "CREATE INDEX %s_index ON " DNAMES_TABLE "(%s)",
                             field_infos[i].field_name, field_infos[i].field_name );

                    DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );

                    rc = db_exec_sql( &conn, strbuf, NULL );
                    if ( rc )
                    {
                        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                    "Failed to create index: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                        return rc;
                    }
                    DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on " DNAMES_TABLE "(%s) created successfully",
                                field_infos[i].field_name );
                }
            }
        }
    }
    else
    {
        /* error */
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Error checking database schema: %s",
                    db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }



#ifdef _LUSTRE
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
            DisplayLog( LVL_FULL, LISTMGR_TAG, "primary key (%s) OK", fieldtab[0] );
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
            DisplayLog( LVL_FULL, LISTMGR_TAG, "validator OK" );
            curr_field_index += 1;
        }

        if ( ( fieldtab[curr_field_index] == NULL )
             || strcmp( fieldtab[curr_field_index], "stripe_count" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema for table "
                        STRIPE_INFO_TABLE
                        " (missing field 'stripe_count'): "DROP_MESSAGE );
            return -1;
        }

        curr_field_index++;

        if ( ( fieldtab[curr_field_index] == NULL )
             || strcmp( fieldtab[curr_field_index], "stripe_size" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema for table "
                        STRIPE_INFO_TABLE
                        " (missing field 'stripesize'): "DROP_MESSAGE );
            return -1;
        }

        curr_field_index++;

        if ( ( fieldtab[curr_field_index] == NULL )
             || strcmp( fieldtab[curr_field_index], "pool_name" ) )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema for table "
                        STRIPE_INFO_TABLE
                        " (missing field 'pool_name'): "DROP_MESSAGE );
            return -1;
        }

        curr_field_index++;

        /* is there any extra field ? */
        if ( has_extra_field( curr_field_index, STRIPE_INFO_TABLE, fieldtab ) )
            return -1;

    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( report_only )
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: "STRIPE_INFO_TABLE" table does not exist" );
        }
        else
        {
            DisplayLog( LVL_EVENT, LISTMGR_TAG,
                        STRIPE_INFO_TABLE " table does not exist: creating it." );

            sprintf( strbuf,
                     "CREATE TABLE " STRIPE_INFO_TABLE
                     " (id "PK_TYPE" PRIMARY KEY, validator INT UNSIGNED, "
                     "stripe_count INT UNSIGNED, stripe_size INT UNSIGNED, pool_name VARCHAR(%u) )",
                     MAX_POOL_LEN - 1 );
#ifdef _MYSQL
        if (lmgr_config.db_config.innodb)
            strcat(strbuf, " ENGINE=InnoDB");
#endif
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " STRIPE_INFO_TABLE " created successfully" );
        }
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
            DisplayLog( LVL_FULL, LISTMGR_TAG, "index (%s) OK", fieldtab[0] );
            curr_field_index += 1;
        }

        if (( fieldtab[curr_field_index] == NULL )
            || strcmp( fieldtab[curr_field_index], "stripe_index" ))
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema for table "
                        STRIPE_ITEMS_TABLE": "DROP_MESSAGE );
            return -1;
        }
        else
        {
            DisplayLog( LVL_FULL, LISTMGR_TAG, "stripe_index OK" );
            curr_field_index += 1;
        }

        if (( fieldtab[curr_field_index] == NULL )
            || strcmp( fieldtab[curr_field_index], "ostidx" ))
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema for table "
                        STRIPE_ITEMS_TABLE": "DROP_MESSAGE );
            return -1;
        }
        else
        {
            DisplayLog( LVL_FULL, LISTMGR_TAG, "ostidx OK" );
            curr_field_index += 1;
        }

        if ( ( fieldtab[curr_field_index] == NULL )
             || strcmp( fieldtab[curr_field_index], "details" )
             || fieldtab[curr_field_index + 1] != NULL )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Incompatible database schema for table "
                        STRIPE_ITEMS_TABLE": "DROP_MESSAGE );
            return -1;
        }
        else
        {
            DisplayLog( LVL_FULL, LISTMGR_TAG, "details OK" );
        }

    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( report_only )
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: "STRIPE_ITEMS_TABLE" table does not exist" );
        }
        else
        {
            DisplayLog( LVL_EVENT, LISTMGR_TAG, STRIPE_ITEMS_TABLE
                        " table does not exist: creating it." );

            sprintf( strbuf,
                    "CREATE TABLE " STRIPE_ITEMS_TABLE
                    " ( id "PK_TYPE", stripe_index INT UNSIGNED, ostidx INT UNSIGNED, details BINARY(%u) )",
                    STRIPE_DETAIL_SZ );
#ifdef _MYSQL
            if (lmgr_config.db_config.innodb)
                strcat(strbuf, " ENGINE=InnoDB");
#endif
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " STRIPE_ITEMS_TABLE " created successfully" );

            strcpy( strbuf, "CREATE INDEX id_index ON " STRIPE_ITEMS_TABLE "(id)" );
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );
            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create index: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on " STRIPE_ITEMS_TABLE "(id) created successfully" );

            strcpy( strbuf, "CREATE INDEX st_index ON " STRIPE_ITEMS_TABLE "(ostidx)" );
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );
            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create index: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on " STRIPE_ITEMS_TABLE "(ostidx) created successfully" );
        }
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
                DisplayLog( LVL_FULL, LISTMGR_TAG, "primary key (%s) OK", fieldtab[0] );
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
            if ( report_only )
            {
                DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: "ANNEX_TABLE" table does not exist" );
            }
            else
            {
                DisplayLog( LVL_EVENT, LISTMGR_TAG, ANNEX_TABLE " does not exist: creating it." );

                /* table does not exist */
                strcpy( strbuf, "CREATE TABLE " ANNEX_TABLE " ( id "PK_TYPE" PRIMARY KEY" );
                next = strbuf + strlen( strbuf );

                for ( i = 0; i < ATTR_COUNT; i++ )
                {
                    if ( is_annex_field( i ) )
                    {
                        next += append_field_def( i, next, 0, NULL );
                    }
                }
                strcpy( next, " )" );
#ifdef _MYSQL
        if (lmgr_config.db_config.innodb)
            strcat(strbuf, " ENGINE=InnoDB");
#endif
                DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

                rc = db_exec_sql( &conn, strbuf, NULL );
                if ( rc )
                {
                    DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                "Failed to create table: Error: %s",
                                db_errmsg( &conn, errmsg_buf, 1024 ) );
                    return rc;
                }

                DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " ANNEX_TABLE " created successfully" );

                /* create indexes on this table */
                for ( i = 0; i < ATTR_COUNT; i++ )
                {
                    if ( is_annex_field( i ) && is_indexed_field( i ) )
                    {
                        sprintf( strbuf, "CREATE INDEX %s_index ON " ANNEX_TABLE "(%s)",
                                 field_infos[i].field_name, field_infos[i].field_name );

                        DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );

                        rc = db_exec_sql( &conn, strbuf, NULL );
                        if ( rc )
                        {
                            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                        "Failed to create index: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                            return rc;
                        }
                        DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on " ANNEX_TABLE "(%s) created successfully",
                                    field_infos[i].field_name );
                    }
                }
            }
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
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Fields (%s, %s, %s, %s) OK",
                        fieldtab[0], fieldtab[1], fieldtab[2], fieldtab[3] );
        }
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( report_only )
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: "SOFT_RM_TABLE" table does not exist" );
        }
        else
        {
            DisplayLog( LVL_EVENT, LISTMGR_TAG, SOFT_RM_TABLE " does not exist: creating it." );

            /* table does not exist */
            strcpy( strbuf, "CREATE TABLE " SOFT_RM_TABLE " ( "
                    "fid VARCHAR(" TOSTRING(DB_FID_LEN) ") PRIMARY KEY, "
                    "fullpath VARCHAR(1023), "
                    "soft_rm_time INT UNSIGNED, "
                    "real_rm_time INT UNSIGNED  )" );
#ifdef _MYSQL
        if (lmgr_config.db_config.innodb)
            strcat(strbuf, " ENGINE=InnoDB");
#endif
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " SOFT_RM_TABLE " created successfully" );

            strcpy( strbuf, "CREATE INDEX rm_time ON " SOFT_RM_TABLE "(real_rm_time)" );

            DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );

            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create index: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on " SOFT_RM_TABLE " created successfully" );
        }
    }
    else
    {
        /* error */
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "Error checking database schema: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
        return rc;
    }
#elif defined(_HSM_LITE)
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
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Fields (%s, %s, %s, %s, %s) OK",
                        fieldtab[0], fieldtab[1], fieldtab[2], fieldtab[3], fieldtab[4] );
        }
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if ( report_only )
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: "SOFT_RM_TABLE" table does not exist" );
        }
        else
        {
            DisplayLog( LVL_EVENT, LISTMGR_TAG, SOFT_RM_TABLE " does not exist: creating it." );

            /* table does not exist */
            strcpy( strbuf, "CREATE TABLE " SOFT_RM_TABLE " ( "
                    "fid VARCHAR(" TOSTRING(DB_FID_LEN) ") PRIMARY KEY, "
                    "fullpath VARCHAR(1023), "
                    "backendpath VARCHAR(1023), "
                    "soft_rm_time INT UNSIGNED, "
                    "real_rm_time INT UNSIGNED  )" );
#ifdef _MYSQL
        if (lmgr_config.db_config.innodb)
            strcat(strbuf, " ENGINE=InnoDB");
#endif
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " SOFT_RM_TABLE " created successfully" );

            strcpy( strbuf, "CREATE INDEX rm_time ON " SOFT_RM_TABLE "(real_rm_time)" );

            DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );

            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create index: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on " SOFT_RM_TABLE " created successfully" );
        }
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
     * ====== CHECKING ACCOUNTING TABLE ==========
     */
    /* Note: when running a report command, check ACCT format too */
    if ( lmgr_config.user_acct || lmgr_config.group_acct || report_only )
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
            if ( ( fieldtab[curr_field_index] == NULL ) || strcmp( fieldtab[curr_field_index], ACCT_FIELD_COUNT ) )
            { 
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Incompatible database schema (missing field '" ACCT_FIELD_COUNT  "' in table "
                            ACCT_TABLE"): "DROP_ACCT_MSG );
                return -1;
            }
            else
            {
                DisplayLog( LVL_FULL, LISTMGR_TAG, "%s OK", fieldtab[curr_field_index] );
                curr_field_index += 1;
            }

            /* check size range fields */
            /* based on log2(size/32) => 0 1 32 1K 32K 1M 32M 1G 32G 1T */
            for (i = 0; i < SZ_PROFIL_COUNT; i++)
            {
                if ((fieldtab[curr_field_index] == NULL) || strcmp(fieldtab[curr_field_index], sz_field[i]))
                {
                    DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                "Incompatible database schema (expected field '%s' at index #%u in table "ACCT_TABLE
                                "): "DROP_ACCT_MSG, sz_field[i], curr_field_index);
                    return -1;
                }
                else
                {
                    DisplayLog( LVL_FULL, LISTMGR_TAG, "%s OK", fieldtab[curr_field_index] );
                    curr_field_index += 1;
                }
            }

            if ( has_extra_field( curr_field_index, ACCT_TABLE, fieldtab ) )
                return -1;

        }
        else if ( rc == DB_NOT_EXISTS )
        {
            if (report_only)
            {
                /* report only and table does not exist: disabling acct options */
                lmgr_config.user_acct = FALSE;
                lmgr_config.group_acct = FALSE;
                /* reset acct masks */
                acct_pk_attr_set = 0;
                acct_attr_set = 0;

                DisplayLog( LVL_VERB, LISTMGR_TAG, "Accounting stats not available" );
            }
            else
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
                        next += append_field_def( i, next, is_first_acct_field, NULL );
                        is_first_acct_field = 0;
                    }
                }

                for ( i = 0; i < ATTR_COUNT; i++ )
                {
                    if ( is_acct_field( i ) )
                    {
                        next += append_field_def( i, next, is_first_acct_field, NULL );
                    } 
                }

                /* count field */
                strcpy ( next, ", " ACCT_FIELD_COUNT  " BIGINT UNSIGNED" );
                next = next + strlen( next );

                /* size range fields */
                for (i = 0; i < SZ_PROFIL_COUNT; i++)
                {
                    next += sprintf( next, ", %s BIGINT UNSIGNED DEFAULT 0", sz_field[i]);
                }

                /* PK definition */
                strcpy ( next, ", PRIMARY KEY ( " );
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

                strcpy( next, " ))" );
#ifdef _MYSQL
        if (lmgr_config.db_config.innodb)
            strcat(strbuf, " ENGINE=InnoDB");
#endif
                DisplayLog( LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf );

                rc = db_exec_sql( &conn, strbuf, NULL );
                if ( rc )
                {
                    DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                "Failed to create table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                    return rc;
                }

                DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " ACCT_TABLE " created successfully" );

                DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Populating accounting table from existing DB content. This can take a while..." );
                FlushLogs(); 

                /* Initial table population for already existing entries */
                next = strbuf;
                APPEND_TXT( next, "INSERT INTO " ACCT_TABLE "( " );
                attrmask2fieldlist( next, acct_pk_attr_set , T_ACCT, FALSE, FALSE, "", "" );
                INCR_NEXT( next );
                attrmask2fieldlist( next, acct_attr_set, T_ACCT, TRUE, FALSE, "", "" );
                INCR_NEXT( next );
                APPEND_TXT( next, ", " ACCT_FIELD_COUNT );
                next += append_size_range_fields(next, TRUE, "");
                APPEND_TXT( next, " ) SELECT " );
                attrmask2fieldlist( next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "" );
                INCR_NEXT( next );
                attrmask2fieldlist( next, acct_attr_set, T_ACCT, TRUE, FALSE, "SUM( ", " )" );
                INCR_NEXT( next );
                APPEND_TXT( next, " ,COUNT( id ), SUM(size=0)");
                for (i=1; i < SZ_PROFIL_COUNT-1; i++) /* 1 to 8 */
                        next += sprintf(next, ",SUM(IFNULL("ACCT_SZ_VAL("size")"=%u,0))", i-1);
                next += sprintf(next, ",SUM(IFNULL("ACCT_SZ_VAL("size")">=%u,0))", i-1);

                next += sprintf( next, " FROM %s  GROUP BY ", acct_info_table );
                attrmask2fieldlist( next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "" ); 
                next = next + strlen( next );

                /* set READ COMMITTED isolation level for the next (big!) request
                 * so locks can be released immediatly after the record is read */
                rc = db_transaction_level(&conn, TRANS_NEXT, TXL_READ_COMMITTED);
                if ( rc )
                    DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                "Failed to set READ_COMMITTED isolation level: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                    /* try to continue */

                rc = db_exec_sql( &conn, strbuf, NULL );
                if ( rc )
                {
                    DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                "Failed to populate accounting table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );

                    /* drop this table, to leave the db in a consistent state (if ACCT_TABLE exists, it must be populated */
                    if (db_exec_sql( &conn, "DROP TABLE " ACCT_TABLE, NULL ))
                        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                    "Failed to drop table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );

                    return rc;
                }
            }
        }
        else
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Error checking database schema: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }

        /*
         * ====== CREATE TRIGGERS ==========
         */

        if (!report_only) /* don't create triggers in report only mode */
        {
            //Trigger on insert
            next = strbuf;
            APPEND_TXT( next, "DECLARE val BIGINT UNSIGNED; "
                              "SET val="ACCT_SZ_VAL("NEW.size")";" );
            APPEND_TXT( next, "INSERT INTO " ACCT_TABLE "( " );
            attrmask2fieldlist( next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "" );
            INCR_NEXT( next );
            attrmask2fieldlist( next, acct_attr_set, T_ACCT, TRUE, FALSE, "", "" );
            INCR_NEXT( next );
            APPEND_TXT( next, ", " ACCT_FIELD_COUNT );
            next += append_size_range_fields(next, TRUE, "");
            APPEND_TXT( next, " ) VALUES ( " );
            attrmask2fieldlist( next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "NEW.", "" );
            INCR_NEXT( next );
            attrmask2fieldlist( next, acct_attr_set, T_ACCT, TRUE, FALSE, "NEW.", "" );
            INCR_NEXT( next );
            APPEND_TXT( next, ", 1");
            next += append_size_range_val(next, TRUE, "NEW.", "val");
            APPEND_TXT( next, " ) ON DUPLICATE KEY UPDATE " );
            attrmask2fieldoperation( next, acct_attr_set, T_ACCT, "NEW.", ADD );
            INCR_NEXT( next );
            APPEND_TXT( next,", " ACCT_FIELD_COUNT "=" ACCT_FIELD_COUNT "+1" );

            /* update size range values */
            next += append_size_range_op(next, TRUE, "NEW.", "val", ADD);

            APPEND_TXT( next,";");
            rc = db_drop_trigger( &conn, ACCT_TRIGGER_INSERT );
            if ( rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to drop " ACCT_TRIGGER_INSERT " trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            rc = db_create_trigger( &conn, ACCT_TRIGGER_INSERT, "AFTER INSERT", acct_info_table, strbuf );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create " ACCT_TRIGGER_INSERT " trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            DisplayLog( LVL_VERB, LISTMGR_TAG, "Trigger " ACCT_TRIGGER_INSERT " created successfully" );

            //Trigger on delete
            next = strbuf;
            APPEND_TXT( next, "DECLARE val BIGINT UNSIGNED; "
                              "SET val="ACCT_SZ_VAL("OLD.size")";");
            APPEND_TXT( next, "UPDATE " ACCT_TABLE " SET " );
            attrmask2fieldoperation( next, acct_attr_set, T_ACCT, "OLD.", SUBSTRACT );
            INCR_NEXT( next );
            APPEND_TXT( next,", " ACCT_FIELD_COUNT  "=" ACCT_FIELD_COUNT  "-1" );

            /* update size range values */
            next += append_size_range_op(next, TRUE, "OLD.", "val", SUBSTRACT);

            APPEND_TXT( next, " WHERE " );
            attrmask2fieldcomparison( next, acct_pk_attr_set, T_ACCT, "", "OLD.", "=", "AND" ); 
            INCR_NEXT( next );
            APPEND_TXT( next, ";" );

            rc = db_drop_trigger( &conn, ACCT_TRIGGER_DELETE );
            if ( rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to drop " ACCT_TRIGGER_DELETE " trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            rc = db_create_trigger( &conn, ACCT_TRIGGER_DELETE, "BEFORE DELETE", acct_info_table, strbuf );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create " ACCT_TRIGGER_DELETE " trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            DisplayLog( LVL_VERB, LISTMGR_TAG, "Trigger " ACCT_TRIGGER_DELETE " created successfully" );

            //Trigger on update
            /* if it is the same owner and group: */
            next = strbuf;
            APPEND_TXT( next, "DECLARE val_old, val_new BIGINT UNSIGNED;");
            APPEND_TXT( next, "SET val_old="ACCT_SZ_VAL("OLD.size")"; "
                              "SET val_new="ACCT_SZ_VAL("NEW.size")";" );
            APPEND_TXT( next, "\nIF " );
            /* generate comparison like NEW.owner=OLD.owner AND NEW.gr_name=OLD.gr_name */
            attrmask2fieldcomparison( next, acct_pk_attr_set, T_ACCT, "NEW.", "OLD.", "=", "AND" );
            INCR_NEXT( next );
            APPEND_TXT( next, "THEN \n\t IF " );
            /********* if one of the attribute value has changed: update the acct table *********/
            /* generate comparison like NEW.size<>=OLD.size OR NEW.blocks<>OLD.blocks */
            attrmask2fieldcomparison( next, acct_attr_set, T_ACCT, "NEW.", "OLD.", "<>", "OR" );
            INCR_NEXT( next );
            APPEND_TXT( next, "THEN \n\t\t UPDATE " ACCT_TABLE " SET " );
            is_first_field = 1;
            for ( i = 0; i < ATTR_COUNT; i++ )
            {
                if ( is_acct_field( i ) )
                {
                    if ( !is_first_field )
                        next += sprintf( next, ", %s=%s+(CAST(NEW.%s as SIGNED)-CAST(OLD.%s as SIGNED)) ",
                                         field_infos[i].field_name, field_infos[i].field_name,
                                         field_infos[i].field_name, field_infos[i].field_name );
                    else
                    {
                        next += sprintf( next, " %s=%s+(CAST(NEW.%s as SIGNED)-CAST(OLD.%s as SIGNED)) ",
                                         field_infos[i].field_name, field_infos[i].field_name,
                                         field_infos[i].field_name, field_infos[i].field_name );
                        is_first_field = 0;
                    }
                }
            }

            /* update size range values */
            next += sprintf( next, "%s%s=CAST(%s as SIGNED)-CAST(((OLD.size=0)+(NEW.size=0)) as SIGNED)",
                             is_first_field?" ":", ", sz_field[0], sz_field[0] );
            is_first_field = 0;
            for (i = 1; i < SZ_PROFIL_COUNT-1; i++) /* 2nd to before the last */
            {
                next += sprintf( next, ", %s=CAST(%s as SIGNED)-CAST((IFNULL(val_old=%u,0)+IFNULL(val_new=%u,0)) as SIGNED)",
                                 sz_field[i], sz_field[i], i-1, i-1 );
            }
            /* last */
            next += sprintf( next, ", %s=CAST(%s as SIGNED)-CAST((IFNULL(val_old>=%u,0)+IFNULL(val_new>=%u,0)) as SIGNED)",
                             sz_field[i], sz_field[i], i-1, i-1 );

            APPEND_TXT( next, " WHERE " );
            /* generate comparison as follows: owner=NEW.owner AND gr_name=NEW.gr_name */
            attrmask2fieldcomparison( next, acct_pk_attr_set, T_ACCT, "", "NEW.", "=", "AND" );
            INCR_NEXT( next );
            APPEND_TXT( next, "; \n\t END IF; \nELSEIF " );
            /********* else if the owner or group is different: add values to the new user or group and substract from the old one *********/
            attrmask2fieldcomparison( next, acct_pk_attr_set, T_ACCT, "NEW.", "OLD.", "<>", "OR" );
            INCR_NEXT( next );
            APPEND_TXT( next,  "THEN \n\tINSERT INTO " ACCT_TABLE "( " );
            /* generate fields as follows: owner, gr_name */
            attrmask2fieldlist( next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "" );
            INCR_NEXT( next );
            /* generate fields as follows: , size, blocks */
            attrmask2fieldlist( next, acct_attr_set, T_ACCT, TRUE, FALSE, "", "" );
            INCR_NEXT( next );
            APPEND_TXT( next, ", " ACCT_FIELD_COUNT );
            next += append_size_range_fields(next, TRUE, "");
            APPEND_TXT( next, " ) VALUES ( " );
            /* generate fields as follows: NEW.owner, NEW.gr_name */
            attrmask2fieldlist( next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "NEW.", "" );
            INCR_NEXT( next );
            attrmask2fieldlist( next, acct_attr_set, T_ACCT, TRUE, FALSE, "NEW.", "" );
            INCR_NEXT( next );
            APPEND_TXT( next, ", 1" );
            next += append_size_range_val(next, TRUE, "NEW.", "val_new");
            APPEND_TXT(next, " ) \n\tON DUPLICATE KEY UPDATE " );
            /* generate operations as follows: size=size+New.size, blocks=blocks+NEW.blocks */
            attrmask2fieldoperation( next, acct_attr_set, T_ACCT, "NEW.", ADD );
            INCR_NEXT( next );
            APPEND_TXT( next, ", " ACCT_FIELD_COUNT "=" ACCT_FIELD_COUNT  "+1" );
            /* update size range values */
            next += append_size_range_op(next, TRUE, "NEW.", "val_new", ADD);
            APPEND_TXT( next, ";" );

            APPEND_TXT( next, "\n\tUPDATE " ACCT_TABLE " SET " );
            /* generate operations as follows: size=size-Old.size, blocks=blocks-Old.blocks */
            attrmask2fieldoperation( next, acct_attr_set, T_ACCT, "OLD.", SUBSTRACT );
            INCR_NEXT( next );
            APPEND_TXT( next, ", " ACCT_FIELD_COUNT "=" ACCT_FIELD_COUNT "-1 " );
            next += append_size_range_op(next, TRUE, "OLD.", "val_old", SUBSTRACT);
            APPEND_TXT( next, " WHERE " );
            attrmask2fieldcomparison( next, acct_pk_attr_set, T_ACCT, "", "OLD.", "=", "AND" );
            INCR_NEXT( next );
            APPEND_TXT( next, ";\nEND IF;\n" );

            rc = db_drop_trigger( &conn, ACCT_TRIGGER_UPDATE );
            if ( rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to drop " ACCT_TRIGGER_UPDATE " trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            rc = db_create_trigger( &conn, ACCT_TRIGGER_UPDATE, "AFTER UPDATE", acct_info_table, strbuf );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to create " ACCT_TRIGGER_UPDATE " trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            DisplayLog( LVL_VERB, LISTMGR_TAG, "Trigger " ACCT_TRIGGER_UPDATE " created successfully" );
        }
    }
    //If accounting is disabled
    else /* don't drop triggers in report_only mode because it matches the previous "if" */
    {
        rc = db_list_table_fields( &conn, ACCT_TABLE, fieldtab, MAX_DB_FIELDS, strbuf, 4096 );
        if ( rc == DB_SUCCESS )
        {
            rc = db_drop_trigger( &conn, ACCT_TRIGGER_INSERT );
            if ( rc == DB_NOT_SUPPORTED )
            {
                DisplayLog( LVL_MAJOR, LISTMGR_TAG,
                            "Triggers are not supported with this database. "
                            "Not a big issue (wanted to disable it)" );
            }
            else if ( rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to drop " ACCT_TRIGGER_INSERT " trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            rc = db_drop_trigger( &conn, ACCT_TRIGGER_DELETE );
            if ( rc == DB_NOT_SUPPORTED )
            {
                DisplayLog( LVL_MAJOR, LISTMGR_TAG,
                            "Triggers are not supported with this database. "
                            "Not a big issue (wanted to disable it)" );
            }
            else if ( rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to drop " ACCT_TRIGGER_DELETE " trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            rc = db_drop_trigger( &conn, ACCT_TRIGGER_UPDATE );
            if ( rc == DB_NOT_SUPPORTED )
            {
                DisplayLog( LVL_MAJOR, LISTMGR_TAG,
                            "Triggers are not supported with this database. "
                            "Not a big issue (wanted to disable it)" );
            }
            else if ( rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to drop " ACCT_TRIGGER_UPDATE " trigger: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }

            strcpy( strbuf, "DROP TABLE " ACCT_TABLE );
            rc = db_exec_sql( &conn, strbuf, NULL );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Failed to drop table: Error: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
                return rc;
            }
            DisplayLog( LVL_VERB, LISTMGR_TAG, "Table " ACCT_TABLE " dropped successfully" );
            DisplayLog( LVL_EVENT, LISTMGR_TAG, "Accounting stats not activated" );

        }
        else if ( rc == DB_NOT_EXISTS )
        {
            DisplayLog( LVL_EVENT, LISTMGR_TAG, "Accounting stats not activated" );
        }
        else
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "Error checking database schema: %s", db_errmsg( &conn, errmsg_buf, 1024 ) );
            return rc;
        }
    }

    /* database is not ready, we can close db_connection */
    db_close_conn( &conn );

    return 0;

}                               /* ListMgr_Init */



int ListMgr_InitAccess( lmgr_t * p_mgr )
{
    int            rc, i;

    rc = db_connect( &p_mgr->conn );

    if ( rc )
        return rc;

    p_mgr->last_commit = 0;
    p_mgr->force_commit = FALSE;

    for (i = 0; i < OPCOUNT; i++)
        p_mgr->nbop[i] = 0;

    return 0;
}


int ListMgr_CloseAccess( lmgr_t * p_mgr )
{
    int            rc;

    /* force to commit queued requests */
    rc = lmgr_flush_commit( p_mgr );

    /* close connexion */
    db_close_conn( &p_mgr->conn );

    return rc;
}
