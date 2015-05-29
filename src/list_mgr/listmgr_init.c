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
#include "RobinhoodMisc.h"
#include <stdio.h>

/* exported symbols */
int            annex_table = FALSE;              /* indicates if an annex table is used */

/* global symbols */
static const char *acct_info_table = NULL;
static int report_only = FALSE;

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
        return sprintf( next, "%s %s ENUM('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                        is_first ? "" : ",", field_infos[i].field_name,
                        STR_TYPE_LINK, STR_TYPE_DIR, STR_TYPE_FILE, STR_TYPE_CHR,
                        STR_TYPE_BLK, STR_TYPE_FIFO, STR_TYPE_SOCK );
        break;
    }
    return 0;
}

#define DROP_MESSAGE "\nyou should: 1)backup current DB using 'rbh-config backup_db' 2)empty the DB using 'rbh-config empty_db' 3)start a new FS scan."
#define DROP_ACCT_MSG "\nyou should: 1)stop robinhood commands 2)drop '"ACCT_TABLE"' table 3)restart robinhood."

/**
 * Check table fields.
 * @param i
 * @param curr_field_index [in,out] field index in currently checked schema
 * @return 0 on success
 * @return -1 on error
 */
static int _check_field_name(const char *name, int *curr_field_index,
                            char *table, char **fieldtab)
{
    if ((*curr_field_index >= MAX_DB_FIELDS)
        || (fieldtab[*curr_field_index] == NULL))
    {
        if (!strcmp(table, ACCT_TABLE))
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Incompatible database schema (missing field '%s' in table %s):"
                       " "DROP_ACCT_MSG, name, table);
        else
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Incompatible database schema (missing field '%s' in table %s):"
                       " "DROP_MESSAGE, name, table);
        return -1;
    }
    /* check that this is the expected field */
    if (!strcmp(name, fieldtab[*curr_field_index]))
    {
        DisplayLog(LVL_FULL, LISTMGR_TAG, "%s OK", name);
        return 0;
    }
    else
    {
        DisplayLog(LVL_DEBUG, LISTMGR_TAG, "%s != %s", name,
                   fieldtab[*curr_field_index]);
        if (!strcmp(table, ACCT_TABLE))
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Incompatible database schema (unexpected field '%s' in table %s): "DROP_ACCT_MSG,
                       fieldtab[*curr_field_index], table);
        else
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Incompatible database schema (unexpected field '%s' in table %s): "DROP_MESSAGE,
                       fieldtab[*curr_field_index], table);
        return -1;
    }
}

static int check_field_name(const char *name, int *curr_field_index,
                            char *table, char **fieldtab)
{
    if (_check_field_name(name,curr_field_index,table,fieldtab) == 0)
    {
        (*curr_field_index)++;
        return 0;
    }
    return -1;
}

static void drop_chars(char *str, int start_off, int end_off)
{
    /* drop len chars */
    int len = (end_off - start_off + 1);
    char *c;

    for (c = str+start_off; *(c+len) != '\0'; c++)
        *c = *(c+len);

    *c = '\0';
}

static int check_type(const char *db_type, const char *expected)
{
    /* convert "int(10)" to "int",
     *         "smallint(5)" to "smallint",
     *         "bigint(20)" to bigint" ...
     */
    char tmp[1024];
    char *w1, *w2;

    rh_strncpy(tmp, db_type, sizeof(tmp));

    /* convert to upper case */
    upperstr(tmp);

    /* remove parenthesis */
    if ((w1 = strstr(tmp, "INT(")) != NULL)
    {
        /* move w1 to '(' */
        w1 += 3;
        w2 = strchr(w1, ')');
        if (w2 != NULL)
            drop_chars(tmp, (w1 - tmp), (w2 - tmp));
    }

    if (strcmp(tmp, expected))
    {
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "DB type '%s' doesn't match the expected type '%s'",
                   tmp, expected);
        return 1;
    }

    return 0;
}

static int convert_field_type(db_conn_t *pconn, const char *table,
                              const char *field, const char *type)
{
    char query[1024];
    char t1[128];
    char t2[128];
    char timestr[256] = "";
    int rc;
    lmgr_t lmgr;
    uint64_t count = 0;
    time_t estimated_max = 0;

    lmgr.conn = *pconn;

    /* get entry count for estimating conversion time (5-10s/million entries) */
    rc = ListMgr_EntryCount(&lmgr, &count);
    if (rc == DB_SUCCESS)
    {
        /* set max to 1 in case count is small, which results in 0s-1s frame */
        estimated_max = 1+(count/100000); /* 10s/1000000 */
        /* min is half of it */
        snprintf(timestr, sizeof(timestr), " (estimated duration: %s-%s)",
                 FormatDurationFloat(t1, sizeof(t1), estimated_max/2),
                 FormatDurationFloat(t2, sizeof(t2), estimated_max));
    }

    snprintf(query, sizeof(query), "ALTER TABLE %s MODIFY COLUMN %s %s", table, field, type);

    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Converting type of %s.%s to '%s'...%s",
               table, field, type, timestr);
    rc = db_exec_sql(pconn, query, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to run database conversion: Error: %s",
                    db_errmsg(pconn, query, sizeof(query)));
        return rc;
    }
    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "%s.%s successfully converted", table, field);
    return 0;
}

/** @return -1 on error, 0 if OK, 1 if conversion is required */
static int check_field_name_type(const char *name, const char *type, int *curr_field_index,
                                 char *table, char **fieldtab, char **typetab)
{
    if (_check_field_name(name, curr_field_index, table, fieldtab) != 0)
        return -1;

    if (check_type(typetab[*curr_field_index], type))
        return 1;

    (*curr_field_index)++;
    return 0;
}


static inline int check_field(int i, int * curr_field_index, char *table, char **fieldtab)
{
    return check_field_name(field_infos[i].field_name, curr_field_index, table, fieldtab);
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

/** check if this mode uses an annex table */
static inline int have_annex_table(void)
{
    int i;
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (field_infos[i].flags & ANNEX_INFO)
            return TRUE;
    }
    return FALSE;
}

/**
 * Check what tables are used as source for accounting.
 * Return the main source table for accounting.
 */
static const char *acct_table(void)
{
    const char *src_table = NULL;
    int is_annex = FALSE;
    int is_main = FALSE;

    if (lmgr_config.user_acct || lmgr_config.group_acct)
    {
        int i;
        for (i = 0; i < ATTR_COUNT; i++)
        {
            if (is_acct_field(i) || is_acct_pk(i))
            {
                if (is_annex_field(i))
                    is_annex = TRUE;
                else if (is_main_field(i))
                    is_main = TRUE;
                else
                    /* BUG */
                    RBH_BUG("Accounting field not in "MAIN_TABLE
                            " or "ANNEX_TABLE" table");
            }
        }

        if (is_annex && is_main)
        {
            RBH_BUG("Accounting info is on several tables (unsupported)");
        }
        else if (is_main)
            src_table = MAIN_TABLE;
        else if (is_annex)
            src_table = ANNEX_TABLE;
        else
            /* BUG */
            RBH_BUG("Accounting info is not in "MAIN_TABLE
                    " or "ANNEX_TABLE" table");
    }
    return src_table;
}

/** generic type for check_table/create_table functions */
typedef int (*check_create_tab_func_t)(db_conn_t *);

static int check_table_vars(db_conn_t *pconn)
{
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];

    int rc = db_list_table_fields(pconn, VAR_TABLE, fieldtab, MAX_DB_FIELDS,
                                  strbuf, sizeof(strbuf));
    if (rc == DB_SUCCESS)
    {
        int curr_index = 0;
        /* check fields */
        if (check_field_name("varname", &curr_index, VAR_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        if (check_field_name("value", &curr_index, VAR_TABLE, fieldtab))
            return DB_BAD_SCHEMA;

        if (has_extra_field(curr_index, VAR_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
    }
    else if (rc != DB_NOT_EXISTS)
    {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Error checking database schema: %s",
                       db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return rc;
}

static int create_table_vars(db_conn_t *pconn)
{
    char strbuf[4096];
    int rc;

    /* table does not exist */
    strcpy(strbuf, "CREATE TABLE "VAR_TABLE" ("
           "varname VARCHAR(255) PRIMARY KEY, "
           "value TEXT)");
#ifdef _MYSQL
    strcat(strbuf, " ENGINE=");
    strcat(strbuf, lmgr_config.db_config.engine);
#endif
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create table: Error: %s",
                    db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Table "VAR_TABLE" created successfully");
    return DB_SUCCESS;
}

static int check_table_main(db_conn_t *pconn)
{
    char strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];
    int rc = db_list_table_fields(pconn, MAIN_TABLE, fieldtab, MAX_DB_FIELDS,
                                  strbuf, sizeof(strbuf));
    if (rc == DB_SUCCESS)
    {
        int i;
        int curr_field_index = 0;

        /* check primary key */
        if (check_field_name("id", &curr_field_index, MAIN_TABLE, fieldtab))
            return DB_BAD_SCHEMA;

        for (i = 0; i < ATTR_COUNT; i++)
        {
            if (is_main_field(i) && !is_funcattr(i))
            {
                if (check_field(i, &curr_field_index, MAIN_TABLE, fieldtab))
                    return DB_BAD_SCHEMA;
            }
        }

        /* is there any extra field ? */
        if (has_extra_field(curr_field_index, MAIN_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
    }
    else if (rc != DB_NOT_EXISTS)
    {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Error checking database schema: %s",
                       db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return rc;
}

static int create_table_main(db_conn_t *pconn)
{
    char        strbuf[4096];
    char       *next;
    int         i, rc;
    db_type_u   default_val;

    strcpy(strbuf, "CREATE TABLE "MAIN_TABLE" (id "PK_TYPE" PRIMARY KEY");
    next = strbuf + strlen(strbuf);

    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_main_field(i) && !is_funcattr(i))
        {
#ifdef ATTR_INDEX_status
            if (i == ATTR_INDEX_status)
            {
                default_val.val_int = 0;
                next += append_field_def(i, next, 0, &default_val);
            }
            else
#endif
            if (i == ATTR_INDEX_owner)
            {
                default_val.val_str = ACCT_DEFAULT_OWNER;
                next += append_field_def(i, next, 0, &default_val);
            }
            else if (i == ATTR_INDEX_gr_name)
            {
                default_val.val_str = ACCT_DEFAULT_GROUP;
                next += append_field_def(i, next, 0, &default_val);
            }
            else
                 next += append_field_def(i, next, 0, NULL);
        }
    }
    strcpy(next, ")");

    #ifdef _MYSQL
    strcat(strbuf, " ENGINE=");
    strcat(strbuf, lmgr_config.db_config.engine);
    #endif
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create table: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Table "MAIN_TABLE" created successfully");

    /* create indexes on this table */
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_main_field(i) && is_indexed_field(i))
        {
            sprintf(strbuf, "CREATE INDEX %s_index ON " MAIN_TABLE "(%s)",
                    field_infos[i].field_name, field_infos[i].field_name);

            DisplayLog(LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s",
                       strbuf);

            rc = db_exec_sql(pconn, strbuf, NULL);
            if (rc)
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG,
                           "Failed to create index: Error: %s",
                           db_errmsg(pconn, strbuf, sizeof(strbuf)));
                return rc;
            }
            DisplayLog(LVL_VERB, LISTMGR_TAG, "Index on "MAIN_TABLE"(%s) created successfully",
                       field_infos[i].field_name);
        }
    }
    return DB_SUCCESS;
}

static int check_table_dnames(db_conn_t *pconn)
{
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];
    int   rc = db_list_table_fields(pconn, DNAMES_TABLE, fieldtab,
                                    MAX_DB_FIELDS, strbuf, sizeof(strbuf));

    if (rc == DB_SUCCESS)
    {
        int i;
        int curr_field_index = 0;

        /* check first fields: id and pkn */
        if (check_field_name("id", &curr_field_index, DNAMES_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        if (check_field_name("pkn", &curr_field_index, DNAMES_TABLE, fieldtab))
            return DB_BAD_SCHEMA;

        for (i = 0; i < ATTR_COUNT; i++)
        {
            if (is_names_field(i) && !is_funcattr(i))
            {
                if (check_field(i, &curr_field_index, DNAMES_TABLE, fieldtab))
                    return DB_BAD_SCHEMA;
            }
        }
        /* is there any extra field ? */
        if (has_extra_field(curr_field_index, DNAMES_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
    }
    else if (rc != DB_NOT_EXISTS)
    {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Error checking database schema: %s",
                       db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return rc;
}

static int create_table_dnames(db_conn_t *pconn)
{
    char        strbuf[4096];
    char       *next;
    int         i, rc;

    /* table does not exist */
    strcpy(strbuf, "CREATE TABLE "DNAMES_TABLE" (id "PK_TYPE", "
                   "pkn VARCHAR(40) PRIMARY KEY");
    next = strbuf + strlen(strbuf);

    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_names_field(i) && !is_funcattr(i))
        {
            next += append_field_def(i, next, 0, NULL);
        }
    }
    strcpy(next, ")");

#ifdef _MYSQL
    strcat(strbuf, " ENGINE=");
    strcat(strbuf, lmgr_config.db_config.engine);
#endif
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create table: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Table "DNAMES_TABLE" created successfully");

    /* create indexes on this table */
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_names_field(i) && is_indexed_field(i))
        {
            sprintf(strbuf, "CREATE INDEX %s_index ON " DNAMES_TABLE "(%s)",
                    field_infos[i].field_name, field_infos[i].field_name);

            DisplayLog(LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf);

            rc = db_exec_sql(pconn, strbuf, NULL);
            if (rc)
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG,
                           "Failed to create index: Error: %s",
                           db_errmsg(pconn, strbuf, sizeof(strbuf)));
                return rc;
            }
            DisplayLog(LVL_VERB, LISTMGR_TAG, "Index on "DNAMES_TABLE"(%s) created successfully",
                       field_infos[i].field_name);
        }
    }

    /* this index is needed to build the fullpath of entries */
    rc = db_exec_sql(pconn, "CREATE INDEX id_index ON "DNAMES_TABLE"(id)", NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create index: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog( LVL_VERB, LISTMGR_TAG, "Index on "DNAMES_TABLE"(id) created successfully");
    return DB_SUCCESS;
}

static int check_table_annex(db_conn_t *pconn)
{
    int rc, i;
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];

    if (!annex_table)
        return DB_SUCCESS;

    rc = db_list_table_fields(pconn, ANNEX_TABLE, fieldtab, MAX_DB_FIELDS,
                              strbuf, sizeof(strbuf));

    if (rc == DB_SUCCESS)
    {
        int curr_field_index = 0;

        /* check primary key */
        if (check_field_name("id", &curr_field_index, ANNEX_TABLE, fieldtab))
            return DB_BAD_SCHEMA;

        for (i = 0; i < ATTR_COUNT; i++)
        {
            if (is_annex_field(i) && !is_funcattr(i))
            {
                if (check_field(i, &curr_field_index, ANNEX_TABLE, fieldtab))
                    return DB_BAD_SCHEMA;
            }
        }

        /* is there any extra field ? */
        if ( has_extra_field(curr_field_index, ANNEX_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
    }
    else if (rc != DB_NOT_EXISTS)
    {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Error checking database schema: %s",
                       db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return rc;
}

static int create_table_annex(db_conn_t *pconn)
{
    char        strbuf[4096];
    char       *next;
    int         i, rc;

    if (!annex_table)
        return DB_SUCCESS;

    strcpy( strbuf, "CREATE TABLE "ANNEX_TABLE" (id "PK_TYPE" PRIMARY KEY");
    next = strbuf + strlen(strbuf);

    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_annex_field(i) && !is_funcattr(i))
        {
            next += append_field_def(i, next, 0, NULL);
        }
    }
    strcpy( next, ")" );
#ifdef _MYSQL
    strcat(strbuf, " ENGINE=");
    strcat(strbuf, lmgr_config.db_config.engine);
#endif
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create table: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Table "ANNEX_TABLE" created successfully");

    /* create indexes on this table */
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_annex_field(i) && is_indexed_field(i))
        {
            sprintf( strbuf, "CREATE INDEX %s_index ON " ANNEX_TABLE "(%s)",
                     field_infos[i].field_name, field_infos[i].field_name );
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf );

            rc = db_exec_sql(pconn, strbuf, NULL);
            if (rc)
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG,
                           "Failed to create index: Error: %s",
                           db_errmsg(pconn, strbuf, sizeof(strbuf)));
                return rc;
            }
            DisplayLog(LVL_VERB, LISTMGR_TAG, "Index on "ANNEX_TABLE"(%s) created successfully",
                       field_infos[i].field_name);
        }
    }
    return DB_SUCCESS;
}

#ifdef _LUSTRE
static int check_table_stripe_info(db_conn_t *pconn)
{
    int rc;
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];
    char *typetab[MAX_DB_FIELDS];

    rc = db_list_table_types(pconn, STRIPE_INFO_TABLE, fieldtab, typetab,
                              MAX_DB_FIELDS, strbuf, sizeof(strbuf));
    if (rc == DB_SUCCESS)
    {
        int curr_field_index = 0;

        /* check primary key */
        if (check_field_name("id", &curr_field_index, STRIPE_INFO_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        /* compat with 2.5.3- */
        switch(check_field_name_type("validator", "INT", &curr_field_index, STRIPE_INFO_TABLE,
                                     fieldtab, typetab))
        {
            case -1: return DB_BAD_SCHEMA;
            case 1:
                DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Detected type change for "
                           STRIPE_INFO_TABLE".validator (<= 2.5.3): running conversion");
                /* run type conversion */
                rc = convert_field_type(pconn, STRIPE_INFO_TABLE, "validator", "INT");
                if (rc)
                    return rc;
                curr_field_index ++;
                break;
            case 0: /* OK */
                break;
        }
        if (check_field_name("stripe_count", &curr_field_index, STRIPE_INFO_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        if (check_field_name("stripe_size", &curr_field_index, STRIPE_INFO_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        if (check_field_name("pool_name", &curr_field_index, STRIPE_INFO_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        /* is there any extra field ? */
        if (has_extra_field(curr_field_index, STRIPE_INFO_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
    }
    else if (rc != DB_NOT_EXISTS)
    {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Error checking database schema: %s",
                       db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return rc;
}

static int create_table_stripe_info(db_conn_t *pconn)
{
    char strbuf[4096];
    int  rc;

    sprintf(strbuf,
            "CREATE TABLE " STRIPE_INFO_TABLE
            " (id "PK_TYPE" PRIMARY KEY, validator INT, "
            "stripe_count INT UNSIGNED, stripe_size INT UNSIGNED, pool_name VARCHAR(%u))",
            MAX_POOL_LEN - 1);
#ifdef _MYSQL
    strcat(strbuf, " ENGINE=");
    strcat(strbuf, lmgr_config.db_config.engine);
#endif
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create table: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Table "STRIPE_INFO_TABLE" created successfully");
    return DB_SUCCESS;
}

static int check_table_stripe_items(db_conn_t *pconn)
{
    int rc;
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];

    rc = db_list_table_fields(pconn, STRIPE_ITEMS_TABLE, fieldtab,
                              MAX_DB_FIELDS, strbuf, sizeof(strbuf));

    if (rc == DB_SUCCESS)
    {
        int curr_field_index = 0;

        /* check index */
        if (check_field_name("id", &curr_field_index, STRIPE_ITEMS_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        if (check_field_name("stripe_index", &curr_field_index, STRIPE_ITEMS_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        if (check_field_name("ostidx", &curr_field_index, STRIPE_ITEMS_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        if (check_field_name("details", &curr_field_index, STRIPE_ITEMS_TABLE, fieldtab))
            return DB_BAD_SCHEMA;

        /* is there any extra field ? */
        if (has_extra_field(curr_field_index, STRIPE_ITEMS_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
    }
    else if (rc != DB_NOT_EXISTS)
    {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Error checking database schema: %s",
                       db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return rc;
}

static int create_table_stripe_items(db_conn_t *pconn)
{
    char strbuf[4096];
    int  rc;

    sprintf(strbuf, "CREATE TABLE "STRIPE_ITEMS_TABLE
            " (id "PK_TYPE", stripe_index INT UNSIGNED, ostidx INT UNSIGNED, details BINARY(%u))",
            STRIPE_DETAIL_SZ);
#ifdef _MYSQL
    strcat(strbuf, " ENGINE=");
    strcat(strbuf, lmgr_config.db_config.engine);
#endif
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create table: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Table "STRIPE_ITEMS_TABLE" created successfully");

    strcpy(strbuf, "CREATE INDEX id_index ON "STRIPE_ITEMS_TABLE"(id)");
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf);
    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create index: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Index on "STRIPE_ITEMS_TABLE"(id) created successfully");

    strcpy(strbuf, "CREATE INDEX st_index ON "STRIPE_ITEMS_TABLE"(ostidx)");
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf);
    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create index: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Index on "STRIPE_ITEMS_TABLE"(ostidx) created successfully");
    return DB_SUCCESS;
}
#endif

static void disable_acct(void)
{
    lmgr_config.user_acct = FALSE;
    lmgr_config.group_acct = FALSE;
    /* reset acct masks */
    acct_pk_attr_set = 0;
    acct_attr_set = 0;
}

static int check_table_acct(db_conn_t *pconn)
{
    int i, rc;
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];

    rc = db_list_table_fields(pconn, ACCT_TABLE, fieldtab, MAX_DB_FIELDS,
                              strbuf, sizeof(strbuf));
    if (rc == DB_SUCCESS)
    {
        int curr_field_index = 0;

        /* When running daemon mode with accounting disabled: drop ACCT table,
         * else it may become inconsistent. */
        if (!lmgr_config.user_acct && !lmgr_config.group_acct && !report_only)
        {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                       "Accounting is disabled: dropping table "ACCT_TABLE);

            rc = db_drop_component(pconn, DBOBJ_TABLE, ACCT_TABLE);
            if (rc != DB_SUCCESS)
                DisplayLog(LVL_CRIT, LISTMGR_TAG,
                           "Failed to drop table: Error: %s",
                           db_errmsg(pconn, strbuf, sizeof(strbuf)));

            return rc;
        }

        /* check primary key */
        for (i = 0; i < ATTR_COUNT; i++)
        {
            if (is_acct_pk(i))
            {
                if (check_field(i, &curr_field_index, ACCT_TABLE, fieldtab))
                    return DB_BAD_SCHEMA;
            }
        }
        /* check other fields */
        for (i = 0; i < ATTR_COUNT; i++)
        {
            if (is_acct_field(i))
            {
                if (check_field(i, &curr_field_index, ACCT_TABLE, fieldtab))
                    return DB_BAD_SCHEMA;
            }
        }
        /* check count field*/
        if (check_field_name(ACCT_FIELD_COUNT, &curr_field_index, ACCT_TABLE, fieldtab))
            return DB_BAD_SCHEMA;

        /* check size range fields */
        /* based on log2(size/32) => 0 1 32 1K 32K 1M 32M 1G 32G 1T */
        for (i = 0; i < SZ_PROFIL_COUNT; i++)
        {
            if (check_field_name(sz_field[i], &curr_field_index, ACCT_TABLE, fieldtab))
                return DB_BAD_SCHEMA;
        }

        if (has_extra_field(curr_field_index, ACCT_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        if (report_only)
        {
            /* report only: remember there is no ACCT table and don't warn */
            DisplayLog(LVL_VERB, LISTMGR_TAG, "Accounting stats not available");
            disable_acct();
            return DB_SUCCESS;
        }
    }
    else
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Error checking database schema: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return rc;
}

static int populate_acct_table(db_conn_t *pconn)
{
    char strbuf[4096];
    char *next = strbuf;
    int i, rc;

    if (acct_info_table == NULL)
        RBH_BUG("Can't populate "ACCT_TABLE" with no source table");

    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Populating accounting table from existing DB content."
               " This can take a while...");
    FlushLogs();

    /* Initial table population for already existing entries */
    APPEND_TXT(next, "INSERT INTO "ACCT_TABLE"(");
    attrmask2fieldlist(next, acct_pk_attr_set , T_ACCT, FALSE, FALSE, "", "");
    INCR_NEXT(next);
    attrmask2fieldlist(next, acct_attr_set, T_ACCT, TRUE, FALSE, "", "");
    INCR_NEXT(next);
    APPEND_TXT(next, ", "ACCT_FIELD_COUNT);
    next += append_size_range_fields(next, TRUE, "");
    APPEND_TXT(next, ") SELECT ");
    attrmask2fieldlist(next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "");
    INCR_NEXT(next);
    attrmask2fieldlist(next, acct_attr_set, T_ACCT, TRUE, FALSE, "SUM(", ")");
    INCR_NEXT(next);
    APPEND_TXT(next, " ,COUNT(id), SUM(size=0)");
    for (i = 1; i < SZ_PROFIL_COUNT-1; i++) /* 1 to 8 */
            next += sprintf(next, ",SUM(IFNULL("ACCT_SZ_VAL("size")"=%u,0))", i-1);
    next += sprintf(next, ",SUM(IFNULL("ACCT_SZ_VAL("size")">=%u,0))", i-1);

    next += sprintf(next, " FROM %s  GROUP BY ", acct_info_table);
    attrmask2fieldlist(next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "");
    next = next + strlen(next);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to populate accounting table: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));

        /* drop this table, to leave the db in a consistent state (if ACCT_TABLE exists, it must be populated) */
        if (db_drop_component(pconn, DBOBJ_TABLE, ACCT_TABLE))
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Failed to drop table: Error: %s",
                       db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return rc;
}

static int create_table_acct(db_conn_t *pconn)
{
    char strbuf[4096];
    int i, rc;
    char *next;
    int first_acct_pk = TRUE;
    int is_first_acct_field = TRUE;

    if (!lmgr_config.user_acct & !lmgr_config.group_acct)
        return DB_SUCCESS;

    /* table does not exist */
    strcpy(strbuf, "CREATE TABLE "ACCT_TABLE" (");
    next = strbuf + strlen(strbuf);

    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_acct_pk(i))
        {
            next += append_field_def(i, next, is_first_acct_field, NULL);
            is_first_acct_field = FALSE;
        }
    }

    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_acct_field(i))
        {
            next += append_field_def(i, next, is_first_acct_field, NULL);
        }
    }

    /* count field */
    strcpy (next, ", " ACCT_FIELD_COUNT  " BIGINT UNSIGNED");
    next = next + strlen(next);

    /* size range fields */
    for (i = 0; i < SZ_PROFIL_COUNT; i++)
    {
        next += sprintf(next, ", %s BIGINT UNSIGNED DEFAULT 0", sz_field[i]);
    }

    /* PK definition */
    strcpy (next, ", PRIMARY KEY ( ");
    next = next + strlen(next);

    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_acct_pk(i))
        {
            if (!first_acct_pk )
                next += sprintf(next, ", %s", field_infos[i].field_name);
            else
            {
                next += sprintf(next, "%s", field_infos[i].field_name);
                first_acct_pk = FALSE;
            }
        }
    }
    strcpy(next,"))");
#ifdef _MYSQL
    strcat(strbuf, " ENGINE=");
    strcat(strbuf, lmgr_config.db_config.engine);
#endif
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create table: Error: %s",
                    db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Table "ACCT_TABLE" created successfully");

    /* now populate it */
    return populate_acct_table(pconn);
}

#ifdef HAVE_RM_POLICY
static int check_table_softrm(db_conn_t *pconn)
{
    int rc;
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];

    rc = db_list_table_fields(pconn, SOFT_RM_TABLE, fieldtab, MAX_DB_FIELDS,
                              strbuf, sizeof(strbuf));
    if (rc == DB_SUCCESS)
    {
        int curr_index = 0;

        if (check_field_name("fid", &curr_index, SOFT_RM_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        if (check_field_name("fullpath", &curr_index, SOFT_RM_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
#ifndef _LUSTRE_HSM
        if (check_field_name("backendpath", &curr_index, SOFT_RM_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
#endif
        if (check_field_name("soft_rm_time", &curr_index, SOFT_RM_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        if (check_field_name("real_rm_time", &curr_index, SOFT_RM_TABLE, fieldtab))
            return DB_BAD_SCHEMA;

        if (has_extra_field(curr_index, SOFT_RM_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
    }
    else if (rc != DB_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Error checking database schema: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return rc;
}

static int create_table_softrm(db_conn_t *pconn)
{
    char strbuf[4096];
    int rc;

    /* table does not exist */
    sprintf(strbuf, "CREATE TABLE "SOFT_RM_TABLE" ("
            "fid "PK_TYPE" PRIMARY KEY, "
            "fullpath VARCHAR(%u), "
#ifndef _LUSTRE_HSM
            "backendpath VARCHAR(%u), "
#endif
            "soft_rm_time INT UNSIGNED, "
            "real_rm_time INT UNSIGNED)",
             field_infos[ATTR_INDEX_fullpath].db_type_size
#ifndef _LUSTRE_HSM
             , field_infos[ATTR_INDEX_backendpath].db_type_size
#endif
           );
#ifdef _MYSQL
    strcat(strbuf, " ENGINE=");
    strcat(strbuf, lmgr_config.db_config.engine);
#endif
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", strbuf);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create table: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Table "SOFT_RM_TABLE" created successfully");

    /* main use case is to sort by real_rm_time */
    strcpy(strbuf, "CREATE INDEX rm_time ON " SOFT_RM_TABLE "(real_rm_time)");
    DisplayLog(LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", strbuf);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create index: Error: %s",
                   strbuf);
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Index on "SOFT_RM_TABLE" created successfully");
    return DB_SUCCESS;
}
#endif


#define VERSION_VAR_FUNC    "VersionFunctionSet"
#define VERSION_VAR_TRIG    "VersionTriggerSet"

#define FUNCTIONSET_VERSION    "1.1"
#define TRIGGERSET_VERSION     "1.1"

static int check_functions_version(db_conn_t *conn)
{
    int rc;
    char val[1024];

    /* check the functions version */
    rc = lmgr_get_var(conn, VERSION_VAR_FUNC, val);
    if (rc == DB_SUCCESS)
    {
        if (strcmp(val, FUNCTIONSET_VERSION))
        {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Wrong functions version (in DB: %s, expected: %s). %s.",
                       val, FUNCTIONSET_VERSION, report_only?"Reports output might be incorrect":
                            "Existing functions will be dropped and re-created");

            return DB_BAD_SCHEMA;
        }
        else
        {
            DisplayLog(LVL_FULL, LISTMGR_TAG, "Functions version is up-to-date (%s)", val);
            return DB_SUCCESS;
        }
    }
    else if (rc == DB_NOT_EXISTS)
    {
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "No function versioning (expected: %s). %s.",
                   FUNCTIONSET_VERSION, report_only?"Reports output might be incorrect":
                            "Existing functions will be dropped and re-created");
        return DB_BAD_SCHEMA;
    }
    else
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Error getting functions version: %s",
                   db_errmsg(conn, val, 1024));
        return rc;
    }
}

static int set_functions_version(db_conn_t *conn)
{
    /* set new functions version */
    int rc = lmgr_set_var(conn, VERSION_VAR_FUNC, FUNCTIONSET_VERSION);
    if (rc)
    {
        char msgbuf[1024];
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Failed to set triggers version: %s",
                   db_errmsg(conn, msgbuf, sizeof(msgbuf)));
    }
    return rc;
}

static int check_triggers_version(db_conn_t *pconn)
{
    int rc;
    char val[1024];

    /* no accounting or report_only: don't check triggers */
    if (!lmgr_config.user_acct && !lmgr_config.group_acct && !report_only)
    {
        DisplayLog(LVL_VERB, LISTMGR_TAG, "Accounting is disabled: all triggers will be dropped.");
        return DB_SUCCESS;
    }
    else if (report_only)
        return DB_SUCCESS; /* don't care about triggers */

    /* check the triggers version */
    rc = lmgr_get_var(pconn, VERSION_VAR_TRIG, val);
    if (rc == DB_SUCCESS)
    {
        if (strcmp(val, TRIGGERSET_VERSION))
        {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Wrong triggers version (in DB: %s, expected: %s). "
                       "Existing triggers will be dropped and re-created.", val, TRIGGERSET_VERSION);
            return DB_BAD_SCHEMA;
        }
        else
        {
            DisplayLog(LVL_FULL, LISTMGR_TAG, "Triggers version is up-to-date (%s)", val);
            return DB_SUCCESS;
        }
    }
    else if (rc == DB_NOT_EXISTS)
    {
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "No trigger versioning (expected: %s). "
                   "Existing triggers will be dropped and re-created.",
                   TRIGGERSET_VERSION);
        return DB_BAD_SCHEMA;
    }
    else
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Error getting trigger version: %s",
                   db_errmsg(pconn, val, sizeof(val)));
        return rc;
    }
}

static int set_triggers_version(db_conn_t *pconn)
{
    /* set new triggers version */
    int rc = lmgr_set_var(pconn, VERSION_VAR_TRIG, TRIGGERSET_VERSION);
    if (rc)
    {
        char msgbuf[1024];
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Failed to set triggers version: %s",
                   db_errmsg(pconn, msgbuf, sizeof(msgbuf)));
    }
    return rc;
}


static int check_trig_acct_insert(db_conn_t *pconn)
{
    int rc;
    char strbuf[4096];
    if (!lmgr_config.user_acct && !lmgr_config.group_acct)
    {
        /* no acct: must delete trigger */
        if (!report_only)
        {
            DisplayLog(LVL_DEBUG, LISTMGR_TAG, "Dropping trigger %s",
                       ACCT_TRIGGER_INSERT);
            rc = db_drop_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_INSERT);
            if (rc == DB_NOT_SUPPORTED)
            {
                DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                           "Triggers are not supported with this database. "
                           "Not a big issue (wanted to disable it)");
            }
            else if (rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS)
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG,
                           "Failed to drop "ACCT_TRIGGER_INSERT "trigger: Error: %s",
                           db_errmsg(pconn, strbuf, sizeof(strbuf)));
                return rc;
            }
        }
        return DB_SUCCESS;
    }

    return db_check_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_INSERT,
                              acct_info_table);
}

static int check_trig_acct_delete(db_conn_t *pconn)
{
    int rc;
    char strbuf[4096];
    if (!lmgr_config.user_acct && !lmgr_config.group_acct)
    {
        /* no acct: must delete trigger */
        if (!report_only)
        {
            DisplayLog(LVL_DEBUG, LISTMGR_TAG, "Dropping trigger %s",
                       ACCT_TRIGGER_DELETE);
            rc = db_drop_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_DELETE);
            if (rc == DB_NOT_SUPPORTED)
            {
                DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                           "Triggers are not supported with this database. "
                           "Not a big issue (wanted to disable it)");
            }
            else if (rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS)
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG,
                           "Failed to drop "ACCT_TRIGGER_DELETE "trigger: Error: %s",
                           db_errmsg(pconn, strbuf, sizeof(strbuf)));
                return rc;
            }
        }
        return DB_SUCCESS;
    }

    return db_check_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_DELETE,
                              acct_info_table);
}

static int check_trig_acct_update(db_conn_t *pconn)
{
    int rc;
    char strbuf[4096];
    if (!lmgr_config.user_acct && !lmgr_config.group_acct)
    {
        /* no acct: must delete trigger */
        if (!report_only)
        {
            DisplayLog(LVL_DEBUG, LISTMGR_TAG, "Dropping trigger %s",
                       ACCT_TRIGGER_UPDATE);
            rc = db_drop_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_UPDATE);
            if (rc == DB_NOT_SUPPORTED)
            {
                DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                           "Triggers are not supported with this database. "
                           "Not a big issue (wanted to disable it)");
            }
            else if (rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS)
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG,
                           "Failed to drop "ACCT_TRIGGER_UPDATE "trigger: Error: %s",
                           db_errmsg(pconn, strbuf, sizeof(strbuf)));
                return rc;
            }
        }
        return DB_SUCCESS;
    }

    return db_check_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_UPDATE,
                              acct_info_table);
}

static int create_trig_acct_insert(db_conn_t *pconn)
{
    int  rc;
    char strbuf[4096];
    char *next;

    /* Trigger on insert */
    next = strbuf;
    APPEND_TXT(next, "DECLARE val BIGINT UNSIGNED; "
                     "SET val="ACCT_SZ_VAL("NEW.size")";");
    APPEND_TXT(next, "INSERT INTO " ACCT_TABLE "(");
    attrmask2fieldlist(next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "");
    INCR_NEXT(next);
    attrmask2fieldlist(next, acct_attr_set, T_ACCT, TRUE, FALSE, "", "");
    INCR_NEXT(next);
    APPEND_TXT(next, ", " ACCT_FIELD_COUNT);
    next += append_size_range_fields(next, TRUE, "");
    APPEND_TXT(next, ") VALUES (");
    attrmask2fieldlist(next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "NEW.", "");
    INCR_NEXT(next);
    attrmask2fieldlist(next, acct_attr_set, T_ACCT, TRUE, FALSE, "NEW.", "");
    INCR_NEXT(next);
    APPEND_TXT(next, ", 1");
    next += append_size_range_val(next, TRUE, "NEW.", "val");
    APPEND_TXT(next, ") ON DUPLICATE KEY UPDATE ");
    attrmask2fieldoperation(next, acct_attr_set, T_ACCT, "NEW.", ADD);
    INCR_NEXT(next);
    APPEND_TXT(next,", " ACCT_FIELD_COUNT "=" ACCT_FIELD_COUNT "+1");

    /* update size range values */
    next += append_size_range_op(next, TRUE, "NEW.", "val", ADD);

    APPEND_TXT(next,";");
    rc = db_drop_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_INSERT);
    if (rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to drop " ACCT_TRIGGER_INSERT " trigger: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }

    rc = db_create_trigger(pconn, ACCT_TRIGGER_INSERT, "AFTER INSERT",
                           acct_info_table, strbuf);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create "ACCT_TRIGGER_INSERT" trigger: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Trigger "ACCT_TRIGGER_INSERT" created successfully");
    return DB_SUCCESS;
}

static int create_trig_acct_delete(db_conn_t *pconn)
{
    int  rc;
    char strbuf[4096];
    char *next;

    /* Trigger on delete */
    next = strbuf;
    APPEND_TXT(next, "DECLARE val BIGINT UNSIGNED; "
                      "SET val="ACCT_SZ_VAL("OLD.size")";");
    APPEND_TXT(next, "UPDATE " ACCT_TABLE " SET ");
    attrmask2fieldoperation(next, acct_attr_set, T_ACCT, "OLD.", SUBSTRACT);
    INCR_NEXT(next);
    APPEND_TXT(next,", " ACCT_FIELD_COUNT  "=" ACCT_FIELD_COUNT  "-1");

    /* update size range values */
    next += append_size_range_op(next, TRUE, "OLD.", "val", SUBSTRACT);

    APPEND_TXT(next, " WHERE ");
    attrmask2fieldcomparison(next, acct_pk_attr_set, T_ACCT, "", "OLD.", "=", "AND");
    INCR_NEXT(next);
    APPEND_TXT(next, ";");

    rc = db_drop_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_DELETE);
    if (rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to drop " ACCT_TRIGGER_DELETE " trigger: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }

    rc = db_create_trigger(pconn, ACCT_TRIGGER_DELETE, "BEFORE DELETE",
                           acct_info_table, strbuf);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create " ACCT_TRIGGER_DELETE " trigger: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Trigger "ACCT_TRIGGER_DELETE" created successfully");
    return DB_SUCCESS;
}

static int create_trig_acct_update(db_conn_t *pconn)
{
    int  rc, i;
    char strbuf[4096];
    char *next;
    int is_first_field = TRUE;

    /* Trigger on update */

    /* In case the owner or group changes, we
     * must substract old information in previous raw
     * and add new information to the new raw.
     */
    /* Simple case: owner and group are still the same */
    next = strbuf;
    APPEND_TXT(next, "DECLARE val_old, val_new BIGINT UNSIGNED;");
    APPEND_TXT(next, "SET val_old="ACCT_SZ_VAL("OLD.size")"; "
                      "SET val_new="ACCT_SZ_VAL("NEW.size")";");
    APPEND_TXT(next, "\nIF ");
    /* generate comparison like NEW.owner=OLD.owner AND NEW.gr_name=OLD.gr_name */
    attrmask2fieldcomparison(next, acct_pk_attr_set, T_ACCT, "NEW.", "OLD.", "=", "AND");
    INCR_NEXT(next);
    APPEND_TXT(next, "THEN \n\t IF ");
    /********* if one of the attribute value has changed: update the acct table *********/
    /* generate comparison like NEW.size<>=OLD.size OR NEW.blocks<>OLD.blocks */
    attrmask2fieldcomparison(next, acct_attr_set, T_ACCT, "NEW.", "OLD.", "<>", "OR");
    INCR_NEXT(next);
    APPEND_TXT(next, "THEN \n\t\t UPDATE " ACCT_TABLE " SET ");
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_acct_field(i))
        {
            if (!is_first_field)
                next += sprintf(next, ", %s=%s+CAST(NEW.%s as SIGNED)-CAST(OLD.%s as SIGNED) ",
                                 field_infos[i].field_name, field_infos[i].field_name,
                                 field_infos[i].field_name, field_infos[i].field_name);
            else
            {
                next += sprintf(next, " %s=%s+CAST(NEW.%s as SIGNED)-CAST(OLD.%s as SIGNED) ",
                                 field_infos[i].field_name, field_infos[i].field_name,
                                 field_infos[i].field_name, field_infos[i].field_name);
                is_first_field = FALSE;
            }
        }
    }

    /* update size range values */
    next += sprintf(next, "%s%s=CAST(%s as SIGNED)-CAST(((OLD.size=0)+(NEW.size=0)) as SIGNED)",
                     is_first_field?" ":", ", sz_field[0], sz_field[0]);
    is_first_field = FALSE;
    for (i = 1; i < SZ_PROFIL_COUNT-1; i++) /* 2nd to before the last */
    {
        next += sprintf(next, ", %s=CAST(%s as SIGNED)-CAST(IFNULL(val_old=%u,0) as SIGNED)+CAST(IFNULL(val_new=%u,0) as SIGNED)",
                         sz_field[i], sz_field[i], i-1, i-1);
    }
    /* last */
    next += sprintf(next, ", %s=CAST(%s as SIGNED)-CAST(IFNULL(val_old>=%u,0) as SIGNED)+CAST(IFNULL(val_new>=%u,0) as SIGNED)",
                     sz_field[i], sz_field[i], i-1, i-1);

    APPEND_TXT(next, " WHERE ");
    /* generate comparison as follows: owner=NEW.owner AND gr_name=NEW.gr_name */
    attrmask2fieldcomparison(next, acct_pk_attr_set, T_ACCT, "", "NEW.", "=", "AND");
    INCR_NEXT(next);
    APPEND_TXT(next, "; \n\t END IF; \nELSEIF ");

    /* tricky case: owner and/or group changed */

    attrmask2fieldcomparison(next, acct_pk_attr_set, T_ACCT, "NEW.", "OLD.", "<>", "OR");
    INCR_NEXT(next);
    APPEND_TXT(next,  "THEN \n\tINSERT INTO " ACCT_TABLE "(");
    /* generate fields as follows: owner, gr_name */
    attrmask2fieldlist(next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "", "");
    INCR_NEXT(next);
    /* generate fields as follows: , size, blocks */
    attrmask2fieldlist(next, acct_attr_set, T_ACCT, TRUE, FALSE, "", "");
    INCR_NEXT(next);
    APPEND_TXT(next, ", " ACCT_FIELD_COUNT);
    next += append_size_range_fields(next, TRUE, "");
    APPEND_TXT(next, ") VALUES (");
    /* generate fields as follows: NEW.owner, NEW.gr_name */
    attrmask2fieldlist(next, acct_pk_attr_set, T_ACCT, FALSE, FALSE, "NEW.", "");
    INCR_NEXT(next);
    attrmask2fieldlist(next, acct_attr_set, T_ACCT, TRUE, FALSE, "NEW.", "");
    INCR_NEXT(next);
    APPEND_TXT(next, ", 1");
    next += append_size_range_val(next, TRUE, "NEW.", "val_new");
    APPEND_TXT(next, ") \n\tON DUPLICATE KEY UPDATE ");
    /* generate operations as follows: size=size+New.size, blocks=blocks+NEW.blocks */
    attrmask2fieldoperation(next, acct_attr_set, T_ACCT, "NEW.", ADD);
    INCR_NEXT(next);
    APPEND_TXT(next, ", " ACCT_FIELD_COUNT "=" ACCT_FIELD_COUNT  "+1");
    /* update size range values */
    next += append_size_range_op(next, TRUE, "NEW.", "val_new", ADD);
    APPEND_TXT(next, ";");

    APPEND_TXT(next, "\n\tUPDATE " ACCT_TABLE " SET ");
    /* generate operations as follows: size=size-Old.size, blocks=blocks-Old.blocks */
    attrmask2fieldoperation(next, acct_attr_set, T_ACCT, "OLD.", SUBSTRACT);
    INCR_NEXT(next);
    APPEND_TXT(next, ", " ACCT_FIELD_COUNT "=" ACCT_FIELD_COUNT "-1 ");
    next += append_size_range_op(next, TRUE, "OLD.", "val_old", SUBSTRACT);
    APPEND_TXT(next, " WHERE ");
    attrmask2fieldcomparison(next, acct_pk_attr_set, T_ACCT, "", "OLD.", "=", "AND");
    INCR_NEXT(next);
    APPEND_TXT(next, ";\nEND IF;\n");

    rc = db_drop_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_UPDATE);
    if (rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to drop "ACCT_TRIGGER_UPDATE" trigger: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }

    rc = db_create_trigger(pconn, ACCT_TRIGGER_UPDATE, "AFTER UPDATE",
                           acct_info_table, strbuf);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create "ACCT_TRIGGER_UPDATE" trigger: Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Trigger "ACCT_TRIGGER_UPDATE" created successfully");
    return DB_SUCCESS;
}


static int check_func_onepath(db_conn_t *pconn)
{
    /* XXX /!\ do not modify the code of DB functions
     * without changing FUNCTIONSET_VERSION!!!!
     */
    return db_check_component(pconn, DBOBJ_FUNCTION, ONE_PATH_FUNC, NULL);
}

static int create_func_onepath(db_conn_t *pconn)
{
    int  rc;
    char strbuf[4096];

    /* XXX /!\ do not modify the code of DB functions
     * without changing FUNCTIONSET_VERSION!!!!
     */
    rc = db_drop_component(pconn, DBOBJ_FUNCTION, ONE_PATH_FUNC);
    if (rc != DB_SUCCESS && rc != DB_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to drop function '"ONE_PATH_FUNC"': Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }
    /* creating function to get one path for a file */
    /* Note: use "DETERMINISTIC" assuming that it returns the same path for the same id in a given request */
    snprintf(strbuf, sizeof(strbuf), "CREATE FUNCTION "ONE_PATH_FUNC"(param "PK_TYPE")"
        " RETURNS VARCHAR(%u) DETERMINISTIC READS SQL DATA"
        " BEGIN"
            " DECLARE p VARCHAR(%u) DEFAULT NULL;"
            " DECLARE pid "PK_TYPE" DEFAULT NULL;"
            " DECLARE n VARCHAR(%u) DEFAULT NULL;"
            // returns path when parent is not found (NULL if id is not found)
            " DECLARE EXIT HANDLER FOR NOT FOUND RETURN CONCAT(pid,'/',p);"
            " SELECT parent_id, name INTO pid, p from NAMES where id=param LIMIT 1;"
            " LOOP"
                " SELECT parent_id, name INTO pid, n from NAMES where id=pid ;"
                " SELECT CONCAT( n, '/', p) INTO p;"
            " END LOOP;"
        " END",
        /* size of fullpath */field_infos[ATTR_INDEX_fullpath].db_type_size,
        /* size of fullpath */field_infos[ATTR_INDEX_fullpath].db_type_size,
        /* size of name */field_infos[ATTR_INDEX_name].db_type_size);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create function '"ONE_PATH_FUNC"': Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
    return rc;
}

static int check_func_thispath(db_conn_t *pconn)
{
    return db_check_component(pconn, DBOBJ_FUNCTION, THIS_PATH_FUNC, NULL);
}

static int create_func_thispath(db_conn_t *pconn)
{
    int rc;
    char        strbuf[4096];
    /* XXX /!\ do not modify the code of DB functions
     * without changing FUNCTIONSET_VERSION!!!!
     */

    /* drop previous versions of the function */
    rc = db_drop_component(pconn, DBOBJ_FUNCTION, THIS_PATH_FUNC);
    if (rc != DB_SUCCESS && rc != DB_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to drop function '"THIS_PATH_FUNC"': Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
        return rc;
    }

    /* creating function to get a path for a file, for the given parent and name  */
    /* Note: use "DETERMINISTIC" assuming that it returns the same path for the same parent+name in a given request */
    snprintf(strbuf, sizeof(strbuf), "CREATE FUNCTION "THIS_PATH_FUNC"(pid_arg "PK_TYPE ", n_arg VARCHAR(%u))"
        " RETURNS VARCHAR(%u) DETERMINISTIC READS SQL DATA"
        " BEGIN"
            " DECLARE p VARCHAR(%u) DEFAULT NULL;"
            " DECLARE pid "PK_TYPE" DEFAULT NULL;"
            " DECLARE n VARCHAR(%u) DEFAULT NULL;"
            // returns path when parent is not found (NULL if id is not found)
            " DECLARE EXIT HANDLER FOR NOT FOUND RETURN CONCAT(pid,'/',p);"
            " SET pid=pid_arg;"
            " SET p=n_arg;"
            " LOOP"
                " SELECT parent_id, name INTO pid, n from NAMES where id=pid ;"
                " SELECT CONCAT( n, '/', p) INTO p;"
            " END LOOP;"
        " END",
        /* size of name */field_infos[ATTR_INDEX_name].db_type_size,
        /* size of fullpath */field_infos[ATTR_INDEX_fullpath].db_type_size,
        /* size of fullpath */field_infos[ATTR_INDEX_fullpath].db_type_size,
        /* size of name */field_infos[ATTR_INDEX_name].db_type_size);

    rc = db_exec_sql(pconn, strbuf, NULL);
    if (rc)
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create function '"THIS_PATH_FUNC"': Error: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
    return rc;
}

typedef struct dbobj_descr {
    db_object_e  o_type;
    const char * o_name;
    check_create_tab_func_t o_check;
    check_create_tab_func_t o_create;
} dbobj_descr_t;


/** list of tables and their check/create functions. */
static const dbobj_descr_t  o_list[] = {
    /* tables */
    {DBOBJ_TABLE, VAR_TABLE,     check_table_vars,    create_table_vars},
    {DBOBJ_TABLE, MAIN_TABLE,    check_table_main,    create_table_main},
    {DBOBJ_TABLE, DNAMES_TABLE,  check_table_dnames,  create_table_dnames},
    {DBOBJ_TABLE, ANNEX_TABLE,   check_table_annex,   create_table_annex},
    {DBOBJ_TABLE, ACCT_TABLE,    check_table_acct,    create_table_acct},
#ifdef _LUSTRE
    {DBOBJ_TABLE, STRIPE_INFO_TABLE,  check_table_stripe_info,
                                      create_table_stripe_info},
    {DBOBJ_TABLE, STRIPE_ITEMS_TABLE, check_table_stripe_items,
                                      create_table_stripe_items},
#endif
#ifdef HAVE_RM_POLICY
    {DBOBJ_TABLE, SOFT_RM_TABLE, check_table_softrm, create_table_softrm},
#endif

    /* triggers */
    {DBOBJ_TRIGGER, ACCT_TRIGGER_INSERT, check_trig_acct_insert,
                                         create_trig_acct_insert},
    {DBOBJ_TRIGGER, ACCT_TRIGGER_DELETE, check_trig_acct_delete,
                                         create_trig_acct_delete},
    {DBOBJ_TRIGGER, ACCT_TRIGGER_UPDATE, check_trig_acct_update,
                                         create_trig_acct_update},

    /* functions */
    {DBOBJ_FUNCTION, ONE_PATH_FUNC,  check_func_onepath,  create_func_onepath},
    {DBOBJ_FUNCTION, THIS_PATH_FUNC, check_func_thispath, create_func_thispath},

    {0, NULL, NULL, NULL} /* STOP item */
};


/**
 * Initialize the database access module and
 * check and create the schema.
 */
int ListMgr_Init(const lmgr_config_t * p_conf, int report_access_only)
{
    int            rc;
    db_conn_t      conn;
    const dbobj_descr_t *o;
    int create_all_functions = FALSE;
    int create_all_triggers = FALSE;

    /* store the configuration */
    lmgr_config = *p_conf;
    report_only = report_access_only;

    /* determine if an annex table is used */
    annex_table = have_annex_table();

    /* initilize attr masks for each table */
    init_attrset_masks(&lmgr_config);

    /* determine source tables for accounting */
    acct_info_table = acct_table();

    /* create a database access */
    rc = db_connect(&conn);
    if (rc)
        return rc;

    /* check if tables exist, and check their schema */
    DisplayLog(LVL_DEBUG, LISTMGR_TAG, "Checking database schema");

    /* check function and trigger version: if wrong, drop and re-create them all */
    if (check_functions_version(&conn) != DB_SUCCESS)
        create_all_functions = TRUE;
    if (check_triggers_version(&conn) != DB_SUCCESS)
        create_all_triggers = TRUE;

    for (o = o_list; o->o_name != NULL; o++)
    {
        /* don't care about triggers for report-only */
        if (report_only && (o->o_type == DBOBJ_TRIGGER))
            continue;

        /* force re-creating triggers and functions, if needed */
        if ((o->o_type == DBOBJ_TRIGGER) && create_all_triggers)
            rc = DB_NOT_EXISTS;
        else if ((o->o_type == DBOBJ_FUNCTION) && create_all_functions)
            rc = DB_NOT_EXISTS;
        else
            rc = o->o_check(&conn);

        switch(rc)
        {
            case DB_SUCCESS: /* OK */
                break;
            case DB_NOT_EXISTS:
                if (report_only)
                    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "WARNING: %s %s"
                               " does not exist", dbobj2str(o->o_type),
                               o->o_name);
                else
                {
                    DisplayLog(LVL_EVENT, LISTMGR_TAG, "%s %s does not exist (or wrong version):"
                               " creating it.", dbobj2str(o->o_type), o->o_name);
                    rc = o->o_create(&conn);
                    if (rc != DB_SUCCESS)
                        goto close_conn;
                }
                break;
            default: /* error */
                goto close_conn;
        }
    }

    if (create_all_triggers && !report_only)
    {
        rc = set_triggers_version(&conn);
        if (rc)
            goto close_conn;
    }

    if (create_all_functions && !report_only)
    {
        rc = set_functions_version(&conn);
        if (rc)
            goto close_conn;
    }

    rc = DB_SUCCESS;

close_conn:
    /* close the connection in any case */
    db_close_conn(&conn);
    return rc;
} /* ListMgr_Init */



int ListMgr_InitAccess( lmgr_t * p_mgr )
{
    int            rc, i;

    rc = db_connect( &p_mgr->conn );

    if ( rc )
        return rc;

    /* set READ COMMITTED isolation level to avoid locking issues and
     * performance drop. */
    rc = db_transaction_level(&p_mgr->conn, TRANS_SESSION, TXL_READ_COMMITTED);
    if (rc)
    {
        char errmsg_buf[1024];

        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to set READ_COMMITTED isolation level: Error: %s",
                   db_errmsg(&p_mgr->conn, errmsg_buf, sizeof(errmsg_buf)));
        return rc;
    }


    p_mgr->last_commit = 0;
    p_mgr->force_commit = FALSE;
    p_mgr->retry_delay = 0;
    p_mgr->retry_count = 0;
    timerclear(&p_mgr->first_error);

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
