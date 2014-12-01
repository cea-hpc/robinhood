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
#include "rbh_logs.h"
#include "rbh_misc.h"
#include <stdio.h>

/* global symbols */
static const char *acct_info_table = NULL;
static bool report_only = false;

#define MAX_DB_FIELDS 64


static void append_status_def(const sm_instance_t *smi, GString *str, int is_first)
{
    int i;

    g_string_append_printf(str, "%s%s_status ENUM('', ", is_first?"":",", smi->instance_name);
    for (i = 0; i < smi->sm->status_count; i++)
    {
        g_string_append_printf(str, "%s'%s'", (i == 0)?"":",",
                               smi->sm->status_enum[i]);
    }
    /* end of "ENUM (" */
    g_string_append(str, ") DEFAULT ''");
}

static void append_field_def(int i, GString *str, int is_first, db_type_u *default_value)
{
    if (i >= ATTR_COUNT)
    {
        append_status_def(get_sm_instance(i - ATTR_COUNT), str, is_first);
        return;
    }

    switch (field_infos[i].db_type)
    {
    case DB_STRIPE_INFO:   /* never in main table (ignored) */
    case DB_STRIPE_ITEMS:
        break;
    case DB_TEXT:
        {
            char strtype[128];

            /* VARCHAR is up to 255. For larger strings, use TEXT. */
            if (field_infos[i].db_type_size < 256)
                snprintf(strtype, sizeof(strtype),"VARCHAR(%u)", field_infos[i].db_type_size);
            else
                rh_strncpy(strtype, "TEXT", sizeof(strtype));

            if (default_value)
                g_string_append_printf(str, "%s %s %s DEFAULT '%s'",is_first ? "" : ",",
                    field_infos[i].field_name, strtype, default_value->val_str);
            else
                g_string_append_printf(str, "%s %s %s",is_first ? "" : ",",
                    field_infos[i].field_name, strtype);
        }
        break;
    case DB_INT:
        if (default_value)
            g_string_append_printf(str, "%s %s INT DEFAULT %d", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_int);
        else
            g_string_append_printf(str, "%s %s INT", is_first ? "" : ",", field_infos[i].field_name);
        break;
    case DB_UINT:
        if (default_value)
            g_string_append_printf(str, "%s %s INT UNSIGNED DEFAULT %u", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_uint);
        else
            g_string_append_printf(str, "%s %s INT UNSIGNED", is_first ? "" : ",", field_infos[i].field_name);
        break;
    case DB_SHORT:
        if (default_value)
            g_string_append_printf(str, "%s %s SMALLINT DEFAULT %hd", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_short);
        else
            g_string_append_printf(str, "%s %s SMALLINT", is_first ? "" : ",", field_infos[i].field_name);
        break;
    case DB_USHORT:
        if (default_value)
            g_string_append_printf(str, "%s %s SMALLINT UNSIGNED DEFAULT %hu", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_ushort);
        else
            g_string_append_printf(str, "%s %s SMALLINT UNSIGNED", is_first ? "" : ",", field_infos[i].field_name);
        break;
    case DB_BIGINT:
        if (default_value)
            g_string_append_printf(str, "%s %s BIGINT DEFAULT %lld", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_bigint);
        else
            g_string_append_printf(str, "%s %s BIGINT", is_first ? "" : ",", field_infos[i].field_name);
        break;
    case DB_BIGUINT:
        if (default_value)
            g_string_append_printf(str, "%s %s BIGINT UNSIGNED DEFAULT %llu", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_biguint);
        else
            g_string_append_printf(str, "%s %s BIGINT UNSIGNED", is_first ? "" : ",", field_infos[i].field_name);
        break;
    case DB_BOOL:
        if (default_value)
            g_string_append_printf(str, "%s %s BOOLEAN DEFAULT %d", is_first ? "" : ",", field_infos[i].field_name,
                default_value->val_bool);
        else
            g_string_append_printf(str, "%s %s BOOLEAN", is_first ? "" : ",", field_infos[i].field_name);
        break;
    case DB_ID:
        g_string_append_printf(str, "%s %s "PK_TYPE, is_first ? "" : ",", field_infos[i].field_name);
        break;
    case DB_ENUM_FTYPE:
        g_string_append_printf(str, "%s %s ENUM('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                        is_first ? "" : ",", field_infos[i].field_name,
                        STR_TYPE_LINK, STR_TYPE_DIR, STR_TYPE_FILE, STR_TYPE_CHR,
                        STR_TYPE_BLK, STR_TYPE_FIFO, STR_TYPE_SOCK);
        break;
    }
}

#define DROP_MESSAGE "\nYou should:\n\t1) backup current DB contents using 'rbh-config backup_db'\n\t2) empty the DB using 'rbh-config empty_db'\n\t3) start a new FS scan."
#define DROP_ACCT_MSG "\nYou should:\n\t1) stop robinhood commands\n\t2) clear accounting info using 'rbh-config reset_acct'\n\t3) restart robinhood."

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
                       "Incompatible database schema (unexpected field '%s' in table %s: '%s' expected): "DROP_ACCT_MSG,
                       fieldtab[*curr_field_index], table, name);
        else
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Incompatible database schema (unexpected field '%s' in table %s: '%s' expected): "DROP_MESSAGE,
                       fieldtab[*curr_field_index], table, name);
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
    return check_field_name(field_name(i), curr_field_index, table, fieldtab);
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
static void append_size_range_val(GString *request, bool leading_comma,
                                  char *prefix, const char *op_subs)
{
    unsigned int i;
    char value[128];

    if (op_subs && op_subs[0])
        strncpy(value, op_subs, sizeof(value));
    else
        snprintf(value, sizeof(value), ACCT_SZ_VAL("%ssize"), prefix);

    g_string_append_printf(request, "%s %ssize=0", leading_comma?",":"", prefix);
    for (i = 1; i < SZ_PROFIL_COUNT-1; i++) /* 2nd to before the last */
    {
        g_string_append_printf(request, ", IFNULL(%s=%u,0)", value, i-1);
    }
    /* last */
    g_string_append_printf(request, ", IFNULL(%s>=%u,0)", value, i-1);
}

/**
 * @param op_subs replacement for 'FLOOR(LOG2(<prefix>.size)/5)' (eg. local variable)
 */
static void append_size_range_op(GString *request, bool leading_comma, char *prefix,
                                 const char * op_subs, operation_type optype)
{
    unsigned int i;
    char         value[128];
    const char  *op = (optype == ADD)?"+":"-";

    if (op_subs && op_subs[0])
        strncpy(value, op_subs, sizeof(value));
    else
        snprintf(value, sizeof(value), ACCT_SZ_VAL("%ssize"), prefix);

    g_string_append_printf(request, "%s %s=CAST(%s as SIGNED)%sCAST((%ssize=0) as SIGNED)",
                           leading_comma?",":"", sz_field[0], sz_field[0], op, prefix);

    for (i = 1; i < SZ_PROFIL_COUNT-1; i++) /* 2nd to before the last */
    {
        g_string_append_printf(request, ", %s=CAST(%s as SIGNED)%sCAST(IFNULL(%s=%u,0) as SIGNED)",
                               sz_field[i], sz_field[i], op, value, i-1);
    }
    /* last */
    g_string_append_printf(request, ", %s=CAST(%s as SIGNED)%sCAST(IFNULL(%s>=%u,0) as SIGNED)",
                           sz_field[i], sz_field[i], op, value, i-1);
}

/**
 * Check what tables are used as source for accounting.
 * Return the main source table for accounting.
 */
static const char *acct_table(void)
{
    const char *src_table = NULL;
    bool is_annex = false;
    bool is_main = false;

    if (lmgr_config.user_acct || lmgr_config.group_acct)
    {
        int i;
        for (i = 0; i < ATTR_COUNT + sm_inst_count; i++)
        {
            if (is_acct_field(i) || is_acct_pk(i))
            {
                if (is_annex_field(i))
                    is_annex = true;
                else if (is_main_field(i))
                    is_main = true;
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
    char    strbuf[4096];
    char   *fieldtab[MAX_DB_FIELDS];

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

/** wrapper for table creation request + display of log messages */
static int run_create_table(db_conn_t *pconn, const char *table_name,
                            const char *request)
{
    int rc;

    DisplayLog(LVL_FULL, LISTMGR_TAG, "Table creation request =\n%s", request);
    rc = db_exec_sql(pconn, request, NULL);
    if (rc != DB_SUCCESS)
    {
        char errmsg[1024];
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Failed to create table %s: Error: %s",
                   table_name, db_errmsg(pconn, errmsg, sizeof(errmsg)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Table %s created successfully", table_name);
    return DB_SUCCESS;
}

/** wrapper for index: creation request + display of log messages */
static int run_create_index(db_conn_t *pconn, const char *table_name,
                            const char *field, const char *request)
{
    int rc;

    DisplayLog(LVL_FULL, LISTMGR_TAG, "Index creation request =\n%s", request);
    rc = db_exec_sql(pconn, request, NULL);
    if (rc != DB_SUCCESS)
    {
        char errmsg[1024];
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Failed to create index of %s(%s): Error: %s",
                   table_name, field, db_errmsg(pconn, errmsg, sizeof(errmsg)));
        return rc;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Index on %s(%s) created successfully",
               table_name, field);
    return DB_SUCCESS;
}

static void append_engine(GString *request)
{
#ifdef _MYSQL
    g_string_append_printf(request, " ENGINE=%s", lmgr_config.db_config.engine);
#endif
}

static int create_table_vars(db_conn_t *pconn)
{
    int      rc;
    GString *request = g_string_new("CREATE TABLE "VAR_TABLE" ("
                            "varname VARCHAR(255) PRIMARY KEY, "
                            "value TEXT)");
    append_engine(request);
    rc = run_create_table(pconn, VAR_TABLE, request->str);
    g_string_free(request, TRUE);
    return rc;
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

        for (i = 0; i < ATTR_COUNT + sm_inst_count; i++)
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
    GString    *request;
    int         i, rc;
    db_type_u   default_val;

    request = g_string_new("CREATE TABLE "MAIN_TABLE" (id "PK_TYPE" PRIMARY KEY");

    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_main_field(i) && !is_funcattr(i))
        {
            if (i == ATTR_INDEX_owner)
            {
                default_val.val_str = ACCT_DEFAULT_OWNER;
                append_field_def(i, request, 0, &default_val);
            }
            else if (i == ATTR_INDEX_gr_name)
            {
                default_val.val_str = ACCT_DEFAULT_GROUP;
                append_field_def(i, request, 0, &default_val);
            }
            else
                 append_field_def(i, request, 0, NULL);
        }
    }

    /** Append status values (depends on policy definitions)
     * @TODO RBHv3: enhance status management in DB:
     * - support change in policy definitions (not order-sensitive? alter table?).
     * - store previous policy definitions to detect changes.
     * - For read-only usage, load status manager instances from DB
     *       instead of relying on client configuration.
     */
    sm_instance_t *smi;

    for (i = 0, smi = get_sm_instance(0); smi != NULL;
         i++, smi = get_sm_instance(i))
    {
        /* status managers with SM_NODB tag have no info in DB */
        if (smi->sm->flags & SM_NODB)
            continue;

        append_status_def(smi, request, 0);
    }
    /* end of field list (null terminated) */
    g_string_append(request, ")");
    append_engine(request);

    rc = run_create_table(pconn, MAIN_TABLE, request->str);
    if (rc)
        goto free_str;

    /* create indexes on this table */
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_main_field(i) && is_indexed_field(i))
        {
            g_string_printf(request, "CREATE INDEX %s_index ON " MAIN_TABLE "(%s)",
                            field_name(i), field_name(i));
            rc = run_create_index(pconn, MAIN_TABLE, field_name(i), request->str);
            if (rc)
                goto free_str;
        }
    }
    rc = DB_SUCCESS;

free_str:
    g_string_free(request, TRUE);
    return rc;
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
    GString    *request;
    int         i, rc;

    request = g_string_new("CREATE TABLE "DNAMES_TABLE" (id "PK_TYPE", "
                           "pkn VARCHAR(40) PRIMARY KEY");

    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_names_field(i) && !is_funcattr(i))
        {
            append_field_def(i, request, 0, NULL);
        }
    }
    g_string_append(request, ")");
    append_engine(request);

    rc = run_create_table(pconn, DNAMES_TABLE, request->str);
    if (rc)
        goto free_str;

    /* create indexes on this table */
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_names_field(i) && is_indexed_field(i))
        {
            g_string_printf(request, "CREATE INDEX %s_index ON " DNAMES_TABLE "(%s)",
                            field_name(i), field_name(i));
            rc = run_create_index(pconn, DNAMES_TABLE, field_name(i), request->str);
            if (rc)
                goto free_str;
        }
    }

    /* this index is needed to build the fullpath of entries */
    rc = run_create_index(pconn, DNAMES_TABLE, "id",
                      "CREATE INDEX id_index ON "DNAMES_TABLE"(id)");
free_str:
    g_string_free(request, TRUE);
    return rc;
}

static int check_table_annex(db_conn_t *pconn)
{
    int rc, i;
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];

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
    GString  *request;
    int       i, rc;

    request = g_string_new("CREATE TABLE "ANNEX_TABLE" (id "PK_TYPE" PRIMARY KEY");

    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_annex_field(i) && !is_funcattr(i))
        {
            append_field_def(i, request, 0, NULL);
        }
    }
    g_string_append(request, ")");
    append_engine(request);

    rc = run_create_table(pconn, ANNEX_TABLE, request->str);
    if (rc)
        goto free_str;

    /* create indexes on this table */
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_annex_field(i) && is_indexed_field(i))
        {
            g_string_printf(request, "CREATE INDEX %s_index ON " ANNEX_TABLE "(%s)",
                            field_name(i), field_name(i));
            rc = run_create_index(pconn, ANNEX_TABLE, field_name(i), request->str);
            if (rc)
                goto free_str;
        }
    }
    rc = DB_SUCCESS;

free_str:
    g_string_free(request, TRUE);
    return rc;
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
    GString *request;
    int      rc;

    request = g_string_new(NULL);
    g_string_printf(request, "CREATE TABLE " STRIPE_INFO_TABLE
            " (id "PK_TYPE" PRIMARY KEY, validator INT, "
            "stripe_count INT UNSIGNED, stripe_size INT UNSIGNED, "
            "pool_name VARCHAR(%u))",
            MAX_POOL_LEN - 1);
    append_engine(request);

    rc = run_create_table(pconn, STRIPE_INFO_TABLE, request->str);
    g_string_free(request, TRUE);
    return rc;
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
    GString *request;
    int  rc;

    request = g_string_new(NULL);
    g_string_printf(request, "CREATE TABLE "STRIPE_ITEMS_TABLE
                    " (id "PK_TYPE", stripe_index INT UNSIGNED, "
                    "ostidx INT UNSIGNED, details BINARY(%u))",
                    STRIPE_DETAIL_SZ);
    append_engine(request);

    rc = run_create_table(pconn, STRIPE_ITEMS_TABLE, request->str);
    g_string_free(request, TRUE);
    if (rc)
        return rc;

    rc = run_create_index(pconn, STRIPE_ITEMS_TABLE, "id",
                          "CREATE INDEX id_index ON "STRIPE_ITEMS_TABLE"(id)");
    if (rc)
        return rc;

    rc = run_create_index(pconn, STRIPE_ITEMS_TABLE, "ostidx",
                          "CREATE INDEX ost_index ON "STRIPE_ITEMS_TABLE"(ostidx)");
    return rc;
}
#endif

static void disable_acct(void)
{
    lmgr_config.user_acct = false;
    lmgr_config.group_acct = false;
    /* reset acct masks */
    acct_pk_attr_set = 0;
    acct_attr_set = 0;
}

static int check_table_acct(db_conn_t *pconn)
{
    int i, rc;
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];

    /* daemon mode with acccounting disabled: don't check ACCT table */
    if (!lmgr_config.user_acct && !lmgr_config.group_acct && !report_only)
        return DB_SUCCESS;

    rc = db_list_table_fields(pconn, ACCT_TABLE, fieldtab, MAX_DB_FIELDS,
                              strbuf, sizeof(strbuf));
    if (rc == DB_SUCCESS)
    {
        int curr_field_index = 0;

        /* check primary key */
        for (i = 0; i < ATTR_COUNT + sm_inst_count; i++)
        {
            if (is_acct_pk(i))
            {
                if (check_field(i, &curr_field_index, ACCT_TABLE, fieldtab))
                    return DB_BAD_SCHEMA;
            }
        }
        /* check other fields */
        for (i = 0; i < ATTR_COUNT + sm_inst_count; i++)
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
    int      i, rc;
    GString *request = NULL;
    char err_buf[1024];

    if (acct_info_table == NULL)
        RBH_BUG("Can't populate "ACCT_TABLE" with no source table");

    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Populating accounting table from existing DB content."
               " This can take a while...");
    FlushLogs();

    /* Initial table population for already existing entries */

    /* INSERT <fields>... */
    request = g_string_new("INSERT INTO "ACCT_TABLE"(");
    attrmask2fieldlist(request, acct_pk_attr_set , T_ACCT, false, false, "", "");
    attrmask2fieldlist(request, acct_attr_set, T_ACCT, true, false, "", "");
    g_string_append(request, ", "ACCT_FIELD_COUNT);
    append_size_range_fields(request, true, "");

    /* ...SELECT <fields>... */
    g_string_append(request, ") SELECT ");
    attrmask2fieldlist(request, acct_pk_attr_set, T_ACCT, false, false, "", "");
    attrmask2fieldlist(request, acct_attr_set, T_ACCT, true, false, "SUM(", ")");
    g_string_append(request, ",COUNT(id),SUM(size=0)");
    for (i = 1; i < SZ_PROFIL_COUNT-1; i++) /* 1 to 8 */
        g_string_append_printf(request, ",SUM(IFNULL("ACCT_SZ_VAL("size")"=%u,0))", i-1);
    g_string_append_printf(request, ",SUM(IFNULL("ACCT_SZ_VAL("size")">=%u,0))", i-1);

    /* FROM ... GROUP BY ... */
    g_string_append_printf(request, " FROM %s  GROUP BY ", acct_info_table);
    attrmask2fieldlist(request, acct_pk_attr_set, T_ACCT, false, false, "", "");

    /* set READ COMMITTED isolation level for the next (big!) request
     * so locks can be released immediatly after the record is read */
    rc = db_transaction_level(pconn, TRANS_NEXT, TXL_READ_COMMITTED);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to set READ_COMMITTED isolation level: Error: %s",
                   db_errmsg(pconn, err_buf, sizeof(err_buf)));
        /* try to continue */
    }

    rc = db_exec_sql(pconn, request->str, NULL);
    g_string_free(request, TRUE);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to populate accounting table: Error: %s",
                   db_errmsg(pconn, err_buf, sizeof(err_buf)));

        /* drop this table, to leave the db in a consistent state (if ACCT_TABLE exists, it must be populated) */
        if (db_drop_component(pconn, DBOBJ_TABLE, ACCT_TABLE))
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Failed to drop table: Error: %s",
                       db_errmsg(pconn, err_buf, sizeof(err_buf)));
    }
    return rc;
}

static int create_table_acct(db_conn_t *pconn)
{
    GString *request;
    int      i, rc;
    bool     first_acct_pk = true;
    bool     is_first_acct_field = true;

    if (!lmgr_config.user_acct && !lmgr_config.group_acct)
        return DB_SUCCESS;

    request = g_string_new("CREATE TABLE "ACCT_TABLE" (");

    for (i = 0; i < ATTR_COUNT + sm_inst_count; i++)
    {
        if (is_acct_pk(i))
        {
            append_field_def(i, request, is_first_acct_field, NULL);
            is_first_acct_field = false;
        }
    }

    for (i = 0; i < ATTR_COUNT + sm_inst_count; i++)
    {
        if (is_acct_field(i))
            append_field_def(i, request, is_first_acct_field, NULL);
    }

    /* count field */
    g_string_append(request, ", " ACCT_FIELD_COUNT  " BIGINT UNSIGNED");

    /* size range fields */
    for (i = 0; i < SZ_PROFIL_COUNT; i++)
    {
        g_string_append_printf(request, ", %s BIGINT UNSIGNED DEFAULT 0",
                               sz_field[i]);
    }

    /* PK definition */
    g_string_append(request, ", PRIMARY KEY ( ");

    for (i = 0; i < ATTR_COUNT + sm_inst_count; i++)
    {
        if (is_acct_pk(i))
        {
            if (!first_acct_pk )
                g_string_append_printf(request, ", %s", field_name(i));
            else
            {
                g_string_append_printf(request, "%s", field_name(i));
                first_acct_pk = false;
            }
        }
    }
    g_string_append(request, "))");
    append_engine(request);

    rc = run_create_table(pconn, ACCT_TABLE, request->str);
    if (rc)
        goto free_str;

    /* now populate it */
    rc = populate_acct_table(pconn);

free_str:
    g_string_free(request, TRUE);
    return rc;
}

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
        int i;

        /* check primary key */
        if (check_field_name("id", &curr_index, SOFT_RM_TABLE, fieldtab))
            return DB_BAD_SCHEMA;

        for (i = 0; i < ATTR_COUNT + sm_inst_count; i++)
        {
            if (is_softrm_field(i)) /* no func attr in softrm table */
            {
                if (check_field(i, &curr_index, SOFT_RM_TABLE, fieldtab))
                    return DB_BAD_SCHEMA;
            }
        }
        /* is there any extra field ? */
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
    GString *request;
    int      rc, i;

    request = g_string_new("CREATE TABLE "SOFT_RM_TABLE" (id "PK_TYPE" PRIMARY KEY");

    for (i = 0; i < ATTR_COUNT + sm_inst_count; i++)
    {
        if (is_softrm_field(i))
            append_field_def(i, request, 0, NULL);
    }
    g_string_append(request, ")");
    append_engine(request);

    rc = run_create_table(pconn, SOFT_RM_TABLE, request->str);
    if (rc)
        goto free_str;

    /* create indexes on this table */
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_softrm_field(i) && is_indexed_field(i))
        {
            g_string_printf(request, "CREATE INDEX %s_index ON " SOFT_RM_TABLE "(%s)",
                            field_name(i), field_name(i));

            rc = run_create_index(pconn, SOFT_RM_TABLE, field_name(i), request->str);
            if (rc)
                goto free_str;
        }
    }
    rc = DB_SUCCESS;

free_str:
    g_string_free(request, TRUE);
    return rc;
}

#define VERSION_VAR_FUNC    "VersionFunctionSet"
#define VERSION_VAR_TRIG    "VersionTriggerSet"

#define FUNCTIONSET_VERSION    "1.1"
#define TRIGGERSET_VERSION     "1.1"

static int check_functions_version(db_conn_t *conn)
{
    int rc;
    char val[1024];

    /* check the functions version */
    rc = lmgr_get_var(conn, VERSION_VAR_FUNC, val, sizeof(val));
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
    rc = lmgr_get_var(pconn, VERSION_VAR_TRIG, val, sizeof(val));
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
    int      rc;
    GString *request;
    char     errbuf[1024];

    /* Trigger on insert */
    request = g_string_new("DECLARE val BIGINT UNSIGNED; "
                           "SET val="ACCT_SZ_VAL("NEW.size")"; "
                           "INSERT INTO " ACCT_TABLE "(");
    /* INSERT(list of fields... */
    attrmask2fieldlist(request, acct_pk_attr_set, T_ACCT, false, false, "", "");
    attrmask2fieldlist(request, acct_attr_set, T_ACCT, true, false, "", "");
    g_string_append(request, ", " ACCT_FIELD_COUNT);
    append_size_range_fields(request, true, "");

    /* ... ) VALUES (... */
    g_string_append(request, ") VALUES (");
    attrmask2fieldlist(request, acct_pk_attr_set, T_ACCT, false, false, "NEW.", "");
    attrmask2fieldlist(request, acct_attr_set, T_ACCT, true, false, "NEW.", "");
    g_string_append(request, ",1");
    append_size_range_val(request, true, "NEW.", "val");
    g_string_append(request, ") ON DUPLICATE KEY UPDATE ");

    /* on duplicate key update... */
    attrmask2fieldoperation(request, acct_attr_set, T_ACCT, "NEW.", ADD);
    g_string_append(request, ", " ACCT_FIELD_COUNT "=" ACCT_FIELD_COUNT "+1");
    append_size_range_op(request, true, "NEW.", "val", ADD);
    g_string_append(request, ";");

    rc = db_drop_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_INSERT);
    if (rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to drop " ACCT_TRIGGER_INSERT " trigger: Error: %s",
                   db_errmsg(pconn, errbuf, sizeof(errbuf)));
        goto free_str;
    }

    rc = db_create_trigger(pconn, ACCT_TRIGGER_INSERT, "AFTER INSERT",
                           acct_info_table, request->str);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create "ACCT_TRIGGER_INSERT" trigger: Error: %s",
                   db_errmsg(pconn, errbuf, sizeof(errbuf)));
        goto free_str;
    }
    rc = DB_SUCCESS;
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Trigger "ACCT_TRIGGER_INSERT" created successfully");

free_str:
    g_string_free(request, TRUE);
    return rc;
}

static int create_trig_acct_delete(db_conn_t *pconn)
{
    int      rc;
    GString *request;
    char     err_buf[1024];

    /* Trigger on delete */
    request = g_string_new("DECLARE val BIGINT UNSIGNED; "
                           "SET val="ACCT_SZ_VAL("OLD.size")";"
                           "UPDATE " ACCT_TABLE " SET ");
    /* update ACCT_TABLE SET ... */
    attrmask2fieldoperation(request, acct_attr_set, T_ACCT, "OLD.", SUBSTRACT);
    g_string_append(request, ", " ACCT_FIELD_COUNT  "=" ACCT_FIELD_COUNT  "-1");
    append_size_range_op(request, true, "OLD.", "val", SUBSTRACT);

    /* ... WHERE ... */
    g_string_append(request, " WHERE ");
    attrmask2fieldcomparison(request, acct_pk_attr_set, T_ACCT, "", "OLD.", "=", "AND");
    g_string_append(request, ";");

    rc = db_drop_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_DELETE);
    if (rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to drop " ACCT_TRIGGER_DELETE " trigger: Error: %s",
                   db_errmsg(pconn, err_buf, sizeof(err_buf)));
        goto free_str;
    }

    rc = db_create_trigger(pconn, ACCT_TRIGGER_DELETE, "BEFORE DELETE",
                           acct_info_table, request->str);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create " ACCT_TRIGGER_DELETE " trigger: Error: %s",
                   db_errmsg(pconn, err_buf, sizeof(err_buf)));
        goto free_str;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Trigger "ACCT_TRIGGER_DELETE" created successfully");
    rc = DB_SUCCESS;

free_str:
    g_string_free(request, TRUE);
    return rc;
}

static int create_trig_acct_update(db_conn_t *pconn)
{
    int      rc, i;
    bool     is_first_field = true;
    GString *request;
    char     err_buf[1024];

    /* Trigger on update */

    /* In case the owner or group changes, we
     * must substract old information in previous raw
     * and add new information to the new raw.
     */
    /* Simple case: owner and group are still the same */
    request = g_string_new("DECLARE val_old, val_new BIGINT UNSIGNED;"
                           "SET val_old="ACCT_SZ_VAL("OLD.size")"; "
                           "SET val_new="ACCT_SZ_VAL("NEW.size")";\n"
                           "IF ");
    /* generate comparison like NEW.owner=OLD.owner AND NEW.gr_name=OLD.gr_name */
    attrmask2fieldcomparison(request, acct_pk_attr_set, T_ACCT, "NEW.", "OLD.", "=", "AND");
    g_string_append(request, "THEN \n\t IF ");
    /********* if one of the attribute value has changed: update the acct table *********/
    /* generate comparison like NEW.size<>=OLD.size OR NEW.blocks<>OLD.blocks */
    attrmask2fieldcomparison(request, acct_attr_set, T_ACCT, "NEW.", "OLD.", "<>", "OR");
    g_string_append(request, "THEN \n\t\t UPDATE " ACCT_TABLE " SET ");
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if (is_acct_field(i))
        {
            if (!is_first_field)
                g_string_append_printf(request, ", %s=%s+CAST(NEW.%s as SIGNED)-CAST(OLD.%s as SIGNED) ",
                                       field_name(i), field_name(i), field_name(i), field_name(i));
            else
            {
                g_string_append_printf(request, " %s=%s+CAST(NEW.%s as SIGNED)-CAST(OLD.%s as SIGNED) ",
                                       field_name(i), field_name(i), field_name(i), field_name(i));
                is_first_field = false;
            }
        }
    }

    /* update size range values */
    g_string_append_printf(request, "%s%s=CAST(%s as SIGNED)-CAST(((OLD.size=0)+(NEW.size=0)) as SIGNED)",
                           is_first_field?" ":", ", sz_field[0], sz_field[0]);
    is_first_field = false;
    for (i = 1; i < SZ_PROFIL_COUNT-1; i++) /* 2nd to before the last */
    {
        g_string_append_printf(request, ", %s=CAST(%s as SIGNED)-CAST(IFNULL(val_old=%u,0) as SIGNED)+CAST(IFNULL(val_new=%u,0) as SIGNED)",
                               sz_field[i], sz_field[i], i-1, i-1);
    }
    /* last */
    g_string_append_printf(request, ", %s=CAST(%s as SIGNED)-CAST(IFNULL(val_old>=%u,0) as SIGNED)+CAST(IFNULL(val_new>=%u,0) as SIGNED)",
                           sz_field[i], sz_field[i], i-1, i-1);
    g_string_append(request, " WHERE ");
    /* generate comparison as follows: owner=NEW.owner AND gr_name=NEW.gr_name */
    attrmask2fieldcomparison(request, acct_pk_attr_set, T_ACCT, "", "NEW.", "=", "AND");
    g_string_append(request, "; \n\t END IF; \nELSEIF ");

    /* tricky case: owner and/or group changed */

    attrmask2fieldcomparison(request, acct_pk_attr_set, T_ACCT, "NEW.", "OLD.", "<>", "OR");
    g_string_append(request, "THEN \n\tINSERT INTO " ACCT_TABLE "(");
    /* generate fields as follows: owner, gr_name */
    attrmask2fieldlist(request, acct_pk_attr_set, T_ACCT, false, false, "", "");
    /* generate fields as follows: , size, blocks */
    attrmask2fieldlist(request, acct_attr_set, T_ACCT, true, false, "", "");
    g_string_append(request, ", " ACCT_FIELD_COUNT);
    append_size_range_fields(request, true, "");
    g_string_append(request, ") VALUES (");
    /* generate fields as follows: NEW.owner, NEW.gr_name */
    attrmask2fieldlist(request, acct_pk_attr_set, T_ACCT, false, false, "NEW.", "");
    attrmask2fieldlist(request, acct_attr_set, T_ACCT, true, false, "NEW.", "");
    g_string_append(request, ",1");
    append_size_range_val(request, true, "NEW.", "val_new");

    g_string_append(request, ") \n\tON DUPLICATE KEY UPDATE ");
    /* generate operations as follows: size=size+New.size, blocks=blocks+NEW.blocks */
    attrmask2fieldoperation(request, acct_attr_set, T_ACCT, "NEW.", ADD);
    g_string_append(request, ", " ACCT_FIELD_COUNT "=" ACCT_FIELD_COUNT  "+1");
    /* update size range values */
    append_size_range_op(request, true, "NEW.", "val_new", ADD);
    g_string_append(request, ";\n"
                    "\tUPDATE " ACCT_TABLE " SET ");

    /* generate operations as follows: size=size-Old.size, blocks=blocks-Old.blocks */
    attrmask2fieldoperation(request, acct_attr_set, T_ACCT, "OLD.", SUBSTRACT);
    g_string_append(request, ", " ACCT_FIELD_COUNT "=" ACCT_FIELD_COUNT "-1 ");
    append_size_range_op(request, true, "OLD.", "val_old", SUBSTRACT);
    g_string_append(request, " WHERE ");
    attrmask2fieldcomparison(request, acct_pk_attr_set, T_ACCT, "", "OLD.", "=", "AND");
    g_string_append(request, ";\nEND IF;\n");

    rc = db_drop_component(pconn, DBOBJ_TRIGGER, ACCT_TRIGGER_UPDATE);
    if (rc != DB_SUCCESS && rc != DB_TRG_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to drop "ACCT_TRIGGER_UPDATE" trigger: Error: %s",
                   db_errmsg(pconn, err_buf, sizeof(err_buf)));
        goto free_str;
    }

    rc = db_create_trigger(pconn, ACCT_TRIGGER_UPDATE, "AFTER UPDATE",
                           acct_info_table, request->str);
    if (rc)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create "ACCT_TRIGGER_UPDATE" trigger: Error: %s",
                   db_errmsg(pconn, err_buf, sizeof(err_buf)));
        goto free_str;
    }
    DisplayLog(LVL_VERB, LISTMGR_TAG, "Trigger "ACCT_TRIGGER_UPDATE" created successfully");
    rc = DB_SUCCESS;

free_str:
    g_string_free(request, TRUE);
    return rc;
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
    int      rc;
    GString *request;
    char     err_buf[1024];

    /* XXX /!\ do not modify the code of DB functions
     * without changing FUNCTIONSET_VERSION!!!!
     */
    rc = db_drop_component(pconn, DBOBJ_FUNCTION, ONE_PATH_FUNC);
    if (rc != DB_SUCCESS && rc != DB_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to drop function '"ONE_PATH_FUNC"': Error: %s",
                   db_errmsg(pconn, err_buf, sizeof(err_buf)));
        return rc;
    }
    /* creating function to get one path for a file */
    /* Note: use "DETERMINISTIC" assuming that it returns the same path for the same id in a given request */
    request = g_string_new(NULL);
    g_string_printf(request, "CREATE FUNCTION "ONE_PATH_FUNC"(param "PK_TYPE")"
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

    rc = db_exec_sql(pconn, request->str, NULL);
    if (rc)
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create function '"ONE_PATH_FUNC"': Error: %s",
                   db_errmsg(pconn, err_buf, sizeof(err_buf)));

    g_string_free(request, TRUE);
    return rc;
}

static int check_func_thispath(db_conn_t *pconn)
{
    return db_check_component(pconn, DBOBJ_FUNCTION, THIS_PATH_FUNC, NULL);
}

static int create_func_thispath(db_conn_t *pconn)
{
    int      rc;
    GString *request;
    char     err_buf[1024];
    /* XXX /!\ do not modify the code of DB functions
     * without changing FUNCTIONSET_VERSION!!!!
     */

    /* drop previous versions of the function */
    rc = db_drop_component(pconn, DBOBJ_FUNCTION, THIS_PATH_FUNC);
    if (rc != DB_SUCCESS && rc != DB_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to drop function '"THIS_PATH_FUNC"': Error: %s",
                   db_errmsg(pconn, err_buf, sizeof(err_buf)));
        return rc;
    }

    /* creating function to get a path for a file, for the given parent and name  */
    /* Note: use "DETERMINISTIC" assuming that it returns the same path for the same parent+name in a given request */
    request = g_string_new(NULL);
    g_string_printf(request, "CREATE FUNCTION "THIS_PATH_FUNC"(pid_arg "PK_TYPE ", n_arg VARCHAR(%u))"
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

    rc = db_exec_sql(pconn, request->str, NULL);
    if (rc)
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to create function '"THIS_PATH_FUNC"': Error: %s",
                   db_errmsg(pconn, err_buf, sizeof(err_buf)));

    g_string_free(request, TRUE);
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
    {DBOBJ_TABLE, SOFT_RM_TABLE, check_table_softrm, create_table_softrm},

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
int ListMgr_Init(bool report_access_only)
{
    int            rc;
    db_conn_t      conn;
    const dbobj_descr_t *o;
    bool create_all_functions = false;
    bool create_all_triggers = false;

    /* store the parameter as a global variable */
    report_only = report_access_only;

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
        create_all_functions = true;
    if (check_triggers_version(&conn) != DB_SUCCESS)
        create_all_triggers = true;

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

    p_mgr->last_commit = 0;
    p_mgr->force_commit = false;
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
