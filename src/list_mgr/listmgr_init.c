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
#include <pwd.h>
#include <grp.h>

/* global symbols */
static const char *acct_info_table = NULL;
static enum lmgr_init_flags init_flags;
#define report_only (!!(init_flags & LIF_REPORT_ONLY))
#define alter_db    (!!(init_flags & LIF_ALTER_DB))

#define MAX_DB_FIELDS 64


static void append_status_def(const sm_instance_t *smi, GString *str, bool is_first)
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

static void append_field(GString *str, bool is_first, db_type_e type,
                         unsigned int size, const char *name,
                         const db_type_u *default_value)
{
    switch (type)
    {
    case DB_STRIPE_INFO:   /* never in main table (ignored) */
    case DB_STRIPE_ITEMS:
        break;
    case DB_TEXT:
        {
            char strtype[128];

            /* VARBINARY length is limited. For larger strings, use TEXT. */
            if (size <= MAX_VARBINARY)
                snprintf(strtype, sizeof(strtype),"VARBINARY(%u)", size);
            else
                rh_strncpy(strtype, "TEXT", sizeof(strtype));

            if (default_value && default_value->val_str != NULL)
                g_string_append_printf(str, "%s %s %s DEFAULT '%s'",is_first ? "" : ",",
                    name, strtype, default_value->val_str);
            else
                g_string_append_printf(str, "%s %s %s",is_first ? "" : ",",
                    name, strtype);
        }
        break;
    case DB_INT:
        if (default_value)
            g_string_append_printf(str, "%s %s INT DEFAULT %d", is_first ? "" : ",", name,
                default_value->val_int);
        else
            g_string_append_printf(str, "%s %s INT", is_first ? "" : ",", name);
        break;
    case DB_UINT:
        if (default_value)
            g_string_append_printf(str, "%s %s INT UNSIGNED DEFAULT %u", is_first ? "" : ",", name,
                default_value->val_uint);
        else
            g_string_append_printf(str, "%s %s INT UNSIGNED", is_first ? "" : ",", name);
        break;
    case DB_SHORT:
        if (default_value)
            g_string_append_printf(str, "%s %s SMALLINT DEFAULT %hd", is_first ? "" : ",", name,
                default_value->val_short);
        else
            g_string_append_printf(str, "%s %s SMALLINT", is_first ? "" : ",", name);
        break;
    case DB_USHORT:
        if (default_value)
            g_string_append_printf(str, "%s %s SMALLINT UNSIGNED DEFAULT %hu", is_first ? "" : ",", name,
                default_value->val_ushort);
        else
            g_string_append_printf(str, "%s %s SMALLINT UNSIGNED", is_first ? "" : ",", name);
        break;
    case DB_BIGINT:
        if (default_value)
            g_string_append_printf(str, "%s %s BIGINT DEFAULT %lld", is_first ? "" : ",", name,
                default_value->val_bigint);
        else
            g_string_append_printf(str, "%s %s BIGINT", is_first ? "" : ",", name);
        break;
    case DB_BIGUINT:
        if (default_value)
            g_string_append_printf(str, "%s %s BIGINT UNSIGNED DEFAULT %llu", is_first ? "" : ",", name,
                default_value->val_biguint);
        else
            g_string_append_printf(str, "%s %s BIGINT UNSIGNED", is_first ? "" : ",", name);
        break;
    case DB_BOOL:
        if (default_value)
            g_string_append_printf(str, "%s %s BOOLEAN DEFAULT %d", is_first ? "" : ",", name,
                default_value->val_bool);
        else
            g_string_append_printf(str, "%s %s BOOLEAN", is_first ? "" : ",", name);
        break;
    case DB_ID:
        g_string_append_printf(str, "%s %s "PK_TYPE, is_first ? "" : ",", name);
        break;
    case DB_ENUM_FTYPE:
        g_string_append_printf(str, "%s %s ENUM('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                        is_first ? "" : ",", name,
                        STR_TYPE_LINK, STR_TYPE_DIR, STR_TYPE_FILE, STR_TYPE_CHR,
                        STR_TYPE_BLK, STR_TYPE_FIFO, STR_TYPE_SOCK);
        break;
    case DB_UIDGID:
        {
            if (global_config.uid_gid_as_numbers)
            {
                if (default_value)
                    g_string_append_printf(str, "%s %s INT DEFAULT %d", is_first ? "" : ",", name,
                                           default_value->val_int);
                else
                    g_string_append_printf(str, "%s %s INT", is_first ? "" : ",", name);
            }
            else
            {
                char strtype[128];

                /* VARBINARY length is limited. For larger strings, use TEXT. */
                if (RBH_LOGIN_MAX-1 <= MAX_VARBINARY)
                    snprintf(strtype, sizeof(strtype),"VARBINARY(%u)", RBH_LOGIN_MAX-1);
                else
                    rh_strncpy(strtype, "TEXT", sizeof(strtype));

                if (default_value && default_value->val_str != NULL)
                    g_string_append_printf(str, "%s %s %s DEFAULT '%s'",is_first ? "" : ",",
                                           name, strtype, default_value->val_str);
                else
                    g_string_append_printf(str, "%s %s %s",is_first ? "" : ",",
                                           name, strtype);
            }
        }
        break;
    }
}

static db_type_u default_uid = {.val_str = ACCT_DEFAULT_OWNER};
static db_type_u default_gid = {.val_str = ACCT_DEFAULT_GROUP};

static void init_default_field_values(void)
{
    if (global_config.uid_gid_as_numbers)
    {
        char buff[4096];
        struct passwd pw;
        struct passwd *p_pw;
        struct group gr;
        struct group *p_gr;

        if (getpwnam_r("nobody", &pw, buff, sizeof(buff), &p_pw) != 0 ||
            p_pw == NULL)
        {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                       "Warning: couldn't resolve uid for user 'nobody'");

            /* nobody is 65534 on most Linux systems. */
            default_uid.val_int = 65534;
        } else {
            default_uid.val_int = pw.pw_uid;
        }

        if ((getgrnam_r("nobody", &gr, buff, sizeof(buff), &p_gr) != 0 ||
             p_gr == NULL) &&
            (getgrnam_r("nogroup", &gr, buff, sizeof(buff), &p_gr) != 0 ||
             p_gr == NULL))
        {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                       "Warning: couldn't resolve gid for group 'nogroup' or 'nobody'");

            /* nogroup is 65534 on Debian. nobody is 99 on RHEL and
             * 65533 on SLES. */
            default_gid.val_int = 65534;
        } else {
            default_gid.val_int = gr.gr_gid;
        }
    }
}

static const db_type_u *default_field_value(int attr_index)
{

    switch (attr_index)
    {
        case ATTR_INDEX_uid:
            return &default_uid;
        case ATTR_INDEX_gid:
            return &default_gid;
        default:
            return NULL;
    }
    UNREACHED();
}

static void append_field_def(int i, GString *str, bool is_first)
{
    unsigned int idx;

    if (is_status_field(i))
    {
        idx = attr2status_index(i);
        append_status_def(get_sm_instance(idx), str, is_first);
        return;
    }
    if (is_sm_info_field(i))
    {
        idx = attr2sminfo_index(i);
        append_field(str, is_first, sm_attr_info[idx].def->db_type,
                     sm_attr_info[idx].def->db_type_size,
                     sm_attr_info[idx].db_attr_name,
                     &sm_attr_info[idx].def->db_default);
        return;
    }

    append_field(str, is_first, field_infos[i].db_type,
                 field_infos[i].db_type_size,
                 field_infos[i].field_name,
                 default_field_value(i));
}

/**
 * Check table fields.
 * @param i
 * @param curr_field_index [in,out] field index in currently checked schema
 * @return 0 on success
 * @return -1 on error
 */
static int _check_field_name(const char *name, int *curr_field_index,
                             const char *table, char **fieldtab)
{
    if ((*curr_field_index >= MAX_DB_FIELDS)
        || (fieldtab[*curr_field_index] == NULL))
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Database schema: missing field '%s' in table %s.",
                   name, table);
        return -1;
    }
    /* check that this is the expected field */
    if (!strcmp(name, fieldtab[*curr_field_index]))
    {
        DisplayLog(LVL_FULL, LISTMGR_TAG, "%s.%s OK", table, name);
        return 0;
    }

    DisplayLog(LVL_DEBUG, LISTMGR_TAG, "%s: '%s' expected, '%s' found", table,
               name, fieldtab[*curr_field_index]);

    DisplayLog(LVL_MAJOR, LISTMGR_TAG,
               "Database schema: unexpected field '%s' in table %s: '%s' expected.",
               fieldtab[*curr_field_index], table, name);
    return -1;
}

static int check_field_name(const char *name, int *curr_field_index,
                            const char *table, char **fieldtab)
{
    if (_check_field_name(name, curr_field_index, table, fieldtab) == 0)
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

/** Rename field 'old_name' to 'new_name' */
static int change_field_name(db_conn_t *pconn, const char *table, const char *old_name,
                              const char *new_name, int field_index)
{
    /* syntax: ALTER TABLE <tablename> CHANGE <OldColumnName> <NewColunmName> <DATATYPE>; */
    GString *query = g_string_new(NULL);
    int rc;

    g_string_printf(query, "ALTER TABLE %s CHANGE %s", table, old_name);
    append_field_def(field_index, query, true);

    DisplayLog(LVL_VERB, LISTMGR_TAG, "sql> %s", query->str);

    rc = db_exec_sql(pconn, query->str, NULL);
    g_string_free(query, TRUE);

    if (rc)
    {
        char buff[1024];

        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to rename field '%s' to '%s': Error: %s",
                    old_name, new_name, db_errmsg(pconn, buff, sizeof(buff)));
        return rc;
    }
    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "%s.%s successfully renamed to %s.%s", table,
               old_name, table, new_name);
    return 0;
}


static int change_id_field(db_conn_t *pconn, const char *table, const char *old_name,
                           const char *new_name)
{
    /* syntax: ALTER TABLE <tablename> CHANGE <OldColumnName> <NewColunmName> <DATATYPE>; */
    GString *query = g_string_new(NULL);
    int rc;

    g_string_printf(query, "ALTER TABLE %s CHANGE %s", table, old_name);
    append_field(query, true, DB_ID, 0, new_name, NULL);

    DisplayLog(LVL_VERB, LISTMGR_TAG, "sql> %s", query->str);

    rc = db_exec_sql(pconn, query->str, NULL);
    g_string_free(query, TRUE);

    if (rc)
    {
        char buff[1024];

        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Failed to rename field '%s' to '%s': Error: %s",
                    old_name, new_name, db_errmsg(pconn, buff, sizeof(buff)));
        return rc;
    }
    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "%s.%s successfully renamed to %s.%s", table,
               old_name, table, new_name);
    return 0;
}

/** Insert field defined by 'def_index' after 'prev_field'. */
static int insert_field(db_conn_t *pconn, const char *table, int def_index,
                        const char *prev_field)
{
    /* syntax: ALTER TABLE <tablename> ADD <field_name> <field_type> AFTER <prev_field_name> */
    GString *query;
    int rc;

    if (!alter_db)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "DB schema change detected: field '%s.%s' must be added "
                   " => Run 'robinhood --alter-db' to apply this change.", table, field_name(def_index));
        return DB_NEED_ALTER;
    }

    if (prev_field)
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "=> Inserting field '%s' in table %s after '%s'",
                   field_name(def_index), table, prev_field);
    else
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "=> Appending field '%s' in table %s",
                   field_name(def_index), table);

    query = g_string_new(NULL);
    g_string_printf(query, "ALTER TABLE %s ADD ", table);
    append_field_def(def_index, query, true);
    if (prev_field != NULL)
        g_string_append_printf(query," AFTER %s", prev_field);

    DisplayLog(LVL_VERB, LISTMGR_TAG, "sql> %s", query->str);

    rc = db_exec_sql(pconn, query->str, NULL);
    g_string_free(query, TRUE);

    if (rc)
    {
        char buff[1024];

        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Failed to insert field '%s': Error: %s",
                   field_name(def_index), db_errmsg(pconn, buff, sizeof(buff)));
        return rc;
    }
    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "%s.%s successfully inserted", table,
               field_name(def_index));
    return 0;
}

static int drop_field(db_conn_t *pconn, const char *table, const char *field)
{
    /* syntax: ALTER TABLE <tablename> DROP <field_name> */
    GString *query;
    int rc;

    if (!alter_db)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG, "DB schema change detected: field '%s.%s' must be DROPPED "
                   " => Run 'robinhood --alter-db' to confirm this change.", table, field);
        return DB_NEED_ALTER;
    }

    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "=> Dropping field '%s' from table %s",
               field, table);

    query = g_string_new(NULL);
    g_string_printf(query, "ALTER TABLE %s DROP %s", table, field);
    DisplayLog(LVL_VERB, LISTMGR_TAG, "sql> %s", query->str);

    rc = db_exec_sql(pconn, query->str, NULL);
    g_string_free(query, TRUE);

    if (rc)
    {
        char buff[1024];

        DisplayLog(LVL_CRIT, LISTMGR_TAG, "Failed to drop field '%s': Error: %s",
                   field, db_errmsg(pconn, buff, sizeof(buff)));
        return rc;
    }
    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "%s.%s successfully dropped.", table,
               field);
    return 0;
}

/** check if the given DB field is in next expected ones */
static bool is_next_expected(table_enum table, const char *db_field_name,
                             int curr_field_def_index, bool allow_func_attr,
                             int *found_index)
{
    int i, cookie;

    if (found_index)
        *found_index = -1;

    /* end of table, must insert as last field */
    if (db_field_name == NULL)
        return false;

    cookie = curr_field_def_index;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {

        if (match_table(table, i) && (allow_func_attr || !is_funcattr(i))
            && !strcmp(db_field_name, field_name(i)))
        {
            if (found_index)
                *found_index = i;
            return true;
        }
    }
    return false;
}

/* describe name change comatibility */
struct name_compat {
    const char *old_name;
    const char *new_name;
};

/**
 * Handle compatibility with old field names.
 * @retval 1 if field has been converted/renamed.
 * @retval 0 id field matches no rename rule.
 * @retval < 0 on error.
 */
static int check_renamed_db_field(db_conn_t *pconn, table_enum table,
                                  int field_index, const char *curr_field_name,
                                  const struct name_compat *compat_table,
                                  bool allow_func_attr)
{
    int i, rc;
    const char *tname = table2name(table);

    /* no table (or end of table) => not a rename */
    if (compat_table == NULL || curr_field_name == NULL)
        return 0;

    /* handle compatibility with old field names */
    for (i = 0; compat_table[i].old_name != NULL; i++)
    {
        /* does the DB field matches the old name? */
        if (strcmp(curr_field_name, compat_table[i].old_name))
            continue;

        /* DB field matches */
        DisplayLog(LVL_FULL, LISTMGR_TAG, "DB field '%s' matches an old name for '%s'",
                   curr_field_name, compat_table[i].new_name);

        /* does it match the currently expected one? */
        if (!strcmp(field_name(field_index), compat_table[i].new_name))
        {
            if (!alter_db)
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG, "DB schema change detected: "
                           "field '%s.%s' renamed to '%s.%s' "
                           " => Run 'robinhood --alter-db' to apply this change.",
                           tname, compat_table[i].old_name,
                           tname, compat_table[i].new_name);
                return -DB_NEED_ALTER;
            }

            DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Renaming old field '%s.%s' "
                       "to '%s.%s'.", tname, compat_table[i].old_name,
                       tname, compat_table[i].new_name);

            rc = change_field_name(pconn, tname, compat_table[i].old_name,
                                   compat_table[i].new_name, field_index);
            if (rc)
                /* db errors codes are > 0 */
                return -rc;

            return 1;
        }
        /* not an handled case */
        DisplayLog(LVL_DEBUG, LISTMGR_TAG, "Unhandled rename case");
        return 0;
    }

    return 0;
}

/** return the oldname for a new field */
static const char *get_old_name(const struct name_compat *compat_table,
                                const char *new_name)
{
    int i;

    /* no compat table => no old name */
    if (compat_table == NULL)
        return NULL;

    /* handle compatibility with old field names */
    for (i = 0; compat_table[i].old_name != NULL; i++)
    {
        /* does the entry matches the new name */
        if (!strcmp(new_name, compat_table[i].new_name))
            return compat_table[i].old_name;
    }
    return NULL;
}

/** check if the given field definition is in the next DB fields */
static bool is_next_db_field(const char *field_def_name, char * const *curr_field,
                                int *shift)
{
    *shift = 0;

    curr_field++; /* start from next field */
    while (*curr_field != NULL)
    {
        (*shift)++;
        if (!strcmp(*curr_field, field_def_name))
            return true;
        curr_field++;
    }
    return false;
}

static inline void swap_db_fields(char **field_tab, int i1, int i2)
{
    char *tmp = field_tab[i1];

    field_tab[i1] = field_tab[i2];
    field_tab[i2] = tmp;
}

/** @return -1 on error, 0 if OK, 1 if conversion is required */
static int check_field_name_type(const char *name, const char *type, int *curr_field_index,
                                 const char *table, char **fieldtab, char **typetab)
{
    if (_check_field_name(name, curr_field_index, table, fieldtab) != 0)
        return -1;

    if (check_type(typetab[*curr_field_index], type))
        return 1;

    (*curr_field_index)++;
    return 0;
}

static inline int check_field(int i, int *curr_field_index, const char *table, char **fieldtab)
{
    return check_field_name(field_name(i), curr_field_index, table, fieldtab);
}

/* Return false if there is no extra field, else return true */
static inline int has_extra_field(int curr_field_index, const char *table,
                                  char **fieldtab, bool warn)
{
        if ((curr_field_index < MAX_DB_FIELDS)
            && (fieldtab[curr_field_index] != NULL))
        {
            if (warn)
                DisplayLog(LVL_CRIT, LISTMGR_TAG,
                           "Database schema: extra field '%s' in table %s.",
                           fieldtab[curr_field_index], table);
            return true;
        }
        return false;
}

/** Check current field and fix the DB schema if 'alter_db' is specified */
static int check_and_fix_field(db_conn_t *pconn,
                               int def_index, int *db_index,
                               table_enum table, char **fieldtab,
                               const struct name_compat *compat_table,
                               const char **last_field, bool allow_func_attr)
{
    int rc, shift;

recheck:
    if (check_field(def_index, db_index, table2name(table), fieldtab) == 0)
    {
        /* check_field should have increased db_index */
        assert(*db_index > 0);
        *last_field = fieldtab[*db_index-1];
        /* OK */
        return 0;
    }

    /*  field appending case (end of table) */
    if (fieldtab[*db_index] == NULL)
    {
        rc = insert_field(pconn, table2name(table), def_index, NULL);
        if (rc == DB_SUCCESS && rc == DB_NEED_ALTER)
            /* NEED_ALTER: still update last_field to check other tables */
            *last_field = field_name(def_index);

        return rc;
    }

    DisplayLog(LVL_FULL, LISTMGR_TAG, "Checking if '%s' is renamed",
               fieldtab[*db_index]);
    /* convert renamed fields */
    rc = check_renamed_db_field(pconn, table, def_index, fieldtab[*db_index],
                                compat_table, allow_func_attr);
    if (rc == 1 || rc == -DB_NEED_ALTER)
    {
        /* NEED_ALTER: still update last_field to check next fields and tables */
        *last_field = field_name(def_index);
        (*db_index) ++;
        return rc == 1 ? 0 : -rc;
    } else if (rc < 0)
        /* DB error */
        return -rc;

    /* The current DB field is one of the next expected ones */
    if (is_next_expected(table, fieldtab[*db_index], def_index,
                            allow_func_attr, NULL))
    {
        int shift;

        /* 2 cases:
         * 1) fields have been shuffled: in this case,
         *    the expected field is one of the next DB fields.
         * 2) the expected field must be inserted.
         */
        if (is_next_db_field(field_name(def_index), fieldtab + *db_index, &shift))
        {
            DisplayLog(LVL_EVENT, LISTMGR_TAG,
                       "Shuffled DB fields: avoid changing "
                       "the order of policy definitions "
                       "to avoid this warning.");
            /* virtually swap the 2 field so that the related next field check
             * will be OK */
            swap_db_fields(fieldtab, *db_index, *db_index + shift);
            /* current field is OK */
            *last_field = fieldtab[*db_index];
            (*db_index) ++;
            return 0;
        }

        rc = insert_field(pconn, table2name(table), def_index, *last_field);
        if (rc == DB_SUCCESS && rc == DB_NEED_ALTER)
            /* NEED_ALTER: still update last_field to check other tables */
            *last_field = field_name(def_index);

        return rc; /* SUCCESS or NEED_ALTER */
    }

    /* If expected field in found later in the table, swap them
     * and keep current DB field for later */
    if (is_next_db_field(field_name(def_index), fieldtab + *db_index, &shift))
    {
        DisplayLog(LVL_FULL, LISTMGR_TAG, "'%s' is in next DB fields",
                   field_name(def_index));
        /* swap fields and keep current DB field for later */
        swap_db_fields(fieldtab, *db_index, *db_index + shift);
        /* current field is OK */
        *last_field = fieldtab[*db_index];
        (*db_index) ++;
        return 0;
    }

    /* does this field has an old name ?*/
    const char *old_name = get_old_name(compat_table, field_name(def_index));
    if (old_name != NULL && is_next_db_field(old_name, fieldtab + *db_index, &shift))
    {
        /* swap and recheck */
        swap_db_fields(fieldtab, *db_index, *db_index + shift);
        goto recheck;
    }

    /* Expected field is not in DB => insert it */
    rc = insert_field(pconn, table2name(table), def_index, *last_field);
    if (rc == 0 || rc == DB_NEED_ALTER)
        *last_field = field_name(def_index);
    return rc;
}

/** drop extra fields at the end of a table */
static int drop_extra_fields(db_conn_t *pconn, int curr_field_index,
                             table_enum table, char **fieldtab)
{
    bool need_alter = false;
    int rc = 0;

    /* is there any extra field ? */
    if (!has_extra_field(curr_field_index, table2name(table), fieldtab,
                         !report_only))
        return 0;

    /* This is allowed, in particular for read-only case, if the report
     * command don't have all policies defined in its configuration file. */
    if (report_only)
    {
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Warning: extra fields found in "
                   "table %s: '%s'", table2name(table), fieldtab[curr_field_index]);
        return need_alter ? DB_NEED_ALTER : 0;
    }

    while (fieldtab[curr_field_index] != NULL)
    {
        rc = drop_field(pconn, table2name(table), fieldtab[curr_field_index]);
        if (rc != 0 && rc != DB_NEED_ALTER)
            return rc;

        if (rc == DB_NEED_ALTER)
            need_alter = true;

        curr_field_index ++;
    }

    return (rc == 0 && need_alter) ? DB_NEED_ALTER : rc;
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

    if (lmgr_config.acct)
    {
        int i, cookie;

        cookie = -1;
        while ((i = attr_index_iter(0, &cookie)) != -1)
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

        if (has_extra_field(curr_index, VAR_TABLE, fieldtab, true))
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

    if (strcasecmp(lmgr_config.db_config.engine, "TokuDB") == 0)
        g_string_append_printf(request, " COMPRESSION=%s",
                               lmgr_config.db_config.tokudb_compression);
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

static struct name_compat main_name_compat[] = {
    {"owner",        "uid"},
    {"gr_name",      "gid"},
/* Lustre/HSM fields */
    {"no_release",   "lhsm_norels"},
    {"no_archive",   "lhsm_noarch"},

    {NULL,      NULL},
};

/* FIXME: these fields were in ANNEX_INFO */
/*
    {"last_archive", "lhsm_lstarc"},
    {"last_restore", "lhsm_lstrst"},
    {"archive_id",    "lhsm_archid"},
*/

static int check_table_main(db_conn_t *pconn)
{
    char strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];
    bool need_alter = false;

    int rc = db_list_table_fields(pconn, MAIN_TABLE, fieldtab, MAX_DB_FIELDS,
                                  strbuf, sizeof(strbuf));
    if (rc == DB_SUCCESS)
    {
        int i, cookie;
        int curr_field_index = 0;
        const char *last = NULL;

        /* check primary key */
        if (check_field_name("id", &curr_field_index, MAIN_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        last = "id";

        /* std fields + SM status + SM specific info */
        cookie = -1;
        while ((i = attr_index_iter(0, &cookie)) != -1)
        {
            /* is this field part of MAIN_TABLE? */
            if (is_main_field(i) && !is_funcattr(i))
            {
                rc = check_and_fix_field(pconn, i, &curr_field_index, T_MAIN,
                                         fieldtab, main_name_compat, &last, false);
                if (rc == DB_NEED_ALTER)
                    need_alter = true;
                    /* don't return immediately, to report about other fields */
                else if (rc)
                    return rc;
            }
        }

        rc = drop_extra_fields(pconn, curr_field_index, T_MAIN, fieldtab);
        if (rc)
            return rc;
    }
    else if (rc != DB_NOT_EXISTS)
    {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Error checking database schema: %s",
                       db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return (rc == 0 && need_alter) ? DB_NEED_ALTER : rc;
}

static int create_table_main(db_conn_t *pconn)
{
    GString    *request;
    int         i, rc, cookie;

    request = g_string_new("CREATE TABLE "MAIN_TABLE" (id "PK_TYPE" PRIMARY KEY");

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (is_main_field(i) && !is_funcattr(i))
            append_field_def(i, request, 0);
    }

    /* end of field list (null terminated) */
    g_string_append(request, ")");
    append_engine(request);

    rc = run_create_table(pconn, MAIN_TABLE, request->str);
    if (rc)
        goto free_str;

    /* create indexes on this table */
    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
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
        int i, cookie;
        int curr_field_index = 0;

        /* check first fields: id and pkn */
        if (check_field_name("id", &curr_field_index, DNAMES_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        if (check_field_name("pkn", &curr_field_index, DNAMES_TABLE, fieldtab))
            return DB_BAD_SCHEMA;

        cookie = -1;
        while ((i = attr_index_iter(0, &cookie)) != -1)
        {
            if (is_names_field(i) && !is_funcattr(i))
            {
                if (check_field(i, &curr_field_index, DNAMES_TABLE, fieldtab))
                    return DB_BAD_SCHEMA;
            }
        }
        /* is there any extra field ? */
        if (has_extra_field(curr_field_index, DNAMES_TABLE, fieldtab, true))
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
    int         i, rc, cookie;

    request = g_string_new("CREATE TABLE "DNAMES_TABLE" (id "PK_TYPE", "
                           "pkn VARBINARY(40) PRIMARY KEY");

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (is_names_field(i) && !is_funcattr(i))
        {
            append_field_def(i, request, 0);
        }
    }
    g_string_append(request, ")");
    append_engine(request);

    rc = run_create_table(pconn, DNAMES_TABLE, request->str);
    if (rc)
        goto free_str;

    /* create indexes on this table */
    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
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
    int rc, i, cookie;
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];
    const char *last = NULL;
    bool need_alter = false;

    rc = db_list_table_fields(pconn, ANNEX_TABLE, fieldtab, MAX_DB_FIELDS,
                              strbuf, sizeof(strbuf));

    if (rc == DB_SUCCESS)
    {
        int curr_field_index = 0;

        /* check primary key */
        if (check_field_name("id", &curr_field_index, ANNEX_TABLE, fieldtab))
            return DB_BAD_SCHEMA;
        last = "id";

        cookie = -1;
        while ((i = attr_index_iter(0, &cookie)) != -1)
        {
            if (is_annex_field(i) && !is_funcattr(i))
            {
                rc = check_and_fix_field(pconn, i, &curr_field_index, T_ANNEX,
                                         fieldtab, NULL, &last, false);
                if (rc)
                    return rc;
            }
        }

        rc = drop_extra_fields(pconn, curr_field_index, T_ANNEX, fieldtab);
        if (rc)
            return rc;
    }
    else if (rc != DB_NOT_EXISTS)
    {
            DisplayLog(LVL_CRIT, LISTMGR_TAG,
                       "Error checking database schema: %s",
                       db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return (rc == 0 && need_alter) ? DB_NEED_ALTER : rc;
}

static int create_table_annex(db_conn_t *pconn)
{
    GString  *request;
    int       i, rc, cookie;

    request = g_string_new("CREATE TABLE "ANNEX_TABLE" (id "PK_TYPE" PRIMARY KEY");

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (is_annex_field(i) && !is_funcattr(i))
        {
            append_field_def(i, request, 0);
        }
    }
    g_string_append(request, ")");
    append_engine(request);

    rc = run_create_table(pconn, ANNEX_TABLE, request->str);
    if (rc)
        goto free_str;

    /* create indexes on this table */
    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
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
            case -1:
                return DB_BAD_SCHEMA;
            case 1:
                /* only run conversion for other programs than reporting commands */
                if (report_only)
                {
                    DisplayLog(LVL_CRIT, LISTMGR_TAG, "Incompatible DB type for "
                               STRIPE_INFO_TABLE".validator");
                    return DB_BAD_SCHEMA;
                }

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
        if (has_extra_field(curr_field_index, STRIPE_INFO_TABLE, fieldtab, true))
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
            "pool_name VARBINARY(%u))",
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
        if (has_extra_field(curr_field_index, STRIPE_ITEMS_TABLE, fieldtab, true))
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
    lmgr_config.acct = false;
    /* reset acct masks */
    acct_pk_attr_set = null_mask;
    acct_attr_set = null_mask;
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
        int cookie;
        int curr_field_index = 0;
        const char *last = NULL;

        /* When running daemon mode with accounting disabled: drop ACCT table,
         * else it may become inconsistent. */
        if (!lmgr_config.acct && !report_only)
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
        cookie = -1;
        while ((i = attr_index_iter(0, &cookie)) != -1)
        {
            if (is_acct_pk(i))
            {
                /* don't insert/append/remove PK fields */
                if (check_field(i, &curr_field_index, ACCT_TABLE, fieldtab))
                {
                    if (!alter_db)
                    {
                        DisplayLog(LVL_CRIT, LISTMGR_TAG, "DB schema change detected: "
                                   "modification in "ACCT_TABLE" primary key requires to drop and repopulate the table"
                                   " => Run 'robinhood --alter-db' to apply this change.");
                        return DB_NEED_ALTER;
                    }
                    DisplayLog(LVL_CRIT, LISTMGR_TAG, "DB schema change detected:"
                               " dropping and repopulating table "ACCT_TABLE);
                    rc = db_drop_component(pconn, DBOBJ_TABLE, ACCT_TABLE);
                    /* on success, return DB_NOT_EXISTS to re-create the table */
                    if (rc == DB_SUCCESS)
                        return DB_NOT_EXISTS;

                    DisplayLog(LVL_CRIT, LISTMGR_TAG,
                               "Failed to drop table: Error: %s",
                               db_errmsg(pconn, strbuf, sizeof(strbuf)));
                    return rc;
                }
            }
        }
        /* check other fields */
        cookie = -1;
        while ((i = attr_index_iter(0, &cookie)) != -1)
        {
            if (is_acct_field(i))
            {
                rc = check_and_fix_field(pconn, i, &curr_field_index, T_ACCT_VAL,
                                         fieldtab, main_name_compat, &last, false);
                if (rc)
                    return rc;
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

        rc = drop_extra_fields(pconn, curr_field_index, T_ACCT, fieldtab);
        if (rc)
            return rc;
    }
    else if (rc == DB_NOT_EXISTS)
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
    int      i, rc, cookie;
    bool     first_acct_pk = true;
    bool     is_first_acct_field = true;

    if (!lmgr_config.acct)
        return DB_SUCCESS;

    request = g_string_new("CREATE TABLE "ACCT_TABLE" (");

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (is_acct_pk(i))
        {
            append_field_def(i, request, is_first_acct_field);
            is_first_acct_field = false;
        }
    }

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (is_acct_field(i))
            append_field_def(i, request, is_first_acct_field);
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

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
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
    int rc, cookie;
    char  strbuf[4096];
    char *fieldtab[MAX_DB_FIELDS];
    bool need_alter = false;

    rc = db_list_table_fields(pconn, SOFT_RM_TABLE, fieldtab, MAX_DB_FIELDS,
                              strbuf, sizeof(strbuf));
    if (rc == DB_SUCCESS)
    {
        const char *last = NULL;
        int curr_index = 0;
        int i;

        /* check primary key */
        if (check_field_name("id", &curr_index, SOFT_RM_TABLE, fieldtab))
        {
            /* check old name 'fid' */
            if (!strcmp("fid", fieldtab[0]))
            {
                if (!alter_db) {
                    DisplayLog(LVL_CRIT, LISTMGR_TAG, "DB schema change detected: "
                               "field '%s.%s' renamed to '%s.%s' "
                               " => Run 'robinhood --alter-db' to apply this change.",
                               SOFT_RM_TABLE, "fid",
                               SOFT_RM_TABLE, "id");
                    need_alter = true;
                }
                else
                {
                    rc = change_id_field(pconn, SOFT_RM_TABLE, "fid", "id");
                    if (rc)
                        return rc;
                    curr_index++;
                }
            }
            else
                return DB_BAD_SCHEMA;
        }
        last = "id";

        cookie = -1;
        while ((i = attr_index_iter(0, &cookie)) != -1)
        {
            if (is_softrm_field(i))
            {
                rc = check_and_fix_field(pconn, i, &curr_index, T_SOFTRM,
                                         fieldtab, main_name_compat, &last, true);
                if (rc == DB_NEED_ALTER)
                    need_alter = true;
                    /* don't return immediately, to report about other fields */
                else if (rc)
                    return rc;
            }
        }

        rc = drop_extra_fields(pconn, curr_index, T_SOFTRM, fieldtab);
        if (rc)
            return rc;
    }
    else if (rc != DB_NOT_EXISTS)
    {
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "Error checking database schema: %s",
                   db_errmsg(pconn, strbuf, sizeof(strbuf)));
    }
    return (rc == 0 && need_alter) ? DB_NEED_ALTER : rc;
}

static int create_table_softrm(db_conn_t *pconn)
{
    GString *request;
    int      rc, i, cookie;

    request = g_string_new("CREATE TABLE "SOFT_RM_TABLE" (id "PK_TYPE" PRIMARY KEY");

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (is_softrm_field(i))
            append_field_def(i, request, 0);
    }
    g_string_append(request, ")");
    append_engine(request);

    rc = run_create_table(pconn, SOFT_RM_TABLE, request->str);
    if (rc)
        goto free_str;

    /* create indexes on this table */
    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
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

#define FUNCTIONSET_VERSION    "1.3"
#define TRIGGERSET_VERSION     "1.2"

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
    if (!lmgr_config.acct && !report_only)
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

    if (!lmgr_config.acct)
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
    if (!lmgr_config.acct)
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
    if (!lmgr_config.acct)
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
    attrmask2fieldoperation(request, acct_attr_set, T_ACCT, "OLD.", SUBTRACT);
    g_string_append(request, ", " ACCT_FIELD_COUNT  "=" ACCT_FIELD_COUNT  "-1");
    append_size_range_op(request, true, "OLD.", "val", SUBTRACT);

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
    int      rc, i, cookie;
    bool     is_first_field = true;
    GString *request;
    char     err_buf[1024];

    /* Trigger on update */

    /* In case the owner or group changes, we
     * must subtract old information in previous raw
     * and add new information to the new raw.
     */
    /* Simple case: owner and group are still the same */
    request = g_string_new("DECLARE val_old, val_new BIGINT UNSIGNED;"
                           "SET val_old="ACCT_SZ_VAL("OLD.size")"; "
                           "SET val_new="ACCT_SZ_VAL("NEW.size")";\n"
                           "IF ");
    /* generate comparison like NEW.uid=OLD.uid AND NEW.gid=OLD.gid */
    attrmask2fieldcomparison(request, acct_pk_attr_set, T_ACCT, "NEW.", "OLD.", "=", "AND");
    g_string_append(request, "THEN \n\t IF ");
    /********* if one of the attribute value has changed: update the acct table *********/
    /* generate comparison like NEW.size<>=OLD.size OR NEW.blocks<>OLD.blocks */
    attrmask2fieldcomparison(request, acct_attr_set, T_ACCT, "NEW.", "OLD.", "<>", "OR");
    g_string_append(request, "THEN \n\t\t UPDATE " ACCT_TABLE " SET ");

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
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
    /* generate comparison as follows: owner=NEW.uid AND gid=NEW.gid */
    attrmask2fieldcomparison(request, acct_pk_attr_set, T_ACCT, "", "NEW.", "=", "AND");
    g_string_append(request, "; \n\t END IF; \nELSEIF ");

    /* tricky case: owner and/or group changed */

    attrmask2fieldcomparison(request, acct_pk_attr_set, T_ACCT, "NEW.", "OLD.", "<>", "OR");
    g_string_append(request, "THEN \n\tINSERT INTO " ACCT_TABLE "(");
    /* generate fields as follows: owner, gid */
    attrmask2fieldlist(request, acct_pk_attr_set, T_ACCT, false, false, "", "");
    /* generate fields as follows: , size, blocks */
    attrmask2fieldlist(request, acct_attr_set, T_ACCT, true, false, "", "");
    g_string_append(request, ", " ACCT_FIELD_COUNT);
    append_size_range_fields(request, true, "");
    g_string_append(request, ") VALUES (");
    /* generate fields as follows: NEW.uid, NEW.gid */
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
    attrmask2fieldoperation(request, acct_attr_set, T_ACCT, "OLD.", SUBTRACT);
    g_string_append(request, ", " ACCT_FIELD_COUNT "=" ACCT_FIELD_COUNT "-1 ");
    append_size_range_op(request, true, "OLD.", "val_old", SUBTRACT);
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
        " RETURNS VARBINARY(%u) DETERMINISTIC READS SQL DATA"
        " BEGIN"
            " DECLARE p VARBINARY(%u) DEFAULT NULL;"
            " DECLARE pid "PK_TYPE" DEFAULT NULL;"
            " DECLARE n VARBINARY(%u) DEFAULT NULL;"
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
    g_string_printf(request, "CREATE FUNCTION "THIS_PATH_FUNC"(pid_arg "PK_TYPE ", n_arg VARBINARY(%u))"
        " RETURNS VARBINARY(%u) DETERMINISTIC READS SQL DATA"
        " BEGIN"
            " DECLARE p VARBINARY(%u) DEFAULT NULL;"
            " DECLARE pid "PK_TYPE" DEFAULT NULL;"
            " DECLARE n VARBINARY(%u) DEFAULT NULL;"
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
int ListMgr_Init(enum lmgr_init_flags flags)
{
    int            rc;
    db_conn_t      conn;
    const dbobj_descr_t *o;
    bool create_all_functions = false;
    bool create_all_triggers = false;

    /* store the parameter as a global variable */
    init_flags = flags;

    /* initialize attr masks for each table */
    init_attrset_masks(&lmgr_config);

    init_default_field_values();

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

            case DB_NEED_ALTER:
                if (report_only)
                    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "WARNING: ALTER required on %s %s",
                               dbobj2str(o->o_type), o->o_name);
                else
                    goto close_conn;
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
