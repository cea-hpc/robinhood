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
#include "listmgr_common.h"
#include "database.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "Memory.h"
#include "listmgr_stripe.h"
#include "xplatform_print.h"
#include <stdio.h>

void printdbtype(lmgr_t *p_mgr, GString *str, db_type_e type, const db_type_u *value_ptr )
{
    switch (type)
    {
        case DB_ID:
        {
            DEF_PK(tmpstr);

            /* convert id to str */
            entry_id2pk(&value_ptr->val_id, tmpstr);
            g_string_append_printf(str, DPK, tmpstr);
            break;
        }
        case DB_UIDGID:
            if (global_config.uid_gid_as_numbers) {
                g_string_append_printf(str, "%d", value_ptr->val_int);
                break;
            }
            /* UID/GID is TEXT. Fall throught ... */

        case DB_TEXT:
        {
            /* escape special characters in value */
            int   len = 2*strlen(value_ptr->val_str) + 1; /* as required by MySQL manual */
            char *tmpstr = MemAlloc(len);

            db_escape_string(&p_mgr->conn, tmpstr, len, value_ptr->val_str);
            g_string_append_printf(str, "'%s'", tmpstr);
            MemFree(tmpstr);
            break;
        }
        case DB_INT:
            g_string_append_printf(str, "%d", value_ptr->val_int);
            break;
        case DB_UINT:
            g_string_append_printf(str, "%u", value_ptr->val_uint);
            break;
        case DB_SHORT:
            g_string_append_printf(str, "%hd", value_ptr->val_short);
            break;
        case DB_USHORT:
            g_string_append_printf(str, "%hu", value_ptr->val_ushort);
            break;
        case DB_BIGINT:
            g_string_append_printf(str, "%lld", value_ptr->val_bigint);
            break;
        case DB_BIGUINT:
            g_string_append_printf(str, "%llu", value_ptr->val_biguint);
            break;
        case DB_BOOL:
            if (value_ptr->val_bool)
                g_string_append(str, "1");
            else
                g_string_append(str, "0");
            break;
        case DB_ENUM_FTYPE:
            /* don't escape: type value is trusted (not from user) */
            g_string_append_printf(str, "'%s'", value_ptr->val_str);
            break;
        case DB_STRIPE_INFO:
        case DB_STRIPE_ITEMS:
            RBH_BUG("Unsupported DB type");
    }
}

/** print attribute value to display to the user
 * @param quote string to quote string types (eg. "'") */
int ListMgr_PrintAttr(char *str, int size, db_type_e type,
                      const db_type_u *value_ptr, const char *quote)
{
    switch (type)
    {
        case DB_ID:
            return snprintf(str, size, DFID, PFID(&value_ptr->val_id));

        case DB_TEXT:
            return snprintf(str, size, "%s%s%s", quote, value_ptr->val_str, quote);

        case DB_UIDGID:
            if (global_config.uid_gid_as_numbers)
                return snprintf(str, size, "%d", value_ptr->val_int);
            else
                return snprintf(str, size, "%s%s%s", quote, value_ptr->val_str, quote);

        case DB_INT:
            return snprintf(str, size, "%d", value_ptr->val_int);

        case DB_UINT:
            return snprintf(str, size, "%u", value_ptr->val_uint);

        case DB_SHORT:
            return snprintf(str, size, "%hd", value_ptr->val_short);
            break;

        case DB_USHORT:
            return snprintf(str, size, "%hu", value_ptr->val_ushort);

        case DB_BIGINT:
            return snprintf(str, size, "%lld", value_ptr->val_bigint);
            break;

        case DB_BIGUINT:
            return snprintf(str, size, "%llu", value_ptr->val_biguint);

        case DB_BOOL:
            if (value_ptr->val_bool)
                strncpy(str, "1", size);
            else
                strncpy(str, "0", size);
            return 1;

        case DB_ENUM_FTYPE:
            return snprintf(str, size, "%s%s%s", quote, value_ptr->val_str, quote);
            break;

        case DB_STRIPE_INFO:
        case DB_STRIPE_ITEMS:
            RBH_BUG("Unsupported DB type");
    }
    DisplayLog(LVL_CRIT, LISTMGR_TAG, "Error: unhandled type %d in %s", type, __func__);
    return 0;
}

int ListMgr_PrintAttrPtr(char *str, int size, db_type_e type,
                         void *value_ptr, const char *quote)
{
    db_type_u u;

    assign_union(&u, type, value_ptr);
    return ListMgr_PrintAttr(str, size, type, &u, quote);
}

/* return the number of parsed items (1) on success */
int parsedbtype(char *str_in, db_type_e type, db_type_u * value_out)
{
    int rc;
    int tmp;
    switch (type)
    {
    case DB_ID:
        /* convert str to id */
        rc = pk2entry_id(NULL, str_in, &value_out->val_id);
        if (rc)
            return 0;
        return 1;

    case DB_TEXT:
    case DB_ENUM_FTYPE:
        value_out->val_str = str_in;
        return 1;

    case DB_UIDGID:
        if (global_config.uid_gid_as_numbers)
        {
            return sscanf(str_in, "%d", &value_out->val_int);
        }
        else
        {
            value_out->val_str = str_in;
            return 1;
        }

    case DB_INT:
        return sscanf(str_in, "%d", &value_out->val_int);

    case DB_UINT:
        return sscanf(str_in, "%u", &value_out->val_uint);

    case DB_SHORT:
        return sscanf(str_in, "%hd", &value_out->val_short);

    case DB_USHORT:
        return sscanf(str_in, "%hu", &value_out->val_ushort);

    case DB_BIGINT:
        return sscanf(str_in, "%lld", &value_out->val_bigint);

    case DB_BIGUINT:
        return sscanf(str_in, "%llu", &value_out->val_biguint);

    case DB_BOOL:
        rc = sscanf(str_in, "%d", &tmp);
        if (rc > 0)
            value_out->val_bool = !(tmp == 0);
        return rc;

    case DB_STRIPE_INFO:
    case DB_STRIPE_ITEMS:
        RBH_BUG("Unsupported DB type");
        return 0;
    }
    DisplayLog(LVL_CRIT, LISTMGR_TAG, "Error: unhandled type %d in %s", type, __func__);
    return 0;
}

static void separated_list2db(const char *list, char *db, int size)
{
    snprintf(db, size, LIST_SEP_STR_ESC"%s"LIST_SEP_STR_ESC, list);
}

static void separated_list2match(const char *list, char *db, int size)
{
    /* <item>  is matched using expression '%+<item>+%' */
    snprintf(db, size, "%%"LIST_SEP_STR_ESC"%s"LIST_SEP_STR_ESC"%%", list);
}

static void separated_db2list(const char *db, char *list, int size)
{
    int len = MIN(strlen(db) - 2, size - 1);
    strncpy(list, db+1, len);
    list[len] = '\0';
}

void separated_db2list_inplace(char *list)
{
    int len = strlen(list);
    int i;
    for (i = 1; i < len - 1; i++)
        list[i-1] = list[i];
    list[len-2] = '\0';
}

/* @TODO use pre-generated field masks? */
static inline bool match_table(table_enum t, unsigned int attr_index)
{
    return ((t == T_MAIN) && is_main_field(attr_index)) ||
               ((t == T_DNAMES) && is_names_field(attr_index)) ||
               ((t == T_ANNEX) && is_annex_field(attr_index)) ||
               ((t == T_RECOV) && is_recov_field(attr_index)) ||
               ((t == T_SOFTRM || t == T_TMP_SOFTRM) && is_softrm_field(attr_index)) ||
               ((t == T_ACCT) && is_acct_field(attr_index)) ||
               ((t == T_ACCT) && is_acct_pk(attr_index));
}

/** get the table for the given attr index */
static inline table_enum field2table(int i)
{
    if (is_main_field(i))
        return T_MAIN;
    else if (is_names_field(i))
        return T_DNAMES;
    else if (is_annex_field(i))
        return T_ANNEX;
    else if (i == ATTR_INDEX_stripe_info)
        return T_STRIPE_INFO;
    else if (i == ATTR_INDEX_stripe_items)
        return T_STRIPE_ITEMS;
    else
        return T_NONE;
}

/* precomputed masks for testing attr sets efficiently (global = zero initially) */
attr_mask_t  main_attr_set = {0};
attr_mask_t  names_attr_set ={0};
attr_mask_t  annex_attr_set = {0};
attr_mask_t  gen_attr_set ={0};
attr_mask_t  stripe_attr_set ={0};
attr_mask_t  dir_attr_set ={0};
attr_mask_t  slink_attr_set ={0};
attr_mask_t  acct_attr_set = {0};
attr_mask_t  acct_pk_attr_set = {0};
attr_mask_t  softrm_attr_set = {0};
attr_mask_t  readonly_attr_set = {0};

void init_attrset_masks(const lmgr_config_t *lmgr_config)
{
    int            i, cookie;
    attr_mask_t    tmp_mask;

    main_attr_set = null_mask;
    names_attr_set = null_mask;
    annex_attr_set = null_mask;
    gen_attr_set = null_mask;
    stripe_attr_set = null_mask;
    dir_attr_set = null_mask;
    slink_attr_set = null_mask;
    acct_attr_set = null_mask;
    acct_pk_attr_set = null_mask;
    softrm_attr_set = null_mask;
    readonly_attr_set = null_mask;

    if (lmgr_config->acct)
    {
        acct_pk_attr_set.std |= ATTR_MASK_uid;
        acct_pk_attr_set.std |= ATTR_MASK_gid;
        acct_pk_attr_set.std |= ATTR_MASK_type;
    }
    /** @TODO RBHv3: implement status accounting.
     * /!\ as a given status can be NULL, it can't be part of the PK.
     * Then, is it possible to group by it?
     */
    acct_pk_attr_set.status |= all_status_mask();

    /* The following fields must be in SOFT_RM table:
     * Posix attributes + fullpath + fields with REMOVED flag
     * + fields indicated by status managers for SOFT_RM.
     */
    softrm_attr_set.std |= POSIX_ATTR_MASK | ATTR_MASK_fullpath;
    tmp_mask = sm_softrm_fields();
    softrm_attr_set = attr_mask_or(&softrm_attr_set, &tmp_mask);

    /* size: also used for size range stats */
    acct_attr_set.std |= ATTR_MASK_size | ATTR_MASK_blocks ;

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        /* is it read only ? */
        if (is_read_only_field(i))
            attr_mask_set_index(&readonly_attr_set, i);

        /* (fields with remove flag) other flags set previously */
        if (test_field_flag(i, REMOVED))
            attr_mask_set_index(&softrm_attr_set, i);

        /* The ID field is both in NAMES and MAIN. (XXX not an attribute) */
        if (is_names_field(i))
            attr_mask_set_index(&names_attr_set, i);

        if (is_main_field(i))
            attr_mask_set_index(&main_attr_set, i);
        else if (is_gen_field(i))
            attr_mask_set_index(&gen_attr_set, i);
        else if (is_annex_field(i))
            attr_mask_set_index(&annex_attr_set, i);
        else if (is_stripe_field(i))
            attr_mask_set_index(&stripe_attr_set, i);
        else if (is_dirattr(i))
            attr_mask_set_index(&dir_attr_set, i);

        /* not mutually exclusive with previous */
        if (is_slinkattr(i))
            attr_mask_set_index(&slink_attr_set, i);
    }
}

/** return byte address where the given attribute is stored */
static inline void *attr_address(attr_set_t *attrs, int attr_index)
{
    return (char *)&attrs->attr_values + field_infos[attr_index].offset;
}

/** const version */
static inline const void *attr_address_const(const attr_set_t *attrs,
                                             int attr_index)
{
    return (char *)&attrs->attr_values + field_infos[attr_index].offset;
}

/**
 * Add source info of generated fields to attr mask.
 * only apply to std attrs.
 */
void add_source_fields_for_gen(uint32_t *std_mask)
{
    int i;
    uint32_t mask = 1;

    /* add attr mask for source info of generated fields */
    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( ((*std_mask) & mask)
             && ((field_infos[i].flags & GENERATED) || (field_infos[i].flags & DIR_ATTR))
             && (field_infos[i].gen_index != -1) )
        {
           (*std_mask) |= (1 << field_infos[i].gen_index);
        }
    }
}

/** generate fields */
void generate_fields(attr_set_t *p_set)
{
    int i;
    uint32_t mask = 1;

    /* only work on standard attributes */
    for (i = 0; i < ATTR_COUNT; i++, mask <<= 1)
    {
        if ((p_set->attr_mask.std & mask) && (field_infos[i].flags & GENERATED))
        {
           const void *src_data;
           void *tgt_data;

           if (field_infos[i].gen_func == NULL)
           {
               /* cannot generate a field without a function */
               DisplayLog(LVL_DEBUG, LISTMGR_TAG,
                           "generated field without generation function: %s",
                           field_infos[i].field_name);
               p_set->attr_mask.std &= ~mask;
               continue;
           }

           /* is it generated from another field ? */
           if (field_infos[i].gen_index != -1)
           {
                uint32_t src_mask = (1 << field_infos[i].gen_index);
                /* is source set? */
                if ((p_set->attr_mask.std & src_mask) == 0)
                {
                    DisplayLog(LVL_FULL, LISTMGR_TAG,
                               "Source info '%s' of generated field '%s' is not set "
                               "in the database",
                               field_infos[field_infos[i].gen_index].field_name,
                               field_infos[i].field_name);
                    p_set->attr_mask.std &= ~mask;
                    continue;
                }

                src_data = attr_address_const(p_set, field_infos[i].gen_index);
           }
           else
           {
                /* nothing needed to generate it */
                src_data = NULL;
           }

           tgt_data = attr_address(p_set, i);

           if (field_infos[i].gen_func(tgt_data, src_data) != 0)
                p_set->attr_mask.std &= ~mask;
           else
                DisplayLog(LVL_FULL, LISTMGR_TAG, "Field '%s' auto-generated",
                            field_infos[i].field_name);

        } /* end if generated */
    } /* end for attr list */
}

/**
 * Generate fields automatically from already existing fields,
 * and check the target mask is satisfied.
 */
int ListMgr_GenerateFields(attr_set_t *p_set, attr_mask_t target_mask)
{
    attr_mask_t save_mask = p_set->attr_mask;

    /* are there generated fields that are not set for the target */
    if (target_mask.std & ~p_set->attr_mask.std & gen_attr_set.std)
    {
        /* try to generate missing fields */
        p_set->attr_mask.std |= (target_mask.std & ~p_set->attr_mask.std
                                 & gen_attr_set.std);
        generate_fields(p_set);

        /* still missing? */
        if (target_mask.std & ~p_set->attr_mask.std)
        {
               DisplayLog(LVL_VERB, LISTMGR_TAG, "Field still missing (can't be generated): %#"PRIX32,
                          target_mask.std & ~p_set->attr_mask.std);
               /* never leave the function with less info than when entering! */
               p_set->attr_mask.std |= save_mask.std;
               return DB_ATTR_MISSING;
        }
    }

    /* never leave the function with less info than when entering! */
    p_set->attr_mask.std |= save_mask.std;

    return DB_SUCCESS;
}


/* function attr_index, arg table, function_name, {arguments} */
typedef struct function_def
{
    int         attr_index;
    table_enum  arg_table;
    char       *fn_name;
    char      **fn_args;
} function_def_t;

static const function_def_t   functions[] =
{
    {ATTR_INDEX_fullpath, T_DNAMES, THIS_PATH_FUNC, (char*[]){"parent_id", "name", NULL}},
    {-1, 0, NULL, NULL}
};

static const function_def_t *get_function_by_attr(int attr_index)
{
    int i;
    for (i = 0; functions[i].fn_name != NULL; i++)
    {
        if (functions[i].attr_index == attr_index)
            return &functions[i];
    }
    return NULL;
}

/* print function call */
static void print_func_call(GString *str, int func_index, const char *prefix)
{
    const function_def_t *func = get_function_by_attr(func_index);
    char **args;
    if (func == NULL) /* unexpected: BUG */
        RBH_BUG("call for non-function attr");

    g_string_append_printf(str, "%s(", func->fn_name);
    for (args = func->fn_args; *args != NULL; args++)
    {
        if (args == func->fn_args) /* first arg */
            g_string_append_printf(str, "%s%s", prefix, *args);
        else
            g_string_append_printf(str, ",%s%s", prefix, *args);
    }
    g_string_append(str, ")");
}

/** test if there are read only fields in the given mask
 * and print an error in this case.
 * @retval false if not.
 * @retval true if some fields in attr_mask are read only.
 */

static bool _check_read_only_fields(const attr_mask_t *mask,
                                    const char *file_name,
                                    const char *func_name)
{
    attr_mask_t tmp;

    if (!readonly_fields(*mask))
        return false;

    tmp = attr_mask_and(mask, &readonly_attr_set);
    DisplayLog(LVL_CRIT, LISTMGR_TAG, "Error in %s:%s: attributes "DMASK
               " are read-only", file_name, func_name, PMASK(&tmp));
    return true;
}

#define check_read_only_fields(_m) _check_read_only_fields(_m, __FILE__, __func__)

/**
 * @param table T_MAIN, T_ANNEX, T_ACCT
 * @param prefix
 * @param suffix
 * @param separator
 * @return nbr of fields
 */
int attrmask2fieldlist(GString *str, attr_mask_t attr_mask, table_enum table,
                       bool leading_comma, bool for_update,
                       const char *prefix, const char *suffix)
{
    int            i, cookie;
    unsigned int   nbfields = 0;
    char          *for_update_str = "";

    /* optim: exit immediatly if no field matches */
    if ((table == T_MAIN) && !main_fields(attr_mask))
        return 0;
    if ((table == T_ANNEX) && !annex_fields(attr_mask))
        return 0;
    if ((table == T_DNAMES) && !names_fields(attr_mask))
        return 0;
    if (for_update && check_read_only_fields(&attr_mask))
        return -DB_READ_ONLY_ATTR;

    if (for_update)
        for_update_str = "=?";

    if ((table == T_STRIPE_INFO) || (table == T_STRIPE_ITEMS))
        return -DB_NOT_SUPPORTED;

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (attr_mask_test_index(&attr_mask, i))
        {
            if (match_table(table, i))
            {
                if (leading_comma || (nbfields > 0))
                    g_string_append(str, ",");

                if (is_funcattr(i) &&
                    /* exception: fullpath is a real field in SOFT_RM and temporary softrm table */
                    !((table == T_SOFTRM || table == T_TMP_SOFTRM)
                      && (i == ATTR_INDEX_fullpath)))
                {
                    print_func_call(str, i, prefix);
                    g_string_append_printf(str, "%s%s", for_update_str, suffix);
                }
                else
                {
                    g_string_append_printf(str, "%s%s%s%s", prefix, field_name(i),
                                           for_update_str, suffix);
                }
                nbfields++;
            }
        }
    }
    return nbfields;
}

/**
 * Generate operation like incrementation or decrementation on fields.
 * @param str
 * @param attr_mask
 * @param table T_MAIN, T_ANNEX, T_ACCT
 * @param prefix
 * @param operation
 * @return nbr of fields
 */
int attrmask2fieldoperation(GString *str, attr_mask_t attr_mask,
                            table_enum table, const char *prefix,
                            operation_type operation)
{
    int i, cookie;
    unsigned int  nbfields = 0;
    char operator;

    if (operation == SUBSTRACT)
        operator = '-';
    else
        operator = '+';

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (attr_mask_test_index(&attr_mask, i))
        {
            if (match_table(table, i))
            {
                g_string_append_printf(str, "%s%s=CAST(%s as SIGNED)%cCAST(%s%s as SIGNED) ",
                                nbfields == 0 ? "" : ",", field_name(i), field_name(i),
                                operator, prefix, field_name(i));
                nbfields++;
            }
        }
    }
    return nbfields;
}

/**
 * Generate comparaison on fields.
 * @param str
 * @param attr_mask
 * @param table T_MAIN, T_ANNEX, T_ACCT
 * @param left_prefix
 * @param right_prefix
 * @param comparator
 * @param separator
 * @return nbr of fields
 */
int attrmask2fieldcomparison(GString *str, attr_mask_t attr_mask,
                             table_enum table, const char *left_prefix,
                             const char *right_prefix,
                             const char *comparator, const char * separator)
{
    int i, cookie;
    unsigned int  nbfields = 0;

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (attr_mask_test_index(&attr_mask, i))
        {
            if (match_table(table, i))
            {
                g_string_append_printf(str, "%s %s%s%s%s%s ",
                            nbfields == 0 ? "" : separator, left_prefix,
                            field_name(i), comparator, right_prefix,
                            field_name(i));
                nbfields++;
            }
        }
    }
    return nbfields;
}

static void print_attr_value(lmgr_t *p_mgr, GString *str, const attr_set_t *p_set,
                             unsigned int attr_index)
{
    char tmp[1024];
    db_type_u typeu;
    db_type_e t;

    if (attr_index < ATTR_COUNT)
    {
        assign_union(&typeu, field_infos[attr_index].db_type,
                     attr_address_const(p_set, attr_index));

        if (is_sepdlist(attr_index))
        {
            separated_list2db(typeu.val_str, tmp, sizeof(tmp));
            typeu.val_str = tmp;
        }
        t = field_infos[attr_index].db_type;
    }
    else if (is_status_field(attr_index))
    {
        unsigned int status_idx = attr2status_index(attr_index);

        assign_union(&typeu, DB_TEXT, p_set->attr_values.sm_status[status_idx]);
        t = DB_TEXT;
    }
    else if (is_sm_info_field(attr_index))
    {
        unsigned int info_idx = attr2sminfo_index(attr_index);

        t = sm_attr_info[info_idx].def->db_type;
        assign_union(&typeu, t, (char *)p_set->attr_values.sm_info[info_idx]);
    }
    else
        RBH_BUG("Attribute index is not in a valid range");

    printdbtype(p_mgr, str, t, &typeu);
}



/**
 * @param table T_MAIN, T_ANNEX
 * @return nbr of fields
 */
int attrset2valuelist(lmgr_t *p_mgr, GString *str, const attr_set_t *p_set,
                      table_enum table, bool leading_coma)
{
    int            i, cookie;
    unsigned int   nbfields = 0;

    if ((table == T_STRIPE_INFO) || (table == T_STRIPE_ITEMS))
        return -DB_NOT_SUPPORTED;

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (attr_mask_test_index(&p_set->attr_mask, i))
        {
            if (match_table(table, i))
            {
                if (leading_coma || (nbfields > 0))
                    g_string_append(str, ",");

                print_attr_value(p_mgr, str, p_set, i);
                nbfields++;
            }
        }
    }
    return nbfields;
}

/**
 * @param table T_MAIN, T_ANNEX
 * @return nbr of fields
 */
int attrset2updatelist(lmgr_t *p_mgr, GString *str, const attr_set_t *p_set,
                       table_enum table, bool leading_coma, bool generic_value)
{
    int            i, cookie;
    unsigned int   nbfields = 0;

    if ((table == T_STRIPE_INFO) || (table == T_STRIPE_ITEMS))
        return -DB_NOT_SUPPORTED;

    if (check_read_only_fields(&p_set->attr_mask))
        return -DB_READ_ONLY_ATTR;

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (attr_mask_test_index(&p_set->attr_mask, i) && match_table(table, i))
        {
            if (leading_coma || (nbfields > 0))
                g_string_append(str, ",");

            g_string_append_printf(str, "%s=", field_name(i));

            if (generic_value)
                g_string_append_printf(str, "VALUES(%s)", field_name(i));
            else
                print_attr_value(p_mgr, str, p_set, i);

            nbfields++;
        }
    }
    return nbfields;
}

int fullpath_attr2db(const char *attr, char *db)
{
    DEF_PK(root_pk);
    char rel[RBH_PATH_MAX];

    /* fullpath 2 relative */
    if (relative_path(attr, global_config.fs_path, rel))
    {
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "fullpath %s is not under FS root %s",
                   attr, global_config.fs_path);
        return -EINVAL;
    }
    /* prefix with root id */
    entry_id2pk(get_root_id(), PTR_PK(root_pk));
    sprintf(db, "%s/%s", root_pk, rel);
    return 0;
}

void fullpath_db2attr(const char *db, char *attr)
{
    DEF_PK(id_from_db);
    DEF_PK(root_pk);

    entry_id2pk(get_root_id(), PTR_PK(root_pk));
    const char *c = strchr(db, '/');
    if (!c)
    {
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Unexpected path format from DB: '%s'", db);
        /* use c = db */
        c = db;
    }
    else
    {
        memset(PTR_PK(id_from_db), 0, sizeof(id_from_db));
        strncpy(id_from_db, db, (ptrdiff_t)(c - db));

        /* check FS root */
        if (strcmp(root_pk, id_from_db) != 0)
        {
            DisplayLog(LVL_EVENT, LISTMGR_TAG, "Entry has incomplete path in DB: "
                       "parent_id='%s', relative_path='%s'", id_from_db, c+1);
            /* copy as is */
            sprintf(attr, "%s", db);
            return;
        }
        c++; /* skip '/' */
    }

    /* relative 2 full */
    if (!strcmp(global_config.fs_path, "/")) /* FS root is '/' */
        sprintf(attr, "/%s", c);
    else
        sprintf(attr, "%s/%s", global_config.fs_path, c);
}

int result2attrset( table_enum table, char **result_tab,
                    unsigned int res_count, attr_set_t * p_set )
{
    int            i, cookie;
    unsigned int   nbfields = 0;
    db_type_u      typeu;

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (attr_mask_test_index(&p_set->attr_mask, i) && match_table(table, i))
        {
            if (log_config.debug_level >= LVL_FULL && result_tab != NULL)
            {
                DisplayLog(LVL_FULL, LISTMGR_TAG, "result[%u]: %s = %s", nbfields,
                           field_name(i), result_tab[nbfields]?result_tab[nbfields]:"<null>");
            }

            /* Parse nbfield'th value */
            if ( nbfields >= res_count )
            {
                return DB_BUFFER_TOO_SMALL;
            }

#ifdef _LUSTRE
            if (i < ATTR_COUNT && field_infos[i].db_type == DB_STRIPE_INFO)
            {
                if ((result_tab == NULL)
                    || (result_tab[nbfields] == NULL)
                    || (result_tab[nbfields+1] == NULL)
                    || (result_tab[nbfields+2] == NULL))
                {
                    /* must skip 3 columns in this case */
                    attr_mask_unset_index(&p_set->attr_mask, i);
                    nbfields+=3;
                    continue;
                }
                ATTR(p_set, stripe_info).stripe_count = atoi( result_tab[nbfields]  );
                ATTR(p_set, stripe_info).stripe_size = atoi( result_tab[nbfields+1]  );
                rh_strncpy(ATTR(p_set, stripe_info).pool_name, result_tab[nbfields+2] , MAX_POOL_LEN);

                /* stripe count, stripe size and pool_name */
                nbfields += 3;
                continue;
            }
            else
#endif
            if ((result_tab == NULL) || (result_tab[nbfields] == NULL))
            {
                attr_mask_unset_index(&p_set->attr_mask, i);
                nbfields++;
                continue;
            }
            else if (!parsedbtype(result_tab[nbfields], field_type(i), &typeu))
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG,
                           "Error: cannot parse field value '%s' (position %u) for %s",
                           result_tab[nbfields], nbfields, field_name(i));
                RBH_BUG("DB value cannot be parsed: DB may be corrupted");
                attr_mask_unset_index(&p_set->attr_mask, i);
                nbfields++;
                continue;
            }

            if ((i == ATTR_INDEX_fullpath) && (table != T_SOFTRM))
            {
                /* special case for fullpath which must be converted from relative to aboslute */
                /* fullpath already includes root for SOFT_RM table */
                fullpath_db2attr(typeu.val_str, ATTR(p_set, fullpath));
            }
            else if (is_status_field(i))
            {
                unsigned int status_idx = attr2status_index(i);

                /* allocate status array */
                sm_status_ensure_alloc(&p_set->attr_values.sm_status);
                /* get the matching status from status enum */
                p_set->attr_values.sm_status[status_idx] =
                    get_status_str(get_sm_instance(status_idx)->sm, typeu.val_str);

                /* status = '' => not set */
                if (p_set->attr_values.sm_status[status_idx] == NULL)
                    attr_mask_unset_index(&p_set->attr_mask, i);
            }
            else if (is_sm_info_field(i))
            {
                unsigned int info_idx = attr2sminfo_index(i);

                /* allocate info array */
                sm_info_ensure_alloc(&p_set->attr_values.sm_info);

                /* allocate a copy of the value */
                p_set->attr_values.sm_info[info_idx] =
                    dup_value(field_type(i), typeu);

                /* status = '' => not set */
                if (p_set->attr_values.sm_info[info_idx] == NULL)
                    attr_mask_unset_index(&p_set->attr_mask, i);
            }
            else if (is_sepdlist(i))
                separated_db2list(typeu.val_str, attr_address(p_set, i),
                                  field_infos[i].db_type_size+1); /* C size is db_type_size+1 */
            else
                union_get_value(attr_address(p_set, i), field_infos[i].db_type,
                                &typeu);
            nbfields++;
        }
    }
    return 0;

}

char          *compar2str( filter_comparator_t compar )
{
    switch ( compar )
    {
    case EQUAL:
        return "=";
    case NOTEQUAL:
        return "<>";
    case LESSTHAN:
        return "<=";
    case MORETHAN:
        return ">=";
    case LESSTHAN_STRICT:
        return "<";
    case MORETHAN_STRICT:
        return ">";
#ifdef _MYSQL
    /* MySQL is case insensitive.
     * To force case-sensitivity, use BINARY keyword. */
    case LIKE:
        return " LIKE BINARY ";
    case UNLIKE:
        return " NOT LIKE BINARY ";
    case RLIKE:
        return " RLIKE BINARY ";
#else
    case LIKE:
        return " LIKE ";
    case UNLIKE:
        return " NOT LIKE ";
#endif
    case IN:
        return " IN ";
    case NOTIN:
        return " NOT IN ";
    case ISNULL:
        return " IS NULL";
    case NOTNULL:
        return " IS NOT NULL";
    default:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Default sign for filter: should never happen !!!" );
        return "=";
    }
}

/**
 * @param filter_str initialized GString.
 * @retval FILTERDIR_NONE if there is no filter on dirattrs
 * @retval FILTERDIR_EMPTY if the test is 'dircount == 0' (no junction needed)
 * @retval FILTERDIR_NONEMPTY if the test is on dircount != 0, >= 0, condition on avgsize
 *                           junction needed, depending on the filter
 *                           test looks like "dirattr >= x"
 */
filter_dir_e dir_filter(lmgr_t *p_mgr, GString *filter_str, const lmgr_filter_t *p_filter,
                        unsigned int *dir_attr_index, const char *prefix)
{
    int i;

    if ( p_filter->filter_type == FILTER_SIMPLE )
    {
        for ( i = 0; i < p_filter->filter_simple.filter_count; i++ )
        {
            unsigned int index = p_filter->filter_simple.filter_index[i];
            if (!is_dirattr(index))
                continue;

            /* condition about empty directory (dircount == 0)? */
            if ((index == ATTR_INDEX_dircount)
                  && (p_filter->filter_simple.filter_value[i].value.val_uint == 0)
                  && (p_filter->filter_simple.filter_compar[i] == EQUAL))
            {
                DisplayLog( LVL_FULL, LISTMGR_TAG, "Special filter on empty directory" );

                /* empty directories are those with no parent_id in NAMES table */
                if (filter_str != NULL) /* allow passing no string */
                {
                    if (prefix)
                        g_string_append_printf(filter_str, "%s.id NOT IN (SELECT distinct(parent_id) "
                                        "FROM "DNAMES_TABLE")", prefix);
                    else
                        g_string_append(filter_str, "id NOT IN (SELECT distinct(parent_id) "
                                        "FROM "DNAMES_TABLE")");
                }
                if (dir_attr_index != NULL) /* allow passing no index */
                    *dir_attr_index = index;

                return FILTERDIR_EMPTY;
            }
            else
            {
                /* dirattrN <comparator> */
                if (filter_str != NULL)
                {
                    g_string_append_printf(filter_str, "dirattr%s",
                             compar2str(p_filter->filter_simple.filter_compar[i]));

                    /* value: (list only apply to OSTs XXX for now) */
                    db_type_u typeu = p_filter->filter_simple.filter_value[i].value;
                    printdbtype(p_mgr, filter_str, field_infos[index].db_type, &typeu);
                }

                if (dir_attr_index != NULL)
                    *dir_attr_index = index;
                return FILTERDIR_OTHER;
            }
        }
    }
    return FILTERDIR_NONE;
}

/**
 * build filter for stored FUNCTIONs
 * @param filter_str    initialized GString
 * @return the number of filtered values
 */
int func_filter(lmgr_t *p_mgr, GString *filter_str, const lmgr_filter_t *p_filter,
                table_enum table, bool leading_and, bool prefix_table)
{
    int i;
    char param1[128];
    char param2[128];
    unsigned int nb_fields = 0;

    if (p_filter->filter_type == FILTER_SIMPLE)
    {
        for (i = 0; i < p_filter->filter_simple.filter_count; i++)
        {
            unsigned int index = p_filter->filter_simple.filter_index[i];

            if (is_funcattr(index))
            {
                db_type_u typeu;

                param1[0] = '\0';
                param2[0] = '\0';

                if (filter_str == NULL) /* no need to check other stuff */
                    goto end_of_loop;

                /* add prefixes or parenthesis, etc. */
                if (leading_and || (nb_fields > 0))
                {
                    if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_OR)
                        g_string_append(filter_str, " OR ");
                    else
                        g_string_append(filter_str, " AND ");
                }

                if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_BEGIN)
                    g_string_append(filter_str, "(");

                if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT)
                {
                    /* NOT (x <cmp> <val>) */
                    g_string_append(filter_str, " NOT (");
                }

                if (index == ATTR_INDEX_fullpath)
                {
                    char relative[RBH_PATH_MAX];

                    if (fullpath_attr2db(p_filter->filter_simple.filter_value[i].value.val_str, relative))
                    {
                        /* condition is always false */
                        g_string_append(filter_str, "FALSE");
                        return 1;
                    }
                    typeu.val_str = relative;

                    /* if the filter applies to DNAMES, exactly filter on each row,
                     * else, filter on any path */
                    if (table == T_DNAMES)
                    {
                        if (prefix_table) {
                            snprintf(param1, sizeof(param1), "%s.parent_id", table2name(table));
                            snprintf(param2,  sizeof(param2), "%s.name", table2name(table));
                        }
                        else
                        {
                            rh_strncpy(param1, "parent_id", sizeof(param1));
                            rh_strncpy(param2, "name", sizeof(param2));
                        }

                        if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                            g_string_append(filter_str, "(");

                        g_string_append_printf(filter_str, THIS_PATH_FUNC"(%s,%s)%s",
                            param1, param2, compar2str(p_filter->filter_simple.filter_compar[i]));
                        printdbtype(p_mgr, filter_str, field_infos[index].db_type, &typeu);

                        if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                            g_string_append_printf(filter_str,
                                " OR "THIS_PATH_FUNC"(%s,%s) IS NULL)", param1, param2);
                    }
                    else
                    {
                        if (prefix_table)
                            snprintf(param1, sizeof(param1), "%s.id", table2name(table));
                        else
                            rh_strncpy(param1, "id", sizeof(param1));

                        if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                            g_string_append(filter_str, "(");

                        g_string_append_printf(filter_str, ONE_PATH_FUNC"(%s)%s", param1,
                                compar2str(p_filter->filter_simple.filter_compar[i]));
                        printdbtype(p_mgr, filter_str, field_infos[index].db_type, &typeu);

                        if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                            g_string_append_printf(filter_str,
                                    " OR "ONE_PATH_FUNC"(%s) IS NULL)", param1);
                    }
                }

                /* add closing parenthesis, etc...*/
                if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT)
                    /* NOT (x <cmp> <val>) */
                    g_string_append(filter_str, ")");

                if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_END)
                    g_string_append(filter_str, ")");

            end_of_loop:
                nb_fields ++;
            }
        }
    }
    return nb_fields;
}


static void attr2filter_field(GString *str, table_enum table,
                              unsigned int attr, bool prefix_table)
{
    if (match_table(table, attr) || (table == T_NONE && !is_stripe_field(attr)))
    {
        /* exception: fullpath is a real field in SOFT_RM and temporary softrm table */
        if (is_funcattr(attr) &&
            !((table == T_SOFTRM || table == T_TMP_SOFTRM)
             && (attr == ATTR_INDEX_fullpath)))
        {
            char prefix[128] = "";

            if (prefix_table)
                snprintf(prefix, sizeof(prefix), "%s.",
                         table2name(table == T_NONE? field2table(attr):table));

            print_func_call(str, attr, prefix);
        }
        else /* std field */
        {
            if (prefix_table)
                g_string_append_printf(str, "%s.",
                    table2name(table == T_NONE? field2table(attr):table));

            g_string_append(str, field_name(attr));
        }
    }
    else if ((table == T_STRIPE_ITEMS || table == T_NONE)
              && (field_type(attr) == DB_STRIPE_ITEMS))
    {
        if (prefix_table)
            g_string_append_printf(str, "%s.", STRIPE_ITEMS_TABLE);

        g_string_append(str, "ostidx");
    }
    else if ((table == T_STRIPE_INFO || table == T_NONE)
              && (field_type(attr) == DB_STRIPE_INFO))
    {
        /* XXX Assume that the only possible filter here is on pool_name */
        if (prefix_table)
            g_string_append_printf(str, "%s.", STRIPE_INFO_TABLE);

        g_string_append(str, "pool_name");
    }
}

int filter2str(lmgr_t *p_mgr, GString *str, const lmgr_filter_t *p_filter,
               table_enum table, bool leading_and, bool prefix_table)
{
    int            i;
    unsigned int   nbfields = 0;
    db_type_u      typeu;

    if (p_filter->filter_type == FILTER_SIMPLE)
    {

        for (i = 0; i < p_filter->filter_simple.filter_count; i++)
        {
            unsigned int   index = p_filter->filter_simple.filter_index[i];
            bool match =  match_table(table, index)
                         || ((table == T_STRIPE_ITEMS) && (index < ATTR_COUNT)
                              && (field_infos[index].db_type == DB_STRIPE_ITEMS))
                         || ((table == T_STRIPE_INFO) && (index < ATTR_COUNT)
                              && (field_infos[index].db_type == DB_STRIPE_INFO));

            /* filter on generated fields are not allowed */
            if (is_dirattr(index))
            {
                DisplayLog(LVL_FULL, LISTMGR_TAG, "Special filter on dir attribute '%s'",
                           field_name(index));
                continue;
            }
            else if (is_gen_field(index))
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG, "Cannot use filter on generated field '%s'",
                           field_name(index));
                return -DB_INVALID_ARG;
            }

            if (match || (table == T_NONE))
            {
                /* add prefixes or parenthesis, etc. */
                if (leading_and || (nbfields > 0))
                {
                    if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_OR)
                        g_string_append(str, " OR ");
                    else
                        g_string_append(str, " AND ");
                }

                if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_BEGIN)
                    g_string_append(str, "(");

                if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT)
                {
                    if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                        /* (NOT (x <cmp> <val>) OR x IS NULL) */
                        g_string_append(str, " (NOT (");
                    else
                        /* NOT (x <cmp> <val>) */
                        g_string_append(str, " NOT (");
                }
                else if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                    /* (x <cmp> <val> OR x IS NULL) */
                    g_string_append(str, "(");
            }

            /* append field name or function call */
            attr2filter_field(str, table, index, prefix_table);

            if (match_table(table, index) || (table == T_NONE && !is_stripe_field(index)))
            {
                /* append comparator */
                if (is_sepdlist(index))
                {
                    /* always match '%+<item>+%' => use LIKE and UNLIKE */
                    if (p_filter->filter_simple.filter_compar[i] == EQUAL)
                        p_filter->filter_simple.filter_compar[i] = LIKE;
                    else if (p_filter->filter_simple.filter_compar[i] == NOTEQUAL)
                        p_filter->filter_simple.filter_compar[i] = UNLIKE;
                }
                g_string_append(str, compar2str(p_filter->filter_simple.filter_compar[i]));

                /* no expected value after IS NULL or IS NOT NULL */
                if (p_filter->filter_simple.filter_compar[i] != ISNULL
                    && p_filter->filter_simple.filter_compar[i] != NOTNULL)
                {
                    /* fullpath already includes root for SOFT_RM table */
                    if ((index == ATTR_INDEX_fullpath) && (table != T_SOFTRM))
                    {
                        char relative[RBH_PATH_MAX];

                        if (fullpath_attr2db(p_filter->filter_simple.filter_value[i].value.val_str, relative))
                        {
                            /* condition is always false */
                            g_string_append(str, "FALSE");
                        }
                        else
                        {
                            typeu.val_str = relative;
                            printdbtype(p_mgr, str, field_infos[index].db_type, &typeu);
                        }
                    }
                    else
                    {
                        char tmp[1024];

                        if (is_sepdlist(index))
                        {
                            /* match '%+<item>+%' */
                            separated_list2match(p_filter->filter_simple.filter_value[i].value.val_str,
                                                 tmp, sizeof(tmp));
                            typeu.val_str = tmp;
                        }
                        else
                            /* single value (list only apply to OSTs XXX for now) */
                            typeu = p_filter->filter_simple.filter_value[i].value;

                        printdbtype(p_mgr, str, field_type(index), &typeu);
                    }
                }
                nbfields++;
            }
            else if ((table == T_STRIPE_ITEMS || table == T_NONE)
                      && (field_type(index) == DB_STRIPE_ITEMS))
            {
                /* single value or a list? */
                if (p_filter->filter_simple.filter_compar[i] == IN
                    || (p_filter->filter_simple.filter_compar[i] == NOTIN))
                {
                    unsigned int j;
                    db_type_u *list;

                    g_string_append_printf(str, "%s(", compar2str(
                        p_filter->filter_simple.filter_compar[i]));

                    list = p_filter->filter_simple.filter_value[i].list.values;
                    for (j = 0; j < p_filter->filter_simple.filter_value[i].list.count; j++)
                    {
                        g_string_append_printf(str, "%s%u", j==0?"":",", list[j].val_uint);
                    }
                    g_string_append(str, ")");
                }
                else /* single value */
                {
                    g_string_append_printf(str, "%s%u",
                                 compar2str(p_filter->filter_simple.filter_compar[i]),
                                 p_filter->filter_simple.filter_value[i].value.val_uint);
                }
                nbfields++;
            }
            else if ((table == T_STRIPE_INFO || table == T_NONE)
                      && (field_type(index) == DB_STRIPE_INFO))
            {
                g_string_append_printf(str, "%s'%s'",
                             compar2str(p_filter->filter_simple.filter_compar[i]),
                             p_filter->filter_simple.filter_value[i].value.val_str);
                nbfields++;
            }

            if (match || table == T_NONE)
            {

                if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT)
                {
                    if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                    {
                        /* (NOT (x <cmp> <val>) OR x IS NULL) */
                        g_string_append(str, ") OR ");
                        attr2filter_field(str, table, index, prefix_table);
                        g_string_append(str, " IS NULL)");
                    }
                    else
                        /* NOT (x <cmp> <val>) */
                        g_string_append(str, ")");
                }
                else if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                {
                    /* OR x IS NULL */
                    g_string_append(str, " OR ");
                    attr2filter_field(str, table, index, prefix_table);
                    g_string_append(str," IS NULL)");
                }

                if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_END)
                    g_string_append(str, ")");
            }
        } /* end for */
    }
    else
    {
        return -DB_NOT_SUPPORTED;
    }
    return nbfields;
}                               /* filter2str */

const char * dirattr2str(unsigned int attr_index)
{
    switch (attr_index)
    {
        case ATTR_INDEX_dircount:
            return "COUNT(*)";
        case ATTR_INDEX_avgsize:
            return "ROUND(AVG(size),0)";
        default:
            DisplayLog( LVL_CRIT, LISTMGR_TAG, "Unexpected attr index %u in %s", attr_index, __func__ );
            return NULL;
    }
}

/** Helper to build a where clause from a list of fields to be filtered
 * @param where initialized empty GString.
 * @param[out] counts count of filter fields in each table.
 * @return the number of created filters.
 */
int filter_where(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                 struct field_count *counts, bool ignore_name_filter,
                 bool leading_and, GString *where)
{
    int nb; /* can be < 0 */
    unsigned int all = 0;

    /* on which table are the filters ?  */
    nb = filter2str(p_mgr, where, p_filter, T_MAIN,
                    leading_and, true);
    if (nb > 0)
    {
        counts->nb_main += nb;
        all += nb;
    }

    nb = filter2str(p_mgr, where, p_filter, T_ANNEX,
                    (all > 0) || leading_and, true);
    if (nb > 0)
    {
        counts->nb_annex += nb;
        all += nb;
    }

    if (!ignore_name_filter)
    {
        nb = filter2str(p_mgr, where, p_filter, T_DNAMES,
                        (all > 0) || leading_and, true);
        if (nb > 0)
        {
            counts->nb_names += nb;
            all += nb;
        }
    }

    /* stripes are only managed for Lustre filesystems */
#ifdef _LUSTRE
    nb = filter2str(p_mgr, where, p_filter, T_STRIPE_INFO,
                     (all > 0) || leading_and, true);
    if (nb > 0)
    {
        counts->nb_stripe_info += nb;
        all += nb;
    }

    nb = filter2str(p_mgr, where, p_filter, T_STRIPE_ITEMS,
                    (all > 0) || leading_and, true);
    if (nb > 0)
    {
        counts->nb_stripe_items += nb;
        all += nb;
    }
#endif

    return all;
}

/** helper to lighten filter_from function */
static inline void append_from_clause(table_enum tab, GString *from,
                                      table_enum *first_table)
{
    const char *tname = table2name(tab);

    if (*first_table == T_NONE)
    {
        *first_table = tab;
        g_string_append(from, tname);
    }
    else
        g_string_append_printf(from, " LEFT JOIN %s ON %s.id=%s.id", tname, /** @FIXME LEFT JOIN or INNER JOIN??? */
                               table2name(*first_table), tname);

    /* XXX INNER join if there is a criteria on right table? */

}


/** Helper to build a 'from' clause (table junction) depending on filter counts
 * It must be called only if filter is non empty.
 * @param[in] counts filter counts filled-in by filter_where() function.
 * @param[in,out] from initialized empty GString.
 * @param[in] is_first_tab indicate if there is already a first table
 * @param[in, out] first_table the first table in the junction.
 * @param[out] select_distinct_id indicate if the request must select distinct ids.
 */
void filter_from(lmgr_t *p_mgr, const struct field_count *counts,
                 bool ignore_names_filter, GString *from, bool is_first_tab,
                 table_enum *first_table, bool *select_distinct_id)
{
    if (!is_first_tab)
        *first_table = T_NONE;

    if (counts->nb_main)
        append_from_clause(T_MAIN, from, first_table);
    if (counts->nb_annex)
        append_from_clause(T_ANNEX, from, first_table);
    if (counts->nb_names && !ignore_names_filter)
    {
        *select_distinct_id = true;
        append_from_clause(T_DNAMES, from, first_table);
    }
    if (counts->nb_stripe_info)
        append_from_clause(T_STRIPE_INFO, from, first_table);
    if (counts->nb_stripe_items)
    {
        *select_distinct_id = true;
        append_from_clause(T_STRIPE_ITEMS, from, first_table);
    }
}


/* special masks values for id2pk and pk2id */
#define MASK_ID2PK  0
#define MASK_PK2ID  1


void entry_id2pk(const entry_id_t * p_id, PK_PARG_T p_pk)
{
#ifndef FID_PK
    snprintf( p_pk, PK_LEN, "%"PRI_DT":%LX", p_id->fs_key,
              (unsigned long long)p_id->inode );
#else /* FID_PK */
    snprintf( p_pk, DB_FID_LEN, DFID_NOBRACE, PFID(p_id) );
#endif
}


int pk2entry_id( lmgr_t * p_mgr, PK_ARG_T pk, entry_id_t * p_id )
{
#ifndef FID_PK
    unsigned long long tmp_ino;

    if (sscanf(pk, "%"PRI_DT":%LX", &p_id->fs_key, &tmp_ino ) != FID_SCAN_CNT)
        return DB_INVALID_ARG;
    else
    {
        p_id->inode = tmp_ino;
        return DB_SUCCESS;
    }
#else /* FID_PK */
    if (sscanf( pk, SFID, RFID(p_id) ) != FID_SCAN_CNT)
        return DB_INVALID_ARG;
    else
        return DB_SUCCESS;
#endif
}

void append_size_range_fields(GString *str, bool leading_comma,
                              const char *prefix)
{
    unsigned int i;

    for (i = 0; i < SZ_PROFIL_COUNT; i++)
        g_string_append_printf(str, "%s %s%s", leading_comma || (i > 0)?",":"",
                               prefix, sz_field[i]);
}

/* those functions are used for begin/commit/rollback */
int _lmgr_begin(lmgr_t *p_mgr, int behavior)
{
    if (behavior == 0)
        /* autocommit */
        return DB_SUCCESS;
    else if (behavior == 1)
        /* commit every transaction */
        return db_exec_sql(&p_mgr->conn, "BEGIN", NULL);
    else
    {
        int rc = DB_SUCCESS;

        /* if last operation was commited, issue a begin statement */
        if (p_mgr->last_commit == 0)
        {
            rc = db_exec_sql(&p_mgr->conn, "BEGIN", NULL);
            if (rc)
                return rc;
        }

        /* increment current op */
        p_mgr->last_commit++;
        return DB_SUCCESS;
    }
}

void _lmgr_rollback(lmgr_t * p_mgr, int behavior)
{
    if (behavior == 0)
        return;
    else
    {
        /* we must rollback all operations since the last commit, to keep database into persistent state */
        db_exec_sql(&p_mgr->conn, "ROLLBACK", NULL);

        p_mgr->last_commit = 0;
    }
}

int _lmgr_commit(lmgr_t * p_mgr, int behavior)
{
    if (behavior == 0)
        return DB_SUCCESS;
    else if (behavior == 1)
        return db_exec_sql(&p_mgr->conn, "COMMIT", NULL);
    else
    {
        /* if the transaction count is reached:
         * commit operations and result transaction count
         */
        if ((p_mgr->last_commit % behavior == 0) || p_mgr->force_commit)
        {
            int            rc;
            rc = db_exec_sql(&p_mgr->conn, "COMMIT", NULL);
            if (rc)
                return rc;

            p_mgr->last_commit = 0;
        }
    }
    return DB_SUCCESS;
}

/** Set force commit behavior */
void ListMgr_ForceCommitFlag(lmgr_t *p_mgr, bool force_commit)
{
    p_mgr->force_commit = force_commit;
}

/** check if the last operation was really committed
 * @return TRUE if the last operation has been commited,
 * @return FALSE if commit is deferred.
 */
bool ListMgr_GetCommitStatus(lmgr_t *p_mgr)
{
    /* operation was not committed if period > 1 and last_commit is not reset yet */
    if ( ( lmgr_config.commit_behavior > 1 ) && ( p_mgr->last_commit != 0 ) )
        return false;
    else
        return true;
}

int _lmgr_flush_commit(lmgr_t *p_mgr, int behavior)
{
    int            rc;
    if ((behavior > 1) && (p_mgr->last_commit != 0))
    {
        rc = db_exec_sql(&p_mgr->conn, "COMMIT", NULL);
        if (rc)
            return rc;

        p_mgr->last_commit = 0;
        return DB_SUCCESS;
    }
    else
        return DB_SUCCESS;
}


/**
 * If p_target_attrset attributes are unset,
 * retrieve them from p_source_attrset.
 */
void ListMgr_MergeAttrSets(attr_set_t *p_target_attrset, const attr_set_t *p_source_attrset, bool update)
{
    int            i, cookie;
    db_type_u      typeu;

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if ((update || !attr_mask_test_index(&p_target_attrset->attr_mask, i))
             && attr_mask_test_index(&p_source_attrset->attr_mask, i))
        {
            /* status attr */
            if (is_status_field(i))
            {
                unsigned int status_idx = attr2status_index(i);

                if (p_source_attrset->attr_values.sm_status == NULL)
                    RBH_BUG("status flag is set but status array is not allocated");

                sm_status_ensure_alloc(&p_target_attrset->attr_values.sm_status);
                /* copy the values as-is (static const strings, not allocated) */
                p_target_attrset->attr_values.sm_status[status_idx]
                    = p_source_attrset->attr_values.sm_status[status_idx];
            }
            /* SM specific info */
            else if (is_sm_info_field(i))
            {
                unsigned int idx = attr2sminfo_index(i);

                if (p_source_attrset->attr_values.sm_info == NULL)
                    RBH_BUG("specific info flag is set but info array is not allocated");

                sm_info_ensure_alloc(&p_target_attrset->attr_values.sm_info);

                /* free the previous value, if any */
                if (p_target_attrset->attr_values.sm_info[idx] != NULL)
                    free(p_target_attrset->attr_values.sm_info[idx]);

                /* duplicate the field according to its type */
                assign_union(&typeu, field_type(i),
                             p_source_attrset->attr_values.sm_info[idx]);

                /* duplicate the value to target */
                p_target_attrset->attr_values.sm_info[idx] =
                    dup_value(field_type(i), typeu);
            }
            else if (!is_stripe_field(i))
            {
                assign_union(&typeu, field_infos[i].db_type,
                             attr_address_const(p_source_attrset, i));
                union_get_value(attr_address(p_target_attrset, i),
                                field_infos[i].db_type, &typeu);
            }
#ifdef _LUSTRE
            else if ( field_infos[i].db_type == DB_STRIPE_ITEMS )
            {
                /* free previous value if set */
                if (attr_mask_test_index(&p_target_attrset->attr_mask, i))
                    free_stripe_items((stripe_items_t *)attr_address(p_target_attrset, i));

                dup_stripe_items((stripe_items_t *)attr_address(p_target_attrset, i),
                                 (stripe_items_t *)attr_address_const(p_source_attrset, i));
            }
            else if (field_infos[i].db_type == DB_STRIPE_INFO)
            {
                memcpy(attr_address(p_target_attrset, i),
                       attr_address_const(p_source_attrset, i),
                       sizeof(stripe_info_t));
            }
#endif

            attr_mask_set_index(&p_target_attrset->attr_mask, i);
        }
    }
    return;
}


void ListMgr_FreeAttrs(attr_set_t *p_set)
{
    if (p_set == NULL)
        return;

#ifdef _LUSTRE
    int i;

    /* Free stripe count attributes (in std attrs) */
    for (i = 0; i < ATTR_COUNT; i++)
    {
        if ((field_infos[i].db_type == DB_STRIPE_ITEMS)
            && attr_mask_test_index(&p_set->attr_mask, i))
        {
            free_stripe_items((stripe_items_t *)attr_address(p_set, i));
        }
    }
#endif
    sm_status_free(&p_set->attr_values.sm_status);
    sm_info_free(&p_set->attr_values.sm_info);
}

/** return the mask of attributes that differ */
attr_mask_t ListMgr_WhatDiff(const attr_set_t *p_tgt, const attr_set_t *p_src)
{
    int         i, cookie;
    attr_mask_t common_mask
            = attr_mask_and(&p_tgt->attr_mask, &p_src->attr_mask);
    attr_mask_t diff_mask = null_mask;

    cookie = -1;
    while ((i = attr_index_iter(0, &cookie)) != -1)
    {
        if (attr_mask_test_index(&common_mask, i))
        {
            bool is_diff = false;

            /* status attr */
            if (is_status_field(i))
            {
                unsigned int status_idx = attr2status_index(i);

                /** array is allocated as we check the common mask */
                if (p_tgt->attr_values.sm_status[status_idx]
                    != p_src->attr_values.sm_status[status_idx])
                    attr_mask_set_index(&diff_mask, i);
            }
            /* SM specific info */
            else if (is_sm_info_field(i))
            {
                unsigned int idx = attr2sminfo_index(i);

                is_diff = diff_union(field_type(i), p_src->attr_values.sm_info[idx],
                                     p_tgt->attr_values.sm_info[idx]);
                if (is_diff)
                    attr_mask_set_index(&diff_mask, i);
            }
            else if (!is_stripe_field(i))
            {
                /* diff the values */
                is_diff = diff_union(field_infos[i].db_type,
                           attr_address_const(p_src, i),
                           attr_address_const(p_tgt, i));
                if (is_diff)
                    attr_mask_set_index(&diff_mask, i);
            }
#ifdef _LUSTRE
            else if ( field_infos[i].db_type == DB_STRIPE_INFO )
            {
                if ((ATTR(p_tgt, stripe_info).stripe_size
                        != ATTR(p_src, stripe_info).stripe_size)
                    || (ATTR(p_tgt, stripe_info).stripe_count
                        != ATTR(p_src, stripe_info).stripe_count)
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
                    || (ATTR(p_tgt, stripe_info).validator
                        != ATTR(p_src, stripe_info).validator)
#endif
                    || (strcmp(ATTR(p_tgt, stripe_info).pool_name,
                           ATTR(p_src, stripe_info).pool_name) != 0))
                {
                    attr_mask_set_index(&diff_mask, i);
                }
            }
            else if ( field_infos[i].db_type == DB_STRIPE_ITEMS )
            {
                if (ATTR(p_tgt, stripe_items).count
                    != ATTR(p_src, stripe_items).count)
                    is_diff = true;
                else
                {
                    int i;
                    for (i = 0; i < ATTR(p_tgt, stripe_items).count; i++)
                    {
                        if ((ATTR(p_tgt,stripe_items).stripe[i].ost_idx !=
                             ATTR(p_src,stripe_items).stripe[i].ost_idx)
                            ||
                            (ATTR(p_tgt,stripe_items).stripe[i].ost_gen !=
                             ATTR(p_src,stripe_items).stripe[i].ost_gen)
                            ||
                            (ATTR(p_tgt,stripe_items).stripe[i].obj_id !=
                             ATTR(p_src,stripe_items).stripe[i].obj_id)
                            ||
                            (ATTR(p_tgt,stripe_items).stripe[i].obj_seq !=
                             ATTR(p_src,stripe_items).stripe[i].obj_seq))
                        {
                            is_diff = true;
                            break;
                        }
                    }
                }
                if (is_diff)
                    attr_mask_set_index(&diff_mask, i);
            }
#endif
        }
    }
    return diff_mask;
}

/** Convert a set notation (eg. "3,5-8,12") to a list of values
 * \param type[in] the type of output array (DB_INT, DB_UINT, ...)
 * \param p_list[out] list of values (the function allocates a buffer for p_list->values)
 */
int lmgr_range2list(const char * set, db_type_e type, value_list_t * p_list)
{
    char *curr, *next;
    char buffer[1024];

    /* check args */
    if (!p_list)
        return -1;
    /* only uint supported */
    if (type != DB_UINT)
        return -1;

    /* local copy for strtok */
    rh_strncpy(buffer, set, 1024);

    /* inialize list */
    p_list->count = 0;
    p_list->values = NULL;

    /* tokenize by ',' */
    curr = strtok_r(buffer, ",", &next);
    while(curr)
    {
        /* check for range notation */
        char * dash = strchr(curr, '-');
        if (!dash)
        {
            /* single value */
            int tmpval;
            tmpval = str2int(curr);
            if (tmpval == -1)
                goto out_free;
            p_list->values = MemRealloc(p_list->values, (1 + p_list->count) * sizeof(*(p_list->values)));
            if (!p_list->values)
                goto out_free;
            p_list->values[p_list->count].val_uint = tmpval;
            p_list->count++;
        }
        else
        {
            /* range */
            int val_start, val_end, i;
            unsigned int j;
            *dash = '\0'; /* tokenize at '-' */
            dash++; /*  points to end value */
            val_start = str2int(curr);
            val_end = str2int(dash);
            if (val_start == -1 || val_end == -1 || val_end < val_start)
                goto out_free;

            p_list->values = MemRealloc(p_list->values, (val_end - val_start + 1 + p_list->count) * sizeof(*(p_list->values)));
            if (!p_list->values)
                goto out_free;
            for (i = 0, j = val_start; j <= val_end; i++, j++)
            {
                p_list->values[p_list->count+i].val_uint = j;
            }
            p_list->count += val_end - val_start + 1;
        }

        curr = strtok_r(NULL, ",", &next);
    }
    return 0;

out_free:
    if (p_list->values)
        MemFree(p_list->values);
    p_list->values = NULL;
    p_list->count = 0;
    return -1;
}


/** manage delayed retry of retryable errors
 * \return != 0 if the transaction must be restarted
 */
int _lmgr_delayed_retry(lmgr_t *lmgr, int errcode, const char *func, int line)
{
    if (!db_is_retryable(errcode))
    {
        /* if a retry was pending, display a success message */
        if (lmgr->retry_delay != 0)
        {
            struct timeval diff, now;
            timerclear(&diff);
            gettimeofday(&now, NULL);
            timersub(&now, &lmgr->first_error, &diff);

            /* Only notify success if the suceeded function
             * is the same as the last error.
             */
            if ((lmgr->last_err_func == func) && (lmgr->last_err_line == line)
                && errcode == DB_SUCCESS)
            {
                DisplayLog(LVL_EVENT, LISTMGR_TAG,
                           "DB operation succeeded after %u retries (%ld.%03ld sec)",
                           lmgr->retry_count, diff.tv_sec, diff.tv_usec/1000);
            }

            /* reset retry delay if no error occured,
             * or if the error is not retryable */
            lmgr->retry_delay = 0;
            lmgr->retry_count = 0;
            timerclear(&lmgr->first_error);
        }
        return 0;
    }

    /* transaction is about to be restarted,
     * sleep for a given time */
    if (lmgr->retry_delay == 0)
    {
        /* first error, first sleep */
        gettimeofday(&lmgr->first_error, NULL);
        lmgr->retry_delay = lmgr_config.connect_retry_min;
    }
    else
    {
        lmgr->retry_delay *= 2;
        if (lmgr->retry_delay > lmgr_config.connect_retry_max)
            lmgr->retry_delay = lmgr_config.connect_retry_max;
    }
    lmgr->last_err_func = func;
    lmgr->last_err_line = line;
    if (lmgr->retry_count == 0)
        DisplayLog(LVL_EVENT, LISTMGR_TAG,
                   "Retryable DB error in %s l.%u. Retrying...",
                   func, line);
    else /* only display for debug level */
        DisplayLog(LVL_DEBUG, LISTMGR_TAG,
                  "Retryable DB error in %s l.%u. Restarting transaction in %u sec...",
                  func, line, lmgr->retry_delay);

    rh_sleep(lmgr->retry_delay);
    lmgr->retry_count ++;
    return 1;
}

/** check attribute mask compatibility for a given table */
static inline bool table_mask_compat(attr_mask_t m1, attr_mask_t m2)
{
    /* attrs in a given table must be the same or 0 */
    if (attr_mask_is_null(m1) || attr_mask_is_null(m2))
        return true;
    else
        return attr_mask_equal(&m1, &m2);
}

/** Check mask compatibility for request batching. */
bool lmgr_batch_compat(attr_mask_t m1, attr_mask_t m2)
{
    if (!table_mask_compat(attr_mask_and(&m1, &main_attr_set),
                           attr_mask_and(&m2, &main_attr_set)))
        return false;
    if (!table_mask_compat(attr_mask_and(&m1, &names_attr_set),
                           attr_mask_and(&m2, &names_attr_set)))
        return false;
    if (!table_mask_compat(attr_mask_and(&m1, &annex_attr_set),
                           attr_mask_and(&m2, &annex_attr_set)))
        return false;
    if (!table_mask_compat(attr_mask_and(&m1, &stripe_attr_set),
                           attr_mask_and(&m2, &stripe_attr_set)))
        return false;

    return true;
}

int parse_entry_id(lmgr_t *p_mgr, const char *str, PK_PARG_T p_pk, entry_id_t *p_id)
{
    int rc;

    if (sscanf(str, SPK, p_pk) != 1)
    {
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Unexpected format for database key: '%s'",
                   str);
        return DB_INVALID_ARG;
    }

    rc = pk2entry_id(p_mgr, p_pk, p_id);
    if (rc)
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Unexpected format for database key: "DPK, p_pk);
    return rc;
}

int attr_index_iter(unsigned int init, int *cookie)
{
    assert(cookie != NULL);

    if (*cookie == -1)
        /* initial step */
        *cookie = init;
    else
        (*cookie)++;

    if (*cookie == ATTR_COUNT)
    {
        /* reached the end of std attrs */
        if (sm_inst_count > 0)
            /* go to first status index */
            *cookie = ATTR_INDEX_FLG_STATUS;
        else if (sm_attr_count > 0)
            /* go to first sminfo index */
            *cookie = ATTR_INDEX_FLG_SMINFO;
        else
            return -1;
    }
    else if (*cookie == (ATTR_INDEX_FLG_STATUS | sm_inst_count))
    {
        /* reached the end of status attrs */
        if (sm_attr_count > 0)
            /* go to first sminfo index */
            *cookie = ATTR_INDEX_FLG_SMINFO;
        else
            return -1;
    }
    else if (*cookie == (ATTR_INDEX_FLG_SMINFO | sm_attr_count))
    {
        /* the end of everything */
        return -1;
    }

    return *cookie;
}

/** unset read-only attributes from mask */
void attr_mask_unset_readonly(attr_mask_t *mask)
{
    *mask = attr_mask_and_not(mask, &readonly_attr_set);
}
