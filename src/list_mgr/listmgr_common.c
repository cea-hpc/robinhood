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
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "Memory.h"
#include "listmgr_stripe.h"
#include "xplatform_print.h"
#include <stdio.h>

int printdbtype( lmgr_t * p_mgr, char *str, db_type_t type, const db_type_u * value_ptr )
{
    char tmpstr[4096];

    switch ( type )
    {
    case DB_ID:
        /* convert id to str */
        entry_id2pk(&value_ptr->val_id, tmpstr);
        return sprintf( str, DPK, tmpstr );
    case DB_TEXT:
        /* escape special characters in value */
        db_escape_string( &p_mgr->conn, tmpstr, 4096, value_ptr->val_str );
        return sprintf( str, "'%s'", tmpstr );
    case DB_INT:
        return sprintf( str, "%d", value_ptr->val_int );
    case DB_UINT:
        return sprintf( str, "%u", value_ptr->val_uint );
    case DB_SHORT:
        return sprintf( str, "%hd", value_ptr->val_short );
    case DB_USHORT:
        return sprintf( str, "%hu", value_ptr->val_ushort );
    case DB_BIGINT:
        return sprintf( str, "%lld", value_ptr->val_bigint );
    case DB_BIGUINT:
        return sprintf( str, "%llu", value_ptr->val_biguint );
    case DB_BOOL:
        if ( value_ptr->val_bool )
            return sprintf( str, "1" );
        else
            return sprintf( str, "0" );
    case DB_ENUM_FTYPE:
        return sprintf( str, "'%s'", value_ptr->val_str );
    default:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Error: unknown type %d in %s", type, __FUNCTION__ );
        return 0;
    }
}


/* return 1 on success */
int parsedbtype( char *str_in, db_type_t type, db_type_u * value_out )
{
    int rc;
    switch ( type )
    {
    case DB_ID:
        /* convert str to id */
        rc = pk2entry_id( NULL, str_in, &value_out->val_id );
        if (rc)
            return 0;
        return 1;
    case DB_ENUM_FTYPE:
    case DB_TEXT:
        value_out->val_str = str_in;
        return 1;
    case DB_INT:
        return sscanf( str_in, "%d", &value_out->val_int );
    case DB_UINT:
        return sscanf( str_in, "%u", &value_out->val_uint );
    case DB_SHORT:
        return sscanf( str_in, "%hd", &value_out->val_short );
    case DB_USHORT:
        return sscanf( str_in, "%hu", &value_out->val_ushort );
    case DB_BIGINT:
        return sscanf( str_in, "%lld", &value_out->val_bigint );
    case DB_BIGUINT:
        return sscanf( str_in, "%llu", &value_out->val_biguint );
    case DB_BOOL:
        return sscanf( str_in, "%d", &value_out->val_bool );
    default:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Error: unknown type %d in %s", type, __FUNCTION__ );
        return 0;
    }
}

#ifdef _HSM_LITE
#define MATCH_TABLE( _t, _i ) ( ( ( _t == T_MAIN ) && is_main_field( _i ) ) || \
                                ( ( _t == T_DNAMES ) && is_names_field( _i ) ) || \
                                ( ( _t == T_ANNEX ) && is_annex_field( _i ) ) || \
                                ( ( _t == T_RECOV ) && is_recov_field( _i ) ) || \
                                ( ( _t == T_SOFTRM ) && is_softrm_field( _i ) ) || \
                                ( ( _t == T_ACCT ) && is_acct_field( _i ) ) || \
                                ( ( _t == T_ACCT ) && is_acct_pk( _i ) ) )

#elif defined( HAVE_RM_POLICY )
#define MATCH_TABLE( _t, _i )   ( ( ( _t == T_MAIN ) && is_main_field( _i ) ) || \
                                ( ( _t == T_DNAMES ) && is_names_field( _i ) ) || \
                                ( ( _t == T_ANNEX ) && is_annex_field( _i ) ) || \
                                ( ( _t == T_SOFTRM ) && is_softrm_field( _i ) ) || \
                                ( ( _t == T_ACCT ) && is_acct_field( _i ) ) || \
                                ( ( _t == T_ACCT ) && is_acct_pk( _i ) ) )

#else
#define MATCH_TABLE( _t, _i ) ( ( ( _t == T_MAIN ) && is_main_field( _i ) ) || \
                                ( ( _t == T_DNAMES ) && is_names_field( _i ) ) || \
                                ( ( _t == T_ANNEX ) && is_annex_field( _i ) ) || \
                                ( ( _t == T_ACCT ) && is_acct_field( _i ) ) || \
                                ( ( _t == T_ACCT ) && is_acct_pk( _i ) ) )

#endif

/* precomputed masks for testing attr sets efficiently */
int            main_attr_set = 0;
int            names_attr_set = 0;
int            annex_attr_set = 0;
int            stripe_attr_set = 0;
int            dir_attr_set = 0;
int            slink_attr_set = 0;
int            readonly_attr_set = 0;
int            gen_attr_set = 0;
int            acct_attr_set = 0;
int            acct_pk_attr_set = 0;

void init_attrset_masks( const lmgr_config_t *lmgr_config )
{
    int            i;
    int            mask = 1;

    main_attr_set = 0;
    names_attr_set = 0;
    annex_attr_set = 0;
    gen_attr_set = 0;
    stripe_attr_set = 0;
    readonly_attr_set = 0;
    acct_pk_attr_set = 0;
    acct_attr_set = 0;
    dir_attr_set = 0;
    slink_attr_set = 0;

    if ( lmgr_config->user_acct )
        acct_pk_attr_set |= ATTR_MASK_owner;
    if ( lmgr_config->group_acct )
        acct_pk_attr_set |= ATTR_MASK_gr_name;
    if ( lmgr_config->user_acct || lmgr_config->group_acct )
    {
        acct_pk_attr_set |= ATTR_MASK_type;
    }
#ifdef ATTR_MASK_status
    if ( lmgr_config->user_acct || lmgr_config->group_acct )
    {
        acct_pk_attr_set |= ATTR_MASK_status;
    }
#endif
    /* size: also used for size range stats */
    acct_attr_set |= ATTR_MASK_size | ATTR_MASK_blocks ;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        /* is it read only ? */
        if ( is_read_only_field( i ) )
            readonly_attr_set |= mask;

        /* The ID field is both in NAMES and MAIN. (XXX not an attribute) */
        if ( is_names_field( i ) )
            names_attr_set |= mask;

        if ( is_main_field( i ) )
            main_attr_set |= mask;
        else if ( is_gen_field( i ) )
            gen_attr_set |= mask;
        else if ( is_annex_field( i ) )
            annex_attr_set |= mask;
        else if ( is_stripe_field( i ) )
            stripe_attr_set |= mask;
        else if ( is_dirattr( i ) )
            dir_attr_set |= mask;

        /* not mutually exclusive with previous */
        if ( is_slinkattr( i ) )
            slink_attr_set |= mask;
    }
}


/**
 * Add source info of generated fields to attr mask.
 */
void add_source_fields_for_gen( int * attr_mask )
{
    int i;
    int mask = 1;

    /* add attr mask for source info of generated fields */
    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( ((*attr_mask) & mask)
             && ((field_infos[i].flags & GENERATED) || (field_infos[i].flags & DIR_ATTR))
             && (field_infos[i].gen_index != -1) )
        {
           (*attr_mask) |= (1 << field_infos[i].gen_index);
        }
    }
}

/** generate fields */
void           generate_fields( attr_set_t * p_set )
{
    int i;
    int mask = 1;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( ( p_set->attr_mask & mask) && (field_infos[i].flags & GENERATED) )
        {
           void * src_data;
           void * tgt_data;

           if ( field_infos[i].gen_func == NULL )
           {
               /* cannot generate a field without a function */
               DisplayLog( LVL_DEBUG, LISTMGR_TAG,
                           "generated field without generation function: %s",
                           field_infos[i].field_name );
               p_set->attr_mask &= ~mask;
               continue;
           }

           /* is it generated from another field ? */
           if ( field_infos[i].gen_index != -1 )
           {
                int src_mask = 1 << field_infos[i].gen_index;
                /* is source set? */
                if ( (p_set->attr_mask & src_mask) == 0 )
                {
                    DisplayLog( LVL_FULL, LISTMGR_TAG,
                                "Source info '%s' of generated field '%s' is not set "
                                "in the database",
                                field_infos[field_infos[i].gen_index].field_name,
                                field_infos[i].field_name );
                    p_set->attr_mask &= ~mask;
                    continue;
                }

                src_data = ( char * ) &p_set->attr_values + field_infos[field_infos[i].gen_index].offset;
           }
           else
           {
                /* nothing needed to generate it */
                src_data = NULL;
           }

           tgt_data = ( char * ) &p_set->attr_values + field_infos[i].offset;

           if ( field_infos[i].gen_func( tgt_data, src_data ) != 0 )
                p_set->attr_mask &= ~mask;
           else
                DisplayLog( LVL_FULL, LISTMGR_TAG, "Field '%s' auto-generated",
                            field_infos[i].field_name );

        } /* end if generated */
    } /* end for attr list */

}

/**
 * Generate fields automatically from already existing fields,
 * and check the target mask is satisfied.
 */
int  ListMgr_GenerateFields( attr_set_t * p_set, int target_mask )
{
    int save_mask = p_set->attr_mask;

    /* are there generated fields that are not set for the target */
    if ( target_mask & ~p_set->attr_mask & gen_attr_set )
    {
        /* try to generate missing fields */
        p_set->attr_mask |= (target_mask & ~p_set->attr_mask & gen_attr_set);
        generate_fields( p_set );

        /* still missing? */
        if ( target_mask & ~p_set->attr_mask )
        {
               DisplayLog( LVL_VERB, LISTMGR_TAG, "Field still missing (can't be generated): %#X",
                           target_mask & ~p_set->attr_mask );
               /* never leave the function with less info than when entering! */
               p_set->attr_mask |= save_mask;
               return DB_ATTR_MISSING;
        }
    }

    /* never leave the function with less info than when entering! */
    p_set->attr_mask |= save_mask;

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
static int print_func_call(char *out, int func_index, const char *prefix)
{
    char *curr = out;
    const function_def_t *func = get_function_by_attr(func_index); 
    char **args;
    if (func == NULL) /* unexpected: BUG */
        RBH_BUG("call for non-function attr");

    curr += sprintf(curr, "%s(", func->fn_name);
    for (args = func->fn_args; *args != NULL; args++)
    {
        if (args == func->fn_args) /* first arg */
            curr += sprintf(curr, "%s%s", prefix, *args);
        else
            curr += sprintf(curr, ",%s%s", prefix, *args);
    }
    strcpy(curr, ")"); /* ) \0 */
    curr++;
    return curr-out;
}

/**
 * @param table T_MAIN, T_ANNEX, T_ACCT
 * @param prefif
 * @param postfix
 * @param separator
 * @return nbr of fields
 */
int attrmask2fieldlist( char *str, int attr_mask, table_enum table, int leading_comma,
                        int for_update, char *prefix, char *postfix )
{
    int            i;
    char          *fields_curr = str;
    unsigned int   nbfields = 0;
    int            mask = 1;
    char          *for_update_str = "";

    /* optim: exit immediatly if no field matches */
    if ((table == T_MAIN) && !main_fields(attr_mask))
        return 0;
    if ((table == T_ANNEX) && (!annex_table || !annex_fields(attr_mask)))
        return 0;
    if ((table == T_DNAMES) && !names_fields(attr_mask))
        return 0;

    if ( for_update )
        for_update_str = "=?";

    if ( ( table == T_STRIPE_INFO ) || ( table == T_STRIPE_ITEMS ) )
        return -DB_NOT_SUPPORTED;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( attr_mask & mask )
        {
            /* NB: no read only fields for update */
            if ( for_update && is_read_only_field(i) )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG, "Error in %s:%s: attribute '%s' is read-only",
                            __FILE__, __FUNCTION__, field_infos[i].field_name );
                return -DB_READ_ONLY_ATTR;
            }

            if (MATCH_TABLE( table, i ))
            {
                if (leading_comma || (nbfields > 0))
                {
                    *fields_curr = ',';
                    fields_curr ++;
                }

                if (is_funcattr(i))
                {
                    fields_curr += print_func_call(fields_curr, i, prefix);
                    fields_curr += sprintf(fields_curr, "%s%s", for_update_str, postfix);
                }
                else
                {
                    fields_curr +=
                        sprintf(fields_curr, "%s%s%s%s", prefix, field_infos[i].field_name,
                                for_update_str, postfix);
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
int attrmask2fieldoperation( char *str, int attr_mask, table_enum table, const char *prefix, operation_type operation )
{
    int i;
    int mask = 1;
    unsigned int  nbfields = 0;
    char *fields_curr = str;
    char operator;

    if ( operation == SUBSTRACT )
        operator = '-';
    else
        operator = '+';

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( attr_mask & mask )
        {
            if ( MATCH_TABLE( table, i ) )
            {
                if ( nbfields == 0 )
                    fields_curr +=
                        sprintf( fields_curr, "%s=CAST(%s as SIGNED)%cCAST(%s%s as SIGNED) ", field_infos[i].field_name,
                                field_infos[i].field_name, operator, prefix, field_infos[i].field_name );
                else
                    fields_curr +=
                        sprintf( fields_curr, ", %s=CAST(%s as SIGNED)%cCAST(%s%s as SIGNED) ", field_infos[i].field_name,
                                field_infos[i].field_name, operator, prefix, field_infos[i].field_name );
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
int attrmask2fieldcomparison( char *str, int attr_mask, table_enum table, const char *left_prefix, const char *right_prefix,
                            const char *comparator, const char * separator )
{
    int i;
    int mask = 1;
    unsigned int  nbfields = 0;
    char *fields_curr = str;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( attr_mask & mask )
        {
            if ( MATCH_TABLE( table, i ) )
            {
                if ( nbfields == 0 )
                    fields_curr +=
                        sprintf( fields_curr, "%s%s%s%s%s ", left_prefix,  field_infos[i].field_name,
                                comparator, right_prefix, field_infos[i].field_name );
                else
                    fields_curr +=
                        sprintf( fields_curr, "%s %s%s%s%s%s ", separator, left_prefix, field_infos[i].field_name,
                                comparator, right_prefix, field_infos[i].field_name );
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
int attrset2valuelist( lmgr_t * p_mgr, char *str, const attr_set_t * p_set,
                       table_enum table, int leading_coma )
{
    int            i;
    char          *values_curr = str;
    unsigned int   nbfields = 0;
    db_type_u      typeu;
    int            mask = 1;

    if ( ( table == T_STRIPE_INFO ) || ( table == T_STRIPE_ITEMS ) )
        return -DB_NOT_SUPPORTED;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( p_set->attr_mask & mask )
        {
            if ( MATCH_TABLE( table, i ) )
            {
                if ( leading_coma || ( nbfields > 0 ) )
                {
                    *values_curr = ',';
                    values_curr++;
                }

                ASSIGN_UNION( typeu, field_infos[i].db_type,
                              ( ( char * ) &p_set->attr_values + field_infos[i].offset ) );

                values_curr += printdbtype( p_mgr, values_curr,
                                            field_infos[i].db_type, &typeu );

                nbfields++;
            }
        }
    }
    /* null terminating char */
    values_curr[0] = '\0';
    return nbfields;
}

/**
 * @param table T_MAIN, T_ANNEX
 * @return nbr of fields
 */
int attrset2updatelist(lmgr_t * p_mgr, char *str, const attr_set_t * p_set,
                       table_enum table, int leading_coma, int generic_value)
{
    int            i;
    char          *values_curr = str;
    unsigned int   nbfields = 0;
    db_type_u      typeu;
    int            mask = 1;

    if ( ( table == T_STRIPE_INFO ) || ( table == T_STRIPE_ITEMS ) )
        return -DB_NOT_SUPPORTED;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( ( p_set->attr_mask & mask ) && MATCH_TABLE( table, i ) )
        {
            if ( is_read_only_field( i ) )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG, "Error in %s:%s: attribute '%s' is read-only",
                            __FILE__, __FUNCTION__, field_infos[i].field_name );
                return -DB_READ_ONLY_ATTR;
            }

            if ( leading_coma || ( nbfields > 0 ) )
            {
                *values_curr = ',';
                values_curr++;
            }

            values_curr += sprintf( values_curr, "%s=", field_infos[i].field_name );

            if (!generic_value)
            {
                ASSIGN_UNION(typeu, field_infos[i].db_type,
                             ((char *)&p_set->attr_values + field_infos[i].offset));
                values_curr += printdbtype(p_mgr, values_curr,
                                           field_infos[i].db_type, &typeu);
            }
            else
                values_curr += sprintf(values_curr, "VALUES(%s)", field_infos[i].field_name);

            nbfields++;
        }
    }
    return nbfields;
}

/**
 * Make a bind list for retrieving attributes.
 * @param type_list must be allocated by caller
 * @param buff_list must be allocated by caller
 * @param size_list must be allocated by caller
 * @return Number of bound fields.
 */
int mk_result_bind_list( const attr_set_t * p_set, table_enum table, db_type_t * type_list,
                         void **buff_list, size_t * size_list )
{
    int            i;
    unsigned int   nbfields = 0;
    int            mask = 1;
    unsigned int   curr_param_index = 0;

    if ( ( table == T_STRIPE_INFO ) || ( table == T_STRIPE_ITEMS ) )
        return -DB_NOT_SUPPORTED;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( p_set->attr_mask & mask )
        {
            if ( MATCH_TABLE( table, i ) )
            {
                type_list[curr_param_index] = field_infos[i].db_type;
                buff_list[curr_param_index] =
                    ( void * ) ( ( char * ) &p_set->attr_values + field_infos[i].offset );
                size_list[curr_param_index] = field_infos[i].db_type_size;
                curr_param_index++;
                nbfields++;
            }
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
    int            i;
    unsigned int   nbfields = 0;
    db_type_u      typeu;
    int            mask = 1;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ((p_set->attr_mask & mask) && MATCH_TABLE(table, i))
        {
            if (result_tab)
                DisplayLog(LVL_FULL, LISTMGR_TAG, "result[%u]: %s = %s", nbfields,
                           field_infos[i].field_name?field_infos[i].field_name:"<null>",
                           result_tab[nbfields]?result_tab[nbfields]:"<null>");

            /* Parse nbfield'th value */
            if ( nbfields >= res_count )
            {
                return DB_BUFFER_TOO_SMALL;
            }

#ifdef _LUSTRE
            if ( field_infos[i].db_type == DB_STRIPE_INFO )
            {
                if ((result_tab == NULL)
                    || (result_tab[nbfields] == NULL)
                    || (result_tab[nbfields+1] == NULL)
                    || (result_tab[nbfields+2] == NULL))
                {
                    /* must skip 3 columns in this case */
                    p_set->attr_mask &= ~( 1 << i );
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
                p_set->attr_mask &= ~( 1 << i );
                nbfields++;
                continue;
            }
            else if ( !parsedbtype( result_tab[nbfields], field_infos[i].db_type, &typeu ) )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Error: cannot parse field value '%s' (position %u) for %s",
                            result_tab[nbfields], nbfields, field_infos[i].field_name );
                RBH_BUG("DB value cannot be parsed: DB may be corrupted");
                p_set->attr_mask &= ~( 1 << i );
                nbfields++;
                continue;
            }

            /* special case for fullpath which must be converted from relative to aboslute */
            if (i == ATTR_INDEX_fullpath)
                fullpath_db2attr(typeu.val_str, ATTR(p_set, fullpath));
            else
                UNION_GET_VALUE(typeu, field_infos[i].db_type,
                                ((char *)&p_set->attr_values + field_infos[i].offset));
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
    default:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Default sign for filter: should never happen !!!" );
        return "=";
    }
}

/**
 * return FILTERDIR_NONE if there is no filter on dirattrs
 * return FILTERDIR_EMPTY if the test is 'dircount == 0' (no junction needed)
 * return FILTERDIR_NONEMPTY if the test is on dircount != 0, >= 0, condition on avgsize
 *                           junction needed, depending on the filter
 *                           test looks like "dirattr >= x"
 */
filter_dir_e dir_filter(lmgr_t * p_mgr, char* filter_str, const lmgr_filter_t * p_filter,
                        unsigned int * dir_attr_index, const char *prefix)
{
    int i;
    filter_str[0] = '\0';

#ifdef ATTR_INDEX_dircount
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
                if (prefix)
                    sprintf(filter_str, "%s.id NOT IN (SELECT distinct(parent_id) from "DNAMES_TABLE")", prefix);
                else
                    strcpy(filter_str, "id NOT IN (SELECT distinct(parent_id) from "DNAMES_TABLE")");
                *dir_attr_index = index;
                return FILTERDIR_EMPTY;
            }
            else
            {
                char val[1024];
                /* single value (list only apply to OSTs XXX for now) */
                db_type_u typeu = p_filter->filter_simple.filter_value[i].value;
                printdbtype( p_mgr, val, field_infos[index].db_type, &typeu );

                sprintf(filter_str, "dirattr%s%s", compar2str( p_filter->filter_simple.filter_compar[i] ),
                        val);
                *dir_attr_index = index;
                return FILTERDIR_OTHER;
            }
        }
    }
#endif
    return FILTERDIR_NONE;
}

/**
 * build filter for stored FUNCTIONs
 * \param str_id: filter for entry id string (eg. id, TABLE.id...)
 * \return the number of filtered values
 */
int func_filter(lmgr_t * p_mgr, char* filter_str, const lmgr_filter_t * p_filter,
                table_enum table, int leading_and, int prefix_table)
{
    int i;
    char param1[128];
    char param2[128];
    unsigned int nb_fields = 0;
    char * curr = filter_str;

    curr[0] = '\0';

    if ( p_filter->filter_type == FILTER_SIMPLE )
    {
        for ( i = 0; i < p_filter->filter_simple.filter_count; i++ )
        {
            unsigned int index = p_filter->filter_simple.filter_index[i];

            if ( field_infos[index].flags & FUNC_ATTR )
            {
                char val[RBH_PATH_MAX];
                db_type_u typeu;

                param1[0] = '\0';
                param2[0] = '\0';

                /* add prefixes or parenthesis, etc. */
                if ( leading_and || ( nb_fields > 0 ) )
                {
                    if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_OR )
                        curr += sprintf( curr, " OR " );
                    else
                        curr += sprintf( curr, " AND " );
                }

                if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_BEGIN )
                     curr += sprintf( curr, "( " );

                if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT )
                {
                    /* NOT (x <cmp> <val>) */
                     curr += sprintf( curr, " NOT (" );
                }

                if (index == ATTR_INDEX_fullpath)
                {
                    char relative[RBH_PATH_MAX];
                    if (fullpath_attr2db(p_filter->filter_simple.filter_value[i].value.val_str, relative))
                    {
                        /* condition is always false */
                        curr += sprintf(curr, "FALSE");
                        return 1;
                    }
                    typeu.val_str = relative;
                    printdbtype( p_mgr, val, field_infos[index].db_type, &typeu );

                    /* if the filter applies to DNAMES, exactly filter on each row,
                     * else, filter on any path */
                    if (table == T_DNAMES)
                    {
                        if ( prefix_table ) {
                            sprintf(param1, "%s.parent_id", table2name(table));
                            sprintf(param2, "%s.name", table2name(table));
                        }
                        else
                        {
                            strcpy(param1, "parent_id");
                            strcpy(param2, "name");
                        }
                        if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                            curr += sprintf(curr, "(");
                        curr += sprintf(curr, THIS_PATH_FUNC"(%s,%s)%s%s", param1, param2,
                                compar2str( p_filter->filter_simple.filter_compar[i] ),
                                val);
                        if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                            curr += sprintf(curr, " OR "THIS_PATH_FUNC"(%s,%s) IS NULL)", param1, param2);
                    }
                    else
                    {
                        if ( prefix_table )
                            sprintf(param1, "%s.id", table2name(table));
                        else
                            strcpy(param1, "id");

                        if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                            curr += sprintf(curr, "(");

                        curr += sprintf(curr, ONE_PATH_FUNC"(%s)%s%s", param1,
                                compar2str( p_filter->filter_simple.filter_compar[i] ),
                                val);

                        if (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL)
                            curr += sprintf(curr, " OR "ONE_PATH_FUNC"(%s) IS NULL)", param1);
                    }
                }

                /* add closing parenthesis, etc...*/
                if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT )
                {
                    /* NOT (x <cmp> <val>) */
                    curr += sprintf( curr, ") " );
                }

                if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_END )
                     curr += sprintf( curr, ") " );

                nb_fields ++;
            }
        }
    }
    return nb_fields;
}




int filter2str( lmgr_t * p_mgr, char *str, const lmgr_filter_t * p_filter,
                table_enum table, int leading_and, int prefix_table )
{
    int            i;
    unsigned int   nbfields = 0;
    db_type_u      typeu;
    char          *values_curr = str;
    char           fname[128];

    if ( p_filter->filter_type == FILTER_SIMPLE )
    {

        for ( i = 0; i < p_filter->filter_simple.filter_count; i++ )
        {
            unsigned int   index = p_filter->filter_simple.filter_index[i];
            int match =  MATCH_TABLE( table, index )
                         || (( table == T_STRIPE_ITEMS )
                              && ( field_infos[index].db_type == DB_STRIPE_ITEMS ))
                         || (( table == T_STRIPE_INFO )
                              && ( field_infos[index].db_type == DB_STRIPE_INFO ) );

            fname[0] = '\0';

            /* filter on generated fields are not allowed */
            if ( field_infos[index].flags & DIR_ATTR )
            {
                DisplayLog( LVL_FULL, LISTMGR_TAG, "Special filter on dir attribute '%s'", field_infos[index].field_name );
                continue;
            }
            else if ( field_infos[index].flags & GENERATED )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG, "Cannot use filter on generated field '%s'", field_infos[index].field_name );
                return -DB_INVALID_ARG;
            }

            if ( match )
            {
                /* add prefixes or parenthesis, etc. */
                if ( leading_and || ( nbfields > 0 ) )
                {
                    if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_OR )
                        values_curr += sprintf( values_curr, " OR " );
                    else
                        values_curr += sprintf( values_curr, " AND " );
                }

                if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_BEGIN )
                     values_curr += sprintf( values_curr, "( " );

                if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT )
                {
                    if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL )
                        /* (NOT (x <cmp> <val>) OR x IS NULL) */
                        values_curr += sprintf( values_curr, " (NOT (" );
                    else
                        /* NOT (x <cmp> <val>) */
                         values_curr += sprintf( values_curr, " NOT (" );
                }

                if ( (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL )
                     && !(p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT ) )
                {
                    /* (x <cmp> <val> OR x IS NULL) */
                    values_curr[0] = '(';
                    values_curr++;
                }
            }

            if ( MATCH_TABLE( table, index ) )
            {
                /* exception: fullpath is a real field in SOFT_RM table */
                if (is_funcattr(index) &&
                    !((table == T_SOFTRM) && (index == ATTR_INDEX_fullpath)))
                {
                    char tmp[128] = "";
                    if (prefix_table)
                        sprintf(tmp, "%s.", table2name(table));
                    print_func_call(fname, index, tmp);
                }
                else /* std field */
                {
                    if ( prefix_table )
                        sprintf(fname, "%s.", table2name(table));

                    strcat(fname, field_infos[index].field_name);
                }

                values_curr +=
                    sprintf( values_curr, "%s%s", fname,
                             compar2str( p_filter->filter_simple.filter_compar[i] ) );

                /* fullpath already includes root for SOFT_RM table */
                if ((index == ATTR_INDEX_fullpath) && (table != T_SOFTRM))
                {
                    char relative[RBH_PATH_MAX];
                    if (fullpath_attr2db(p_filter->filter_simple.filter_value[i].value.val_str, relative))
                    {
                        /* condition is always false */
                        values_curr += sprintf(values_curr, "FALSE");
                    }
                    else
                    {
                        typeu.val_str = relative;
                        values_curr += printdbtype(p_mgr, values_curr,
                                                   field_infos[index].db_type, &typeu);
                    }
                }
                else
                {
                    /* single value (list only apply to OSTs XXX for now) */
                    typeu = p_filter->filter_simple.filter_value[i].value;

                    values_curr += printdbtype( p_mgr, values_curr,
                                                field_infos[index].db_type, &typeu );
                }
                nbfields++;
            }
            else if ( ( table == T_STRIPE_ITEMS )
                      && ( field_infos[index].db_type == DB_STRIPE_ITEMS ) )
            {
                if ( prefix_table )
                    sprintf(fname, "%s.", STRIPE_ITEMS_TABLE );

                strcat( fname, "ostidx" );

                /* single value or a list? */
                if (p_filter->filter_simple.filter_compar[i] == IN
                    || (p_filter->filter_simple.filter_compar[i] == NOTIN))
                {
                    /* FIXME the length of this query can be very important,
                     * so we may overflow the output string */
                    values_curr += sprintf(values_curr, "%s%s(", fname,
                                           compar2str( p_filter->filter_simple.filter_compar[i] ));
                    unsigned int j;
                    db_type_u * list = p_filter->filter_simple.filter_value[i].list.values;

                    for (j = 0; j < p_filter->filter_simple.filter_value[i].list.count; j++)
                    {
                        values_curr +=
                            sprintf( values_curr, "%s%u", j==0?"":",", list[j].val_uint );
                    }
                    strcpy(values_curr, ")");
                    values_curr++;
                }
                else /* single value */
                {
                    values_curr +=
                        sprintf( values_curr, "%s%s%u", fname,
                                 compar2str( p_filter->filter_simple.filter_compar[i] ),
                                 p_filter->filter_simple.filter_value[i].value.val_uint );
                }

                nbfields++;
            }
            else if ( ( table == T_STRIPE_INFO )
                      && ( field_infos[index].db_type == DB_STRIPE_INFO ) )
            {

                /* We XXX Assume that the only possible filter here is on pool_name */

                if ( prefix_table )
                    sprintf(fname, "%s.", STRIPE_INFO_TABLE );

                strcat( fname, "pool_name" );

                values_curr +=
                    sprintf( values_curr, "%s%s'%s'", fname,
                             compar2str( p_filter->filter_simple.filter_compar[i] ),
                             p_filter->filter_simple.filter_value[i].value.val_str );

                nbfields++;
            }

            if ( match )
            {
                if ( (p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL )
                     && !(p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT ) )
                {
                    values_curr += sprintf( values_curr, " OR %s IS NULL)", fname );
                }

                if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT )
                {
                    if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_ALLOW_NULL )
                        /* (NOT (x <cmp> <val>) OR x IS NULL) */
                        values_curr += sprintf( values_curr, ") OR %s IS NULL)", fname );
                    else
                        /* NOT (x <cmp> <val>) */
                        values_curr += sprintf( values_curr, ") " );
                }

                if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_END )
                     values_curr += sprintf( values_curr, ") " );
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

unsigned int append_size_range_fields(char * str, int leading_comma, char *prefix)
{
    unsigned int i, l;
    l=0;
    for (i = 0; i < SZ_PROFIL_COUNT; i++)
        l += sprintf( str+l, "%s %s%s", leading_comma || (i > 0)?",":"",
                      prefix, sz_field[i] );
    return l;
}

/* those functions are used for begin/commit/rollback */
int lmgr_begin( lmgr_t * p_mgr )
{
    if ( lmgr_config.commit_behavior == 0 )
        /* autocommit */
        return DB_SUCCESS;
    else if ( lmgr_config.commit_behavior == 1 )
        /* commit every transaction */
        return db_exec_sql( &p_mgr->conn, "BEGIN", NULL );
    else
    {
        int            rc = DB_SUCCESS;

        /* if last operation was commited, issue a begin statement */
        if ( p_mgr->last_commit == 0 )
        {
            rc = db_exec_sql( &p_mgr->conn, "BEGIN", NULL );
            if ( rc )
                return rc;
        }

        /* increment current op */
        p_mgr->last_commit++;
        return DB_SUCCESS;
    }
}

void lmgr_rollback( lmgr_t * p_mgr )
{
    if ( lmgr_config.commit_behavior == 0 )
        return;
    else
    {
        /* we must rollback all operations since the last commit, to keep database into persistent state */
        db_exec_sql( &p_mgr->conn, "ROLLBACK", NULL );

        p_mgr->last_commit = 0;
    }
}

int lmgr_commit( lmgr_t * p_mgr )
{
    if ( lmgr_config.commit_behavior == 0 )
        return DB_SUCCESS;
    else if ( lmgr_config.commit_behavior == 1 )
        return db_exec_sql( &p_mgr->conn, "COMMIT", NULL );
    else
    {
        /* if the transaction count is reached:
         * commit operations and result transaction count
         */
        if ( ( p_mgr->last_commit % lmgr_config.commit_behavior == 0 ) || p_mgr->force_commit )
        {
            int            rc;
            rc = db_exec_sql( &p_mgr->conn, "COMMIT", NULL );
            if ( rc )
                return rc;

            p_mgr->last_commit = 0;
        }
    }
    return DB_SUCCESS;
}

/** Set force commit behavior */
void ListMgr_ForceCommitFlag( lmgr_t * p_mgr, int force_commit )
{
    p_mgr->force_commit = force_commit;
}

/** check if the last operation was really committed
 * @return TRUE if the last operation has been commited,
 * @return FALSE if commit is deferred.
 */
int ListMgr_GetCommitStatus( lmgr_t * p_mgr )
{
    /* operation was not committed if period > 1 and last_commit is not reset yet */
    if ( ( lmgr_config.commit_behavior > 1 ) && ( p_mgr->last_commit != 0 ) )
        return FALSE;
    else
        return TRUE;
}

int lmgr_flush_commit( lmgr_t * p_mgr )
{
    int            rc;
    if ( ( lmgr_config.commit_behavior > 1 ) && ( p_mgr->last_commit != 0 ) )
    {
        rc = db_exec_sql( &p_mgr->conn, "COMMIT", NULL );
        if ( rc )
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
void ListMgr_MergeAttrSets( attr_set_t * p_target_attrset, attr_set_t * p_source_attrset, int update )
{
    int            i;
    int            mask = 1;
    db_type_u      typeu;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( (update || !( p_target_attrset->attr_mask & mask ))
             && ( p_source_attrset->attr_mask & mask ) )
        {
            if ( !is_stripe_field( i ) )
            {

                ASSIGN_UNION( typeu, field_infos[i].db_type,
                              ( ( char * ) &p_source_attrset->attr_values +
                                field_infos[i].offset ) );
                UNION_GET_VALUE( typeu, field_infos[i].db_type,
                                 ( ( char * ) &p_target_attrset->attr_values +
                                   field_infos[i].offset ) );
            }
            else if ( field_infos[i].db_type == DB_STRIPE_ITEMS )
            {
                memcpy( ( char * ) &p_target_attrset->attr_values + field_infos[i].offset,
                        ( char * ) &p_source_attrset->attr_values + field_infos[i].offset,
                        sizeof( stripe_items_t ) );
            }
            else if ( field_infos[i].db_type == DB_STRIPE_INFO )
            {
                memcpy( ( char * ) &p_target_attrset->attr_values + field_infos[i].offset,
                        ( char * ) &p_source_attrset->attr_values + field_infos[i].offset,
                        sizeof( stripe_info_t ) );
            }

            p_target_attrset->attr_mask |= mask;
        }
    }
    return;

}


void ListMgr_FreeAttrs( attr_set_t * p_set )
{
#ifdef _LUSTRE
    int            i;
    int            mask = 1;

    if (p_set == NULL || p_set->attr_mask == 0)
        return;

    /* Free stripe count attributes */
    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( ( field_infos[i].db_type == DB_STRIPE_ITEMS ) && ( p_set->attr_mask & mask ) )
        {
            free_stripe_items( ( stripe_items_t * ) ( ( char * ) &p_set->attr_values +
                                                      field_infos[i].offset ) );
        }
    }
#endif
}

/** return the mask of attributes that differ */
int ListMgr_WhatDiff(const attr_set_t * p_tgt, const attr_set_t * p_src)
{
    int            i;
    int            bit = 1;
    int common_mask = p_tgt->attr_mask & p_src->attr_mask;
    int diff_mask = 0;

    for ( i = 0; i < ATTR_COUNT; i++, bit <<= 1 )
    {
        if (bit & common_mask)
        {
            int is_diff = 0;
            if ( !is_stripe_field( i ) )
            {
                /* diff the values */
                DIFF_UNION(is_diff, field_infos[i].db_type,
                           ((char *)&p_src->attr_values +
                                field_infos[i].offset),
                           ((char *)&p_tgt->attr_values +
                                field_infos[i].offset));
                if (is_diff)
                    diff_mask |= bit;
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
                    diff_mask |= bit;
                }
            }
            else if ( field_infos[i].db_type == DB_STRIPE_ITEMS )
            {
                if (ATTR(p_tgt, stripe_items).count
                    != ATTR(p_src, stripe_items).count)
                    is_diff = 1;
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
                            is_diff = 1;
                            break;
                        }
                    }
                }
                if (is_diff)
                     diff_mask |= bit;
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
int lmgr_range2list(const char * set, db_type_t type, value_list_t * p_list)
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
static inline bool table_mask_compat(int m1, int m2)
{
    /* attrs in a given table must be the same or 0 */
    if (m1 == 0 || m2 == 0)
        return true;
    else
        return (m1 == m2);
}

/** Check mask compatibility for request batching. */
bool lmgr_batch_compat(int m1, int m2)
{
    if (!table_mask_compat(m1 & main_attr_set, m2 & main_attr_set))
        return false;
    if (!table_mask_compat(m1 & names_attr_set, m2 & names_attr_set))
        return false;
    if (!table_mask_compat(m1 & annex_attr_set, m2 & annex_attr_set))
        return false;
    if (!table_mask_compat(m1 & stripe_attr_set, m2 & stripe_attr_set))
        return false;
    return true;
}
