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
#include "listmgr_prep_stmt.h"
#include "database.h"
#include "RobinhoodLogs.h"
#include "listmgr_stripe.h"
#include <stdio.h>

int printdbtype( char *str, db_type_t type, db_type_u * value_ptr )
{

    switch ( type )
    {
    case DB_TEXT:
        return sprintf( str, "'%s'", value_ptr->val_str );
    case DB_INT:
        return sprintf( str, "%d", value_ptr->val_int );
    case DB_UINT:
        return sprintf( str, "%u", value_ptr->val_uint );
    case DB_BIGINT:
        return sprintf( str, "%lld", value_ptr->val_bigint );
    case DB_BIGUINT:
        return sprintf( str, "%llu", value_ptr->val_biguint );
    case DB_BOOL:
        if ( value_ptr->val_bool )
            return sprintf( str, "1" );
        else
            return sprintf( str, "0" );
    default:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Error: unknown type %d in %s", type, __FUNCTION__ );
        return 0;
    }
}


/* return 1 on success */
int parsedbtype( char *str_in, db_type_t type, db_type_u * value_out )
{
    switch ( type )
    {
    case DB_TEXT:
        value_out->val_str = str_in;
        return 1;
    case DB_INT:
        return sscanf( str_in, "%d", &value_out->val_int );
    case DB_UINT:
        return sscanf( str_in, "%u", &value_out->val_uint );
        break;
    case DB_BIGINT:
        return sscanf( str_in, "%lld", &value_out->val_bigint );
        break;
    case DB_BIGUINT:
        return sscanf( str_in, "%llu", &value_out->val_biguint );
        break;
    case DB_BOOL:
        return sscanf( str_in, "%d", &value_out->val_bool );
    default:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Error: unknown type %d in %s", type, __FUNCTION__ );
        return 0;
    }
}

#define MATCH_TABLE( _t, _i ) ( ( ( _t == T_MAIN ) && is_main_field( _i ) ) || ( ( _t == T_ANNEX ) && is_annex_field( _i ) ) )

/* precomputed masks for testing attr sets efficiently */
int            main_attr_set = 0;
int            annex_attr_set = 0;
int            stripe_attr_set = 0;
int            readonly_attr_set = 0;
int            gen_attr_set = 0;

void init_attrset_masks(  )
{
    int            i;
    int            mask = 1;

    main_attr_set = 0;
    annex_attr_set = 0;
    gen_attr_set = 0;
    stripe_attr_set = 0;
    readonly_attr_set = 0;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        /* is it read only ? */
        if ( is_read_only_field( i ) )
            readonly_attr_set |= mask;

        if ( is_main_field( i ) )
            main_attr_set |= mask;
        else if ( is_gen_field( i ) )
            gen_attr_set |= mask;
        else if ( is_annex_field( i ) )
            annex_attr_set |= mask;
        else if ( is_stripe_field( i ) )
            stripe_attr_set |= mask;
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
        if ( ((*attr_mask) & mask) && (field_infos[i].flags & GENERATED)
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
               DisplayLog( LVL_CRIT, LISTMGR_TAG, "BUG: generated field without generation function!" );
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
                    DisplayLog( LVL_FULL, LISTMGR_TAG, "Source info '%s' of generated field '%s' is not set "
                                "in the database" );
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
                DisplayLog( LVL_FULL, LISTMGR_TAG, "Field %s auto-generated",
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
    /* are there generated fields that are not set for the target */
    if ( target_mask & ~p_set->attr_mask & gen_attr_set )
    {
        /* try to generate missing fields */
        p_set->attr_mask = target_mask;
        generate_fields( p_set );

        /* still missing? */
        if ( target_mask & ~p_set->attr_mask )
        {
               DisplayLog( LVL_VERB, LISTMGR_TAG, "Field still missing (can't be generated): %#X", 
                           target_mask & ~p_set->attr_mask );
               return DB_ATTR_MISSING;
        }
    }

    return DB_SUCCESS;
    
}



/**
 * @param table T_MAIN, T_ANNEX
 * @return nbr of fields
 */
int attrmask2fieldlist( char *str, int attr_mask, table_enum table, int leading_coma,
                        int for_update )
{
    int            i;
    char          *fields_curr = str;
    unsigned int   nbfields = 0;
    int            mask = 1;
    char          *for_update_str = "";

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
 
            if ( MATCH_TABLE( table, i ) )
            {
                if ( !leading_coma && ( nbfields == 0 ) )
                    fields_curr +=
                        sprintf( fields_curr, "%s%s", field_infos[i].field_name, for_update_str );
                else
                    fields_curr +=
                        sprintf( fields_curr, ",%s%s", field_infos[i].field_name, for_update_str );
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
int attrset2valuelist( char *str, const attr_set_t * p_set, table_enum table, int leading_coma,
                       int prep_stmt )
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

                if ( prep_stmt )
                {
                    values_curr[0] = '?';
                    values_curr++;
                }
                else
                {
                    ASSIGN_UNION( typeu, field_infos[i].db_type,
                                  ( ( char * ) &p_set->attr_values + field_infos[i].offset ) );

                    values_curr += printdbtype( values_curr, field_infos[i].db_type, &typeu );
                }

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
int attrset2updatelist( char *str, const attr_set_t * p_set, table_enum table, int leading_coma )
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

            ASSIGN_UNION( typeu, field_infos[i].db_type,
                          ( ( char * ) &p_set->attr_values + field_infos[i].offset ) );

            values_curr += printdbtype( values_curr, field_infos[i].db_type, &typeu );

            nbfields++;
        }
    }
    return nbfields;
}


#ifndef _DISABLE_PREP_STMT

/**
 * Bind prepared statement parameters to an attr_set_t structure.
 * @return Number of bound fields.
 */
int prep_stmt_bind_attrs( prep_stmt_t prep, const attr_set_t * p_set,
                          table_enum table, unsigned int start_index )
{
    int            i, rc;
    unsigned int   nbfields = 0;
    int            mask = 1;
    unsigned int   curr_param_index = start_index;

    if ( ( table == T_STRIPE_INFO ) || ( table == T_STRIPE_ITEMS ) )
        return -DB_NOT_SUPPORTED;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( p_set->attr_mask & mask )
        {
            if ( MATCH_TABLE( table, i ) )
            {
                rc = db_bind_param( prep, curr_param_index, field_infos[i].db_type,
                                    ( void * ) ( ( char * ) &p_set->attr_values +
                                                 field_infos[i].offset ),
                                    field_infos[i].db_type_size );

                if ( rc )
                    return -rc;
                curr_param_index++;
                nbfields++;
            }
        }
    }
    return nbfields;
}

#endif

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

#ifndef _DISABLE_PREP_STMT

int unset_null_results( attr_set_t * p_set, table_enum table, prep_stmt_t stmt, unsigned int shift )
{
    int            i;
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
                /* unset the parameter if it is null */
                if ( db_is_null_result( stmt, curr_param_index + shift ) )
                {
#ifdef _DEBUG_DB
                    DisplayLog( LVL_FULL, LISTMGR_TAG, "Attribute %s is null",
                                field_infos[i].field_name );
#endif
                    p_set->attr_mask &= ~mask;
                }

                curr_param_index++;
            }
        }
    }
    return 0;

}
#endif

int pkfields( char *str, int with_validator )
{
#ifdef FID_PK
    strcpy( str, "fid_seq,fid_oid" );
#else
    if ( with_validator )
        strcpy( str, "inum,dev,validator" );
    else
        strcpy( str, "inum,dev" );
#endif
    return 0;
}


int pkvalues( char *str, const entry_id_t * p_id, int with_validator )
{
#ifdef FID_PK
    sprintf( str, "%llu,%u", p_id->f_seq, p_id->f_oid );
#else
    if ( with_validator )
        sprintf( str, "%llu,%llu,%u", ( unsigned long long ) p_id->inode,
                 ( unsigned long long ) p_id->device, p_id->validator );
    else
        sprintf( str, "%llu,%llu", ( unsigned long long ) p_id->inode,
                 ( unsigned long long ) p_id->device );
#endif
    return 0;
}


char          *pkfilter( char *str, const entry_id_t * p_id )
{
#ifdef FID_PK
    /* FID Primary key */
    sprintf( str, "fid_seq=%llu AND fid_oid=%u", p_id->f_seq, p_id->f_oid );
#else
    /* inum/dev primary key */
    sprintf( str, "inum=%llu AND dev=%llu",
             ( unsigned long long ) p_id->inode, ( unsigned long long ) p_id->device );
#endif
    return str;
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
        if ( ( p_set->attr_mask & mask ) && ( MATCH_TABLE( table, i ) ) )
        {
#ifdef _DEBUG_DB
            DisplayLog( LVL_FULL, LISTMGR_TAG, "result[%u] =  %s", nbfields, result_tab[nbfields] );
#endif

            /* Parse nbfield'th value */
            if ( nbfields >= res_count )
            {
                return DB_BUFFER_TOO_SMALL;
            }

            if ( result_tab[nbfields] == NULL )
            {
                p_set->attr_mask &= ~( 1 << i );
                nbfields++;
                continue;
            }

            if ( !parsedbtype( result_tab[nbfields], field_infos[i].db_type, &typeu ) )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Error: cannot parse field value '%s'", result_tab[nbfields] );
                p_set->attr_mask &= ~( 1 << i );
                nbfields++;
                continue;
            }

            UNION_GET_VALUE( typeu, field_infos[i].db_type,
                             ( ( char * ) &p_set->attr_values + field_infos[i].offset ) );

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
    case LIKE:
        return " LIKE ";
    case UNLIKE:
        return " NOT LIKE ";
    default:
        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Default sign for filter: should never happen !!!" );
        return "=";
    }
}


int filter2str( char *str, const lmgr_filter_t * p_filter, table_enum table,
                int leading_and, int prefix_table )
{
    int            i;
    unsigned int   nbfields = 0;
    db_type_u      typeu;
    char          *values_curr = str;

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

            /* filter on generated fields are not allowed */
            if ( field_infos[index].flags & GENERATED )
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
                     values_curr += sprintf( values_curr, " NOT (" );
            }

            if ( MATCH_TABLE( table, index ) )
            {
                if ( prefix_table )
                {
                    if ( table == T_MAIN )
                        values_curr += sprintf( values_curr, "%s.", MAIN_TABLE );
                    else if ( table == T_ANNEX )
                        values_curr += sprintf( values_curr, "%s.", ANNEX_TABLE );
                }

                values_curr +=
                    sprintf( values_curr, "%s%s",
                             field_infos[index].field_name,
                             compar2str( p_filter->filter_simple.filter_compar[i] ) );

                typeu = p_filter->filter_simple.filter_value[i];

                values_curr += printdbtype( values_curr, field_infos[index].db_type, &typeu );

                nbfields++;
            }
            else if ( ( table == T_STRIPE_ITEMS )
                      && ( field_infos[index].db_type == DB_STRIPE_ITEMS ) )
            {
                if ( prefix_table )
                    values_curr += sprintf( values_curr, "%s.", STRIPE_ITEMS_TABLE );

                values_curr +=
                    sprintf( values_curr, "storage_item%s%u",
                             compar2str( p_filter->filter_simple.filter_compar[i] ),
                             p_filter->filter_simple.filter_value[i].val_uint );

                nbfields++;
            }
            else if ( ( table == T_STRIPE_INFO )
                      && ( field_infos[index].db_type == DB_STRIPE_INFO ) )
            {

                /* We XXX Assume that the only possible filter here is on pool_name */

                if ( prefix_table )
                    values_curr += sprintf( values_curr, "%s.", STRIPE_INFO_TABLE );

                values_curr +=
                    sprintf( values_curr, "pool_name%s'%s'",
                             compar2str( p_filter->filter_simple.filter_compar[i] ),
                             p_filter->filter_simple.filter_value[i].val_str );

                nbfields++;
            }

            if ( match )
            {

                if ( p_filter->filter_simple.filter_flags[i] & FILTER_FLAG_NOT )
                     values_curr += sprintf( values_curr, ") " );
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


/* special masks values for MAPPING TABLE */
#define MASK_ID2PK  0
#define MASK_PK2ID  1


int entry_id2pk( lmgr_t * p_mgr, const entry_id_t * p_id, int add_if_not_exists,
                 PK_PARG_T p_pk )
{
#ifndef FID_PK
#ifndef _DISABLE_PREP_STMT
    unsigned long long id1, id2;
    int            rc;
    db_type_t      input_types[2] = { DB_BIGUINT, DB_BIGUINT };
    void          *input_buffs[2] = { ( void * ) &id1, ( void * ) &id2 };

    db_type_t      output_types[1] = { DB_BIGUINT };
    void          *output_buffs[1] = { ( void * ) p_pk };

    prep_stmt_t    stmt = prep_stmt_build_or_get( p_mgr, OP_SELECT, TAB_IDMAP, MASK_ID2PK,
                                                  "SELECT id FROM " MAPPING_TABLE
                                                  " WHERE inum=? AND dev=?" );
    if ( !stmt )
        return DB_REQUEST_FAILED;

    /* bind arguments */

    id1 = p_id->inode;
    id2 = p_id->device;

    rc = db_bind_params( stmt, input_types, input_buffs, NULL, 2, TRUE );

    if ( rc == DB_CONNECT_FAILED )
        /* This error is due to a disconnection, we must invalidate all statements */
        invalidate_statements( p_mgr );

    if ( rc )
        return rc;

    /* execute statement */
    rc = db_exec_prepared( stmt, TRUE );

    if ( rc == DB_CONNECT_FAILED )
        /* This error is due to a disconnection, we must invalidate all statements */
        invalidate_statements( p_mgr );

    if ( rc )
        return rc;

    /* retrieve pk */
    rc = db_next_prepared_record( stmt, output_types, output_buffs, NULL, 1, TRUE );

    db_clean_prepared( stmt );

    if ( rc == DB_END_OF_LIST )
    {
        if ( add_if_not_exists )
        {
            prep_stmt_t    stmt_ins;
            stmt_ins =
                prep_stmt_build_or_get( p_mgr, OP_INSERT, TAB_IDMAP, 0,
                                        "INSERT INTO " MAPPING_TABLE " (inum,dev) VALUES (?,?)" );
            if ( !stmt )
                return DB_REQUEST_FAILED;

            rc = db_bind_params( stmt_ins, input_types, input_buffs, NULL, 2, TRUE );
            if ( rc )
                return rc;

            /* execute statement */
            rc = db_exec_prepared( stmt_ins, FALSE );
            if (rc)
                return rc;

            db_clean_prepared( stmt_ins );

            *p_pk =  db_last_id( &p_mgr->conn );
            return DB_SUCCESS;
        }
        else
            return DB_NOT_EXISTS;
    }
    else if ( rc )
    {
        if ( rc == DB_CONNECT_FAILED )
            /* This error is due to a disconnection, we must invalidate all statements */
            invalidate_statements( p_mgr );

        return rc;
    }

    return DB_SUCCESS;

#else /* No prepared statements */

    char           request[1024];
    char           test[256];
    char           pkval[256];
    char           pkfield[256];
    result_handle_t result;
    char          *result_val;
    int            rc;

    sprintf( request, "SELECT id FROM " MAPPING_TABLE " WHERE %s", pkfilter( test, p_id ) );

    rc = db_exec_sql( &p_mgr->conn, request, &result );
    if ( rc )
        return rc;

    rc = db_next_record( &p_mgr->conn, &result, &result_val, 1 );
    if ( rc == DB_END_OF_LIST )
    {
        if ( add_if_not_exists )
        {
            db_result_free( &p_mgr->conn, &result );

            pkvalues( pkval, p_id, FALSE );
            pkfields( pkfield, FALSE );

            /* create it ! */
            sprintf( request, "INSERT INTO " MAPPING_TABLE " (%s) VALUES (%s)", pkfield, pkval );

            rc = db_exec_sql( &p_mgr->conn, request, NULL );
            if ( rc )
                return rc;

            *p_pk = db_last_id( &p_mgr->conn );
            return DB_SUCCESS;
        }
        else
            goto free_res;
    }
    else if ( rc )
        return rc;

    if ( !sscanf( result_val, "%llu", p_pk ) )
        goto free_res;

    db_result_free( &p_mgr->conn, &result );
    return DB_SUCCESS;

  free_res:
    db_result_free( &p_mgr->conn, &result );
    return DB_NOT_EXISTS;

#endif
#else /* FID_PK */
    snprintf( p_pk, FID_LEN, DFID_NOBRACE, PFID(p_id) );
    return DB_SUCCESS;
#endif
}


int pk2entry_id( lmgr_t * p_mgr, PK_ARG_T pk, entry_id_t * p_id )
{
#ifndef FID_PK
#ifndef _DISABLE_PREP_STMT
    unsigned long long id1, id2;
    int            rc;

    db_type_t      input_types[1] = { DB_BIGUINT };
    void          *input_buffs[1] = { ( void * ) &pk };

    db_type_t      output_types[2] = { DB_BIGUINT, DB_BIGUINT };
    void          *output_buffs[2] = { ( void * ) &id1, ( void * ) &id2 };

    prep_stmt_t    stmt = prep_stmt_build_or_get( p_mgr, OP_SELECT, TAB_IDMAP, MASK_PK2ID,
                                                  "SELECT inum,dev FROM " MAPPING_TABLE
                                                  " WHERE id=?" );
    if ( !stmt )
        return DB_REQUEST_FAILED;

    /* bind arguments */
    rc = db_bind_params( stmt, input_types, input_buffs, NULL, 1, TRUE );
    if ( rc )
        return rc;

    /* execute statement */
    rc = db_exec_prepared( stmt, TRUE );

    if ( rc == DB_CONNECT_FAILED )
        /* This error is due to a disconnection, we must invalidate all statements */
        invalidate_statements( p_mgr );

    if ( rc )
        return rc;

    /* retrieve id */
    rc = db_next_prepared_record( stmt, output_types, output_buffs, NULL, 2, TRUE );

    db_clean_prepared( stmt );

    if ( rc == DB_END_OF_LIST )
        return DB_NOT_EXISTS;

    if ( rc == DB_CONNECT_FAILED )
        /* This error is due to a disconnection, we must invalidate all statements */
        invalidate_statements( p_mgr );

    if ( rc )
        return rc;

    p_id->inode = id1;
    p_id->device = id2;

    return DB_SUCCESS;

#else /* No prepared statements */

    char           fields[256];
    char           query[1024];
    result_handle_t result;
    char          *result_val[2];
    int            rc;
    unsigned long long tmpval;

    pkfields( fields, FALSE );

    sprintf( query, "SELECT %s FROM " MAPPING_TABLE " WHERE id=%llu", fields, pk );

    rc = db_exec_sql( &p_mgr->conn, query, &result );
    if ( rc )
        return rc;

    rc = db_next_record( &p_mgr->conn, &result, result_val, 2 );

    if ( rc == DB_END_OF_LIST )
        rc = DB_NOT_EXISTS;
    if ( rc )
        goto free_res;

    p_id->validator = 0;

    rc = sscanf( result_val[0], "%llu", &tmpval );
    p_id->inode = ( ino_t ) tmpval;

    rc += sscanf( result_val[1], "%llu", &tmpval );
    p_id->device = ( ino_t ) tmpval;

    if ( rc == 2 )
        rc = DB_SUCCESS;
    else
        rc = DB_REQUEST_FAILED;

  free_res:
    db_result_free( &p_mgr->conn, &result );
    return rc;
#endif
#else /* FID_PK */
    if ( sscanf( pk, SFID, RFID(p_id) ) != 3 )
        return DB_INVALID_ARG;
    else
        return DB_SUCCESS;
#endif
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
         * commit operations and result transction count
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
void ListMgr_MergeAttrSets( attr_set_t * p_target_attrset, attr_set_t * p_source_attrset )
{
    int            i;
    int            mask = 1;
    db_type_u      typeu;

    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( !( p_target_attrset->attr_mask & mask ) && ( p_source_attrset->attr_mask & mask ) )
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
    int            i;
    int            mask = 1;

    /* Free stripe count attributes */
    for ( i = 0; i < ATTR_COUNT; i++, mask <<= 1 )
    {
        if ( ( field_infos[i].db_type == DB_STRIPE_ITEMS ) && ( p_set->attr_mask & mask ) )
        {
            free_stripe_items( ( stripe_items_t * ) ( ( char * ) &p_set->attr_values +
                                                      field_infos[i].offset ) );
        }
    }
}
