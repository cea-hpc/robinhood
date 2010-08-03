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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "listmgr_internal.h"
#include "listmgr_common.h"
#include "listmgr_stripe.h"
#include "listmgr_prep_stmt.h"
#include "database.h"
#include "RobinhoodLogs.h"

#include <stdio.h>
#include <stdlib.h>


int ListMgr_Exists( lmgr_t * p_mgr, const entry_id_t * p_id )
{
#ifndef _DISABLE_PREP_STMT
    int            rc;
    unsigned int   count = 0;
    DEF_PK(pk);
    db_type_t      input_types[1] = { PK_DB_TYPE };
    void          *input_buffs[1] = { ( void * ) PTR_PK(pk) };

    db_type_t      output_types[1] = { DB_UINT };
    void          *output_buffs[1] = { ( void * ) &count };
    prep_stmt_t    stmt;

    /* retrieve primary key */
    rc = entry_id2pk( p_mgr, p_id, FALSE, PTR_PK(pk) );
    if ( rc == DB_NOT_EXISTS )
        return 0;
    else if (rc)
        return -rc;


    /* get a prepared statement for this request */
    stmt = prep_stmt_build_or_get( p_mgr, OP_SELECT, TAB_MAIN,
                                   /* Only count, no attrs => mask 0.
                                    * /!\ make sure this mask is not used for
                                    * another request on the same table. */
                                   0, "SELECT COUNT(*) FROM " MAIN_TABLE " WHERE id=?" );
    if ( !stmt )
        return -DB_REQUEST_FAILED;

    /* bind arguments */
    rc = db_bind_params( stmt, input_types, input_buffs, NULL, 1, TRUE );

    if ( rc == DB_CONNECT_FAILED )
        /* This error is due to a disconnection, we must invalidate all statements */
        invalidate_statements( p_mgr );

    if ( rc )
        return -rc;

    /* execute statement */
    rc = db_exec_prepared( stmt, TRUE );

    if ( rc == DB_CONNECT_FAILED )
        /* This error is due to a disconnection, we must invalidate all statements */
        invalidate_statements( p_mgr );

    if ( rc )
        return -rc;

    /* retrieve count */
    rc = db_next_prepared_record( stmt, output_types, output_buffs, NULL, 1, TRUE );

    db_clean_prepared( stmt );

    if ( rc == DB_CONNECT_FAILED )
        /* This error is due to a disconnection, we must invalidate all statements */
        invalidate_statements( p_mgr );

    if ( rc )
        return -rc;

    /* result */
    if ( count > 0 )
        return TRUE;
    else
        return FALSE;


#else /* ----- No prepared statements ------ */

    char           request[4096];
    int            rc;
    result_handle_t result;
    char          *str_count = NULL;
    DEF_PK( pk );

    /* retrieve primary key */
    rc = entry_id2pk( p_mgr, p_id, FALSE, PTR_PK(pk) );
    if ( rc == DB_NOT_EXISTS )
        return 0;
    else if (rc)
        return -rc;

    /* verify it exists in main table */

    sprintf( request, "SELECT COUNT(*) FROM " MAIN_TABLE " WHERE id="DPK, pk );

    /* execute the request */
    rc = db_exec_sql( &p_mgr->conn, request, &result );
    if ( rc )
        return -rc;

    rc = db_next_record( &p_mgr->conn, &result, &str_count, 1 );
    if ( rc )
        return -rc;
    if ( str_count == NULL )
        return -1;

#ifdef _DEBUG_DB
    DisplayLog( LVL_FULL, LISTMGR_TAG, "in %s: str_count=%s\n", __FUNCTION__, str_count );
#endif

    /* result */
    if ( atoi( str_count ) > 0 )
        rc = 1;
    else
        rc = 0;

    db_result_free( &p_mgr->conn, &result );
    return rc;
#endif
}

/**
 *  Retrieve entry attributes from its primary key
 */
int listmgr_get_by_pk( lmgr_t * p_mgr, PK_ARG_T pk, attr_set_t * p_info )
{
#ifndef _DISABLE_PREP_STMT
    prep_stmt_t    stmt = NULL;
    int            field_count, rc;
    db_type_t      input_types[1] = { PK_DB_TYPE };
    void          *input_buffs[1] = { ( void * ) PTR_PK(pk) };

    /* max size array, to avoid alloc/free */
    db_type_t      output_types[ATTR_COUNT];
    void          *output_ptrs[ATTR_COUNT];
    size_t         output_sizes[ATTR_COUNT];

    /* retrieve source info for generated fields */
    add_source_fields_for_gen( &p_info->attr_mask );

    /* get attributes from main table */
    if ( main_fields( p_info->attr_mask ) )
    {
        /* try to retrieve a prepared statement */
        stmt = prep_stmt_get( p_mgr, OP_SELECT, TAB_MAIN,
                              main_fields( p_info->attr_mask ) );

        /* if it does not exist, build a request */
        if ( stmt == NULL )
        {
            char           fields[2048];
            char           query[4096];

            /* build the request */
            field_count =
                attrmask2fieldlist( fields, main_fields( p_info->attr_mask ), T_MAIN, FALSE,
                                    FALSE );
            if ( field_count < 0 )
                return -field_count;

            sprintf( query, "SELECT %s FROM " MAIN_TABLE " WHERE id=?", fields );

#ifdef _DEBUG_DB
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Prepared statement cache miss for request: %s",
                        query );
#endif
            stmt = db_create_prepared( &p_mgr->conn, query );

            if ( !stmt )
                return DB_REQUEST_FAILED;

            if ( prep_stmt_insert( p_mgr, OP_SELECT, TAB_MAIN,
                                   main_fields( p_info->attr_mask ), stmt ) )
            {
                db_destroy_prepared( stmt );
                return DB_REQUEST_FAILED;
            }
        }

        /* finalize binding */
        rc = db_bind_params( stmt, input_types, input_buffs, NULL, 1, TRUE );
        if ( rc )
            return rc;

        /* execute the statement */
        rc = db_exec_prepared( stmt, TRUE );

        if ( rc == DB_CONNECT_FAILED )
            /* This error is due to a disconnection, we must invalidate all statements */
            invalidate_statements( p_mgr );

        if ( rc )
            return rc;

        /* bind output values */

        field_count = 0;

        if ( main_fields( p_info->attr_mask ) )
        {
            field_count =
                mk_result_bind_list( p_info, T_MAIN, output_types, output_ptrs, output_sizes );
#ifdef _DEBUG_DB
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Fields in main table: %d", field_count );
#endif
            if ( field_count < 0 )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG, "An error occured when binding fields" );
                rc = -rc;
                goto clean_stmt;
            }
        }

#ifdef _DEBUG_DB
        DisplayLog( LVL_FULL, LISTMGR_TAG, "Total field count in main table: %d", field_count );
#endif
        /* retrieve attributes */
        rc = db_next_prepared_record( stmt, output_types, output_ptrs, output_sizes,
                                      field_count, TRUE );
        if ( rc == 0 )
            unset_null_results( p_info, T_MAIN, stmt, 0 );

        db_clean_prepared( stmt );

        if ( rc == DB_END_OF_LIST )
            return DB_NOT_EXISTS;

        if ( rc == DB_CONNECT_FAILED )
            /* This error is due to a disconnection, we must invalidate all statements */
            invalidate_statements( p_mgr );

        if ( rc )
            return rc;

    }                           /* end of request on main table */


    /* now do the same on annex table */
    if ( annex_fields( p_info->attr_mask ) )
    {
        /* try to retrieve a prepared statement */
        stmt = prep_stmt_get( p_mgr, OP_SELECT, TAB_ANNEX,
                              annex_fields( p_info->attr_mask ) );

        /* if it does not exist, build a request */
        if ( stmt == NULL )
        {
            char           fields[2048];
            char           query[4096];

            /* build the request */
            field_count =
                attrmask2fieldlist( fields, annex_fields( p_info->attr_mask ), T_ANNEX, FALSE,
                                    FALSE );
            if ( field_count < 0 )
                return -field_count;

            sprintf( query, "SELECT %s FROM " ANNEX_TABLE " WHERE id=?", fields );

#ifdef _DEBUG_DB
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Prepared statement cache miss for request: %s",
                        query );
#endif

            stmt = db_create_prepared( &p_mgr->conn, query );

            if ( !stmt )
                return DB_REQUEST_FAILED;

            if ( prep_stmt_insert( p_mgr, OP_SELECT, TAB_ANNEX,
                                   annex_fields( p_info->attr_mask ),
                                   stmt ) )
            {
                db_destroy_prepared( stmt );
                return DB_REQUEST_FAILED;
            }
        }

        /* finalize binding */
        rc = db_bind_params( stmt, input_types, input_buffs, NULL, 1, TRUE );
        if ( rc )
            return rc;

        /* execute the statement */
        rc = db_exec_prepared( stmt, TRUE );

        if ( rc == DB_CONNECT_FAILED )
            /* This error is due to a disconnection, we must invalidate all statements */
            invalidate_statements( p_mgr );

        if ( rc )
            return rc;

        /* bind output values */
        field_count = 0;

        if ( annex_fields( p_info->attr_mask ) )
        {
            field_count =
                mk_result_bind_list( p_info, T_ANNEX, output_types, output_ptrs, output_sizes );
            if ( field_count < 0 )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG, "An error occured when binding fields" );
                rc = -rc;
                goto clean_stmt;
            }
        }

        /* retrieve attributes */
        rc = db_next_prepared_record( stmt, output_types, output_ptrs, output_sizes,
                                      field_count, TRUE );
        if ( rc == 0 )
            unset_null_results( p_info, T_ANNEX, stmt, 0 );

        db_clean_prepared( stmt );

        if ( rc == DB_END_OF_LIST )
            return DB_END_OF_LIST;

        if ( rc == DB_CONNECT_FAILED )
            /* This error is due to a disconnection, we must invalidate all statements */
            invalidate_statements( p_mgr );

        if ( rc )
            return rc;


    }                           /* end of request on annex table */

    /* get stripe info if asked */
    if ( ATTR_MASK_TEST( p_info, stripe_info ) || ATTR_MASK_TEST( p_info, stripe_items ) )
    {
        rc = get_stripe_info( p_mgr, pk, &ATTR( p_info, stripe_info ),
                              ATTR_MASK_TEST( p_info, stripe_items ) ? &ATTR( p_info,
                                                                              stripe_items ) :
                              NULL );
        if ( rc == DB_ATTR_MISSING || rc == DB_NOT_EXISTS )
        {
            p_info->attr_mask &= ~ATTR_MASK_stripe_info;

            if ( ATTR_MASK_TEST( p_info, stripe_items ) )
                p_info->attr_mask &= ~ATTR_MASK_stripe_items;
        }
        else if ( rc )
            return rc;
    }

    /* compute generated fields if asked */
    generate_fields( p_info );

    return DB_SUCCESS;

  clean_stmt:
    db_clean_prepared( stmt );
    return rc;


#else /* ------- no prepared statement ---------- */

    int            count, rc;
    char           fieldlist[1024];
    char           query[4096];
    /* we assume there is not more than 128 fields */
    char          *result_tab[128];
    result_handle_t result;

    /* init entry info */
    memset( &p_info->attr_values, 0, sizeof( entry_info_t ) );
    fieldlist[0] = '\0';

    /* retrieve source info for generated fields */
    add_source_fields_for_gen( &p_info->attr_mask );

    /* get info from main table (if asked) */
    count = attrmask2fieldlist( fieldlist, p_info->attr_mask, T_MAIN, FALSE, FALSE );
    if ( count < 0 )
        return -count;

    if ( count > 0 )
    {
        sprintf( query, "SELECT %s FROM " MAIN_TABLE " WHERE id="DPK, fieldlist, pk );
        rc = db_exec_sql( &p_mgr->conn, query, &result );
        if ( rc )
            return rc;

        rc = db_next_record( &p_mgr->conn, &result, result_tab, count );
        if ( rc == DB_END_OF_LIST )
        {
            rc = DB_NOT_EXISTS;
            goto free_res;
        }

        /* set info from result */
        rc = result2attrset( T_MAIN, result_tab, count, p_info );
        if ( rc )
            goto free_res;

        db_result_free( &p_mgr->conn, &result );
    }


    if ( annex_table )
    {
        /* get annex info (if asked) */
        count = attrmask2fieldlist( fieldlist, p_info->attr_mask, T_ANNEX, FALSE, FALSE );
        if ( count < 0 )
            return -count;

        if ( count > 0 )
        {
            sprintf( query, "SELECT %s FROM " ANNEX_TABLE " WHERE id="DPK, fieldlist, pk );
            rc = db_exec_sql( &p_mgr->conn, query, &result );
            if ( rc )
                return rc;

            rc = db_next_record( &p_mgr->conn, &result, result_tab, count );
            if ( rc == DB_END_OF_LIST )
            {
                rc = DB_NOT_EXISTS;
                goto free_res;
            }

            /* set info from result */
            rc = result2attrset( T_ANNEX, result_tab, count, p_info );
            if ( rc )
                goto free_res;

            db_result_free( &p_mgr->conn, &result );
        }
    }

    /* get stripe info if asked */
    if ( ATTR_MASK_TEST( p_info, stripe_info ) || ATTR_MASK_TEST( p_info, stripe_items ) )
    {
        rc = get_stripe_info( p_mgr, pk, &ATTR( p_info, stripe_info ),
                              ATTR_MASK_TEST( p_info, stripe_items ) ? &ATTR( p_info,
                                                                              stripe_items ) :
                              NULL );
        if ( rc == DB_ATTR_MISSING || rc == DB_NOT_EXISTS )
        {
            p_info->attr_mask &= ~ATTR_MASK_stripe_info;

            if ( ATTR_MASK_TEST( p_info, stripe_items ) )
                p_info->attr_mask &= ~ATTR_MASK_stripe_items;
        }
        else if ( rc )
            return rc;
    }

    /* compute generated fields if asked */
    generate_fields( p_info );

    return DB_SUCCESS;

  free_res:
    db_result_free( &p_mgr->conn, &result );
    return rc;
#endif
}                               /* listmgr_get_by_pk */



int ListMgr_Get( lmgr_t * p_mgr, const entry_id_t * p_id, attr_set_t * p_info )
{
    DEF_PK(pk);
    int rc;

    rc = entry_id2pk( p_mgr, p_id, FALSE, PTR_PK(pk) );
    if (rc)
        return rc;

    return listmgr_get_by_pk( p_mgr, pk, p_info );

}
