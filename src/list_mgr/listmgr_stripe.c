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

#include "listmgr_stripe.h"
#include "database.h"
#include "listmgr_common.h"
#include "listmgr_prep_stmt.h"
#include "Memory.h"
#include "RobinhoodLogs.h"
#include <stdio.h>
#include <stdlib.h>


/* special masks for prepared statements on STRIPE_INFO_TABLE */
#define VALIDATOR_MASK      0X00000001
#define STRIPE_COUNT_MASK   0X00000002
#define STRIPE_SIZE_MASK    0X00000004


int delete_stipe_info( lmgr_t * p_mgr, PK_ARG_T pk )
{
    int            rc;
    char           query[4096];

    sprintf( query, "DELETE FROM " STRIPE_ITEMS_TABLE " WHERE id="DPK, pk );

    rc = db_exec_sql( &p_mgr->conn, query, NULL );
    if ( rc )
        return rc;

    sprintf( query, "DELETE FROM " STRIPE_INFO_TABLE " WHERE id="DPK, pk );

    rc = db_exec_sql( &p_mgr->conn, query, NULL );
    if ( rc )
        return rc;

    return 0;
}


int insert_stripe_info( lmgr_t * p_mgr, PK_ARG_T pk, 
                        int validator, const stripe_info_t * p_stripe,
                        const stripe_items_t * p_items )
{
#ifdef _ENABLE_PREP_STMT      /* ---- prepared statements enabled ----- */
    int            rc;
    int            i;
    char           msg[1024];

    db_type_t      input_types[5] = { PK_DB_TYPE, DB_UINT, DB_UINT, DB_BIGUINT, DB_TEXT };
    void          *input_buffs[5] = { ( void * ) PTR_PK(pk), ( void * ) &validator,
        ( void * ) ( &p_stripe->stripe_count ),
        ( void * ) &( p_stripe->stripe_size ),
        ( void * ) p_stripe->pool_name
    };
    size_t         input_sizes[5] = { PK_LEN, 0, 0, 0, MAX_POOL_LEN };

    prep_stmt_t    stmt = NULL;
    int            created = FALSE;

    /* get a prepared statement for this request */
    stmt = prep_stmt_build_or_get( p_mgr, OP_INSERT, TAB_STRIPE_INFO,
                                   STRIPE_COUNT_MASK | STRIPE_SIZE_MASK | VALIDATOR_MASK,
                                   "INSERT INTO " STRIPE_INFO_TABLE
                                   "(id,validator, stripe_count,stripe_size, pool_name) "
                                   "VALUES (?,?,?,?,?)" );
    if ( !stmt )
        return DB_REQUEST_FAILED;

    /* bind arguments */
    rc = db_bind_params( stmt, input_types, input_buffs, input_sizes, 5, TRUE );
    if ( rc )
        return rc;

    do
    {
        /* execute statement (no result to fetch) */
        rc = db_exec_prepared( stmt, FALSE );

        if ( rc == 0 )
            created = TRUE;
        else if ( rc == DB_ALREADY_EXISTS )
        {
            /* remove previous stripe info */
            DisplayLog( LVL_FULL, LISTMGR_TAG,
                        "A stripe info already exists with this identifier, removing it" );
            rc = delete_stipe_info( p_mgr, pk );
        }

        /* invalidate statements if connection to db is lost */
        if ( rc == DB_CONNECT_FAILED )
        {
            invalidate_statements( p_mgr );
        }
        else if ( rc != 0 )     /* other error */
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "DB query failed in %s line %d: code=%d: %s",
                        __FUNCTION__, __LINE__, rc, db_errmsg( &p_mgr->conn, msg, 1024 ) );
            return rc;
        }

    }
    while ( !created );

    db_clean_prepared( stmt );

    if ( p_stripe->stripe_count )
    {
        /* 1 prepared statement for each stripe count (mask=stripe count) */
        stmt = prep_stmt_get( p_mgr, OP_INSERT, TAB_STRIPE_ITEMS, p_stripe->stripe_count );

        /* if it does not exist, build a request */
        if ( stmt == NULL )
        {
            ssize_t        len;
            char           query[4096];

            strcpy( query, "INSERT INTO " STRIPE_ITEMS_TABLE "(id, storage_item) VALUES " );
            len = strlen( query );

            /* first item */
            strcpy( query + len, "(?,?)" );
            len += 5;

            /* next items */
            for ( i = 1; i < p_stripe->stripe_count; i++ )
            {
                if ( len + 6 >= 4096 )
                {
                    DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                "Error in %s: query too long (>4096 bytes long)", __FUNCTION__ );
                    return DB_BUFFER_TOO_SMALL;
                }
                strcpy( query + len, ",(?,?)" );
                len += 6;
            }

#ifdef _DEBUG_DB
            DisplayLog( LVL_FULL, LISTMGR_TAG,
                        "Prepared statement cache miss for request (stripe_count=%u): %s",
                        p_stripe->stripe_count, query );
#endif
            stmt = db_create_prepared( &p_mgr->conn, query );

            if ( !stmt )
                return DB_REQUEST_FAILED;

            /* insert the statement to the cache */
            rc = prep_stmt_insert( p_mgr, OP_INSERT, TAB_STRIPE_ITEMS, p_stripe->stripe_count,
                                   stmt );
            if ( rc )
            {
                db_destroy_prepared( stmt );
                return rc;
            }

        }                       /* statement now created */

        /* bind all stripe items */
        for ( i = 0; i < p_items->count; i++ )
        {
            /* bind id at index 2i */
            rc = db_bind_param( stmt, 2 * i, PK_DB_TYPE, PTR_PK(pk), PK_LEN );

            if ( rc )
                goto clean_stmt;

            /* bind storage id at index 2i+1 */
            rc = db_bind_param( stmt, 2 * i + 1, DB_UINT, &p_items->stripe_units[i], 0 );

            if ( rc )
                goto clean_stmt;
        }

        /* finalize binding */
        rc = db_bind_params( stmt, NULL, NULL, NULL, 0, TRUE );
        if ( rc )
            goto clean_stmt;

        /* execute the statement */
        rc = db_exec_prepared( stmt, FALSE );

        /* invalidate statements if connection to db is lost */
        if ( rc == DB_CONNECT_FAILED )
            invalidate_statements( p_mgr );

        if ( rc )
            goto clean_stmt;

        /* clean the statement */
        db_clean_prepared( stmt );


    }                           /* end if stripe_count > 0 */

    return DB_SUCCESS;

  clean_stmt:
    db_clean_prepared( stmt );
    return rc;

#else /* ---- prepared statements disabled ---- */
    int            i, rc;
    char           query[4096];
    int            created = FALSE;

    do
    {

        /* First insert info into STRIPE_INFO_TABLE,
         * so if a file is already present with the same id,
         * we will remove its previous stripe info */

        sprintf( query, "INSERT INTO " STRIPE_INFO_TABLE
                 "(id,validator, stripe_count,stripe_size,pool_name) "
                 "VALUES ("DPK",%u,%u,%u,'%s')", pk, validator,
                 p_stripe->stripe_count, ( unsigned int ) p_stripe->stripe_size,
                 p_stripe->pool_name );

        rc = db_exec_sql( &p_mgr->conn, query, NULL );

        if ( rc == 0 )
        {
            created = TRUE;
        }
        else if ( rc == DB_ALREADY_EXISTS )
        {
            /* remove previous stripe info */
            DisplayLog( LVL_FULL, LISTMGR_TAG,
                        "A stripe info already exists with this identifier, removing it" );
            rc = delete_stipe_info( p_mgr, pk );
        }

        if ( rc != 0 )
        {
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "DB query failed in %s line %d: code=%d: %s",
                        __FUNCTION__, __LINE__, rc, db_errmsg( &p_mgr->conn, query, 4096 ) );
            return rc;
        }

    }
    while ( !created );         /* retry loop in case a similar entry already exists */

    /* then insert stripe items */
    if ( p_items->count > 0 )
    {
        ssize_t        len;

        strcpy( query, "INSERT INTO " STRIPE_ITEMS_TABLE "(id, storage_item) VALUES " );
        len = strlen( query );

        /* first stripe item */
        len += snprintf( query + len, 4096 - len, "("DPK",%u)", pk, p_items->stripe_units[0] );

        /* next items */
        for ( i = 1; i < p_items->count; i++ )
        {
            len += snprintf( query + len, 4096 - len, ",("DPK",%u)", pk, p_items->stripe_units[i] );

            if ( len >= 4096 )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Error in %s(): query too long (>4096 bytes long)", __FUNCTION__ );
                return DB_BUFFER_TOO_SMALL;
            }
        }
    }

    rc = db_exec_sql( &p_mgr->conn, query, NULL );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "DB query failed in %s line %d: code=%d: %s",
                    __FUNCTION__, __LINE__, rc, db_errmsg( &p_mgr->conn, query, 4096 ) );
        return rc;
    }

    return 0;
#endif
}


int get_stripe_info( lmgr_t * p_mgr, PK_ARG_T pk, stripe_info_t * p_stripe_info,
                     stripe_items_t * p_items )
{
#ifdef _ENABLE_PREP_STMT      /* ----- prepared statements enabled ----- */
    int            rc;

    db_type_t      input_types[1] = { PK_DB_TYPE };
    void          *input_buffs[1] = { ( void * ) PTR_PK(pk) };
    size_t         input_sizes[1] = { PK_LEN };

    db_type_t      output_types[3] = { DB_UINT, DB_BIGUINT, DB_TEXT };
    void          *output_buffs[3] = { ( void * ) &( p_stripe_info->stripe_count ),
        ( void * ) &( p_stripe_info->stripe_size ),
        ( void * ) p_stripe_info->pool_name
    };
    size_t         output_sz[3] = { 0, 0, MAX_POOL_LEN };

    storage_unit_id_t storage_id;
    db_type_t      storitem_out_type[1] = { DB_UINT };
    void          *storitem_out[1] = { ( void * ) &storage_id };

    prep_stmt_t    stmt = NULL;

    /* get a prepared statement for this request */
    stmt =
        prep_stmt_build_or_get( p_mgr, OP_SELECT, TAB_STRIPE_INFO,
                                STRIPE_COUNT_MASK | STRIPE_SIZE_MASK,
                                "SELECT stripe_count, stripe_size, pool_name FROM "
                                STRIPE_INFO_TABLE " WHERE id=?" );
    if ( !stmt )
        return DB_REQUEST_FAILED;

    /* bind arguments */
    rc = db_bind_params( stmt, input_types, input_buffs, input_sizes, 1, TRUE );
    if ( rc )
        return rc;

    /* execute statement */
    rc = db_exec_prepared( stmt, TRUE );

    /* invalidate statements if connection to db is lost */
    if ( rc == DB_CONNECT_FAILED )
        invalidate_statements( p_mgr );

    if ( rc )
        return rc;

    /* retrieve stripe parameters */
    rc = db_next_prepared_record( stmt, output_types, output_buffs, output_sz, 3, TRUE );

    db_clean_prepared( stmt );

    /* invalidate statements if connection to db is lost */
    if ( rc == DB_CONNECT_FAILED )
        invalidate_statements( p_mgr );

    if ( rc == DB_END_OF_LIST )
        return DB_NOT_EXISTS;
    else if ( rc )
        return rc;

    if ( p_items )
    {
        if ( p_stripe_info->stripe_count > 0 )
        {
            int            i;

            p_items->count = p_stripe_info->stripe_count;

            /* alloc stripe array */
            p_items->stripe_units = MemCalloc( p_items->count, sizeof( storage_unit_id_t ) );
            if ( p_items->stripe_units == NULL )
                return DB_NO_MEMORY;

            stmt = prep_stmt_build_or_get( p_mgr, OP_SELECT, TAB_STRIPE_ITEMS, 0,       /* should be the only select request on this table */
                                           "SELECT storage_item FROM " STRIPE_ITEMS_TABLE
                                           " WHERE id=?" );
            if ( !stmt )
                return DB_REQUEST_FAILED;

            /* bind arguments */
            rc = db_bind_params( stmt, input_types, input_buffs, input_sizes, 1, TRUE );
            if ( rc )
                return rc;

            /* execute statement */
            rc = db_exec_prepared( stmt, TRUE );

            /* invalidate statements if connection to db is lost */
            if ( rc == DB_CONNECT_FAILED )
                invalidate_statements( p_mgr );

            if ( rc )
                return rc;


            for ( i = 0; i < p_items->count; i++ )
            {
                /* db_next_prepared_record() is called several times here with the same output buffer
                 * buffer changes only at the first loop (i==0).
                 */

                rc = db_next_prepared_record( stmt, storitem_out_type, storitem_out, NULL, 1,
                                              ( i == 0 ) );

                /* invalidate statements if connection to db is lost */
                if ( rc == DB_CONNECT_FAILED )
                    invalidate_statements( p_mgr );

                if ( rc )
                {
                    DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                "Error in %s: only %u stripe units found/%u expected.",
                                __FUNCTION__, i, p_items->count );
                    p_items->count = i;
                    break;
                }

                p_items->stripe_units[i] = storage_id;
            }

            db_clean_prepared( stmt );
        }
        else
        {
            p_items->count= 0;
            p_items->stripe_units = NULL;
        }
    }

    return DB_SUCCESS;


#else /* ---------- prepared statements disabled -------------- */

    char           query[1024];
    char          *res[3];
    result_handle_t result;
    int            i;
    int            rc = DB_SUCCESS;

    /* retrieve basic stripe info */
    sprintf( query,
             "SELECT stripe_count, stripe_size, pool_name FROM " STRIPE_INFO_TABLE " WHERE id="DPK,
             pk );

    rc = db_exec_sql( &p_mgr->conn, query, &result );
    if ( rc )
        goto out;

    rc = db_next_record( &p_mgr->conn, &result, res, 3 );
    if ( rc == DB_END_OF_LIST )
        rc = DB_NOT_EXISTS;
    if ( rc )
        goto res_free;

    if ( res[0] == NULL || res[1] == NULL || res[2] == NULL )
    {
        rc = DB_ATTR_MISSING;
        goto res_free;
    }

    p_stripe_info->stripe_count = atoi( res[0] );
    p_stripe_info->stripe_size = atoi( res[1] );
    strncpy( p_stripe_info->pool_name, res[2], MAX_POOL_LEN );

    db_result_free( &p_mgr->conn, &result );

    if ( p_items )
    {
        /* retrieve stripe list */
        sprintf( query, "SELECT storage_item FROM " STRIPE_ITEMS_TABLE " WHERE id="DPK, pk );

        rc = db_exec_sql( &p_mgr->conn, query, &result );
        if ( rc )
            goto out;

        if ( p_stripe_info->stripe_count != db_result_nb_records( &p_mgr->conn, &result ) )
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG,
                        "Warning: the number of stripe items (%d) doesn't match stripe count (%u)! (Pk="DPK")",
                        db_result_nb_records( &p_mgr->conn, &result ), p_stripe_info->stripe_count, pk );
        }
        p_items->count = db_result_nb_records( &p_mgr->conn, &result );

        if ( p_items->count > 0 )
        {

            /* allocate stripe array */
            p_items->stripe_units = MemCalloc( p_items->count, sizeof( storage_unit_id_t ) );

            if ( !p_items->stripe_units )
            {
                rc = DB_NO_MEMORY;
                goto res_free;
            }

            /* fill stripe units */
            for ( i = 0; i < p_items->count; i++ )
            {
                rc = db_next_record( &p_mgr->conn, &result, res, 1 );
                if ( rc )
                    goto stripe_free;

                if ( res[0] == NULL )
                {
                    rc = DB_ATTR_MISSING;
                    goto stripe_free;
                }

                p_items->stripe_units[i] = atoi( res[0] );
            }
        }
        else
            p_items->stripe_units = NULL;

        /* last query result must be freed */
        rc = DB_SUCCESS;
        goto res_free;
    }

    /* nothing to free */
    return DB_SUCCESS;

    stripe_free:
    MemFree( p_items->stripe_units );
    p_items->stripe_units = NULL;
    p_items->count = 0;
    p_stripe_info->stripe_count = 0;
    res_free:
    db_result_free( &p_mgr->conn, &result );
    out:
    return rc;
#endif /* no prep stmt */
}

/** release stripe information */
void free_stripe_items( stripe_items_t * p_stripe_items )
{
    if ( p_stripe_items->stripe_units )
        MemFree( p_stripe_items->stripe_units );
    p_stripe_items->stripe_units = NULL;
    p_stripe_items->count = 0;
}


/* check that validator is matching for a given entry */
int ListMgr_CheckStripe( lmgr_t * p_mgr, const entry_id_t * p_id )
{
#ifdef _ENABLE_PREP_STMT      /* prepared statements enabled */
    DEF_PK( pk );
    int            rc;
    unsigned int   validator = 0;

    db_type_t      input_types[1] = { PK_DB_TYPE };
    void          *input_buffs[1] = { ( void * ) PTR_PK(pk) };
    size_t         input_sz[1] = { PK_LEN };

    db_type_t      output_types[1] = { DB_UINT };
    void          *output_buffs[1] = { ( void * ) &validator };
    prep_stmt_t    stmt;

    /* get primary key */
    rc = entry_id2pk( p_mgr, p_id, FALSE, PTR_PK(pk) );
    if ( rc )
        return rc; 

    /* get a prepared statement for this request */
    stmt = prep_stmt_build_or_get( p_mgr, OP_SELECT, TAB_STRIPE_INFO, VALIDATOR_MASK,
                                   "SELECT validator FROM " STRIPE_INFO_TABLE " WHERE id=?" );
    if ( !stmt )
        return DB_REQUEST_FAILED;

    /* bind arguments */
    rc = db_bind_params( stmt, input_types, input_buffs, input_sz, 1, TRUE );
    if ( rc )
        return rc;

    /* execute statement */
    rc = db_exec_prepared( stmt, TRUE );

    /* invalidate statements if connection to db is lost */
    if ( rc == DB_CONNECT_FAILED )
        invalidate_statements( p_mgr );

    if ( rc )
        return rc;

    /* retrieve validator */
    rc = db_next_prepared_record( stmt, output_types, output_buffs, NULL, 1, TRUE );

    db_clean_prepared( stmt );

    /* invalidate statements if connection to db is lost */
    if ( rc == DB_CONNECT_FAILED )
        invalidate_statements( p_mgr );

    if ( rc == DB_END_OF_LIST )
        return DB_NOT_EXISTS;
    else if ( rc )
        return rc;

    /* final result */
    if ( validator != VALID(p_id) )
    {
        delete_stipe_info( p_mgr, pk );
        return DB_OUT_OF_DATE;
    }
    else
        return DB_SUCCESS;


#else /* ----------- no prepared statements ------------ */

    char           query[1024];
    char          *res;
    result_handle_t result;
    int            rc = DB_SUCCESS;
    DEF_PK(pk);

    rc = entry_id2pk( p_mgr, p_id, FALSE, PTR_PK(pk) );
    if (rc)
        return rc;

    sprintf( query, "SELECT validator FROM " STRIPE_INFO_TABLE " WHERE id="DPK, pk );

    rc = db_exec_sql( &p_mgr->conn, query, &result );
    if ( rc )
        goto out;

    rc = db_next_record( &p_mgr->conn, &result, &res, 1 );

    if ( rc == DB_END_OF_LIST )
        rc = DB_NOT_EXISTS;

    if ( rc )
        goto res_free;

    if ( res == NULL )
    {
        rc = DB_ATTR_MISSING;
        goto res_free;
    }

    if ( atoi( res ) != VALID(p_id) )
    {
        delete_stipe_info( p_mgr, pk );
        rc = DB_OUT_OF_DATE;
    }
    else
        rc = DB_SUCCESS;

  res_free:
    db_result_free( &p_mgr->conn, &result );
  out:
    return rc;
#endif
}


int ListMgr_SetStripe( lmgr_t * p_mgr, const entry_id_t * p_id,
                       stripe_info_t * p_stripe_info, stripe_items_t * p_stripe_items )
{
    DEF_PK(pk);

    int rc = entry_id2pk( p_mgr, p_id, FALSE, PTR_PK(pk) );

    if (rc)
        return rc;

    return insert_stripe_info( p_mgr, pk, VALID(p_id), p_stripe_info, p_stripe_items );
}
