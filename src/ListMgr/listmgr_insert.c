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
#include "database.h"
#include "listmgr_common.h"
#include "listmgr_stripe.h"
#include "listmgr_prep_stmt.h"
#include "RobinhoodLogs.h"
#include <stdio.h>
#include <stdlib.h>



int ListMgr_Insert( lmgr_t * p_mgr, entry_id_t * p_id, const attr_set_t * p_info )
{
#ifndef _DISABLE_PREP_STMT      /* ---- prepared statements enabled ---- */
    int            rc;
    DEF_PK( pk );
    prep_stmt_t    stmt;
    int            field_count;
    char           msg[1024];

    /* read only fields in info mask? */
    if ( readonly_attr_set & p_info->attr_mask )
    {
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error: trying to insert read only values: attr_mask=%#x",
                    readonly_attr_set & p_info->attr_mask );
        return DB_INVALID_ARG;
    }


    /* We want insert operation set to be atomic */
    rc = lmgr_begin( p_mgr );
    if ( rc )
        return rc;

    rc = entry_id2pk( p_mgr, p_id, TRUE, PTR_PK(pk) );
    if (rc)
    {
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d converting entry id to primary key", rc );
        goto rollback;
    }

    /* ======== insert stripe information, if available ========= */
    if ( ATTR_MASK_TEST( p_info, stripe_info ) && ATTR_MASK_TEST( p_info, stripe_items ) )
    {
        rc = insert_stripe_info( p_mgr, pk, VALID(p_id), &ATTR( p_info, stripe_info ),
                                 &ATTR( p_info, stripe_items ) );
        if ( rc )
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d inserting stripe information to DB", rc );
            goto rollback;
        }
    }

    /* ========= insert info in main table ============ */

    /* try to retrieve a prepared statement  */
    stmt = prep_stmt_get( p_mgr, OP_INSERT, TAB_MAIN, main_fields( p_info->attr_mask ) );

    if ( stmt == NULL )
    {
        char           query[4096];
        char           fields[1024];
        char          *fields_curr;
        char           values[256];              /* small buffer (only constains '?') */
        char          *values_curr;

        /* Always insert id, even with no attributes
         * because CheckExists is based on the present in this table.
         */
        strcpy( fields, "id" );
        strcpy( values, "?" );
        fields_curr = fields + strlen( fields );
        values_curr = values + strlen( values );

        /* Create field and values lists.
         */
        if ( attrmask2fieldlist( fields_curr, p_info->attr_mask, T_MAIN, TRUE, FALSE ) > 0 )
            attrset2valuelist( values_curr, p_info, T_MAIN, TRUE, TRUE );

        snprintf( query, 4096, "INSERT INTO " MAIN_TABLE "(%s) VALUES (%s)", fields, values );

        /* create prepared statement and store it to the cache */
#ifdef _DEBUG_DB
        DisplayLog( LVL_FULL, LISTMGR_TAG, "Prepared statement cache miss for request: %s", query );
#endif
        stmt = db_create_prepared( &p_mgr->conn, query );

        if ( !stmt )
        {
            rc = DB_REQUEST_FAILED;
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error in db_create_prepared(%s)", query );
            goto rollback;
        }

        if ( prep_stmt_insert( p_mgr, OP_INSERT, TAB_MAIN,
                               main_fields( p_info->attr_mask ), stmt ) )
        {
            db_destroy_prepared( stmt );
            rc = DB_REQUEST_FAILED;
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error in prep_stmt_insert()" );
            goto rollback;
        }
    }

    /* statement is now created */

    /* bind pk */
    rc = db_bind_param( stmt, 0, PK_DB_TYPE, ( void * ) PTR_PK(pk), PK_LEN );
    if ( rc )
    {
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d in db_bind_param()", rc);
        goto clean_stmt;
    }

    /* then, bind attributes */
    field_count = prep_stmt_bind_attrs( stmt, p_info, T_MAIN, 1 );
    if ( field_count < 0 )
    {
        rc = -field_count;
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d in prep_stmt_bind_attrs()", rc);
        goto clean_stmt;
    }

    /* finalize binding */
    rc = db_bind_params( stmt, NULL, NULL, NULL, 0, TRUE );
    if ( rc )
    {
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d in final db_bind_params()", rc);
        goto clean_stmt;
    }

    /* execute the statement */
    rc = db_exec_prepared( stmt, FALSE );
    if ( rc )
    {
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d in db_exec_prepared()", rc);
        goto clean_stmt;
    }

    /* clean the statement */
    db_clean_prepared( stmt );


    /* operation on annex table */

    if ( annex_table && ( annex_fields( p_info->attr_mask ) ) )
    {
        /* try to retrieve a prepared statement  */
        stmt = prep_stmt_get( p_mgr, OP_INSERT, TAB_ANNEX, annex_fields( p_info->attr_mask ) );

        if ( stmt == NULL )
        {
            char           query[4096];
            char           fields[1024];
            char          *fields_curr;
            char           values[256];          /* small buffer (only constains '?') */
            char          *values_curr;

            strcpy( fields, "id" );
            strcpy( values, "?" );
            fields_curr = fields + strlen( fields );
            values_curr = values + strlen( values );

            /* Create field and values lists.
             */
            if ( attrmask2fieldlist( fields_curr, p_info->attr_mask, T_ANNEX, TRUE, FALSE ) <= 0 )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Warning: no field to be set in annex table whereas the attribute mask is matching..." );
                stmt = NULL;
            }
            else
            {
                /* create statement and put it to the cache */

                attrset2valuelist( values_curr, p_info, T_ANNEX, TRUE, TRUE );

                snprintf( query, 4096, "INSERT INTO " ANNEX_TABLE "(%s) VALUES (%s)", fields,
                          values );

                /* create prepared statement and store it to the cache */
#ifdef _DEBUG_DB
                DisplayLog( LVL_FULL, LISTMGR_TAG, "Prepared statement cache miss for request: %s",
                            query );
#endif
                stmt = db_create_prepared( &p_mgr->conn, query );

                if ( !stmt )
                {
                    rc = DB_REQUEST_FAILED;
                    DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error in db_create_prepared(%s)", query );
                    goto rollback;
                }

                if ( prep_stmt_insert( p_mgr, OP_INSERT, TAB_ANNEX,
                                       annex_fields( p_info->attr_mask ), stmt ) )
                {
                    db_destroy_prepared( stmt );
                    rc = DB_REQUEST_FAILED;
                    DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error in prep_stmt_insert()" );
                    goto rollback;
                }
            }
        }

        /* only do something if stmt != NULL */

        if ( stmt != NULL )
        {
            /* bind pk */
            rc = db_bind_param( stmt, 0, PK_DB_TYPE, ( void * ) PTR_PK(pk), PK_LEN );
            if ( rc )
            {
                DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d in db_bind_param()", rc );
                goto clean_stmt;
            }

            /* then, bind attributes */
            field_count = prep_stmt_bind_attrs( stmt, p_info, T_ANNEX, 1 );
            if ( field_count < 0 )
            {
                rc = -field_count;
                DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d in prep_stmt_bind_attrs()", rc );
                goto clean_stmt;
            }

            /* finalize binding */
            rc = db_bind_params( stmt, NULL, NULL, NULL, 0, TRUE );
            if ( rc )
            {
                DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d in final db_bind_params()", rc );
                goto clean_stmt;
            }

            /* execute the statement */
            rc = db_exec_prepared( stmt, FALSE );
            if ( rc )
            {
                DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error %d in db_exec_prepared()", rc );
                goto clean_stmt;
            }

            /* clean the statement */
            db_clean_prepared( stmt );
        }

    }                           /* end if something to be done in annex table */

    return lmgr_commit( p_mgr );

  clean_stmt:
    db_clean_prepared( stmt );
  rollback:
    DisplayLog( LVL_CRIT, LISTMGR_TAG,
                "DB query failed in %s: code=%d: %s",
                __FUNCTION__, rc, db_errmsg( &p_mgr->conn, msg, 1024 ) );
    lmgr_rollback( p_mgr );
    return rc;

#else /* ---- prepared statements disabled ---- */

    int            rc;
    char           query[4096];
    char           fields[1024];
    char           values[4096];
    char          *fields_curr;
    char          *values_curr;
    DEF_PK( pk );

    /* read only fields in info mask? */
    if ( readonly_attr_set & p_info->attr_mask )
    {
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error: trying to insert read only values: attr_mask=%#x",
                    readonly_attr_set & p_info->attr_mask );
        return DB_INVALID_ARG;
    }

    /* We want insert operation set to be atomic */
    rc = lmgr_begin( p_mgr );
    if ( rc )
        return rc;

    rc = entry_id2pk( p_mgr, p_id, TRUE, PTR_PK(pk) );
    if (rc)
    {
            lmgr_rollback( p_mgr );
            return rc;
    }

    /* ======== insert stripe information, if available ========= */
    if ( ATTR_MASK_TEST( p_info, stripe_info ) && ATTR_MASK_TEST( p_info, stripe_items ) )
    {
        rc = insert_stripe_info( p_mgr, pk, VALID(p_id), &ATTR( p_info, stripe_info ),
                                 &ATTR( p_info, stripe_items ) );
        if ( rc )
        {
            lmgr_rollback( p_mgr );
            return rc;
        }
    }


    /* ========= insert info in main table ============ */

    strcpy( fields, "id" );
    sprintf( values, DPK, pk );
    fields_curr = fields + strlen( fields );
    values_curr = values + strlen( values );

    /* create field and values lists */
    attrmask2fieldlist( fields_curr, p_info->attr_mask, T_MAIN, TRUE, FALSE );
    attrset2valuelist( values_curr, p_info, T_MAIN, TRUE, FALSE );

    sprintf( query, "INSERT INTO " MAIN_TABLE "(%s) VALUES (%s)", fields, values );

    rc = db_exec_sql( &p_mgr->conn, query, NULL );

    if ( rc )
    {
        lmgr_rollback( p_mgr );
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "DB query failed in %s line %d: pk="DPK", code=%d: %s",
                    __FUNCTION__, __LINE__, pk, rc, db_errmsg( &p_mgr->conn, query, 4096 ) );
        return rc;
    }


    /* insert all info in annex table, if any */

    if ( annex_table )
    {
        strcpy( fields, "id" );
        sprintf( values, DPK, pk );
        fields_curr = fields + strlen( fields );
        values_curr = values + strlen( values );

        /* Create field and values lists.
         * Do nothing if no fields are to be set.
         */
        if ( attrmask2fieldlist( fields_curr, p_info->attr_mask, T_ANNEX, TRUE, FALSE ) > 0 )
        {

            attrset2valuelist( values_curr, p_info, T_ANNEX, TRUE, FALSE );

            sprintf( query, "INSERT INTO " ANNEX_TABLE "(%s) VALUES (%s)", fields, values );

            rc = db_exec_sql( &p_mgr->conn, query, NULL );
            if ( rc )
            {
                lmgr_rollback( p_mgr );
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "DB query failed in %s line %d: code=%d: %s",
                            __FUNCTION__, __LINE__, rc, db_errmsg( &p_mgr->conn, query, 4096 ) );
                return rc;
            }
        }
    }

    return lmgr_commit( p_mgr );
#endif
}
