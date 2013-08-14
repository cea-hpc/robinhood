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
#include "RobinhoodLogs.h"
#include <stdio.h>
#include <stdlib.h>



int ListMgr_Insert( lmgr_t * p_mgr, entry_id_t * p_id, const attr_set_t * p_info,
                    int update_if_exists )
{
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

    entry_id2pk(p_id, PTR_PK(pk));

#ifdef _LUSTRE
    /* ======== insert stripe information, if available ========= */
    if ( ATTR_MASK_TEST( p_info, stripe_info ) && ATTR_MASK_TEST( p_info, stripe_items ) )
    {
        rc = insert_stripe_info( p_mgr, pk, VALID(p_id), &ATTR( p_info, stripe_info ),
                                 &ATTR( p_info, stripe_items ), update_if_exists );

        if ( (rc == DB_ALREADY_EXISTS) && update_if_exists )
            return ListMgr_Update( p_mgr, p_id, p_info );

        if ( rc )
        {
            lmgr_rollback( p_mgr );
            return rc;
        }
    }
#endif

    /* ========= insert info in main table ============ */

    strcpy( fields, "id" );
    sprintf( values, DPK, pk );
    fields_curr = fields + strlen( fields );
    values_curr = values + strlen( values );

    /* create field and values lists */
    attrmask2fieldlist( fields_curr, p_info->attr_mask, T_MAIN, TRUE, FALSE, "", "" );
    attrset2valuelist( p_mgr, values_curr, p_info, T_MAIN, TRUE );

    sprintf( query, "INSERT INTO " MAIN_TABLE "(%s) VALUES (%s)", fields, values );

    if ( update_if_exists )
    {
        /* no warning if we allow update */
        rc = db_exec_sql_quiet( &p_mgr->conn, query, NULL );
        if ( rc == DB_ALREADY_EXISTS )
            return ListMgr_Update( p_mgr, p_id, p_info );
    }
    else
        rc = db_exec_sql( &p_mgr->conn, query, NULL );

    if ( rc )
    {
        lmgr_rollback( p_mgr );
        DisplayLog( LVL_CRIT, LISTMGR_TAG,
                    "DB query failed in %s line %d: pk="DPK", code=%d: %s",
                    __FUNCTION__, __LINE__, pk, rc, db_errmsg( &p_mgr->conn, query, 4096 ) );
        return rc;
    }

    /* allow inserting entries in MAIN_TABLE, without name information */
    if (ATTR_MASK_TEST(p_info, name) && ATTR_MASK_TEST(p_info, parent_id))
    {
        /* Insert into names */
        strcpy( fields, "id" );
        sprintf( values, DPK, pk );
        fields_curr = fields + strlen( fields );
        values_curr = values + strlen( values );

        /* create field and values lists */
        attrmask2fieldlist( fields_curr, p_info->attr_mask, T_DNAMES, TRUE, FALSE, "", "" );
        attrset2valuelist( p_mgr, values_curr, p_info, T_DNAMES, TRUE );

        // FIXME this update operation may zero column content if some values are not specified
        static const char set[] = "id=VALUES(id), parent_id=VALUES(parent_id), name=VALUES(name), "
                                  "pkn="HNAME_DEF", path_update=VALUES(path_update)";
        sprintf(query, "INSERT INTO " DNAMES_TABLE "(%s, pkn) VALUES (%s, "HNAME_DEF") "
                       "ON DUPLICATE KEY UPDATE %s", fields, values, set);

        rc = db_exec_sql( &p_mgr->conn, query, NULL );

        if ( rc )
        {
            lmgr_rollback( p_mgr );
            DisplayLog( LVL_CRIT, LISTMGR_TAG,
                        "DB query failed in %s line %d: pk="DPK", code=%d: %s",
                        __FUNCTION__, __LINE__, pk, rc, db_errmsg( &p_mgr->conn, query, 4096 ) );
            return rc;
        }
    } else {
        DEF_PK(ppk);

        entry_id2pk(& ATTR(p_info, parent_id ), PTR_PK(ppk));

        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "WARNING: entry "DPK" created "
                   "without name ('%s') or parent ("DPK") information", pk,
                   ATTR_MASK_TEST(p_info, name) ? ATTR(p_info, name ) : "",
                   ATTR_MASK_TEST(p_info, parent_id) ? ppk : "");
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
        if ( attrmask2fieldlist( fields_curr, p_info->attr_mask, T_ANNEX, TRUE, FALSE, "", "" ) > 0 )
        {

            attrset2valuelist( p_mgr, values_curr, p_info, T_ANNEX, TRUE );

            sprintf( query, "INSERT INTO " ANNEX_TABLE "(%s) VALUES (%s)", fields, values );

            if ( update_if_exists )
            {
                /* no warning if we allow update */
                rc = db_exec_sql_quiet( &p_mgr->conn, query, NULL );
                if ( rc == DB_ALREADY_EXISTS )
                    return ListMgr_Update( p_mgr, p_id, p_info );
            }
            else
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


    rc = lmgr_commit( p_mgr );

    /* success, count it */
    if (!rc)
        p_mgr->nbop[OPIDX_INSERT]++;
    return rc;
}
