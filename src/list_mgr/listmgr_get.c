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
#include "database.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"

#include <stdio.h>
#include <stdlib.h>


int ListMgr_Exists( lmgr_t * p_mgr, const entry_id_t * p_id )
{
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

    sprintf( request, "SELECT id FROM " MAIN_TABLE " WHERE id="DPK, pk );

    /* execute the request */
    rc = db_exec_sql( &p_mgr->conn, request, &result );
    if ( rc )
        return -rc;

    rc = db_next_record( &p_mgr->conn, &result, &str_count, 1 );
    if ( rc == 0 )
        rc = 1;
    else if (rc != DB_END_OF_LIST)
        rc = -rc;
    else
        rc = 0;

    db_result_free( &p_mgr->conn, &result );
    return rc;
}

/** retrieve directory attributes (nbr of entries, avg size of entries)*/
int listmgr_get_dirattrs( lmgr_t * p_mgr, PK_ARG_T dir_pk, attr_set_t * p_attrs )
{
    if (ATTR_MASK_TEST( p_attrs, type) &&  (strcmp( ATTR(p_attrs, type), STR_TYPE_DIR ) != 0))
    {
        DisplayLog( LVL_FULL, LISTMGR_TAG, "Type='%s' != 'dir' => unsetting dirattrs in attr mask",
                    ATTR(p_attrs, type) );
        p_attrs->attr_mask &= ~dir_attr_set;
        return 0;
    }
#ifdef ATTR_INDEX_dircount
    char            query[1024];
    result_handle_t result;
    char            *str_info[1];
    int rc;
    int       tmp_val;
    long long tmp_long;

    /* get child entry count from DNAMES_TABLE */

    if (ATTR_MASK_TEST(p_attrs, dircount))
    {
        sprintf( query, "SELECT %s FROM "DNAMES_TABLE" WHERE parent_id="DPK,
                 dirattr2str(ATTR_INDEX_dircount), dir_pk );
        rc = db_exec_sql( &p_mgr->conn, query, &result );
        if ( rc )
            return rc;
        rc = db_next_record( &p_mgr->conn, &result, str_info, 1 );
        if (rc == DB_END_OF_LIST)
            ATTR_MASK_UNSET(p_attrs, dircount);
        else if (rc != DB_SUCCESS)
            return rc;
        else
        {
            if (str_info[0] == NULL)
                /* count(*) should at least return 0 */
                return DB_REQUEST_FAILED;

            tmp_val = str2int(str_info[0]);
            if (tmp_val != -1)
            {
                ATTR_MASK_SET(p_attrs, dircount);
                ATTR( p_attrs, dircount ) = tmp_val;
            }
            else
                /* invalid output format */
                return DB_REQUEST_FAILED;
        }
    }

    /* get avgsize of child entries from MAIN_TABLE */
    if (ATTR_MASK_TEST(p_attrs, avgsize))
    {
        sprintf( query, "SELECT %s FROM "MAIN_TABLE" m, "DNAMES_TABLE" d WHERE m.id = d.id and type='file' and d.parent_id="DPK,
                 dirattr2str(ATTR_INDEX_avgsize), dir_pk );
        rc = db_exec_sql( &p_mgr->conn, query, &result );
        if ( rc )
            return rc;
        rc = db_next_record( &p_mgr->conn, &result, str_info, 1 );
        if (rc == DB_END_OF_LIST)
            ATTR_MASK_UNSET(p_attrs, avgsize);
        else if (rc != DB_SUCCESS)
            return rc;
        else
        {
            if (str_info[0] == NULL)
            {
                /* NULL if no entry matches the criteria */
                ATTR_MASK_UNSET(p_attrs, avgsize);
                return DB_SUCCESS;
            }

            tmp_long = str2bigint(str_info[0]);
            if (tmp_long != -1LL)
            {
                ATTR_MASK_SET(p_attrs, avgsize);
                ATTR( p_attrs, avgsize ) = tmp_long;
            }
            else
                /* invalid output format */
                return DB_REQUEST_FAILED;
        }
    }
#endif
    return DB_SUCCESS;
}

/**
 *  Retrieve entry attributes from its primary key
 */
int listmgr_get_by_pk( lmgr_t * p_mgr, PK_ARG_T pk, attr_set_t * p_info )
{
    int            count, rc;
    char           fieldlist[1024];
    char           query[4096];
    /* we assume there is not more than 128 fields */
    char          *result_tab[128];
    result_handle_t result;
    int checkmain = 1;

    if (p_info == NULL)
        return 0;

    /* init entry info */
    memset( &p_info->attr_values, 0, sizeof( entry_info_t ) );
    fieldlist[0] = '\0';

    /* retrieve source info for generated fields */
    add_source_fields_for_gen( &p_info->attr_mask );

    /* get info from main table (if asked) */
    count = attrmask2fieldlist( fieldlist, p_info->attr_mask, T_MAIN, FALSE, FALSE, "", "" );
    if ( count < 0 )
        return -count;

    if ( count > 0 )
    {
        checkmain = 0;
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

    /* remove slink attrs if it is not a symlink */
    if (slinkattr_fields(p_info->attr_mask) && ATTR_MASK_TEST(p_info, type)
        && strcmp(ATTR(p_info, type), STR_TYPE_LINK) != 0)
    {
        p_info->attr_mask &= ~slink_attr_set;
    }
    /* remove stripe info if it is not a file */
    if (stripe_fields(p_info->attr_mask) && ATTR_MASK_TEST(p_info, type)
        && strcmp(ATTR(p_info, type), STR_TYPE_FILE) != 0)
    {
        p_info->attr_mask &= ~stripe_attr_set;
    }

    /* get info from NAMES table, if needed */
    if (names_fields(p_info->attr_mask))
    {
        count = attrmask2fieldlist( fieldlist, p_info->attr_mask, T_DNAMES, FALSE, FALSE, "", "" );
        if ( count < 0 )
            return -count;

        if ( count > 0 )
        {
            sprintf( query, "SELECT %s FROM " DNAMES_TABLE " WHERE id="DPK, fieldlist, pk );
            rc = db_exec_sql( &p_mgr->conn, query, &result );
            if ( rc )
                return rc;

            rc = db_next_record( &p_mgr->conn, &result, result_tab, count );

            if (rc == 0)
                checkmain = 0; /* entry exists */

            if ( rc == DB_END_OF_LIST )
            {
                /* clear missing fields */
                rc = result2attrset( T_DNAMES, NULL, count, p_info );
                if ( rc )
                    goto free_res;
            }
            else
            {
                /* set info from result */
                rc = result2attrset( T_DNAMES, result_tab, count, p_info );
                if ( rc )
                    goto free_res;
            }

            db_result_free( &p_mgr->conn, &result );
        }
    }

    if ( annex_table )
    {
        /* get annex info (if asked) */
        count = attrmask2fieldlist( fieldlist, p_info->attr_mask, T_ANNEX, FALSE, FALSE, "", "" );
        if ( count < 0 )
            return -count;

        if ( count > 0 )
        {
            sprintf( query, "SELECT %s FROM " ANNEX_TABLE " WHERE id="DPK, fieldlist, pk );
            rc = db_exec_sql( &p_mgr->conn, query, &result );
            if ( rc )
                return rc;

            rc = db_next_record( &p_mgr->conn, &result, result_tab, count );

            if (rc == 0)
                checkmain = 0; /* entry exists */

            if ( rc == DB_END_OF_LIST )
            {
                /* clear missing fields */
                rc = result2attrset( T_ANNEX, NULL, count, p_info );
                if ( rc )
                    goto free_res;
            }
            else
            {
                /* set info from result */
                rc = result2attrset( T_ANNEX, result_tab, count, p_info );
                if ( rc )
                    goto free_res;
            }

            db_result_free( &p_mgr->conn, &result );
        }
    }

    /* get stripe info if asked */
#ifdef _LUSTRE
    if (stripe_fields( p_info->attr_mask ))
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
        else
            checkmain = 0; /* entry exists */
    }
#endif

    /* special field dircount */
    if (dirattr_fields( p_info->attr_mask ))
    {
        if (listmgr_get_dirattrs(p_mgr, pk, p_info))
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "listmgr_get_dirattrs failed for "DPK, pk );
            p_info->attr_mask &= ~dir_attr_set;
        }
    }

    if (checkmain)
    {
        /* verify it exists in main table */
        sprintf( query, "SELECT id FROM " MAIN_TABLE " WHERE id="DPK, pk );

        /* execute the request */
        rc = db_exec_sql( &p_mgr->conn, query, &result );
        if ( rc )
            return rc;

        rc = db_next_record( &p_mgr->conn, &result, result_tab, 1 );
        db_result_free( &p_mgr->conn, &result );
        if (rc)
            return DB_NOT_EXISTS;
    }

    /* compute generated fields if asked */
    generate_fields( p_info );

    p_mgr->nbop[OPIDX_GET]++;

    return DB_SUCCESS;

  free_res:
    db_result_free( &p_mgr->conn, &result );
    return rc;
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

