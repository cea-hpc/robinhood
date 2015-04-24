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
    entry_id2pk(p_id, PTR_PK(pk));

    /* verify it exists in main table */

    sprintf( request, "SELECT id FROM " MAIN_TABLE " WHERE id="DPK, pk );

retry:
    /* execute the request */
    rc = db_exec_sql(&p_mgr->conn, request, &result);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return -rc;

    rc = db_next_record( &p_mgr->conn, &result, &str_count, 1 );
    if (rc == 0)
        rc = 1;
    else if (rc != DB_END_OF_LIST)
    {
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        rc = -rc;
    }
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
    int rc = 0;
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
        {
            ATTR_MASK_UNSET(p_attrs, dircount);
            rc = DB_SUCCESS;
        }
        else if (rc == DB_SUCCESS)
        {
            if (str_info[0] == NULL)
                /* count(*) should at least return 0 */
                rc = DB_REQUEST_FAILED;
            else
            {
                tmp_val = str2int(str_info[0]);
                if (tmp_val != -1)
                {
                    ATTR_MASK_SET(p_attrs, dircount);
                    ATTR( p_attrs, dircount ) = tmp_val;
                    rc = DB_SUCCESS;
                }
                else
                    /* invalid output format */
                    rc = DB_REQUEST_FAILED;
            }
        }
        db_result_free( &p_mgr->conn, &result );
        if (rc)
            return rc;
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
        else if (rc == DB_SUCCESS)
        {
            if (str_info[0] == NULL)
            {
                /* NULL if no entry matches the criteria */
                ATTR_MASK_UNSET(p_attrs, avgsize);
                rc = DB_SUCCESS;
            }
            else
            {
                tmp_long = str2bigint(str_info[0]);
                if (tmp_long != -1LL)
                {
                    ATTR_MASK_SET(p_attrs, avgsize);
                    ATTR( p_attrs, avgsize ) = tmp_long;
                    rc = DB_SUCCESS;
                }
                else
                    /* invalid output format */
                    rc = DB_REQUEST_FAILED;
            }
        }
        db_result_free( &p_mgr->conn, &result );
    }

    return rc;
#endif
}

/**
 *  Retrieve entry attributes from its primary key
 */
int listmgr_get_by_pk( lmgr_t * p_mgr, PK_ARG_T pk, attr_set_t * p_info )
{
    int            rc;
    char           fieldlist[4096] = "";
    char          *first_table = NULL;
    char           from[1024] = "";
    char           query[4096] = "";
    /* we assume there is not more than 128 fields */
    char          *result_tab[128];
    result_handle_t result;
    int checkmain = 1;
    int main_count = 0, annex_count = 0, name_count = 0;

    if (p_info == NULL)
        return 0;

    /* init entry info */
    memset( &p_info->attr_values, 0, sizeof( entry_info_t ) );
    fieldlist[0] = '\0';

    /* retrieve source info for generated fields */
    add_source_fields_for_gen( &p_info->attr_mask );

    /* get info from main table (if asked) */
    main_count = attrmask2fieldlist(fieldlist, p_info->attr_mask, T_MAIN, FALSE,
                                    FALSE, "", "");
    if (main_count < 0)
        return -main_count;
    else if (main_count > 0)
    {
        checkmain = 0;
        first_table = MAIN_TABLE;
        sprintf(from, MAIN_TABLE);
    }

    annex_count = attrmask2fieldlist(fieldlist + strlen(fieldlist),
                                     p_info->attr_mask, T_ANNEX,
                                     first_table != NULL, FALSE, "", "");
    if (annex_count < 0)
        return -annex_count;
    else if (annex_count > 0)
    {
        if (first_table)
            sprintf(from + strlen(from), " LEFT JOIN "ANNEX_TABLE" ON %s.id="
                    ANNEX_TABLE".id", first_table);
        else
        {
            first_table = ANNEX_TABLE;
            sprintf(from, ANNEX_TABLE);
        }
    }

    name_count = attrmask2fieldlist(fieldlist + strlen(fieldlist),
                                    p_info->attr_mask, T_DNAMES,
                                    first_table != NULL, FALSE, "", "");
    if (name_count < 0)
        return -name_count;
    else if (name_count > 0)
    {
        if (first_table)
            /* it's OK to JOIN with NAMES table here even if there are multiple paths,
             * as we only take one result record. The important thing is to return
             * consistent values for parent_id, name and fullpath. */
            sprintf(from + strlen(from), " LEFT JOIN "DNAMES_TABLE" ON %s.id="
                    DNAMES_TABLE".id", first_table);
        else
        {
            first_table = DNAMES_TABLE;
            sprintf(from, DNAMES_TABLE);
        }
    }

    if (first_table != NULL)
    {
        int shift = 0;
        sprintf(query, "SELECT %s FROM %s WHERE %s.id="DPK, fieldlist, from,
                first_table, pk);

        rc = db_exec_sql(&p_mgr->conn, query, &result);
        if (rc)
            return rc;

        rc = db_next_record(&p_mgr->conn, &result, result_tab,
                            main_count + annex_count + name_count);
        /* END_OF_LIST means it does not exist */
        if (rc == DB_END_OF_LIST)
        {
            p_info->attr_mask &= ~(main_attr_set|annex_attr_set|names_attr_set);

            /* not found, but did not check MAIN yet */
            if (checkmain)
                goto next_table;

            rc = DB_NOT_EXISTS;
        }
        if (rc)
            goto free_res;

        /* set info from result */
        if (main_count)
        {
            rc = result2attrset(T_MAIN, result_tab + shift, main_count, p_info);
            shift += main_count;
            if (rc)
                goto free_res;
        }
        if (annex_count)
        {
            rc = result2attrset(T_ANNEX, result_tab + shift, annex_count,
                                p_info);
            shift += annex_count;
            if (rc)
                goto free_res;
        }
        if (name_count)
        {
            rc = result2attrset(T_DNAMES, result_tab + shift, name_count,
                                p_info);
            shift += name_count;
            if (rc)
                goto free_res;
        }

next_table:
        db_result_free(&p_mgr->conn, &result);
    }

    /* remove stripe info if it is not a file */
    if (stripe_fields(p_info->attr_mask) && ATTR_MASK_TEST(p_info, type)
        && strcmp(ATTR(p_info, type), STR_TYPE_FILE) != 0)
    {
        p_info->attr_mask &= ~stripe_attr_set;
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
#else
    /* POSIX: always clean stripe bits */
    p_info->attr_mask &= ~(ATTR_MASK_stripe_info | ATTR_MASK_stripe_items);
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
    int rc;
    DEF_PK(pk);

    entry_id2pk(p_id, PTR_PK(pk));
retry:
    rc = listmgr_get_by_pk(p_mgr, pk, p_info);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    return rc;
}


/* Retrieve the FID from the database given the parent FID and the file name. */
int ListMgr_Get_FID_from_Path( lmgr_t * p_mgr, const entry_id_t * parent_fid,
                               const char *name, entry_id_t * fid)
{
    result_handle_t result;
    char           query[4096];
    char           escaped[RBH_NAME_MAX*2];
    DEF_PK(pk);
    int rc;
    char            *str_info[1];

    entry_id2pk(parent_fid, PTR_PK(pk));

    db_escape_string(&p_mgr->conn, escaped, RBH_NAME_MAX*2, name);

    sprintf(query, "SELECT id FROM "DNAMES_TABLE" WHERE pkn="HNAME_FMT,
            pk, escaped);

retry:
    rc = db_exec_sql(&p_mgr->conn, query, &result);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    rc = db_next_record( &p_mgr->conn, &result, str_info, 1 );

    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc != DB_SUCCESS)
        goto free_res;

    rc = pk2entry_id(p_mgr, str_info[0], fid);

  free_res:
    db_result_free( &p_mgr->conn, &result );
    return rc;
}
