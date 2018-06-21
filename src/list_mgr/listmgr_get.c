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
#include "rbh_logs.h"
#include "rbh_misc.h"

#include <stdio.h>
#include <stdlib.h>


int ListMgr_Exists(lmgr_t *p_mgr, const entry_id_t *p_id)
{
    GString        *req;
    int             rc;
    result_handle_t result;
    char           *str_count = NULL;
    DEF_PK(pk);
    int             retry_status;

    /* retrieve primary key */
    entry_id2pk(p_id, PTR_PK(pk));

    /* verify it exists in main table */
    req = g_string_new("SELECT id FROM " MAIN_TABLE " WHERE id=");
    g_string_append_printf(req, DPK, pk);

retry:
    /* execute the request (must return negative value on error) */
    rc = -db_exec_sql(&p_mgr->conn, req->str, &result);
    retry_status = lmgr_delayed_retry(p_mgr, -rc);
    if (retry_status == 1)
        goto retry;
    else if (retry_status == 2) {
        rc = ECANCELED;
        goto free_str;
    } else if (rc)
        goto free_str;

    rc = db_next_record(&p_mgr->conn, &result, &str_count, 1);
    if (rc == 0)
        rc = 1; /* return 1 if entry exists */
    else if (rc != DB_END_OF_LIST)
    {
        retry_status = lmgr_delayed_retry(p_mgr, -rc);
        if (retry_status == 1)
            goto retry;
        else if (retry_status == 2) {
            rc = ECANCELED;
            goto free_result;
        }
    }
    else
        rc = 0;

free_result:
    db_result_free(&p_mgr->conn, &result);

free_str:
    g_string_free(req, TRUE);
    return rc;
}

/** retrieve directory attributes (nbr of entries, avg size of entries)*/
int listmgr_get_dirattrs( lmgr_t * p_mgr, PK_ARG_T dir_pk, attr_set_t * p_attrs )
{
    GString         *req;
    result_handle_t  result;
    char            *str_info[1];
    int              rc = 0;
    int              tmp_val;
    long long        tmp_long;

    if (ATTR_MASK_TEST(p_attrs, type) &&
        (strcmp(ATTR(p_attrs, type), STR_TYPE_DIR) != 0))
    {
        DisplayLog(LVL_FULL, LISTMGR_TAG,
                   "Type='%s' != 'dir' => unsetting dirattrs in attr mask",
                   ATTR(p_attrs, type));
        p_attrs->attr_mask = attr_mask_and_not(&p_attrs->attr_mask, &dir_attr_set);
        return 0;
    }

    req = g_string_new(NULL);

    /* get child entry count from DNAMES_TABLE */
    if (ATTR_MASK_TEST(p_attrs, dircount))
    {
        g_string_printf(req, "SELECT %s FROM "DNAMES_TABLE" WHERE parent_id="DPK,
                        dirattr2str(ATTR_INDEX_dircount), dir_pk);

        rc = db_exec_sql(&p_mgr->conn, req->str, &result);
        if (rc)
            goto free_str;

        rc = db_next_record(&p_mgr->conn, &result, str_info, 1);
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
                    ATTR(p_attrs, dircount) = tmp_val;
                    rc = DB_SUCCESS;
                }
                else
                    /* invalid output format */
                    rc = DB_REQUEST_FAILED;
            }
        }
        db_result_free(&p_mgr->conn, &result);
        if (rc)
            goto free_str;
    }

    /* get avgsize of child entries from MAIN_TABLE */
    if (ATTR_MASK_TEST(p_attrs, avgsize))
    {
        g_string_printf(req, "SELECT %s FROM "MAIN_TABLE" m, "DNAMES_TABLE" d"
                        " WHERE m.id = d.id and type='file' and d.parent_id="DPK,
                        dirattr2str(ATTR_INDEX_avgsize), dir_pk);

        rc = db_exec_sql(&p_mgr->conn, req->str, &result);
        if (rc)
            goto free_str;

        rc = db_next_record(&p_mgr->conn, &result, str_info, 1);
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
                    ATTR(p_attrs, avgsize) = tmp_long;
                    rc = DB_SUCCESS;
                }
                else
                    /* invalid output format */
                    rc = DB_REQUEST_FAILED;
            }
        }
        db_result_free(&p_mgr->conn, &result);
    }

free_str:
    g_string_free(req, TRUE);
    return rc;
}

/** only keep supported bits in the given mask */
static void supported_bits_only(attr_mask_t *p_mask)
{
    /* don't get fields that are not in main, names, annex, stripe...
     * This allows the caller to set all bits 'on' to get everything.
     */
    p_mask->std &= (main_attr_set.std | names_attr_set.std
                              | annex_attr_set.std | stripe_attr_set.std
                              | dir_attr_set.std | slink_attr_set.std);

    p_mask->status &= (main_attr_set.status | names_attr_set.status
                              | annex_attr_set.status | stripe_attr_set.status
                              | dir_attr_set.status | slink_attr_set.status);

    p_mask->sm_info &= (main_attr_set.sm_info | names_attr_set.sm_info
                              | annex_attr_set.sm_info | stripe_attr_set.sm_info
                              | dir_attr_set.sm_info | slink_attr_set.sm_info);
}

/** clean bits of attributes in main, annex and names */
static void clean_std_table_bits(attr_mask_t *p_mask)
{
    p_mask->std &= ~(main_attr_set.std | annex_attr_set.std
                               | names_attr_set.std);
    p_mask->status &= ~(main_attr_set.status | annex_attr_set.status
                               | names_attr_set.status);
    p_mask->sm_info &= ~(main_attr_set.sm_info | annex_attr_set.sm_info
                               | names_attr_set.sm_info);
}

/**
 *  Retrieve entry attributes from its primary key
 */
int listmgr_get_by_pk( lmgr_t * p_mgr, PK_ARG_T pk, attr_set_t * p_info )
{
    int             rc;
    char           *first_table = NULL;
    GString        *req, *from;
    /* attribute count is up to 1 per bit (8 per byte).
     * x2 for bullet proofing */
    char           *result_tab[2*8*sizeof(p_info->attr_mask)];
    result_handle_t result;
    bool            checkmain   = true;
    int             main_count  = 0,
                    annex_count = 0,
                    name_count  = 0;
    attr_mask_t     gen = gen_fields(p_info->attr_mask);

    if (p_info == NULL)
        return 0;

    /* init entry info */
    memset(&p_info->attr_values, 0, sizeof(entry_info_t));
    req = g_string_new("SELECT ");
    from = g_string_new(" FROM ");

    /* retrieve source info for generated fields (only about std fields)*/
    add_source_fields_for_gen(&p_info->attr_mask.std);

    /* don't get fields that are not in main, names, annex, stripe...
     * This allows the caller to set all bits 'on' to get everything.
     * Note: this also clear generated fields. They will be restored after.
     */
    supported_bits_only(&p_info->attr_mask);

    /* get info from main table (if asked) */
    main_count = attrmask2fieldlist(req, p_info->attr_mask, T_MAIN, "", "", 0);
    if (main_count < 0)
    {
        rc = -main_count;
        goto free_str;
    }
    else if (main_count > 0)
    {
        checkmain = false;
        first_table = MAIN_TABLE;
        g_string_append(from, MAIN_TABLE);
    }

    annex_count = attrmask2fieldlist(req, p_info->attr_mask, T_ANNEX, "", "",
                                     first_table != NULL ? AOF_LEADING_SEP : 0);
    if (annex_count < 0)
    {
        rc = -annex_count;
        goto free_str;
    }
    else if (annex_count > 0)
    {
        if (first_table != NULL)
            g_string_append_printf(from, " LEFT JOIN "ANNEX_TABLE" ON %s.id="
                                   ANNEX_TABLE".id", first_table);
        else
        {
            first_table = ANNEX_TABLE;
            g_string_append(from, ANNEX_TABLE);
        }
    }

    name_count = attrmask2fieldlist(req, p_info->attr_mask, T_DNAMES, "", "",
                                    first_table != NULL ? AOF_LEADING_SEP : 0);
    if (name_count < 0)
    {
        rc = -name_count;
        goto free_str;
    }
    else if (name_count > 0)
    {
        if (first_table)
            /* it's OK to JOIN with NAMES table here even if there are multiple paths,
             * as we only take one result record. The important thing is to return
             * consistent values for parent_id, name and fullpath. */
            g_string_append_printf(from, " LEFT JOIN "DNAMES_TABLE" ON %s.id="
                                   DNAMES_TABLE".id", first_table);
        else
        {
            first_table = DNAMES_TABLE;
            g_string_append(from, DNAMES_TABLE);
        }
    }

    if (first_table != NULL)
    {
        int shift = 0;

        g_string_append_printf(req, "%s WHERE %s.id="DPK, from->str,
                               first_table, pk);

        rc = db_exec_sql(&p_mgr->conn, req->str, &result);
        if (rc)
            goto free_str;

        rc = db_next_record(&p_mgr->conn, &result, result_tab,
                            main_count + annex_count + name_count);
        /* END_OF_LIST means it does not exist */
        if (rc == DB_END_OF_LIST)
        {
            clean_std_table_bits(&p_info->attr_mask);

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
        p_info->attr_mask = attr_mask_and_not(&p_info->attr_mask, &stripe_attr_set);
    }

    /* get stripe info if asked */
#ifdef _LUSTRE
    if (stripe_fields(p_info->attr_mask))
    {
        rc = get_stripe_info(p_mgr, pk, &ATTR(p_info, stripe_info),
                             ATTR_MASK_TEST(p_info, stripe_items)?
                                &ATTR(p_info, stripe_items) : NULL);
        if (rc == DB_ATTR_MISSING || rc == DB_NOT_EXISTS)
        {
            /* stripe info is in std mask */
            p_info->attr_mask.std &= ~ATTR_MASK_stripe_info;

            if (ATTR_MASK_TEST(p_info, stripe_items))
                p_info->attr_mask.std &= ~ATTR_MASK_stripe_items;
        }
        else if (rc)
            goto free_str;
        else
            checkmain = false; /* entry exists */
    }
#else
    /* POSIX: always clean stripe bits */
    p_info->attr_mask = attr_mask_and_not(&p_info->attr_mask, &stripe_attr_set);
#endif

    /* special field dircount */
    if (dirattr_fields(p_info->attr_mask))
    {
        if (listmgr_get_dirattrs(p_mgr, pk, p_info))
        {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG, "listmgr_get_dirattrs failed for "DPK, pk);
            p_info->attr_mask = attr_mask_and_not(&p_info->attr_mask, &dir_attr_set);
        }
    }

    if (checkmain)
    {
        /* verify it exists in main table */
        g_string_printf(req, "SELECT id FROM " MAIN_TABLE " WHERE id="DPK, pk);

        /* execute the request */
        rc = db_exec_sql(&p_mgr->conn, req->str, &result);
        if (rc)
            goto free_str;

        rc = db_next_record(&p_mgr->conn, &result, result_tab, 1);
        db_result_free(&p_mgr->conn, &result);
        if (rc)
        {
            rc = DB_NOT_EXISTS;
            goto free_str;
        }
    }

    /* restore generated fields in attr mask */
    p_info->attr_mask = attr_mask_or(&p_info->attr_mask, &gen);
    /* generate them */
    generate_fields(p_info);

    /* update operation stats */
    p_mgr->nbop[OPIDX_GET]++;

    rc = DB_SUCCESS;
    goto free_str;

  free_res:
    db_result_free(&p_mgr->conn, &result);
  free_str:
    g_string_free(req, TRUE);
    g_string_free(from, TRUE);
    return rc;
} /* listmgr_get_by_pk */



int ListMgr_Get( lmgr_t * p_mgr, const entry_id_t * p_id, attr_set_t * p_info )
{
    int rc;
    DEF_PK(pk);
    int retry_status;

    entry_id2pk(p_id, PTR_PK(pk));
retry:
    rc = listmgr_get_by_pk(p_mgr, pk, p_info);
    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    else if (retry_status == 2)
        rc = ECANCELED;
    return rc;
}


/* Retrieve the FID from the database given the parent FID and the file name. */
int ListMgr_Get_FID_from_Path( lmgr_t * p_mgr, const entry_id_t * parent_fid,
                               const char *name, entry_id_t * fid)
{
    result_handle_t result;
    GString        *req = NULL;
    char            escaped[RBH_NAME_MAX*2+1];
    DEF_PK(pk);
    int rc;
    char            *str_info[1];
    int             retry_status;

    entry_id2pk(parent_fid, PTR_PK(pk));

    db_escape_string(&p_mgr->conn, escaped, sizeof(escaped), name);

    req = g_string_new("SELECT id FROM "DNAMES_TABLE" WHERE pkn=");
    g_string_append_printf(req, HNAME_FMT, pk, escaped);

retry:
    rc = db_exec_sql(&p_mgr->conn, req->str, &result);
    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    else if (retry_status == 2) {
        rc = ECANCELED;
        goto free_str;
    } else if (rc)
        goto free_str;

    rc = db_next_record(&p_mgr->conn, &result, str_info, 1);

    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    else if (retry_status == 2) {
        rc = ECANCELED;
        goto free_res;
    } else if (rc != DB_SUCCESS)
        goto free_res;

    rc = pk2entry_id(p_mgr, str_info[0], fid);

free_res:
    db_result_free(&p_mgr->conn, &result);
free_str:
    g_string_free(req, TRUE);
    return rc;
}
