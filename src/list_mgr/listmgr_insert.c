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
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "Memory.h"
#include <stdio.h>
#include <stdlib.h>

static void no_name_warning(const PK_PARG_T pk, const attr_set_t *p_attrs,
                            unsigned int count)
{
    DEF_PK(ppk);
    char msg[256];

    entry_id2pk(&ATTR(p_attrs, parent_id), PTR_PK(ppk));

    if (count > 1)
        snprintf(msg, sizeof(msg), "%u entries", count);
    else
        rh_strncpy(msg, "entry", sizeof(msg));

    DisplayLog(LVL_MAJOR, LISTMGR_TAG, "WARNING: %s created without"
               " name or parent information: pk="DPK", name='%s', parent='%s'",
               msg, pk, ATTR_MASK_TEST(p_attrs, name) ? ATTR(p_attrs, name) : "",
               ATTR_MASK_TEST(p_attrs, parent_id) ? ppk : "");
}


/**
 * Check if a given entry must be inserted to a given table.
 * May display a warning in some cases.
 * @retval false if the entry must be skipped for the given table.
 * @retval true  if the entry must be inserted to the given table.
 */
static bool entry_filter(table_enum table, bool update,
                         const PK_PARG_T pk, const attr_set_t *p_attrs)
{
    switch(table)
    {
        case T_MAIN:
            /* don't set this entry if no attribute is in the table */
            if (!main_fields(p_attrs->attr_mask))
                return false;
            return true;
        case T_DNAMES:
            /* don't set this entry if parent or name is missing */
            if (!ATTR_MASK_TEST(p_attrs, name) || !ATTR_MASK_TEST(p_attrs, parent_id))
            {
                /* warn for create operations without name information */
                if (!update)
                    no_name_warning(pk, p_attrs, 1);
                return false;
            }
            return true;
        case T_ANNEX:
            if (!annex_fields(p_attrs->attr_mask))
                return false;
            return true;
        default:
            return true;
    }
}

/**
 * Build and execute a batch insert request for the given table.
 * @param full_mask     the sum of all entries attribute masks
 * @param match_mask    insert an entry in the table if it matches attrs in
 *                      this mask.
 * @param mandatory_mask don't insert an entry in the table if it misses an
 *                       attribute in this mask.
 */
static int run_batch_insert(lmgr_t *p_mgr,
                            attr_mask_t full_mask,
                            pktype *const pklist,
                            attr_set_t **p_attrs, unsigned int count,
                            table_enum table,
                            bool update, bool id_is_pk,
                            const char* extra_field_name,
                            const char* extra_field_value)
{
    GString    *req = NULL;
    int         rc = DB_SUCCESS;
    int         i;
    bool        first;

    if (unlikely(extra_field_name != NULL && extra_field_value == NULL))
        return DB_INVALID_ARG;

    /* build batch request for the table */
    req = g_string_new("INSERT INTO ");
    g_string_append_printf(req, "%s(id", table2name(table));

    /* do nothing if no field is to be set */
    if ((attrmask2fieldlist(req, full_mask, table, "", "", AOF_LEADING_SEP) <= 0)
        && (extra_field_name == NULL))
        goto free_str;

    if (extra_field_name != NULL)
        g_string_append_printf(req,",%s) VALUES ", extra_field_name);
    else
        g_string_append(req, ") VALUES ");

    first = true;
    /* append ",(id,values)" to the query */
    for (i = 0; i < count; i++)
    {
        if (!entry_filter(table, update, pklist[i], p_attrs[i]))
            continue;

        g_string_append_printf(req, "%s("DPK, first ? "" : ",", pklist[i]);
        attrset2valuelist(p_mgr, req, p_attrs[i], table, AOF_LEADING_SEP);

        if (extra_field_value != NULL)
            g_string_append_printf(req,",%s)", extra_field_value);
        else
            g_string_append(req,")");
        first = false;
    }

    if (update)
    {
        /* fake attribute struct, to write "field=VALUES(field)"
         * based on full_mask attr mask */
        attr_set_t  fake_attrs = *(p_attrs[0]);

        g_string_append(req, " ON DUPLICATE KEY UPDATE ");
        /* explicitely update the id if it is not part of the pk */
        if (!id_is_pk)
            g_string_append(req, "id=VALUES(id),");

        /* append x=VALUES(x) for all values */
        fake_attrs.attr_mask = full_mask;
        attrset2updatelist(p_mgr, req, &fake_attrs, table, AOF_GENERIC_VAL);
    }

    rc = db_exec_sql(&p_mgr->conn, req->str, NULL);

free_str:
    g_string_free(req, TRUE);
    return rc;
}

int listmgr_batch_insert_no_tx(lmgr_t * p_mgr, entry_id_t **p_ids,
                               attr_set_t **p_attrs,
                               unsigned int count,
                               bool update_if_exists)
{
    int            rc = 0;
    int            i;
    attr_mask_t       full_mask;
    attr_mask_t       all_bits_on = {.std = ~0, .status = ~0, .sm_info = ~0LL};
    pktype        *pklist = NULL;

    full_mask = sum_masks(p_attrs, count, all_bits_on);
    pklist = (pktype *)MemCalloc(count, sizeof(pktype));
    if (pklist == NULL)
        return DB_NO_MEMORY;

    for (i = 0; i < count; i++)
    {
        /* check attr mask */
        if (!lmgr_batch_compat(full_mask, p_attrs[i]->attr_mask))
        {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Incompatible attr mask "
                       "in batched operation: "DMASK" vs. "DMASK,
                       PMASK(&p_attrs[i]->attr_mask), PMASK(&full_mask));
            rc = DB_INVALID_ARG;
            goto out_free;
        }
        /* fill pk array */
        entry_id2pk(p_ids[i], PTR_PK(pklist[i])); /* The same for all tables? */
    }

    rc = run_batch_insert(p_mgr, full_mask, pklist, p_attrs,
                          count, T_MAIN, update_if_exists,
                          true, NULL, NULL);
    if (rc)
        goto out_free;

    /* allow inserting entries in MAIN_TABLE, without name information */

    /* both parent and name are defined */
    if (attr_mask_test_index(&full_mask, ATTR_INDEX_name)
        &&  attr_mask_test_index(&full_mask, ATTR_INDEX_parent_id))
    {
        rc = run_batch_insert(p_mgr, full_mask, pklist, p_attrs, count,
                              T_DNAMES, true, false, "pkn", HNAME_DEF);
        if (rc)
            goto out_free;
    }
    else if (!update_if_exists) /* warn for create operations without name information */
    {
        /* if we get here, the name information is missing in all fields.
         * use entry[0] as example for the warning message. */
        no_name_warning(pklist[0], p_attrs[0], count);
    }

    /* insert info in annex table */
    /* Always update as having the entry in the main table
     * is the reference to know if we knew the entry */

    /* append "on duplicate key ..." */
    rc = run_batch_insert(p_mgr, full_mask, pklist, p_attrs, count, T_ANNEX,
                          true, true, NULL, NULL);
    if (rc)
        goto out_free;

#ifdef _LUSTRE
    /* batch insert of striping info */
    if (stripe_fields(full_mask))
    {
        /* create validator list */
        int *validators = (int*)MemCalloc(count, sizeof(int));
        if (!validators)
        {
            rc =  DB_NO_MEMORY;
            goto out_free;
        }
        for (i = 0; i < count; i++)
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
            validators[i] = ATTR_MASK_TEST(p_attrs[i], stripe_info)?
                                ATTR(p_attrs[i],stripe_info).validator:VALID_NOSTRIPE;
#else
            validators[i] = VALID(p_ids[i]);
#endif

        rc = batch_insert_stripe_info(p_mgr, pklist, validators, p_attrs,
                                      count, true);
        MemFree(validators);
        if (rc)
            goto out_free;
    }
#endif

out_free:
    MemFree(pklist);
    return rc;
}


int ListMgr_Insert(lmgr_t *p_mgr, entry_id_t *p_id, attr_set_t *p_info,
                   bool update_if_exists)
{
    int rc;
    char err_buff[4096];
    int retry_status;

    /* retry the whole transaction when the error is retryable */
retry:
    rc = lmgr_begin(p_mgr);
    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    else if (retry_status == 2)
        return ECANCELED;
    else if (rc)
        return rc;

    rc = listmgr_batch_insert_no_tx(p_mgr, &p_id, &p_info, 1, update_if_exists);
    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    else if (rc || retry_status == 2)
    {
        lmgr_rollback(p_mgr);
        if (retry_status == 2)
            rc = ECANCELED;
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "DB query failed in %s line %d: code=%d: %s",
                   __FUNCTION__, __LINE__, rc,
                   db_errmsg(&p_mgr->conn, err_buff, sizeof(err_buff)));
        return rc;
    }
    rc = lmgr_commit(p_mgr);
    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;

    /* success, count it */
    if (!rc)
        p_mgr->nbop[OPIDX_INSERT]++;
    return rc;
}

/**
 * Insert a batch of entries into the database.
 * All entries must have the same attr mask.
 */
int            ListMgr_BatchInsert(lmgr_t * p_mgr, entry_id_t ** p_ids,
                                   attr_set_t ** p_attrs,
                                   unsigned int count,
                                   bool update_if_exists)
{
    int rc;
    int retry_status;
    char err_buff[4096];

    if (count == 0)
        return DB_SUCCESS;
    else if (p_ids == NULL || p_attrs == NULL)
        RBH_BUG("NULL pointer argument");

    /* read only fields in info mask? */
    if (readonly_fields(p_attrs[0]->attr_mask))
    {
        attr_mask_t and = attr_mask_and(&p_attrs[0]->attr_mask, &readonly_attr_set);
        DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                   "Error: trying to insert read only values: attr_mask="
                    DMASK, PMASK(&and));
        return DB_INVALID_ARG;
    }

    /* retry the whole transaction when the error is retryable */
retry:
    /* We want insert operation set to be atomic */
    rc = lmgr_begin(p_mgr);
    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    else if (retry_status == 2)
        return ECANCELED;
    else if (rc)
        return rc;

    rc = listmgr_batch_insert_no_tx(p_mgr, p_ids, p_attrs, count, update_if_exists);

    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    else if (rc || retry_status == 2)
    {
        lmgr_rollback(p_mgr);
        if (retry_status == 2)
            rc = ECANCELED;
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "DB query failed in %s line %d: code=%d: %s",
                   __FUNCTION__, __LINE__, rc,
                   db_errmsg(&p_mgr->conn, err_buff, sizeof(err_buff)));
        return rc;
    }

    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc) == 1)
        goto retry;
    /* success, count it */
    if (!rc)
    {
        if (update_if_exists)
            p_mgr->nbop[OPIDX_UPDATE] += count;
        else
            p_mgr->nbop[OPIDX_INSERT] += count;
    }
    return rc;
}
