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
#include "RobinhoodMisc.h"
#include "Memory.h"
#include "var_str.h"
#include <stdio.h>
#include <stdlib.h>

static void no_name_warning(PK_PARG_T pk, attr_set_t *p_attrs, unsigned int count)
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


int listmgr_batch_insert_no_tx(lmgr_t * p_mgr, entry_id_t **p_ids,
                               attr_set_t **p_attrs,
                               unsigned int count,
                               int update_if_exists)
{
    int            rc = 0;
    int            i, full_mask;
    char           fields[1024]; /* field list */
    pktype        *pklist = NULL;
    var_str        query = VAR_STR_NULL;
    char           values[4096] = "";
    char          *values_curr = NULL;
    bool           first;
    /* fake attribute struct, to write generic name fields */
    attr_set_t fake_attrs = *(p_attrs[0]);

    full_mask = sum_masks(p_attrs, count, ~0);
    fake_attrs.attr_mask = full_mask;

    pklist = (pktype *)MemCalloc(count, sizeof(pktype));
    if (pklist == NULL)
        return DB_NO_MEMORY;

    for (i = 0; i < count; i++)
    {
        /* check attr mask */
        if (!lmgr_batch_compat(full_mask, p_attrs[i]->attr_mask))
        {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Incompatible attr mask in batched operation: %#x vs. %#x",
                       p_attrs[i]->attr_mask, full_mask);
            rc = DB_INVALID_ARG;
            goto out_free;
        }
        /* fill pk array */
        entry_id2pk(p_ids[i], PTR_PK(pklist[i])); /* The same for all tables? */
    }

    /* build batch request for main table */
    attrmask2fieldlist(fields, full_mask, T_MAIN, TRUE, FALSE, "", "");

    var_str_append(&query, "INSERT INTO " MAIN_TABLE "(id");
    var_str_append(&query, fields);
    var_str_append(&query, ") VALUES ");

    first = true;
    for (i = 0; i < count; i++)
    {
        /* don't set this entry if no attribute is in main table */
        if ((p_attrs[i]->attr_mask & main_attr_set) == 0)
            continue;

        values_curr = values + sprintf(values, DPK, pklist[i]);
        attrset2valuelist(p_mgr, values_curr, p_attrs[i], T_MAIN, TRUE);

        /* add "[,](values)" to query */
        var_str_append(&query, first ? "(" : ",(");
        var_str_append(&query, values);
        var_str_append(&query, ")");
        first = false;
    }

    if (update_if_exists)
    {
        /* append "on duplicate key ..." */
        attrset2updatelist(p_mgr, values, &fake_attrs, T_MAIN, FALSE, TRUE);
        var_str_append(&query, " ON DUPLICATE KEY UPDATE ");
        var_str_append(&query, values);
    }

    rc = db_exec_sql(&p_mgr->conn, VAR_STR_START(query), NULL);
    if (rc)
        goto out_free;

    var_str_reset(&query);

    /* allow inserting entries in MAIN_TABLE, without name information */

    /* both parent and name are defined */
    if ((full_mask & ATTR_MASK_name) && (full_mask & ATTR_MASK_parent_id))
    {
        /* build batch request for names table */
        attrmask2fieldlist(fields, full_mask, T_DNAMES, TRUE, FALSE, "", "");

        var_str_append(&query, "INSERT INTO " DNAMES_TABLE "(id");
        var_str_append(&query, fields);
        var_str_append(&query, ",pkn) VALUES ");

        first = true;
        for (i = 0; i < count; i++)
        {
            /* don't set this entry if parent or name is missing */
            if (!ATTR_MASK_TEST(p_attrs[i], name) || !ATTR_MASK_TEST(p_attrs[i], parent_id))
            {
                /* warn for create operations without name information */
                if (!update_if_exists)
                    no_name_warning(pklist[i], p_attrs[i], 1);

                continue;
            }

            values_curr = values + sprintf(values, DPK, pklist[i]);
            attrset2valuelist(p_mgr, values_curr, p_attrs[i], T_DNAMES, TRUE);

            /* add "[,](values,<pk>)" to query */
            var_str_append(&query, first ? "(" : ",(");
            var_str_append(&query, values);
            var_str_append(&query, ","HNAME_DEF")");
            first = false;
        }

        values_curr = values + sprintf(values, "id=VALUES(id)"); /* not part of the PK */
        attrset2updatelist(p_mgr, values_curr, &fake_attrs, T_DNAMES, TRUE, TRUE);
        var_str_append(&query, " ON DUPLICATE KEY UPDATE ");
        var_str_append(&query, values);

        rc = db_exec_sql(&p_mgr->conn, VAR_STR_START(query), NULL);
        if (rc)
            goto out_free;

    } else if (!update_if_exists) { /* only warn for create operations */

        /* if we get here, the name information is missing in all fields.
         * use entry[0] as example for the warning message. */
        no_name_warning(pklist[0], p_attrs[0], count);
    }

    var_str_reset(&query);

    /* insert all info in annex table, if any */
    if (annex_table && (full_mask & annex_attr_set))
    {
        /* Create field and values lists.
         * Do nothing if no fields are to be set.
         */
        if (attrmask2fieldlist(fields, full_mask, T_ANNEX, TRUE, FALSE, "", "") > 0)
        {

            var_str_append(&query, "INSERT INTO "ANNEX_TABLE "(id");
            var_str_append(&query, fields);
            var_str_append(&query, ") VALUES ");

            first = true;
            for (i = 0; i < count; i++)
            {
                char           values[4096] = "";
                char          *values_curr = NULL;

                /* don't set this entry if no attribute is in annex table */
                if ((p_attrs[i]->attr_mask & annex_attr_set) == 0)
                    continue;

                sprintf(values, DPK, pklist[i]);
                values_curr = values + strlen(values);
                attrset2valuelist(p_mgr, values_curr, p_attrs[i], T_ANNEX, TRUE);

                /* add "[,](values)" to query */
                var_str_append(&query, first ? "(" : ",(");
                var_str_append(&query, values);
                var_str_append(&query, ")");
                first = false;
            }

            /* always update as having the entry in the main table
             * is the reference to know if we knew the entry */
            /* append "on duplicate key ..." */
            attrset2updatelist(p_mgr, values, &fake_attrs, T_ANNEX, FALSE, TRUE);
            var_str_append(&query, " ON DUPLICATE KEY UPDATE ");
            var_str_append(&query, values);

            rc = db_exec_sql(&p_mgr->conn, VAR_STR_START(query), NULL);
            if (rc)
                goto out_free;
        }
    }

#ifdef _LUSTRE
    /* batch insert of striping info */
    if (ATTR_MASK_TEST(&fake_attrs, stripe_info) || ATTR_MASK_TEST(&fake_attrs, stripe_items))
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
                                      count, TRUE);
        MemFree(validators);
        if (rc)
            goto out_free;
    }
#endif

out_free:
    var_str_free(&query);
    MemFree(pklist);
    return rc;
}


int ListMgr_Insert(lmgr_t *p_mgr, entry_id_t *p_id, attr_set_t *p_info,
                   int update_if_exists)
{
    int rc;
    char buff[4096];

    /* retry the whole transaction when the error is retryable */
retry:
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    rc = listmgr_batch_insert_no_tx(p_mgr, &p_id, &p_info, 1, update_if_exists);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
    {
        lmgr_rollback(p_mgr);
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "DB query failed in %s line %d: code=%d: %s",
                   __FUNCTION__, __LINE__, rc, db_errmsg(&p_mgr->conn, buff, 4096));
        return rc;
    }
    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
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
                                   int update_if_exists)
{
    int rc;
    char buff[4096];

    if (count == 0)
        return DB_SUCCESS;
    else if (p_ids == NULL || p_attrs == NULL)
        RBH_BUG("NULL pointer argument");

    /* read only fields in info mask? */
    if (readonly_attr_set & p_attrs[0]->attr_mask)
    {
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Error: trying to insert read only values: attr_mask=%#x",
                   readonly_attr_set & p_attrs[0]->attr_mask);
        return DB_INVALID_ARG;
    }

    /* retry the whole transaction when the error is retryable */
retry:
    /* We want insert operation set to be atomic */
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    rc = listmgr_batch_insert_no_tx(p_mgr, p_ids, p_attrs, count, update_if_exists);

    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
    {
        lmgr_rollback(p_mgr);
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "DB query failed in %s line %d: code=%d: %s",
                   __FUNCTION__, __LINE__, rc, db_errmsg(&p_mgr->conn, buff, 4096));
        return rc;
    }

    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
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
