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
#include "Memory.h"
#include "var_str.h"
#include <stdio.h>
#include <stdlib.h>


int listmgr_batch_insert_no_tx(lmgr_t * p_mgr, entry_id_t **p_ids,
                               attr_set_t **p_attrs,
                               unsigned int count,
                               int update_if_exists)
{
    int            rc = 0;
    int            i, mask_init;
    char           fields[1024]; /* field list */
    pktype        *pklist = NULL;
    var_str        query = VAR_STR_NULL;
    char           values[4096] = "";
    char          *values_curr = NULL;

    mask_init = p_attrs[0]->attr_mask;

    pklist = (pktype *)MemCalloc(count, sizeof(pktype));
    if (pklist == NULL)
        return DB_NO_MEMORY;

    for (i = 0; i < count; i++)
    {
        /* check attr mask */
        if (p_attrs[i]->attr_mask != mask_init)
        {
            rc = DB_INVALID_ARG;
            goto out_free;
        }
        /* fill pk array */
        entry_id2pk(p_ids[i], PTR_PK(pklist[i]));
    }

    /* build batch request for main table */
    attrmask2fieldlist(fields, p_attrs[0]->attr_mask, T_MAIN, TRUE, FALSE, "", "");

    var_str_append(&query, "INSERT INTO " MAIN_TABLE "(id");
    var_str_append(&query, fields);
    var_str_append(&query, ") VALUES ");

    for (i = 0; i < count; i++)
    {
        values_curr = values + sprintf(values, DPK, pklist[i]);
        attrset2valuelist(p_mgr, values_curr, p_attrs[i], T_MAIN, TRUE);

        /* add "[,](values)" to query */
        var_str_append(&query, (i == 0) ? "(" : ",(");
        var_str_append(&query, values);
        var_str_append(&query, ")");
    }

    if (update_if_exists)
    {
        /* append "on duplicate key ..." */
        attrset2updatelist(p_mgr, values, p_attrs[0], T_MAIN, FALSE, TRUE);
        var_str_append(&query, " ON DUPLICATE KEY UPDATE ");
        var_str_append(&query, values);
    }

    rc = db_exec_sql(&p_mgr->conn, VAR_STR_START(query), NULL);
    if (rc)
        goto out_free;

    var_str_reset(&query);

    /* allow inserting entries in MAIN_TABLE, without name information */
    if (ATTR_MASK_TEST(p_attrs[0], name) && ATTR_MASK_TEST(p_attrs[0], parent_id))
    {
        /* build batch request for names table */
        attrmask2fieldlist(fields, p_attrs[0]->attr_mask, T_DNAMES, TRUE, FALSE, "", "");

        var_str_append(&query, "INSERT INTO " DNAMES_TABLE "(id");
        var_str_append(&query, fields);
        var_str_append(&query, ",pkn) VALUES ");

        for (i = 0; i < count; i++)
        {
            values_curr = values + sprintf(values, DPK, pklist[i]);
            attrset2valuelist(p_mgr, values_curr, p_attrs[i], T_DNAMES, TRUE);

            /* add "[,](values,<pk>)" to query */
            var_str_append(&query, (i == 0) ? "(" : ",(");
            var_str_append(&query, values);
            var_str_append(&query, ","HNAME_DEF")");
        }

        values_curr = values + sprintf(values, "id=VALUES(id)"); /* not part of the PK */
        attrset2updatelist(p_mgr, values_curr, p_attrs[0], T_DNAMES, TRUE, TRUE);
        var_str_append(&query, " ON DUPLICATE KEY UPDATE ");
        var_str_append(&query, values);

        rc = db_exec_sql(&p_mgr->conn, VAR_STR_START(query), NULL);
        if (rc)
            goto out_free;
    } else if (!update_if_exists) { /* only warn for create operations */
        DEF_PK(ppk);

        entry_id2pk(& ATTR(p_attrs[0], parent_id), PTR_PK(ppk));

        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "WARNING: %u entries created without"
                   " name or parent information. entry[0]: pk="DPK", name='%s', parent='%s'",
                    count, pklist[0], ATTR_MASK_TEST(p_attrs[0], name) ? ATTR(p_attrs[0], name) : "",
                   ATTR_MASK_TEST(p_attrs[0], parent_id) ? ppk : "");
    }

    var_str_reset(&query);

    /* insert all info in annex table, if any */
    if (annex_table)
    {
        /* Create field and values lists.
         * Do nothing if no fields are to be set.
         */
        if (attrmask2fieldlist(fields, p_attrs[0]->attr_mask, T_ANNEX, TRUE, FALSE, "", "") > 0)
        {

            var_str_append(&query, "INSERT INTO "ANNEX_TABLE "(id");
            var_str_append(&query, fields);
            var_str_append(&query, ") VALUES ");

            for (i = 0; i < count; i++)
            {
                char           values[4096] = "";
                char          *values_curr = NULL;
                sprintf(values, DPK, pklist[i]);
                values_curr = values + strlen(values);
                attrset2valuelist(p_mgr, values_curr, p_attrs[i], T_ANNEX, TRUE);

                /* add "[,](values)" to query */
                var_str_append(&query, (i == 0) ? "(" : ",(");
                var_str_append(&query, values);
                var_str_append(&query, ")");
            }

            if (update_if_exists)
            {
                /* append "on duplicate key ..." */
                attrset2updatelist(p_mgr, values, p_attrs[0], T_ANNEX, FALSE, TRUE);
                var_str_append(&query, " ON DUPLICATE KEY UPDATE ");
                var_str_append(&query, values);
            }

            rc = db_exec_sql(&p_mgr->conn, VAR_STR_START(query), NULL);
            if (rc)
                goto out_free;
        }
    }

#ifdef _LUSTRE
    /* batch insert of striping info */
    if (ATTR_MASK_TEST(p_attrs[0], stripe_info) && ATTR_MASK_TEST(p_attrs[0], stripe_items))
    {
        /* create validator list */
        int *validators = (int*)MemCalloc(count, sizeof(int));
        if (!validators)
        {
            rc =  DB_NO_MEMORY;
            goto out_free;
        }
        for (i = 0; i < count; i++)
            validators[i] = VALID(p_ids[i]);

        rc = batch_insert_stripe_info(p_mgr, pklist, validators, p_attrs,
                                      count, update_if_exists);
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

    rc = lmgr_begin(p_mgr);
    if (rc)
        return rc;

    rc = listmgr_batch_insert_no_tx(p_mgr, &p_id, &p_info, 1, update_if_exists);
    if (rc)
    {
        lmgr_rollback(p_mgr);
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "DB query failed in %s line %d: code=%d: %s",
                   __FUNCTION__, __LINE__, rc, db_errmsg(&p_mgr->conn, buff, 4096));
        return rc;
    }
    rc = lmgr_commit(p_mgr);

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

    /* We want insert operation set to be atomic */
    rc = lmgr_begin(p_mgr);
    if (rc)
        return rc;

    rc = listmgr_batch_insert_no_tx(p_mgr, p_ids, p_attrs, count, update_if_exists);
    if (rc)
    {
        lmgr_rollback(p_mgr);
        DisplayLog(LVL_CRIT, LISTMGR_TAG,
                   "DB query failed in %s line %d: code=%d: %s",
                   __FUNCTION__, __LINE__, rc, db_errmsg(&p_mgr->conn, buff, 4096));
        return rc;
    }

    rc = lmgr_commit(p_mgr);
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
