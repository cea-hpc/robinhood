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
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>


int ListMgr_Update(lmgr_t *p_mgr, const entry_id_t *p_id,
                   const attr_set_t *p_update_set)
{
    int            rc;
    GString       *req;
    DEF_PK(pk);

    /* read only fields in info mask? */
    if (readonly_fields(p_update_set->attr_mask))
    {
        attr_mask_t and = attr_mask_and(&p_update_set->attr_mask, &readonly_attr_set);
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Error: trying to update read only "
                   "values: attr_mask="DMASK, PMASK(&and));
        return DB_INVALID_ARG;
    }

    entry_id2pk(p_id, PTR_PK(pk));

    req = g_string_new(NULL);

retry:
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;

    /* update fields in main table */
    if (main_fields(p_update_set->attr_mask))
    {
        g_string_assign(req, "UPDATE "MAIN_TABLE" SET ");

        rc = attrset2updatelist(p_mgr, req, p_update_set, T_MAIN, 0);
        if (rc < 0)
        {
            rc = -rc;
            goto free_str;
        }
        else if (rc > 0)
        {
            g_string_append_printf(req, " WHERE id="DPK, pk);
            rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
            if (lmgr_delayed_retry(p_mgr, rc))
                goto retry;
            else if (rc)
                goto rollback;
        }
    }

    /* update names table */
    if (ATTR_MASK_TEST(p_update_set, name) && ATTR_MASK_TEST(p_update_set, parent_id))
    {
        g_string_assign(req, "INSERT INTO " DNAMES_TABLE "(id");
        attrmask2fieldlist(req, p_update_set->attr_mask, T_DNAMES, "", "",
                           AOF_LEADING_SEP);
        g_string_append_printf(req, ",pkn) VALUES ("DPK, pk);
        attrset2valuelist(p_mgr, req, p_update_set, T_DNAMES,
                          AOF_LEADING_SEP);
        g_string_append(req, ","HNAME_DEF") ON DUPLICATE KEY UPDATE id=VALUES(id)");
        attrset2updatelist(p_mgr, req, p_update_set, T_DNAMES,
                           AOF_LEADING_SEP | AOF_GENERIC_VAL);

        rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;
    }
    else if (ATTR_MASK_TEST(p_update_set, name) || ATTR_MASK_TEST(p_update_set, parent_id))
    {
        DisplayLog(LVL_DEBUG, LISTMGR_TAG, "WARNING: missing attribute to update name information"
                   " (entry "DPK"): name %s, parent_id %s", pk,
                   ATTR_MASK_TEST(p_update_set, name) ? "is set" : "is not set",
                   ATTR_MASK_TEST(p_update_set, parent_id) ? "is set" : "is not set");
    }

    /* update annex table */
    if (annex_fields(p_update_set->attr_mask))
    {
        g_string_assign(req, "UPDATE "ANNEX_TABLE" SET ");
        rc = attrset2updatelist(p_mgr, req, p_update_set, T_ANNEX, 0);
        if (rc < 0)
        {
            rc = -rc;
            goto free_str;
        }
        else if (rc > 0)
        {
            g_string_append_printf(req, " WHERE id="DPK, pk);
            rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
            if (lmgr_delayed_retry(p_mgr, rc))
                goto retry;
            else if (rc)
                goto rollback;
        }
    }

#ifdef _LUSTRE
    if (ATTR_MASK_TEST(p_update_set, stripe_info))
    {
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
        int validator = ATTR(p_update_set, stripe_info).validator;
#else
        int validator = VALID(p_id);
#endif

        const stripe_items_t *p_items = NULL;

        if (ATTR_MASK_TEST(p_update_set, stripe_items))
            p_items = &ATTR(p_update_set, stripe_items);

        rc = update_stripe_info(p_mgr, pk, validator,
                                &ATTR(p_update_set, stripe_info), p_items, true);
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;
    }
#endif

    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    if (rc == DB_SUCCESS)
        p_mgr->nbop[OPIDX_UPDATE]++;

    goto free_str;

rollback:
    lmgr_rollback(p_mgr);
free_str:
    g_string_free(req, TRUE);
    return rc;
}

/** XXX ListMgr_MassUpdate() is not used => dropped in v3.0 */

int ListMgr_Replace(lmgr_t *p_mgr, entry_id_t *old_id, attr_set_t *old_attrs,
                    entry_id_t *new_id, attr_set_t *new_attrs,
                    bool src_is_last, bool update_target_if_exists)
{
    GString *req = NULL;
    DEF_PK(oldpk);
    DEF_PK(newpk);
    int rc;

retry:
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    /* delete the old entry */
    rc = listmgr_remove_no_tx(p_mgr, old_id, old_attrs, src_is_last);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    /* create the new one */
    rc = listmgr_batch_insert_no_tx(p_mgr, &new_id, &new_attrs, 1,
                                    update_target_if_exists);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    /* update parent ids in NAMES table */
    entry_id2pk(old_id, PTR_PK(oldpk));
    entry_id2pk(new_id, PTR_PK(newpk));

    req = g_string_new("UPDATE "DNAMES_TABLE);
    g_string_append_printf(req, " SET parent_id="DPK" WHERE parent_id="DPK,
                           newpk, oldpk);
    rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else
    {
        g_string_free(req, TRUE);
        return rc;
    }

rollback:
    lmgr_rollback(p_mgr);
    if (req != NULL)
        g_string_free(req, TRUE);
    return rc;
}
