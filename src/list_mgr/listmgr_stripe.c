/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008-2014 CEA/DAM
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
#include "Memory.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include <stdio.h>
#include <stdlib.h>

#define STRIPE_INFO_FIELDS "id,validator,stripe_count,stripe_size,pool_name"
#define STRIPE_INFO_SET_VALUES "validator=VALUES(validator),"       \
                               "stripe_count=VALUES(stripe_count)," \
                               "stripe_size=VALUES(stripe_size),"   \
                               "pool_name=VALUES(pool_name)"

#define STRIPE_ITEMS_FIELDS "id,stripe_index,ostidx,details"

int update_stripe_info(lmgr_t *p_mgr, PK_ARG_T pk, int validator,
                       const stripe_info_t *p_stripe,
                       const stripe_items_t *p_items, bool insert_if_absent)
{
    attr_set_t fake_attr;
    attr_set_t *p_attr = &fake_attr;
    pktype list[1];

    rh_strncpy(list[0], pk, sizeof(*list));

    ATTR_MASK_INIT(&fake_attr);
    if (p_stripe) {
        ATTR_MASK_SET(&fake_attr, stripe_info);
        ATTR(&fake_attr, stripe_info) = *p_stripe;
    }
    if (p_items) {
        ATTR_MASK_SET(&fake_attr, stripe_items);
        ATTR(&fake_attr, stripe_items) = *p_items;
    }

    return batch_insert_stripe_info(p_mgr, list, &validator, &p_attr, 1, true);
}

int insert_stripe_info(lmgr_t *p_mgr, PK_ARG_T pk,
                       int validator, const stripe_info_t *p_stripe,
                       const stripe_items_t *p_items, bool update_if_exists)
{
    attr_set_t fake_attr;
    attr_set_t *p_attr = &fake_attr;
    pktype list[1];

    rh_strncpy(list[0], pk, sizeof(*list));

    ATTR_MASK_INIT(&fake_attr);
    if (p_stripe) {
        ATTR_MASK_SET(&fake_attr, stripe_info);
        ATTR(&fake_attr, stripe_info) = *p_stripe;
    }
    if (p_items) {
        ATTR_MASK_SET(&fake_attr, stripe_items);
        ATTR(&fake_attr, stripe_items) = *p_items;
    }

    return batch_insert_stripe_info(p_mgr, list, &validator, &p_attr, 1,
                                    update_if_exists);
}

int batch_insert_stripe_info(lmgr_t *p_mgr, pktype *pklist, int *validators,
                             attr_set_t **p_attrs, unsigned int count,
                             bool update_if_exists)
{
    bool first;
    int i, rc = 0;
    int total_si;
    GString *req = g_string_new("");
    attr_mask_t tmp_mask = { ATTR_MASK_stripe_info, 0, 0LL };

    if (!attr_mask_is_null(sum_masks(p_attrs, count, tmp_mask))) {
        /* build batch request for STRIPE_INFO table */
        g_string_assign(req, "INSERT INTO " STRIPE_INFO_TABLE " ("
                        STRIPE_INFO_FIELDS ") VALUES ");

        first = true;
        for (i = 0; i < count; i++) {
            /* no request if the entry has no stripe info */
            if (!ATTR_MASK_TEST(p_attrs[i], stripe_info))
                continue;

            g_string_append_printf(req, "%s(" DPK ",%d,%u,%u,'%s')",
                                   first ? "" : ",", pklist[i], validators[i],
                                   ATTR(p_attrs[i], stripe_info).stripe_count,
                                   (unsigned int)ATTR(p_attrs[i],
                                                      stripe_info).stripe_size,
                                   ATTR(p_attrs[i], stripe_info).pool_name);
            first = false;
        }

        if (update_if_exists)
            /* append "on duplicate key ..." */
            g_string_append(req,
                            " ON DUPLICATE KEY UPDATE " STRIPE_INFO_SET_VALUES);

        if (!first) {   /* do nothing if no entry had stripe info */
            rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
            if (rc)
                goto out;
        }
        /* reset the string */
        g_string_assign(req, "");
    }

    /* Stripe items more tricky because we want to delete previous items
     * on update. */
    /* If update_if_exists is false, insert them all as a batch.
     * For the update case, remove previous items before bluk insert.
     */
    if (update_if_exists) {
        for (i = 0; i < count; i++) {
            /* no request if the entry has no stripe items */
            if (!ATTR_MASK_TEST(p_attrs[i], stripe_items))
                continue;

            g_string_printf(req,
                            "DELETE FROM " STRIPE_ITEMS_TABLE " WHERE id=" DPK,
                            pklist[i]);

            rc = db_exec_sql(&p_mgr->conn, req->str, NULL);
            if (rc)
                goto out;
        }
    }

    /* bulk insert stripe items (if any is set) */
    tmp_mask.std = ATTR_MASK_stripe_items;
    if (attr_mask_is_null(sum_masks(p_attrs, count, tmp_mask)))
        goto out;

    total_si = 0;
    first = true;
    g_string_assign(req, "INSERT INTO " STRIPE_ITEMS_TABLE
                    " (" STRIPE_ITEMS_FIELDS ") VALUES ");

    /* loop on all entries and all stripe items */
    for (i = 0; i < count; i++) {
        int s;
        const stripe_items_t *p_items;

        /* skip the entry if it has no stripe items */
        if (!ATTR_MASK_TEST(p_attrs[i], stripe_items))
            continue;

        p_items = &ATTR(p_attrs[i], stripe_items);
        for (s = 0; s < p_items->count; s++) {
            char buff[2 * STRIPE_DETAIL_SZ + 1];

            total_si++;
            if (buf2hex
                (buff, sizeof(buff),
                 (unsigned char *)(&p_items->stripe[s].ost_gen),
                 STRIPE_DETAIL_SZ) < 0) {
                DisplayLog(LVL_CRIT, LISTMGR_TAG,
                           "Buffer too small to store details stripe info");
                memset(buff, 0, sizeof(buff));
            }
            g_string_append_printf(req, "%s(" DPK ",%u,%u,x'%s')",
                                   first && (s == 0) ? "" : ",", pklist[i],
                                   s, p_items->stripe[s].ost_idx, buff);
            first = false;
        }
    }

    /* only execute it if there was some stripe items */
    if (total_si > 0)
        rc = db_exec_sql(&p_mgr->conn, req->str, NULL);

 out:
    g_string_free(req, TRUE);
    return rc;
}

int get_stripe_info(lmgr_t *p_mgr, PK_ARG_T pk, stripe_info_t *p_stripe_info,
                    stripe_items_t *p_items)
{
/* stripe_count, stripe_size, pool_name, validator => 4 */
#define STRIPE_INFO_COUNT 4
    char *res[STRIPE_INFO_COUNT];
    result_handle_t result;
    int i;
    int rc = DB_SUCCESS;
    GString *req;

    /* retrieve basic stripe info */
    req =
        g_string_new
        ("SELECT stripe_count, stripe_size, pool_name,validator FROM "
         STRIPE_INFO_TABLE " WHERE id=");
    g_string_append_printf(req, DPK, pk);

    rc = db_exec_sql(&p_mgr->conn, req->str, &result);
    if (rc)
        goto out;

    rc = db_next_record(&p_mgr->conn, &result, res, STRIPE_INFO_COUNT);

    if (rc == DB_END_OF_LIST)
        rc = DB_NOT_EXISTS;
    if (rc)
        goto res_free;

    for (i = 0; i < STRIPE_INFO_COUNT; i++) {
        DisplayLog(LVL_FULL, LISTMGR_TAG, "stripe_res[%u] = %s", i,
                   res[i] ? res[i] : "<null>");
        if (res[i] == NULL) {
            rc = DB_ATTR_MISSING;
            goto res_free;
        }
    }

    p_stripe_info->stripe_count = atoi(res[0]);
    p_stripe_info->stripe_size = atoi(res[1]);
    rh_strncpy(p_stripe_info->pool_name, res[2], MAX_POOL_LEN);
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
    p_stripe_info->validator = atoi(res[3]);
#endif

    db_result_free(&p_mgr->conn, &result);

    if (p_items) {
        /* retrieve stripe list */
        g_string_printf(req, "SELECT stripe_index,ostidx,details FROM "
                        STRIPE_ITEMS_TABLE " WHERE id=" DPK
                        " ORDER BY stripe_index ASC", pk);

        rc = db_exec_sql(&p_mgr->conn, req->str, &result);
        if (rc)
            goto out;

#ifndef _LUSTRE_HSM
        /* this is abnormal if LUSTRE/HSM feature is not present */
        if (p_stripe_info->stripe_count !=
            db_result_nb_records(&p_mgr->conn, &result)) {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                       "Warning: the number of stripe items (%d) doesn't match stripe count (%u)! (Pk="
                       DPK ")", db_result_nb_records(&p_mgr->conn, &result),
                       p_stripe_info->stripe_count, pk);
        }
#endif
        p_items->count = db_result_nb_records(&p_mgr->conn, &result);

        if (p_items->count > 0) {

            /* allocate stripe array */
            p_items->stripe = MemCalloc(p_items->count, sizeof(stripe_item_t));

            if (!p_items->stripe) {
                rc = DB_NO_MEMORY;
                goto res_free;
            }

            /* fill stripe units */
            for (i = 0; i < p_items->count; i++) {
                rc = db_next_record(&p_mgr->conn, &result, res,
                                    STRIPE_INFO_COUNT);
                if (rc)
                    goto stripe_free;

                if (res[0] == NULL) {
                    rc = DB_ATTR_MISSING;
                    goto stripe_free;
                }

                if (i != atoi(res[0])) {
                    DisplayLog(LVL_MAJOR, LISTMGR_TAG,
                               "Warning: inconsistent stripe order: stripe %s returned in position %u",
                               res[0], i);
                }
                p_items->stripe[i].ost_idx = atoi(res[1]);
                /* raw copy of binary buffer (last 3 fields of stripe_item_t
                 *                            = address of ost_gen field) */
                memcpy(&p_items->stripe[i].ost_gen, res[2], STRIPE_DETAIL_SZ);
            }
        } else
            p_items->stripe = NULL;

        /* last query result must be freed */
        rc = DB_SUCCESS;
        goto res_free;
    }

    rc = DB_SUCCESS;
    /* result is already freed */
    goto out;

 stripe_free:
    free_stripe_items(p_items);
 res_free:
    db_result_free(&p_mgr->conn, &result);
 out:
    g_string_free(req, TRUE);
    return rc;
}

/** release stripe information */
void free_stripe_items(stripe_items_t *p_stripe_items)
{
    if (p_stripe_items->stripe)
        MemFree(p_stripe_items->stripe);
    p_stripe_items->stripe = NULL;
    p_stripe_items->count = 0;
}

/** duplicate stripe information */
int dup_stripe_items(stripe_items_t *p_stripe_out,
                     const stripe_items_t *p_stripe_in)
{
    if (p_stripe_in == NULL || p_stripe_out == NULL)
        return DB_INVALID_ARG;

    p_stripe_out->count = p_stripe_in->count;

    if (p_stripe_in->stripe == NULL) {
        p_stripe_out->stripe = NULL;
        return 0;
    }

    p_stripe_out->stripe = MemAlloc(p_stripe_out->count
                                    * sizeof(stripe_item_t));
    if (p_stripe_out->stripe == NULL)
        return DB_NO_MEMORY;

    memcpy(p_stripe_out->stripe, p_stripe_in->stripe,
           p_stripe_out->count * sizeof(stripe_item_t));
    return 0;
}

/** check that validator is matching for a given entry */
int ListMgr_CheckStripe(lmgr_t *p_mgr, const entry_id_t *p_id, int validator)
{
    char *res;
    result_handle_t result;
    int rc = DB_SUCCESS;
    GString *req = NULL;
    DEF_PK(pk);

#ifndef HAVE_LLAPI_FSWAP_LAYOUTS
    if (validator != VALID_EXISTS)
        validator = VALID(p_id);
#endif

    entry_id2pk(p_id, PTR_PK(pk));

    req = g_string_new("SELECT validator FROM " STRIPE_INFO_TABLE " WHERE id=");
    g_string_append_printf(req, DPK, pk);

 retry:
    rc = db_exec_sql(&p_mgr->conn, req->str, &result);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto out;

    rc = db_next_record(&p_mgr->conn, &result, &res, 1);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;

    if (rc == DB_END_OF_LIST)
        rc = DB_NOT_EXISTS;

    if (rc)
        goto res_free;

    if (res == NULL) {
        rc = DB_ATTR_MISSING;
        goto res_free;
    }

    if (validator == VALID_EXISTS) {
        DisplayLog(LVL_FULL, LISTMGR_TAG, DFID ": validator exists (%s): OK",
                   PFID(p_id), res);
        /* just check it exists */
        rc = DB_SUCCESS;
    } else if (atoi(res) != validator) {
        DisplayLog(LVL_FULL, LISTMGR_TAG,
                   DFID ": stripe change detected: gen %s->%d", PFID(p_id), res,
                   validator);
        rc = DB_OUT_OF_DATE;
    } else {    /* validator matches */

        DisplayLog(LVL_FULL, LISTMGR_TAG, DFID ": stripe gen is unchanged (%d)",
                   PFID(p_id), validator);
        rc = DB_SUCCESS;
    }

 res_free:
    db_result_free(&p_mgr->conn, &result);
 out:
    g_string_free(req, TRUE);
    DisplayLog(LVL_FULL, LISTMGR_TAG, DFID ": %s returns with status=%d",
               PFID(p_id), __func__, rc);
    return rc;
}

int ListMgr_SetStripe(lmgr_t *p_mgr, const entry_id_t *p_id,
                      stripe_info_t *p_stripe_info,
                      stripe_items_t *p_stripe_items)
{
    DEF_PK(pk);
    int rc;
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
    int validator = (p_stripe_info ? p_stripe_info->validator : VALID_NOSTRIPE);
#else
    int validator = VALID(p_id);
#endif

    entry_id2pk(p_id, PTR_PK(pk));
 retry:
    rc = insert_stripe_info(p_mgr, pk, validator, p_stripe_info, p_stripe_items,
                            true);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;

    return rc;
}
