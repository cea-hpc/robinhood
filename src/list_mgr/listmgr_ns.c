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
/**
 * Namespace related functions
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "listmgr_common.h"
#include "listmgr_internal.h"
#include "listmgr_stripe.h"
#include "database.h"
#include "Memory.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include <stdio.h>
#include <stdlib.h>

/**
 * print parent condition depending on parent list count:
 *      parent_id == xxx or parent_id IN ( xxx, yyy, zzz )
 * \return db error code
 */
static int append_parent_cond(lmgr_t *p_mgr, GString *str, const wagon_t *parent_list,
                              unsigned int parent_count, const char *prefix)
{
    DEF_PK(pk);

    if (unlikely(parent_count == 0))
    {
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Warning: parent list is empty in %s()", __func__ );
        return DB_INVALID_ARG;
    }

    if (likely(parent_count == 1)) /* the only expected for now */
    {
        entry_id2pk(&parent_list[0].id, PTR_PK(pk));
        g_string_append_printf(str, "%sparent_id="DPK, prefix ? prefix : "", pk);
    }
    else
    {
        int i;

        g_string_append_printf(str, "%sparent_id IN (", prefix ? prefix : "");
        for (i = 0; i < parent_count; i++)
        {
            entry_id2pk(&parent_list[i].id, PTR_PK(pk));
            g_string_append_printf(str, "%s"DPK, (i == 0)? "":",", pk);
        }
        g_string_append(str,")");
    }
    return DB_SUCCESS;
}

/**
 * Get the list of children of a given parent (or list of parents).
 * \param parent_list       [in]  list of parents to get the child of
 * \param parent_count      [in]  number of ids in parent list
 * \param attr_mask         [in]  required attributes for children
 * \param child_id_list     [out] ptr to array of child ids
 * \param child_attr_list   [out] ptr to array of child attrs
 * \param child_count       [out] number of returned children
 */
int ListMgr_GetChild(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                     const wagon_t *parent_list, unsigned int parent_count,
                     attr_mask_t attr_mask,
                     wagon_t **child_id_list, attr_set_t **child_attr_list,
                     unsigned int *child_count)
{
    result_handle_t result;
    char *path = NULL;
    int path_len;
    int                rc, i;
    GString           *req = NULL;
    GString           *fields = NULL;
    GString           *from = NULL;
    GString           *where = NULL;
    struct field_count field_cnt = {0};
    struct field_count filter_cnt = {0};
    table_enum         query_tab = T_DNAMES;
    bool               distinct = false;
    int                retry_status;

    /* XXX: querying children from several parent cannot work, since
     * we need to get the paths of the children. Or we could do a
     * lookup into parent_list to find the right one. In the meantime,
     * try not to mess up the code. */
    if (unlikely(parent_count != 1))
        RBH_BUG("cannot get children for several parent simultaneously");

    /* always request for name to build fullpath in wagon */
    attr_mask_set_index(&attr_mask, ATTR_INDEX_name);

    fields = g_string_new(NULL);

    /* append fields for all tables */
    if (!attr_mask_is_null(attr_mask))
    {
        /* retrieve source info for generated fields */
        add_source_fields_for_gen(&attr_mask.std);

        field_cnt.nb_names = attrmask2fieldlist(fields, attr_mask, T_DNAMES,
                                                DNAMES_TABLE".", "",
                                                AOF_LEADING_SEP);

        field_cnt.nb_main = attrmask2fieldlist(fields, attr_mask, T_MAIN,
                                               MAIN_TABLE".", "",
                                               AOF_LEADING_SEP);

        field_cnt.nb_annex = attrmask2fieldlist(fields, attr_mask, T_ANNEX,
                                                ANNEX_TABLE".", "",
                                                AOF_LEADING_SEP);
    }
    else
    {
        /* no returned attrs */
        if (child_attr_list != NULL)
            *child_attr_list = NULL;
    }

    where = g_string_new(NULL);

    /* starts with condition on parent */
    rc = append_parent_cond(p_mgr, where, parent_list, parent_count, DNAMES_TABLE".");
    if (rc != DB_SUCCESS)
        goto free_str;

    /* check filters on other tables */
    if (!no_filter(p_filter))
    {
        if (unlikely(dir_filter(p_mgr, NULL, p_filter, NULL, NULL) != FILTERDIR_NONE))
        {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Directory filter not supported in %s()", __func__);
            rc = DB_NOT_SUPPORTED;
            goto free_str;
        }
        else if (unlikely(func_filter(p_mgr, NULL, p_filter, T_MAIN, 0)))
        {
            DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Function filter not supported in %s()", __func__);
            rc = DB_NOT_SUPPORTED;
            goto free_str;
        }

        /* There is always a filter on T_DNAMES, which is the parent condition.
         * Look for optional filters.
         */
        filter_where(p_mgr, p_filter, &filter_cnt, where,
                     AOF_LEADING_SEP | AOF_SKIP_NAME);
        /** @FIXME process other filters on NAMES */
    }

    from = g_string_new(DNAMES_TABLE);

    /* add filter_count + field_count to build the FROM clause.
     * Preserve field count which is needed to interpret the result.
     */
    filter_cnt.nb_main += field_cnt.nb_main;
    filter_cnt.nb_annex += field_cnt.nb_annex;
    filter_cnt.nb_names += field_cnt.nb_names;
    /* query tab is DNAMES, skip_name=true, is_first_tab=T_DNAMES */
    filter_from(p_mgr, &filter_cnt, from, &query_tab, &distinct,
                AOF_LEADING_SEP | AOF_SKIP_NAME);

    /* request is always on the DNAMES table (which contains [parent_id, id] relationship */
    if (distinct)
        req = g_string_new("SELECT DISTINCT("DNAMES_TABLE".id) as id");
    else
        req = g_string_new("SELECT "DNAMES_TABLE".id as id");

    /* build the whole request */
    g_string_append_printf(req, "%s FROM %s WHERE %s", fields->str, from->str, where->str);

retry:
    rc = db_exec_sql(&p_mgr->conn, req->str, &result);
    retry_status = lmgr_delayed_retry(p_mgr, rc);
    if (retry_status == 1)
        goto retry;
    else if (retry_status == 2) {
        rc = DB_RBH_SIG_SHUTDOWN;
        goto free_str;
    } else if (rc)
        goto free_str;

    /* copy result to output structures */
    *child_count = db_result_nb_records(&p_mgr->conn, &result);

    /* allocate entry_id array */
    *child_id_list = MemCalloc(*child_count, sizeof(wagon_t));
    if (*child_id_list == NULL)
    {
        rc = DB_NO_MEMORY;
        goto free_str;
    }

    if (child_attr_list)
    {
        *child_attr_list = MemCalloc(*child_count, sizeof(attr_set_t));
        if (*child_attr_list == NULL)
        {
            rc = DB_NO_MEMORY;
            goto array_free;
        }
    }

    /* Allocate a string long enough to contain the parent path and a
     * child name. */
    path_len = strlen(parent_list[0].fullname) + RBH_NAME_MAX + 2;
    path = malloc(path_len);
    if (!path) {
        DisplayLog(LVL_MAJOR, LISTMGR_TAG, "Can't alloc enough memory (%d bytes)",
                    path_len);
        rc = DB_NO_MEMORY;
        goto array_free;
    }

    for (i = 0; i < *child_count; i++)
    {
        char *res[128]; /* 128 fields per record is large enough */

        rc = db_next_record(&p_mgr->conn, &result, res, sizeof(res)/sizeof(*res));
        if (rc)
            goto array_free;

        /* copy id to array */
        pk2entry_id(p_mgr, res[0], &((*child_id_list)[i].id));

        /* copy attributes to array */
        if (child_attr_list)
        {
            unsigned int shift = 1; /* first was NAMES.id */

            (*child_attr_list)[i].attr_mask = attr_mask;

            /* first id, then dnames attrs, then main attrs, then annex attrs */
            if (field_cnt.nb_names > 0)
            {
                /* shift of 1 for id */
                rc = result2attrset(T_DNAMES, res + shift, field_cnt.nb_names, &((*child_attr_list)[i]));
                if (rc)
                    goto array_free;
                shift += field_cnt.nb_names;
            }

            if (field_cnt.nb_main > 0)
            {
                /* first id, then main attrs, then annex attrs */
                /* shift of 1 for id */
                rc = result2attrset(T_MAIN, res + shift, field_cnt.nb_main, &((*child_attr_list)[i]));
                if (rc)
                    goto array_free;
                shift += field_cnt.nb_main;
            }

            if (field_cnt.nb_annex > 0)
            {
                /* shift of main_attrs count */
                rc = result2attrset(T_ANNEX, res + shift, field_cnt.nb_annex,
                                     &((*child_attr_list)[i]));
                if (rc)
                    goto array_free;
                shift += field_cnt.nb_annex;
            }

#ifdef _LUSTRE
            if (stripe_fields(attr_mask))
            {
                if (get_stripe_info(p_mgr, res[0], &ATTR(&(*child_attr_list)[i], stripe_info),
                                     &ATTR(&(*child_attr_list)[i], stripe_items)))
                {
                    ATTR_MASK_UNSET(&(*child_attr_list)[i], stripe_info);
                    ATTR_MASK_UNSET(&(*child_attr_list)[i], stripe_items);
                }
            }
#endif

            generate_fields(&((*child_attr_list)[i]));

            /* Note: path is properly sized already to not overflow. */
            snprintf(path, path_len, "%s/%s", parent_list[0].fullname,
                     (*child_attr_list)[i].attr_values.name);
            (*child_id_list)[i].fullname = strdup(path);
        }
    }

    if (path)
        free(path);

    db_result_free(&p_mgr->conn, &result);
    g_string_free(req, TRUE);
    g_string_free(fields, TRUE);
    g_string_free(from, TRUE);
    g_string_free(where, TRUE);
    return 0;

array_free:
    if (path)
        free(path);
    if (child_attr_list && *child_attr_list)
    {
        MemFree(*child_attr_list);
        *child_attr_list = NULL;
    }
    MemFree(*child_id_list);
    *child_id_list = NULL;
free_str:
    g_string_free(req, TRUE);
    g_string_free(fields, TRUE);
    g_string_free(from, TRUE);
    g_string_free(where, TRUE);
    return rc;
}
