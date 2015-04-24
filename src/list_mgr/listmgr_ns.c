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
//#include "listmgr_stripe.h"
#include "listmgr_internal.h"
#include "listmgr_stripe.h"
#include "database.h"
#include "Memory.h"
#include "RobinhoodLogs.h"
#include <stdio.h>
#include <stdlib.h>

/**
 * print parent condition depending on parent list count:
 *      parent_id == xxx or parent_id IN ( xxx, yyy, zzz )
 */
static char * parent_cond(lmgr_t * p_mgr, char * buff, size_t buffsz,
                          const wagon_t * parent_list, unsigned int parent_count,
                          const char * prefix)
{
    DEF_PK(pk);

    if (parent_count == 0)
    {
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Warning: parent list is empty in %s()", __func__ );
        return NULL;
    }
    if (parent_count == 1)
    {
        entry_id2pk(&parent_list[0].id, PTR_PK(pk));
        sprintf(buff, "%sparent_id="DPK, prefix ? prefix : "", pk);
    }
    else
    {
        int i;
        char * curr = buff;
        curr += sprintf(curr, "%sparent_id IN (", prefix ? prefix : "");
        for (i = 0; i < parent_count; i++)
        {
            entry_id2pk(&parent_list[i].id, PTR_PK(pk));
            if ((ssize_t)(curr - buff) + strlen(pk) + 2 >= buffsz)
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG, "ERROR: request overflow in %s(): parent_list length=%u, current buff_usage=%zu",
                           __func__, parent_count, (size_t)(curr - buff));
                return NULL;
            }
            if (i == 0)
                curr += sprintf(curr, DPK, pk);
            else
                curr += sprintf(curr, ","DPK, pk);
        }
        strcpy(curr, ")");
    }
    return buff;
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
int ListMgr_GetChild( lmgr_t * p_mgr, const lmgr_filter_t * p_filter,
                      const wagon_t * parent_list, unsigned int parent_count,
                      int attr_mask,
                      wagon_t ** child_id_list, attr_set_t ** child_attr_list,
                      unsigned int * child_count)
{
    result_handle_t result;
    char *curr;
    int  filter_main = 0;
    int  filter_annex = 0;
    int main_attrs = 0;
    int dnames_attrs = 0;
    int annex_attrs = 0;
    char query[4096];
    char fieldlist_main[1024] = "";
    char fieldlist_dnames[1024] = "";
    char fieldlist_annex[1024] = "";
    char filter_str_main[1024] = "";
    char filter_str_annex[1024] = "";
    char tmp[2048];
    char *path = NULL;
    int path_len;
    char * pc;
    int rc, i;

    /* TODO: querying children from several parent cannot work, since
     * we need to get the paths of the children. Or we could do a
     * lookup into parent_list to find the right one. In the meantime,
     * try not to mess up the code. */
    if (parent_count != 1)
        RBH_BUG("cannot get children for several parent simultaneously");

    /* always request for name to build fullpath in wagon */
    attr_mask |= ATTR_MASK_name;

    /* request is always on the MAIN table (which contains [parent_id, id] relationship */

    /* /!\ possible cases:
     * - simplest: the fields of the filter and the attributes to be retrieved are in the MAIN table
     * - harder: the fields of the filter and attributes are in a different table
     */

    /* 1) location of filters */
    if ( p_filter )
    {
        char           dummy_str[1024];
        unsigned int   dummy_uint;
        if (dir_filter(p_mgr, dummy_str, p_filter, &dummy_uint, NULL) != FILTERDIR_NONE)
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Directory filter not supported in %s()", __func__ );
            return DB_NOT_SUPPORTED;
        }
        else if (func_filter(p_mgr, dummy_str, p_filter, T_MAIN, FALSE, FALSE))
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Function filter not supported in %s()", __func__ );
            return DB_NOT_SUPPORTED;
        }

        /* There is always a filter on T_DNAMES, which is the parent condition.
         * Look for optional filters:
         */
        filter_main = filter2str( p_mgr, filter_str_main, p_filter, T_MAIN,
                                  FALSE, TRUE );

        if ( annex_table )
            filter_annex = filter2str( p_mgr, filter_str_annex, p_filter,
                                       T_ANNEX, FALSE, TRUE );
        else
            filter_annex = 0;

        /* @TODO to be implemented */
#if 0
        filter_stripe_info =
            filter2str( p_mgr, filter_str_stripe_info, p_filter, T_STRIPE_INFO,
                        ( filter_main > 0 ) || ( filter_annex > 0 ), TRUE );

        filter_stripe_items =
            filter2str( p_mgr, filter_str_stripe_items, p_filter, T_STRIPE_ITEMS,
                        ( filter_main > 0 ) || ( filter_annex > 0 )
                        || ( filter_stripe_info > 0 ), TRUE );
#endif
    }

    /* 2) location of requested attributes */
    if (attr_mask)
    {
        /* retrieve source info for generated fields */
        add_source_fields_for_gen( &attr_mask );

        main_attrs = attrmask2fieldlist( fieldlist_main, attr_mask, T_MAIN,
                                         /* leading comma */ TRUE, /* for update */ FALSE,
                                         /* prefix */ MAIN_TABLE".", /* postfix */ "" );

        dnames_attrs += attrmask2fieldlist( fieldlist_dnames, attr_mask, T_DNAMES,
                                            /* leading comma */ TRUE, /* for update */ FALSE,
                                            /* prefix */ DNAMES_TABLE".", /* postfix */ "" );

        if ( annex_table )
            annex_attrs = attrmask2fieldlist( fieldlist_annex, attr_mask, T_ANNEX,
                                             /* leading comma */ TRUE, /* for update */ FALSE,
                                             /* prefix */ ANNEX_TABLE".", /* postfix */ "" );
        else
            annex_attrs = 0;
    }
    else
    {
        /* no returned attrs */
        if (child_attr_list)
            *child_attr_list = NULL;
    }
    pc = parent_cond(p_mgr, tmp, sizeof(tmp), parent_list, parent_count, DNAMES_TABLE".");
    if (!pc)
        return DB_BUFFER_TOO_SMALL;

    curr = query;

    /* SELECT clause */
    /* id + dname fields */
    curr += sprintf(curr, "SELECT "DNAMES_TABLE".id%s", fieldlist_dnames);
    /* main attrs */
    if (main_attrs)
        curr += sprintf(curr, "%s", fieldlist_main);
    /* annex attrs */
    if (annex_attrs)
        curr += sprintf(curr, "%s", fieldlist_annex);

    /* FROM clause */
    curr += sprintf(curr, " FROM "DNAMES_TABLE);
    if (main_attrs || filter_main)
        curr += sprintf(curr, " LEFT JOIN "MAIN_TABLE
                              " ON "DNAMES_TABLE".id="MAIN_TABLE".id");
    if (annex_attrs || filter_annex)
        curr += sprintf(curr, " LEFT JOIN "ANNEX_TABLE
                              " ON "DNAMES_TABLE".id="ANNEX_TABLE".id");

    /* WHERE clause */
    curr += sprintf(curr, " WHERE %s", pc);
    if (filter_main)
        curr += sprintf(curr, " AND %s", filter_str_main);
    if (filter_annex)
        curr += sprintf(curr, " AND %s", filter_str_annex);

retry:
    rc = db_exec_sql(&p_mgr->conn, query, &result);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    /* copy result to output structures */
    *child_count = db_result_nb_records(&p_mgr->conn, &result);

    /* allocate entry_id array */
    *child_id_list = MemCalloc(*child_count, sizeof(wagon_t));
    if (*child_id_list == NULL)
        return DB_NO_MEMORY;

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
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Can't alloc enough memory (%d bytes)",
                    path_len );
        rc = DB_NO_MEMORY;
        goto array_free;
    }

    for (i = 0; i < *child_count; i++)
    {
        char *res[128]; /* 128 fields per row is large enough */
        rc = db_next_record(&p_mgr->conn, &result, res, 128);
        if ( rc )
            goto array_free;

        /* copy id to array */
        pk2entry_id(p_mgr, res[0], &((*child_id_list)[i].id));

        /* copy attributes to array */
        if (child_attr_list)
        {
            (*child_attr_list)[i].attr_mask = attr_mask;

            /* first id, then dnames attrs, then main attrs, then annex attrs */
            if (dnames_attrs)
            {
                /* shift of 1 for id */
                rc = result2attrset( T_DNAMES, res + 1, dnames_attrs, &((*child_attr_list)[i]) );
                if ( rc )
                    goto array_free;
            }

            if (main_attrs)
            {
                /* first id, then main attrs, then annex attrs */
                /* shift of 1 for id */
                rc = result2attrset( T_MAIN, res + dnames_attrs + 1, main_attrs, &((*child_attr_list)[i]) );
                if ( rc )
                    goto array_free;
            }

            if (annex_attrs)
            {
                /* shift of main_attrs count */
                rc = result2attrset( T_ANNEX, res + dnames_attrs + main_attrs + 1, annex_attrs,
                                     &((*child_attr_list)[i]) );
                if ( rc )
                    goto array_free;
            }

#ifdef _LUSTRE
            if (stripe_fields(attr_mask))
            {
                if (get_stripe_info( p_mgr, res[0], &ATTR(&(*child_attr_list)[i], stripe_info),
                                     &ATTR(&(*child_attr_list)[i], stripe_items) ))
                {
                    ATTR_MASK_UNSET(&(*child_attr_list)[i], stripe_info);
                    ATTR_MASK_UNSET(&(*child_attr_list)[i], stripe_items);
                }
            }
#endif

            generate_fields(&((*child_attr_list)[i]));

            /* Note: path is properly sized already to not overflow. */
            sprintf(path, "%s/%s", parent_list[0].fullname,
                    (*child_attr_list)[i].attr_values.name);
            (*child_id_list)[i].fullname = strdup(path);
        }
    }

    if (path)
        free(path);
    db_result_free( &p_mgr->conn, &result );
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
    return rc;
}
