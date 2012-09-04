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
char * parent_cond(lmgr_t * p_mgr, char * buff, size_t buffsz, const entry_id_t * parent_list, unsigned int parent_count,
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
        if (entry_id2pk(p_mgr, &parent_list[0], FALSE, PTR_PK(pk)))
            return NULL;
        sprintf(buff, "%sparent_id="DPK, prefix ? prefix : "", pk);
    }
    else
    {
        int i;
        char * curr = buff;
        curr += sprintf(curr, "%sparent_id IN (", prefix ? prefix : "");
        for (i = 0; i < parent_count; i++)
        {
            if (entry_id2pk(p_mgr, &parent_list[i], FALSE, PTR_PK(pk)))
                return NULL;
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
                      const entry_id_t * parent_list, unsigned int parent_count,
                      int attr_mask,
                      entry_id_t ** child_id_list, attr_set_t ** child_attr_list,
                      unsigned int * child_count )
{
    result_handle_t result;
    char *curr;
    int  filter_main = 0;
    int  filter_annex = 0;
    int main_attrs = 0;
    int annex_attrs = 0;
    char query[4096];
    char fieldlist_main[1024] = "";
    char fieldlist_annex[1024] = "";
    char filter_str_main[1024] = "";
    char filter_str_annex[1024] = "";
#define TMPBUFSZ 2048
    char tmp[TMPBUFSZ];
    char * pc;
    int rc, i;

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
        if (dir_filter(p_mgr, p_filter, dummy_str, &dummy_uint) != FILTERDIR_NONE)
        {
            DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Directory filter not supported in %s()", __func__ );
            return DB_NOT_SUPPORTED;
        }

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

    pc = parent_cond(p_mgr, tmp, TMPBUFSZ, parent_list, parent_count, MAIN_TABLE".");
    if (!pc)
        return DB_BUFFER_TOO_SMALL;

    /* no annex table involved */
    curr = query;
    if (!annex_attrs && !filter_annex)
    {
        curr += sprintf(curr, "SELECT "MAIN_TABLE".id%s FROM "MAIN_TABLE
                        " WHERE %s", fieldlist_main, pc);
        if (filter_main)
            curr +=  sprintf(curr, " AND %s", filter_str_main);
    }
    else
    {
        /* filter/attrs on annex */
        curr += sprintf(curr, "SELECT "MAIN_TABLE".id%s%s FROM "MAIN_TABLE" LEFT JOIN "
                        ANNEX_TABLE" ON "MAIN_TABLE".id="ANNEX_TABLE".id WHERE %s",
                        fieldlist_main, fieldlist_annex, pc);
        if (filter_main)
            curr +=  sprintf(curr, " AND %s", filter_str_main);
        if (filter_annex)
            curr +=  sprintf(curr, " AND %s", filter_str_annex);
    }

    rc = db_exec_sql(&p_mgr->conn, query, &result);
    if (rc)
        return rc;

    /* copy result to output structures */
    *child_count = db_result_nb_records(&p_mgr->conn, &result);

    /* allocate entry_id array */
    *child_id_list = MemCalloc(*child_count, sizeof(entry_id_t));
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

    for (i = 0; i < *child_count; i++)
    {
        char *res[128]; /* 128 fields per row is large enough */
        rc = db_next_record(&p_mgr->conn, &result, res, 128);
        if ( rc )
            goto array_free;

        /* copy id to array */
        pk2entry_id(p_mgr, res[0], &((*child_id_list)[i]));

        /* copy attributes to array */
        if (child_attr_list)
        {
            (*child_attr_list)[i].attr_mask = attr_mask;

            if (main_attrs)
            {
                /* first id, then main attrs, then annex attrs */
                /* shift of 1 for id */
                rc = result2attrset( T_MAIN, res + 1, main_attrs, &((*child_attr_list)[i]) );
                if ( rc )
                    goto array_free;
            }

            if (annex_attrs)
            {
                /* shift of main_attrs count */
                rc = result2attrset( T_ANNEX, res + main_attrs + 1, annex_attrs,
                                     &((*child_attr_list)[i]) );
                if ( rc )
                    goto array_free;
            }

            if (stripe_fields(attr_mask))
            {
                if (get_stripe_info( p_mgr, res[0], &ATTR(&(*child_attr_list)[i], stripe_info),
                                     &ATTR(&(*child_attr_list)[i], stripe_items) ))
                {
                    ATTR_MASK_UNSET(&(*child_attr_list)[i], stripe_info);
                    ATTR_MASK_UNSET(&(*child_attr_list)[i], stripe_items);
                }
            }

            generate_fields(&((*child_attr_list)[i]));
        }
    }

    db_result_free( &p_mgr->conn, &result );
    return 0;

array_free:
    if (child_attr_list && *child_attr_list)
    {
        MemFree(*child_attr_list);
        *child_attr_list = NULL;
    }
    MemFree(*child_id_list);
    *child_id_list = NULL;
    return rc;
}
