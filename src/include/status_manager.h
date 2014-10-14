/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file  status_manager.h
 * \brief status manager definitions
 */

#ifndef _STATUS_MGR_H
#define _STATUS_MGR_H

#include "list_mgr.h"
#include "policy_rules.h"

struct sm_instance;

/** function prototype to get the status of an entry. */
typedef int (*sm_status_func_t)(struct sm_instance *smi,
                             const entry_id_t *id, const attr_set_t *attrs,
                             attr_set_t *refreshed_attrs);

/** function prototype for changelog callback */
typedef int (*sm_cl_cb_func_t)(struct sm_instance *smi,
                               const CL_REC_TYPE *logrec,
                               const entry_id_t *id, const attr_set_t *attrs,
                               attr_set_t *refreshed_attrs, bool *getit);

/** function prototype for status manager "executor" */
typedef int (*sm_executor_func_t)(struct sm_instance *smi,
                                  const policy_action_t *action,
 /* arguments for the action : */ const entry_id_t *id, attr_set_t *attrs,
                                  const char *hints, post_action_e *what_after,
                                  db_cb_func_t db_cb_fn, void *db_cb_arg);

/** function prototype to determine if a deleted entry must be inserted to SOFTRM table
 * @return <0 on error, 0 for false, 1 for true.
 */
typedef int (*softrm_filter_func_t)(struct sm_instance *smi,
                                    const entry_id_t *id, const attr_set_t *attrs);

/** Function to undelete an entry.
 *  If multiple status manager can undelete an entry,
 *  the first one, or the best one (option driven) create it,
 *  and the next satatus managers are called with already_recovered = true.
 * @paramo[in] p_old_id old entry id
 * @paramo[in] p_old_attrs old entry attributes (from SOFTRM table)
 * @param [in,out] p_new_id new entry id (already set if already_recovered==true)
 * @param [in,out] p_new_attrs new entry attributes (already set if already_recovered==true)
 * @param [out] p_new_attrs new entry attributes
 */
typedef recov_status_t (*undelete_func_t)(struct sm_instance *smi,
                                          const entry_id_t *p_old_id,
                                          const attr_set_t *p_old_attrs,
                                          entry_id_t *p_new_id,
                                          attr_set_t *p_new_attrs,
                                          bool already_recovered);

typedef int (*init_func_t)(struct sm_instance *smi, int flags);

#define SM_NAME_MAX 128

typedef enum {
    SM_SHARED   = (1<<0), /* indicate the status manager can be shared between policies */
    SM_NODB     = (1<<1), /* the status is not stored in DB */
    SM_DELETED  = (1<<2), /* this status manager can manage deleted entries */
} sm_flags;

/** status manager definition */
typedef struct status_manager {
    const char *name;
    sm_flags    flags;

    /* possible values for status */
    const char ** status_enum;
    unsigned int  status_count;

    /** masks of needed attributes (cached or fresh) to get the status of an entry */
    uint64_t status_needs_attrs_cached;
    uint64_t status_needs_attrs_fresh;

    /** retrieve the status of an entry */
    sm_status_func_t   get_status_func;

    /** callback for changelogs */
    sm_cl_cb_func_t    changelog_cb;

    /** callback for policy actions */
    /// FIXME how to know what action has been done?

    /** If provided, the status manager wraps the action run */
    sm_executor_func_t  executor;

    /* ---- mask and function to manage deleted entries ---- */

    /** needed attributes to determine if the entry is to be moved to softrm */
    uint64_t                softrm_filter_mask;
    /** determine if a deleted entry must be inserted to softrm table */
    softrm_filter_func_t    softrm_filter_func;

    /** mask of attributes to be saved in SOFTRM table (needed to re-create the inode,
     *  schedule the 'remove' policy and recover/rebind the entry). */
    uint64_t                softrm_table_mask;

    /** undelete an entry */
    undelete_func_t         undelete_func; /* NULL if the status manager can't run 'undelete' */

    /* XXX about full disaster recovery: must recreate all metadata (incl. symlinks => need link field)
       not only the entries managed by the policy. */

    /* ---- setup functions ---- */

    /** functions to load Status Manager configuration */
    const mod_cfg_funcs_t  *cfg_funcs;

    /** Initialize status manager resources */
    init_func_t             init_func;

} status_manager_t;

typedef struct sm_instance
{
    /** status manager instance name:
     * If the status manager is shared between policies,
     * it just consists of the status manager name.
     * Else, it is named as the policy.
     * The corresponding DB field name is:
     *    <policy_name(truncated)> + "_status".
     */
    char  *instance_name;
    char  *db_field;
    /** pointer to the status manager definition */
    const status_manager_t *sm;
    /** instance index: useful for status attribute index. */
    unsigned int smi_index;

} sm_instance_t;

extern unsigned int sm_inst_count; /* defined in status_manager.c */

/** allocate status array */
void sm_status_ensure_alloc(char const ***p_tab);
/** free status array */
void sm_status_free(char const ***p_tab);

/** create a status manager instance */
sm_instance_t *create_sm_instance(const char *pol_name,const char *sm_name);

/** get the Nth status manager instance */
sm_instance_t *get_sm_instance(unsigned int n);

/** get the constant string that match the input string */
const char *get_status_str(const status_manager_t *sm, const char *in_str);

/** return the list of allowed status for a status manager */
char *allowed_status_str(const status_manager_t *sm, char *buf, int sz);

int run_all_cl_cb(const CL_REC_TYPE *logrec, const entry_id_t *id,
                  const attr_set_t *attrs, attr_set_t *refreshed_attrs,
                  uint64_t *status_need);


/** return the mask of all status, expect those for deleted entries */
static inline uint64_t all_status_mask(void)
{
    int i;
    uint64_t ret = 0;

    for (i = 0; i < sm_inst_count; i++)
    {
        sm_instance_t *smi = get_sm_instance(i);

        /* is there an information to be stored in DB? */
        if (smi->sm->flags & SM_NODB)
            continue;

        ret |= SMI_MASK(i);
    }

    return ret;
}

/** translate a generic mask SMI_MASK(0) to the given smi index */
static inline uint64_t translate_status_mask(uint64_t mask, unsigned int smi_index)
{
    if (!(mask & SMI_MASK(0)))
        return mask;

    /* remove SMI_MASK(0) and add SMI_MASK(i) */
    return (mask & ~SMI_MASK(0)) | SMI_MASK(smi_index);
}

/** translate a generic mask SMI_MASK(0) to all status mask (except status for deleted entries) */
static inline uint64_t translate_all_status_mask(uint64_t mask)
{
    if (!(mask & SMI_MASK(0)))
        return mask;

    /* remove SMI_MASK(0) and add all status mask */
    return (mask & ~SMI_MASK(0)) | all_status_mask();
}

static inline uint64_t smi_needed_attrs(const sm_instance_t *smi, bool fresh)
{
    if (fresh)
        return translate_status_mask(smi->sm->status_needs_attrs_fresh,
                                     smi->smi_index);
    else
        return translate_status_mask(smi->sm->status_needs_attrs_cached,
                                     smi->smi_index);
}

/** Get attribute mask for status in the given mask */
static inline uint64_t attrs_for_status_mask(uint64_t mask, bool fresh)
{
    int i = 0;
    uint64_t m;
    uint64_t ret = 0;

    for (i = 0, m = (1LL << ATTR_COUNT); i < sm_inst_count; i++, m <<= 1)
    {
        if (mask & m)
        {
            sm_instance_t *smi = get_sm_instance(i);
            if (fresh)
                ret |= translate_status_mask(smi->sm->status_needs_attrs_fresh, i);
            else
                ret |= translate_status_mask(smi->sm->status_needs_attrs_cached, i);
        }
    }
    return ret;
}

static inline uint64_t status_need_fresh_attrs(void)
{
    uint64_t needed = 0;
    int i = 0;
    sm_instance_t *smi;
    while ((smi = get_sm_instance(i)) != NULL)
    {
        needed |= translate_status_mask(smi->sm->status_needs_attrs_fresh, i);
        i++;
    }
    return needed;
}

static inline uint64_t status_allow_cached_attrs(void)
{
    uint64_t needed = 0;
    int i = 0;
    sm_instance_t *smi;
    while ((smi = get_sm_instance(i)) != NULL)
    {
        needed |= translate_status_mask(smi->sm->status_needs_attrs_cached, i);
        i++;
    }
    return needed;
}

static inline bool smi_manage_deleted(sm_instance_t *smi)
{
    if (smi == NULL)
        return false;
    return !!(smi->sm->flags & SM_DELETED); /* the status manager handles file removal */
}

static inline uint64_t sm_softrm_fields(void)
{
    uint64_t       all = 0;
    int            i = 0;
    sm_instance_t *smi;

    while ((smi = get_sm_instance(i)) != NULL)
    {
        if (smi_manage_deleted(smi))
            all |= translate_status_mask(smi->sm->softrm_table_mask, i);
        i++;
    }
    return all;
}

#include "rbh_misc.h"

/** Get name of status in the given mask */
static inline char *name_status_mask(uint64_t mask, char *buf, int sz)
{
    int i = 0;
    uint64_t m;
    buf[0] = '\0';
    char *cur = buf;

    for (i = 0, m = (1LL << ATTR_COUNT); i < sm_inst_count; i++, m <<= 1)
    {
        if (mask & m)
        {
            sm_instance_t *smi = get_sm_instance(i);
            /* append smi name */
            if (!EMPTY_STRING(buf))
            {
                *cur = ',';
                cur++;
            }
            rh_strncpy(cur, smi->instance_name, sz - (ptrdiff_t)(cur - buf));
            cur += strlen(cur);
        }
    }
    return buf;
}

static inline sm_instance_t *smi_by_name(const char *smi_name)
{
    int i = 0;
    sm_instance_t *smi;
    while ((smi = get_sm_instance(i)) != NULL)
    {
        if (!strcmp(smi->instance_name, smi_name))
            return smi;
        i++;
    }
    /* not found */
    return NULL;
}

#endif
