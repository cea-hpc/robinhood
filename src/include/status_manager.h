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
#include "rbh_misc.h"

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

#define SM_NAME_MAX 128

/** status manager definition */
typedef struct status_manager {
    const char *name;

#define SM_SHARED (1<<0) /* indicate the status manager can be shared between policies */
    int         flags;

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
} status_manager_t;

typedef struct sm_instance
{
    /** status manager instance name:
     * If the status manager is shared between policies,
     * it just consists of the status manager name.
     * Else, it is <status manager name> + <policy name>.
     * The corresponding DB field name is:
     *    <SM name> + <policy_name(truncated)> + "_status".
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

static inline uint64_t all_status_mask(void)
{
    int i;
    uint64_t ret = 0;

    for (i = 0; i < sm_inst_count; i++)
        ret |= SMI_MASK(i);

    return ret;
}

static inline uint64_t status_mask_missing(uint64_t missing)
{
    uint64_t ret = 0;
    int i = 0;
    uint64_t m;
    for (i = 0, m = (1LL << ATTR_COUNT); i < sm_inst_count; i++, m <<= 1)
    {
        if (missing & m)
            ret |= m;
    }
    return ret;
}

/** Get attribute mask for missing statuses */
static inline uint64_t attrs_for_missing_status(uint64_t missing, bool fresh)
{
    int i = 0;
    uint64_t m;
    uint64_t ret = 0;

    for (i = 0, m = (1LL << ATTR_COUNT); i < sm_inst_count; i++, m <<= 1)
    {
        if (missing & m)
        {
            sm_instance_t *smi = get_sm_instance(i);
            if (fresh)
                ret |= smi->sm->status_needs_attrs_fresh;
            else
                ret |= smi->sm->status_needs_attrs_cached;
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
        needed |= smi->sm->status_needs_attrs_fresh;
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
        needed |= smi->sm->status_needs_attrs_cached;
        i++;
    }
    return needed;
}

/** Get attribute mask for missing statuses */
static inline char *name_missing_status(uint64_t missing, char *buf, int sz)
{
    int i = 0;
    uint64_t m;
    buf[0] = '\0';
    char *cur = buf;

    for (i = 0, m = (1LL << ATTR_COUNT); i < sm_inst_count; i++, m <<= 1)
    {
        if (missing & m)
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
#endif
