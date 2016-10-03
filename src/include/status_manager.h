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
#include "rbh_cfg_helpers.h"

struct sm_instance;

/** function prototype to get the status of an entry. */
typedef int (*sm_status_func_t)(struct sm_instance *smi,
                                const entry_id_t *id,
                                const attr_set_t *attrs,
                                attr_set_t *refreshed_attrs);

/**
 * Changelog callback can indicate an  action for the record or the related
 * entry. Actions are ordered by priority (if a policy returns a higher value
 * than others, this corresponding action is undertaken).
 */
typedef enum {
    PROC_ACT_NONE = 0,         /* no specific action */
    PROC_ACT_RM_ALL,           /* remove entry from DB */
    PROC_ACT_SOFTRM_IF_EXISTS, /* soft remove the entry if it was in DB */
    PROC_ACT_SOFTRM_ALWAYS,    /* insert into SOFTRM even if it was not in DB */
} proc_action_e;

#ifdef HAVE_CHANGELOGS
/** function prototype for changelog callback */
typedef int (*sm_cl_cb_func_t)(struct sm_instance *smi,
                               const CL_REC_TYPE * logrec,
                               const entry_id_t *id, const attr_set_t *attrs,
                               attr_set_t *refreshed_attrs, bool *getit,
                               proc_action_e *rec_action);
#endif

/** function prototype for status manager "executor" */
typedef int (*sm_executor_func_t)(struct sm_instance *smi,
                                  const char *implements,
                                  const policy_action_t *action,
                                  /* arguments for the action : */
                                  const entry_id_t *id, attr_set_t *attrs,
                                  const action_params_t *params,
                                  post_action_e *what_after,
                                  db_cb_func_t db_cb_fn, void *db_cb_arg);

/** function prototype for action callbacks
 * @param[in,out] smi        Status manager instance
 * @param[in]     implements Action type name
 * @param[in]     id         The impacted entry id
 * @param[in,out] attrs      Entry attributes
 * @param[in,out] what_after What to do with the entry (already set by the
 *                           action, but can be overriden in this action
 *                           callback).
 */
typedef int (*sm_action_cb_func_t)(struct sm_instance *smi,
                                   const char *implements, int action_status,
                                   const entry_id_t *id, attr_set_t *attrs,
                                   post_action_e *what_after);

/** When an entry is deleted, this function indicates what action is to be taken
 * regarding the given status manager (remove from DB, move to softrm, ...)
 */
typedef proc_action_e (*softrm_filter_func_t)(struct sm_instance *smi,
                                              const entry_id_t *id,
                                              const attr_set_t *attrs);

/** Function to undelete an entry.
 *  If multiple status manager can undelete an entry,
 *  the first one, or the best one (option driven) create it,
 *  and the next satatus managers are called with already_recovered = true.
 * @paramo[in] p_old_id old entry id
 * @paramo[in] p_old_attrs old entry attributes (from SOFTRM table)
 * @param [in,out] p_new_id new entry id (already set if
 *                          already_recovered==true)
 * @param [in,out] p_new_attrs new entry attributes (already set if
 *                             already_recovered==true)
 * @param [out] p_new_attrs new entry attributes
 */
typedef recov_status_t (*undelete_func_t)(struct sm_instance *smi,
                                          const entry_id_t *p_old_id,
                                          const attr_set_t *p_old_attrs,
                                          entry_id_t *p_new_id,
                                          attr_set_t *p_new_attrs,
                                          bool already_recovered);

typedef int (*init_func_t)(struct sm_instance *smi, run_flags_t flags);

#define SM_NAME_MAX 128

typedef enum {
    SM_SHARED       = (1 << 0), /**< indicate the status manager can be shared
                                     between policies */
    SM_DELETED      = (1 << 1), /**< this status manager can manage deleted
                                     entries */
    SM_MULTI_ACTION = (1 << 2)  /**< this status manager handles multiple type
                                     of actions */
} sm_flags;

/** descriptor of SM specific info */
typedef struct sm_info_def {
    const char     *user_name;  /**< full name for user interface (config,
                                     display...) */
    const char     *db_name;    /**< short name for db storage */
    db_type_e       db_type;
    unsigned int    db_type_size; /**< size for strings */
    db_type_u       db_default;   /**< default value */
    cfg_param_type  crit_type;    /**< type for config criteria */
} sm_info_def_t;

/** Status manager definition */
typedef struct status_manager {
    const char          *name;
    sm_flags             flags;

    /** possible values for status */
    const char         **status_enum;
    unsigned int         status_count;

    /** number of policy specific information */
    unsigned int         nb_info;
    /** type and size of policy specific information */
    const sm_info_def_t *info_types;

    /** masks of needed attributes (cached or fresh) to get the status of an entry */
    attr_mask_t          status_needs_attrs_cached;
    attr_mask_t          status_needs_attrs_fresh;

    /** retrieve the status of an entry */
    sm_status_func_t     get_status_func;

#ifdef HAVE_CHANGELOGS
    /** callback for changelogs */
    sm_cl_cb_func_t      changelog_cb;
#endif

    /** for multi-action status managers, check the status manager knowns
     * the given action name */
      bool (*check_action_name)(const char *);

    /** callback for policy actions (action_name) */
    sm_action_cb_func_t  action_cb;

    /** If provided, the status manager wraps the action run */
    sm_executor_func_t   executor;

    /* ---- mask and function to manage deleted entries ---- */

    /** needed attributes to determine if the entry is to be moved to softrm */
    attr_mask_t          softrm_filter_mask;
    /** determine if a deleted entry must be inserted to softrm table */
    softrm_filter_func_t softrm_filter_func;

    /** mask of attributes to be saved in SOFTRM table (needed to re-create the
     * inode, schedule the 'remove' policy and recover/rebind the entry). */
    attr_mask_t          softrm_table_mask;

    /** undelete an entry */
    undelete_func_t      undelete_func;  /**< NULL if the status manager can't
                                              run 'undelete' */

    /* XXX about full disaster recovery: must recreate all metadata
     * (incl. symlinks => need link field)
     * not only the entries managed by the policy. */

    /* ---- setup functions ---- */

    /** functions to load Status Manager configuration */
    const mod_cfg_funcs_t *cfg_funcs;

    /** Initialize status manager resources */
    init_func_t          init_func;

} status_manager_t;

/** Status manager instance.
 * There can be one instance of a status manager
 * by policy, in the case status manager is not shared.
 */
typedef struct sm_instance {
    /** status manager instance name:
     * If the status manager is shared between policies,
     * it just consists of the status manager name.
     * Else, it is named as the policy.
     * The corresponding DB field name is:
     *    <policy_name(truncated)> + "_status".
     */
    char            *instance_name;
    /** name of the related field in DB, using for storing status. */
    char            *db_field;
    /** name for user interface (config, reports...) */
    char            *user_name;
    /** pointer to the status manager definition */
    const status_manager_t *sm;
    /** instance index: useful for status attribute index. */
    unsigned int     smi_index;

    /** offset of specific info in attr_set_t.sm_info array */
    unsigned int     sm_info_offset;

    /** translated masks to get status */
    attr_mask_t      status_mask_fresh;
    attr_mask_t      status_mask_cached;

    /** translated mask for softrm filter */
    attr_mask_t      softrm_filter_mask;

    /** translated mask to insert into SOFTRM table */
    attr_mask_t      softrm_table_mask;

    /** status manager global context */
    void            *context;

} sm_instance_t;

/** number of loaded status manager instances */
extern unsigned int sm_inst_count;  /* defined in 'status_manager.c' */

/** number of status manager specific informations */
extern unsigned int sm_attr_count;

static inline bool is_std_attr(unsigned int index)
{
    return (((index & ATTR_INDEX_FLG_MASK) == 0) && (index < ATTR_COUNT));
}

static inline bool is_status(unsigned int index)
{
    return ((index & ATTR_INDEX_FLG_STATUS) &&
            (attr2status_index(index) < sm_inst_count));
}

static inline bool is_sm_info(unsigned int index)
{
    return ((index & ATTR_INDEX_FLG_SMINFO) &&
            (attr2sminfo_index(index) < sm_attr_count));
}

/** pointers to SM specific information */
struct _sm_attr_info {
    const char          *db_attr_name;
    const char          *user_attr_name;
    const sm_info_def_t *def;
    sm_instance_t       *smi;
};
extern struct _sm_attr_info *sm_attr_info;

/** allocate status array */
void sm_status_ensure_alloc(char const ***p_tab);
/** free status array */
void sm_status_free(char const ***p_tab);

/** allocate sm_info array */
void sm_info_ensure_alloc(void ***p_tab);
/** free info array */
void sm_info_free(void ***p_tab);

/** create a status manager instance */
sm_instance_t *create_sm_instance(const char *pol_name, const char *sm_name);

/** get the Nth status manager instance */
sm_instance_t *get_sm_instance(unsigned int n);

/** wraps config handlers for all status managers */
extern mod_cfg_funcs_t smi_cfg_hdlr;

/** set status and attribute masks of status manager instances,
 * once they are all loaded */
void smi_update_masks(void);

/** initialize all status managers (if they have init functions)
 * @param flags daemon runtime flags
 */
int smi_init_all(run_flags_t flags);

/** get the constant string that matches the input string
 * @param[in] sm status manager that manages the matched status name
 * @param[in] in_str status name to match
 */
const char *get_status_str(const status_manager_t *sm, const char *in_str);

/** return the list of allowed statuses for a status manager
 * (to be displayed in command help).
 * @param[in]     sm   status manager to query for its status list
 * @param[in,out] buf  buffer to write status list string
 * @param[in]     sz   buffer size
 * @return buf
 */
char *allowed_status_str(const status_manager_t *sm, char *buf, int sz);

#ifdef HAVE_CHANGELOGS
/** Call changelog callbacks for all status manager instances
 * @param[in]     logrec   incoming changelog record
 * @param[in]     id       related entry id
 * @param[in]     attrs    related entry attrs (current)
 * @param[out]    refreshed_attrs  updated entry attrs
 * @param[in,out] status_need   points to the mask of needed attributes
 *                              to determine entry status.
 * @param[in]     status_mask   mask of status managers that apply to the entry
 *                              (determined by policy scopes).
 * @param[out]    post_action  action to take with this changelog record or
 *                             the related entry.
 */
int run_all_cl_cb(const CL_REC_TYPE *logrec,
                  const entry_id_t  *id,
                  const attr_set_t  *attrs,
                  attr_set_t        *refreshed_attrs,
                  attr_mask_t       *status_need,
                  uint32_t           status_mask,
                  proc_action_e     *post_action);
#endif

/** When an entry is deleted, this function indicates what action is to be taken
 * by querying all status manager (remove from DB, move to softrm, ...)
 */
proc_action_e match_all_softrm_filters(const entry_id_t *id,
                                       const attr_set_t *attrs);

/** return a mask with n bits 1 starting from offset.
 * e.g. bit_range(5,3) = 011100000
 */
static inline uint64_t bit_range(unsigned int offset, unsigned int bits)
{
    return ((1LL << bits) - 1) << offset;
}

/** return the mask of all statuses */
static inline uint32_t all_status_mask(void)
{
    return bit_range(0, sm_inst_count);
}

/** return the mask of all specific info */
static inline uint64_t all_sm_info_mask(void)
{
    return bit_range(0, sm_attr_count);
}

/**
 * As status managers don't know their index instance by advance,
 * they provide generic masks as if there were only their own status and
 * attributes.
 * This macro if a helper for setting a mask of policy-specific attributes.
 */
#define GENERIC_INFO_OFFSET  (0)
#define GENERIC_INFO_BIT(_i) (1LL << (_i))

static inline unsigned int smi_status_index(const sm_instance_t *smi)
{
    return ATTR_INDEX_FLG_STATUS | smi->smi_index;
}

/** return the attribute index of the <n>th status manager specific info */
static inline unsigned int smi_info_index(const sm_instance_t *smi,
                                          unsigned int n)
{
    return ATTR_INDEX_FLG_SMINFO | (smi->sm_info_offset + n);
}

/** return the sm_info mask of <n>th status manager specific info */
static inline uint64_t smi_info_bit(const sm_instance_t *smi, unsigned int n)
{
    return 1LL << (smi->sm_info_offset + n);
}

/** return the attribute mask for all specific info of the status manager */
static inline uint64_t smi_info_bits(const sm_instance_t *smi)
{
    return bit_range(smi->sm_info_offset, smi->sm->nb_info);
}

/** helper to set/overwrite a SM info */
int set_sm_info(const sm_instance_t *smi, attr_set_t *pattrs,
                unsigned int attr_index, void *val);

/** Translate a generic mask SMI_MASK(0) and GENERIC_INFO_OFFSET to all status
 * and info masks */
attr_mask_t translate_all_status_mask(attr_mask_t mask);

/**
 * Return needed attributes to determine entry status for the given
 * status manager instance.
 * @param fresh true, to get the list of attributes that must be up-to-date,
 *              false, to get the list of attribute that can be cached
 *                  (retrieved from DB).
 */
static inline attr_mask_t smi_needed_attrs(const sm_instance_t *smi,
                                           bool fresh)
{
    if (smi == NULL)
        return null_mask;

    if (fresh)
        return smi->status_mask_fresh;
    else
        return smi->status_mask_cached;
}

/**
 * Get attribute mask to get status in the given mask.
 * Note: it doesn't check policy scope, as is its supposed to
 * be checked to build the input mask.
 */
static inline attr_mask_t attrs_for_status_mask(uint32_t status_mask,
                                                bool fresh)
{
    int i = 0;
    uint32_t m;
    attr_mask_t ret = { 0 };

    for (i = 0, m = 1; i < sm_inst_count; i++, m <<= 1) {
        if (status_mask & m) {
            attr_mask_t attr_need = smi_needed_attrs(get_sm_instance(i), fresh);

            ret = attr_mask_or(&ret, &attr_need);
        }
    }

    return ret;
}

/** indicate if the status manager handles file deletion */
static inline bool smi_manage_deleted(sm_instance_t *smi)
{
    if (smi == NULL)
        return false;
    /* the status manager handles file removal */
    return smi->sm->flags & SM_DELETED;
}

/** indicate if the status manager handles several types of actions */
static inline bool smi_multi_action(sm_instance_t *smi)
{
    if (smi == NULL)
        return false;
    /* the status manager handles multiple types of actions */
    return smi->sm->flags & SM_MULTI_ACTION;
}

/** check the status manager knows the given action name */
static inline bool smi_support_action(sm_instance_t *smi, const char *name)
{
    if (smi == NULL || smi->sm == NULL || smi->sm->check_action_name == NULL)
        return false;
    return smi->sm->check_action_name(name);
}

/**
 * Retrieve the mask of attributes to be saved in SOFTRM table for all policies.
 * (needed to re-create the inode, schedule the 'remove' policy,
 * and recover/rebind the entry).
 */
static inline attr_mask_t sm_softrm_fields(void)
{
    attr_mask_t all = null_mask;
    int i = 0;
    sm_instance_t *smi;

    /** XXX based on policies or status managers? what about the scope? */
    while ((smi = get_sm_instance(i)) != NULL) {
        if (smi_manage_deleted(smi)) {
            all = attr_mask_or(&all, &smi->softrm_table_mask);
        }
        i++;
    }
    return all;
}

/**
 * Retrieve the mask of attributes to check if an entry must be saved in SOFTRM table.
 */
static inline attr_mask_t sm_softrm_mask(void)
{
    attr_mask_t all = null_mask;
    int i = 0;
    sm_instance_t *smi;

    /** XXX based on policies or status managers? what about the scope? */
    while ((smi = get_sm_instance(i)) != NULL) {
        if (smi_manage_deleted(smi))
            all = attr_mask_or(&all, &smi->softrm_filter_mask);
        i++;
    }
    return all;
}

/** build a string with the list of statuses in the given mask */
char *name_status_mask(uint32_t status_mask, char *buf, int sz);

/** retrieve a status manager from its name */
sm_instance_t *smi_by_name(const char *smi_name);

/** Search the given attribute name (status or policy specific info).
 * If there is no smi in the context, name must be of the form '<sm_instance_name>.<attr_name>'
 * else, it can be just <attr_name> (implicit sm_instance name).
 * @return   0 on success, < 0 on failure.
 * @retval  -ENOENT if the requested attribute is not set in attributes structure.
 * @retval  -EINVAL if status manager or attr name is invalid.
 */
int sm_attr_get(const sm_instance_t *smi, const attr_set_t *p_attrs,
                const char *name, void **val, const sm_info_def_t **ppdef,
                unsigned int *attr_index);

#endif
