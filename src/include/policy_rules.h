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
 * \file  policy_rules.h
 * \brief policy rules definition
 */

#ifndef _POLICIES_H
#define _POLICIES_H

#include "rbh_boolexpr.h"
#include "list_mgr.h"
#include "rbh_params.h"
#include <sys/time.h>

/** whitelist item is just a boolean expression */
typedef struct whitelist_item_t {
    bool_node_t     bool_expr;
    attr_mask_t     attr_mask; /**< summary of attributes involved in boolean
                                    expression */
} whitelist_item_t;

#define POLICY_NAME_LEN  128
#define RULE_ID_LEN      128
#define FILESET_ID_LEN   128
#define HINTS_LEN       4096

typedef struct rbh_params action_params_t;

/* fileset definition */
typedef struct fileset_item_t {
    char fileset_id[FILESET_ID_LEN];

    /** condition for files to be in this fileset */
    bool_node_t definition;
    /** summary of attributes involved in boolean expression */
    attr_mask_t attr_mask;

    /* user tunable */
    unsigned int matchable:1;   /* is the fileset matchable or is it a temporary
                                 * fileset to build another one? */
    /* flags for internal management */
    unsigned int used_in_policy:1;  /* is the fileset referenced in a policy? */

    /* action parameters for policies (merged with parameters from "policy" and
     * "rule", and overrides them).
     * Each hash table key is a policy name (lower case),
     * and the associated value is an action_params_t structure. */
    GHashTable *policy_action_params;

    /** @TODO aggregation policy */

} fileset_item_t;

/**
 * Return the action parameters associated to a fileset for the given policy.
 */
action_params_t *get_fileset_policy_params(const fileset_item_t *fileset,
                                           const char *policy_name);

/* what to do with the entry after the policy action.
 * returned by action_function */
typedef enum {
    PA_NONE,
    PA_RM_ONE,
    PA_RM_ALL,
    PA_UPDATE
} post_action_e;

typedef int (*db_cb_func_t) (void *cb_arg, operation_type_e op,
                             const entry_id_t *id, const attr_set_t *attrs);

typedef int (*action_func_t) (const entry_id_t *id, attr_set_t *attrs,
                              const action_params_t *params,
                              post_action_e *what_after, db_cb_func_t db_cb_fn,
                              void *db_cb_arg);

typedef enum {
    ACTION_UNSET, /**< not set */
    ACTION_NONE,  /**< explicit noop */
    ACTION_FUNCTION,
    ACTION_COMMAND
} action_type_e;

struct action_func_info {
    action_func_t call;
    char *name;
};

typedef struct policy_action {
    action_type_e type;
    union {
        char **command;
        struct action_func_info func;
    } action_u; /* command for ACTION_COMMAND,
                 * function for ACTION_FUNCTION, ... */
} policy_action_t;

/** policy rule */
typedef struct rule_item_t {
    char rule_id[RULE_ID_LEN];

    fileset_item_t **target_list;
    unsigned int target_count;

    /** condition for purging/migrating files */
    bool_node_t condition;

    /** if specified, overrides policy defaults */
    policy_action_t action;
    /** merged with default action_params from the policy and overrides them.
     *  merged with fileclass action_params (overridden by them). */
    action_params_t action_params;

    /** attributes involved in condition, action and action_params */
    attr_mask_t attr_mask;

} rule_item_t;

/** list of rules for a policy */
typedef struct policy_rules_t {
    whitelist_item_t   *whitelist_rules;
    unsigned int        whitelist_count;

    fileset_item_t    **ignore_list;
    unsigned int        ignore_count;

    rule_item_t        *rules; /* one of them can be the default policy */
    unsigned int        rule_count;

    /* minimum set of attributes to check rules and build action_params */
    attr_mask_t         run_attr_mask;

} policy_rules_t;

#define NO_POLICY(p_list) (((p_list)->whitelist_count + (p_list)->ignore_count \
                           + (p_list)->rule_count) == 0)

static bool inline has_default_policy(policy_rules_t *list)
{
    int i;

    for (i = 0; i < list->rule_count; i++) {
        if (!strcasecmp(list->rules[i].rule_id, "default"))
            return true;
    }
    return false;
}

/* ======================================================================
 * Function for managing all policy configuration (migration, purge, unlink)
 * ======================================================================*/

/** config handlers */
extern mod_cfg_funcs_t policies_cfg_hdlr;

/** policy descriptor */
typedef struct policy_descr_t {
    /** @TODO store policy info a persistent way for later check */
    char                name[POLICY_NAME_LEN];
    bool_node_t         scope;
    attr_mask_t         scope_mask;

    /* In the case of 'multi-action' status managers,indicate the implemented
     * action. */
    char               *implements;

    /* status of entries for which an action is running...
     * (used to check status of outstanding entries)
     */
    const char         *status_current;
    struct sm_instance *status_mgr;
    policy_action_t     default_action;

    /* attr index of the sort order (e.g. last_mod, creation_time, ...) */
    /* default value for policy_run_config_t.lru_sort_attr */
    unsigned int        default_lru_sort_attr;

    policy_rules_t      rules;

    /* does this policy manage deleted entries? */
    bool                manage_deleted;
} policy_descr_t;

typedef struct policies_t {
    policy_descr_t     *policy_list;
    unsigned int        policy_count;

    /* status mask for all policies that provide a get_status() function */
    attr_mask_t         global_status_mask;

    fileset_item_t     *fileset_list;
    unsigned int        fileset_count;
    attr_mask_t         global_fileset_mask;    /**< mask for all filesets */

    /* is there any policy that manages deleted entries? */
    unsigned int        manage_deleted:1;

} policies_t;
extern struct policies_t policies;

/**
 * Test if a policy exists and gives its index in policies.policy_list.
 * \param[out] index index in the policies.policy_list array.
 */
bool policy_exists(const char *name, int *index);

/** Indicate if any policy manages deleted entries */

static inline bool has_deletion_policy(void)
{
    return !!policies.manage_deleted;
}

/** determine the fileclasses an entry matches for reports (report != no)*/
int match_classes(const entry_id_t *id, attr_set_t *p_attrs_new,
                  const attr_set_t *p_attrs_cached);

/* return values for matching */
typedef enum {
    POLICY_MATCH = 0,
    POLICY_NO_MATCH,
    POLICY_MISSING_ATTR,
    POLICY_ERR
} policy_match_t;

/** time modifier */
typedef struct time_modifier {
    double time_factor;
    time_t time_min;
} time_modifier_t;

/** retrieve fileset structure from its name */
fileset_item_t *get_fileset_by_name(const policies_t *policies,
                                    const char *name);

/** get the first matching policy case for the given file
 *  \param pp_fileset(out) set to the matching fileset
 *         or NULL for the default policy case
 */
rule_item_t *policy_case(const policy_descr_t *policy,
                         const entry_id_t *p_entry_id,
                         const attr_set_t *p_entry_attr,
                         fileset_item_t **pp_fileset);

/** get the policy case for the given fileclass.
 *  \param pp_fileset is set to the matching fileset
 *         or NULL for the default policy case
 */
rule_item_t *class_policy_case(const policy_descr_t *policy,
                               const char *class_id,
                               fileset_item_t **pp_fileset);

/** test if an entry is in policy scope */
policy_match_t match_scope(const policy_descr_t *pol, const entry_id_t *id,
                           const attr_set_t *attrs, bool warn);

/** Add status attributes mask according to all matching policy scopes.
 * @param tolerant If false, display a warning and don't set a status in the
 *                 mask attributes are missing to check the scope.
 *                 If true, set a status in the mask if the entry can't be
 *                 matched against a scope (no warning is issued).
 */
void add_matching_scopes_mask(const entry_id_t *id, const attr_set_t *attr,
                              bool tolerant, uint32_t *mask);

/** @TODO RBHv3 check if all these functions are used */

/**
 * Check if an entry has a chance to be matched in any policy condition.
 * (does not report warnings if attrs are missing).
 * \param pp_fileset(out) the matched fileclass.
 */
policy_match_t policy_match_all(const policy_descr_t *policy,
                                const entry_id_t *p_entry_id,
                                const attr_set_t *p_entry_attr,
                                const time_modifier_t *time_mod,
                                fileset_item_t **pp_fileset);

/* @TODO: for commands only:
 * Some specific strings in parameter values are interpreted and replaced,
 * according to the given entry id and its attributes:
 * {fid}, {path}, {name}, {rule}, {fileclass}, {ost_pool}.
 * For action functions: get these values directly from id and attrs arguments.
 */

/* Check if an entry is whitelisted for the given policy.
 * \param pp_fileset(out) the matched fileclass.
 */
policy_match_t is_whitelisted(const policy_descr_t *policy,
                              const entry_id_t *p_entry_id,
                              const attr_set_t *p_entry_attr,
                              fileset_item_t **fileset);

/** determine if a class is whitelisted for the given policy */
bool class_is_whitelisted(const policy_descr_t *policy, const char *class_id);

/* check if entry matches a boolean expression */
policy_match_t entry_matches(const entry_id_t *p_entry_id,
                             const attr_set_t *p_entry_attr,
                             bool_node_t *p_node,
                             const time_modifier_t *p_pol_mod,
                             const struct sm_instance *smi);

/* read an action params block from config */
int read_action_params(config_item_t param_block, action_params_t *params,
                       attr_mask_t *mask, char *msg_out);

/* parse policy action value from config */
int parse_policy_action(const char *name, const char *value,
                        char **extra, unsigned int extra_cnt,
                        policy_action_t *action,
                        attr_mask_t *mask, char *msg_out);

/**
 * Convert criteria to ListMgr data
 * \param p_comp        IN: the condition to be converted
 * \param p_attr_index  OUT: related attribute index
 * \param p_compar      OUT: listmgr comparator
 * \param db_type_u     OUT: value
 * \param p_must_release OUT: set to true if the db_type_u.val_str string must
 *                            be released.
 * \return -1 if this is not a criteria stored in DB.
 */
struct sm_instance;
int criteria2filter(const compare_triplet_t *p_comp,
                    unsigned int *p_attr_index, filter_comparator_t *p_compar,
                    filter_value_t *p_value, bool *p_must_release,
                    const struct sm_instance *smi,
                    const time_modifier_t *time_mod);

#endif
