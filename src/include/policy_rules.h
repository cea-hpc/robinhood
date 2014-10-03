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

#include "config_parsing.h"
#include "status_manager.h"
#include <sys/time.h>
#include "list_mgr.h"


typedef enum {
    COMP_NONE = 0,              /**<     not set */
    COMP_GRTHAN,                /**<     > */
    COMP_GRTHAN_EQ,             /**<     >= */
    COMP_LSTHAN,                /**<     < */
    COMP_LSTHAN_EQ,             /**<     <= */
    COMP_EQUAL,                 /**<     == */
    COMP_DIFF,                  /**<     != */
    COMP_LIKE,                  /**<     regexp matching */
    COMP_UNLIKE                 /**<     regexp not matching */
} compare_direction_t;

typedef enum {
    CRITERIA_TREE = 0,
    CRITERIA_PATH,
    CRITERIA_FILENAME,
    CRITERIA_TYPE,
    CRITERIA_OWNER,
    CRITERIA_GROUP,
    CRITERIA_SIZE,
    CRITERIA_DEPTH,
    CRITERIA_DIRCOUNT,
    CRITERIA_LAST_ACCESS,
    CRITERIA_LAST_MOD,
#ifdef ATTR_INDEX_last_restore
    CRITERIA_LAST_RESTORE,
#endif
#ifdef ATTR_INDEX_last_archive
    CRITERIA_LAST_ARCHIVE,
#endif
#ifdef ATTR_INDEX_creation_time
    CRITERIA_CREATION,
#endif
#ifdef ATTR_INDEX_rm_time
    CRITERIA_RMTIME,
#endif
#ifdef _LUSTRE
    CRITERIA_POOL,
    CRITERIA_OST,
#endif
    CRITERIA_STATUS,
    /* /!\ str2criteria relies on the fact that CRITERIA_XATTR is the last criteria */
    CRITERIA_XATTR,
} compare_criteria_t;

typedef enum {
    BOOL_ERR = 0,
    BOOL_NOT,
    BOOL_OR,
    BOOL_AND
} bool_op_t;


typedef enum {
    TYPE_NONE = 0,
    TYPE_LINK,
    TYPE_DIR,
    TYPE_FILE,
    TYPE_CHR,
    TYPE_BLK,
    TYPE_FIFO,
    TYPE_SOCK
} obj_type_t;


/* string representation in policies */
static const char *type_cfg_name[] = {"?", "symlink", "directory", "file",
    "char", "block", "fifo", "socket"};

static inline const char *type2str(obj_type_t type)
{
    if (type > TYPE_SOCK)
        return type_cfg_name[TYPE_NONE];

    return type_cfg_name[type];
}

static inline obj_type_t str2type(const char *str)
{
    obj_type_t i;

    for (i = TYPE_NONE; i <= TYPE_SOCK; i++)
    {
        if (!strcasecmp(str, type_cfg_name[i]))
            return i;
    }
    return TYPE_NONE;
}

/* string representation in database (not in config file)
 *
 * When adding a new type, fix the database enum in
 * listmgr_init.c:append_field_def() */
#define STR_TYPE_LINK   "symlink"
#define STR_TYPE_DIR    "dir"
#define STR_TYPE_FILE   "file"
#define STR_TYPE_CHR    "chr"
#define STR_TYPE_BLK    "blk"
#define STR_TYPE_FIFO   "fifo"
#define STR_TYPE_SOCK   "sock"

/* type conversion functions */
const char *policy2lmgr_type(obj_type_t type);
obj_type_t lmgr2policy_type(const char *str_type);

typedef union
{
    char               str[RBH_PATH_MAX]; /* for all conditions based on a string */
    unsigned long long size;              /* for size-based conditions */
    unsigned int       integer;           /* for int base conditions */
    time_t             duration;          /* for last access and last mod condition */
    obj_type_t         type;              /* for conditions based on object type */
} compare_value_t;

/* indicates that the compare triplet is for mathcing any level
 * of directories.
 */
#define CMP_FLG_ANY_LEVEL 0x00000001

/* whitelist rules are defined by a tree of comparators */

/** <attribute> <comparator> <value> triplet */
typedef struct compare_triplet_t
{
    int flags;
    compare_criteria_t crit;
    char               xattr_name[RBH_NAME_MAX]; /* for xattrs */
    compare_direction_t op;
    compare_value_t val;
} compare_triplet_t;

/** Type of boolean expression: unary, binary or criteria */
typedef enum {
    NODE_CONDITION,
    NODE_UNARY_EXPR,
    NODE_BINARY_EXPR
} node_type_t;

/** Recursive definition of a Boolean expression */
typedef struct bool_node_t
{
    node_type_t    node_type;
    union
    {
        compare_triplet_t *condition;            /**< for final condition on any field */
        struct
        {
            bool_op_t      bool_op;              /**< boolean operator */
            struct bool_node_t *expr1;           /**< for unary or binary operators */
            struct bool_node_t *expr2;           /**< for binary operators */

            /* this tag indicates if expressions 1 and 2
             * are allocated by the owner of this structure
             * (boolean expression or set of classes) */
            unsigned int owner:1;
        } bool_expr;
    } content_u;
} bool_node_t;

/** whitelist item is just a boolean expression */
typedef struct whitelist_item_t
{
    bool_node_t    bool_expr;
    uint64_t       attr_mask;   /**< summary of attributes involved in boolean expression */
} whitelist_item_t;

#ifdef HAVE_RMDIR_POLICY

typedef struct rmdir_policy_t
{
    time_t         age_rm_empty_dirs;

    /* ignored directories */
    whitelist_item_t *whitelist_rules;
    unsigned int   whitelist_count;

    /* directory to be removed recursively (even if not empty) */
    whitelist_item_t *recursive_rmdir_rules;
    unsigned int      recursive_rmdir_count;

    uint64_t          global_attr_mask;             /**< minimum set of attributes for checking all rules */
} rmdir_policy_t;

#define NO_DIR_POLICY( p_pol ) (((p_pol)->whitelist_count + (p_pol)->recursive_rmdir_count == 0) && ((p_pol)->age_rm_empty_dirs == 0))


#endif

#define POLICY_NAME_LEN  128
#define RULE_ID_LEN      128
#define FILESET_ID_LEN   128
#define HINTS_LEN       4096

typedef struct action_hint_t
{
    struct policy_descr_t *policy; /* hint for what policy? */
    char                   hint_str[HINTS_LEN]; /* the hints itself */
} action_hint_t;

/* fileset definition */
typedef struct fileset_item_t
{
    char           fileset_id[FILESET_ID_LEN];

    /** condition for files to be in this fileset */
    bool_node_t    definition;
    /** summary of attributes involved in boolean expression */
    uint64_t       attr_mask;

    /* user tunable */
    unsigned int   matchable:1;                /* is the fileset matchable or is it a temporary
                                                * fileset to build another one? */
    /* flags for internal management */
    unsigned int   used_in_policy:1;           /* is the fileset referenced in a policy? */

    /* action hints for policies */
    action_hint_t *action_hints;
    unsigned int   hints_count;
    /** summary of attributes needed to build hints */
    uint64_t       hints_attr_mask;

    /** @TODO aggregation policy */

} fileset_item_t;

/** policy rule */
typedef struct rule_item_t
{
    char           rule_id[RULE_ID_LEN];

    fileset_item_t **target_list;
    unsigned int   target_count;

    /* TODO add support for LUA rules */

    /** condition for purging/migrating files */
    bool_node_t    condition;
    /** summary of attributes involved in boolean expression */
    uint64_t       attr_mask;

    char           action_hints[HINTS_LEN];
    // TODO: implement action per rule
} rule_item_t;

/** list of rules for a policy */
typedef struct policy_rules_t
{
    whitelist_item_t *whitelist_rules;
    unsigned int   whitelist_count;

    fileset_item_t **ignore_list;
    unsigned int   ignore_count;

    rule_item_t   *rules;      /* one of them can be the default policy */
    unsigned int   rule_count;

    /* minimum set of attributes for checking rules and building action_hints */
    uint64_t       run_attr_mask;

} policy_rules_t;

#define NO_POLICY(p_list) (((p_list)->whitelist_count + (p_list)->ignore_count \
                           + (p_list)->rule_count) == 0)

static bool inline has_default_policy(policy_rules_t *list)
{
    int i;

    for (i = 0; i < list->rule_count; i++)
    {
        if (!strcasecmp(list->rules[i].rule_id, "default"))
            return true;
    }
    return false;
}

typedef enum
{
    ACTION_NONE,
    ACTION_FUNCTION,
    ACTION_COMMAND
} action_type_e;

/* what to do with the entry after the policy action.
 * returned by action_function */
typedef enum {
    PA_NONE,
    PA_RM_ONE,
    PA_RM_ALL,
    PA_UPDATE
} post_action_e;

typedef  int (*action_func_t)(const entry_id_t *,attr_set_t *, const char *,
                              post_action_e *after); /* hints */

action_func_t action_name2function(const char *fname);

typedef struct policy_action_t
{
	action_type_e  type;
    union {
	    char            command[RBH_PATH_MAX];
        action_func_t   function;
    } action_u; /* command for ACTION_COMMAND, function for ACTION_FUNCTION, ... */
} policy_action_t ;

#ifdef HAVE_RM_POLICY
typedef struct unlink_policy
{
    bool    hsm_remove; /* feature enabled? */
    time_t  deferred_remove_delay; /* 0=ASAP */
} unlink_policy_t;
#endif

/* ======================================================================
 * Function for managing all policy configuration (migration, purge, unlink)
 * ======================================================================*/

int            set_default_policies(void *module_config, char *msg_out);
int            read_policies(config_file_t config, void *module_config,
                             char *msg_out, bool for_reload);
int            reload_policies(void *module_config);
int            write_policy_template(FILE *output);
int            write_policy_default(FILE *output);

/** policy descriptor */
typedef struct policy_descr_t {
    /** @TODO store policy info a persistent way for later check */
    char                name[POLICY_NAME_LEN];
    bool_node_t         scope;
    uint64_t            scope_mask;
    struct sm_instance *status_mgr;
    policy_action_t     default_action;
    /* attr index of the sort order (e.g. last_mod, creation_time, ...) */
    int    default_lru_sort_attr; /* default value for policy_run_config_t.lru_sort_attr */
    policy_rules_t      rules;
} policy_descr_t;

/* template policy name */
#define TEMPL_POLICY_NAME "rm_tmp"

typedef struct policies_t
{
    policy_descr_t  *policy_list;
    unsigned int     policy_count;
    uint64_t         global_status_mask; // mask for all policies that provide a get_status function

    fileset_item_t  *fileset_list;
    unsigned int     fileset_count;
    uint64_t         global_fileset_mask; // mask for all filesets

    unsigned int     manage_deleted:1; // is there any policy that manages deleted entries?

} policies_t;
extern struct  policies_t policies;

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

#if 0
typedef struct policies_t
{
#ifdef HAVE_PURGE_POLICY
    policy_list_t  purge_policies;
#endif
#ifdef HAVE_MIGR_POLICY
    policy_list_t  migr_policies;
#endif
#ifdef HAVE_RM_POLICY
    unlink_policy_t unlink_policy;
#endif
#ifdef HAVE_RMDIR_POLICY
    rmdir_policy_t rmdir_policy;
#endif
    fileset_list_t filesets;

    updt_policy_t  updt_policy;
} policies_t;
extern policies_t policies;

/* ==============================================
 *  Functions for applying policies to entries
 * ==============================================*/

static inline int is_file_class_defined(void)
{
#ifdef HAVE_PURGE_POLICY
    if ( !NO_POLICY( &policies.purge_policies ) )
        return true;
#endif
#ifdef HAVE_MIGR_POLICY
   if ( !NO_POLICY( &policies.migr_policies ) )
        return true;
#endif

    return false;
}

static inline int is_dir_class_defined(void)
{
#ifdef HAVE_RMDIR_POLICY
    if ( !NO_DIR_POLICY( &policies.rmdir_policy) )
        return true;
#endif
    return false;
}
#endif

int match_classes(const entry_id_t *id, attr_set_t *p_attrs_new,
                  const attr_set_t *p_attrs_cached);

/* return values for matching */
typedef enum {
    POLICY_MATCH,
    POLICY_NO_MATCH,
    POLICY_MISSING_ATTR,
    POLICY_ERR
} policy_match_t;

/** time modifier */
typedef struct time_modifier
{
   double time_factor;
   time_t time_min;
} time_modifier_t;

/** retrieve fileset structure from its name */
fileset_item_t *get_fileset_by_name(const policies_t *policies, const char *name);

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
rule_item_t * class_policy_case(const policy_descr_t *policy,
                                const char *class_id,
                                fileset_item_t **pp_fileset);

/** test if an entry is in policy scope */
policy_match_t match_scope(const policy_descr_t *pol, const attr_set_t *attrs);

//int match_scope_deleted(policy);


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

char          *build_action_hints(const policy_descr_t *policy,
                                  const rule_item_t *policy_case,
                                  const fileset_item_t *fileset,
                                  const entry_id_t *p_entry_id,
                                  const attr_set_t *p_entry_attr);
void           free_action_hints(char *hints);

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
policy_match_t entry_matches(const entry_id_t *p_entry_id, const attr_set_t *p_entry_attr,
                             bool_node_t *p_node, const time_modifier_t *p_pol_mod,
                             const struct sm_instance *smi);

/**
 * Compare 2 boolean expressions
 * @return 1 if expression structure changed.
 * @return 0 if they have the same structure,
 * @return  -1 on error.
 */
int compare_boolexpr(const bool_node_t *expr1, const bool_node_t *expr2);


/**
 * Update the numerical values of a boolean expression.
 * /!\ compare_boolexpr() must have returned 0 (else, unguarantied behavior).
 * @param tgt Boolean expression to be updated
 * @param src Boolean expression to take values from.
 * @return true if expression values have been changed
 * @return false if nothing has been changed
 */
bool update_boolexpr(const bool_node_t *tgt, const bool_node_t *src);


/**
 * Convert criteria to ListMgr data
 * \param p_comp        IN: the condition to be converted
 * \param p_attr_index  OUT: related attribute index
 * \param p_compar      OUT: listmgr comparator
 * \param db_type_u     OUT: value
 * \param p_must_release OUT: set to true if the db_type_u.val_str string must be released
 * \return -1 if this is not a criteria stored in DB.
 */
struct sm_instance;
int criteria2filter(const compare_triplet_t *p_comp, int *p_attr_index,
                    filter_comparator_t *p_compar, filter_value_t *p_value,
                    bool *p_must_release, const struct sm_instance *smi);

#endif
