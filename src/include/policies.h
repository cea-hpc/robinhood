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
 * \file  policies.h
 * \brief policies management.
 */

#ifndef _POLICIES_H
#define _POLICIES_H

#include "config_parsing.h"
#include "list_mgr.h"
#include <sys/time.h>


typedef enum
{
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

typedef enum
{
    CRITERIA_TREE,
    CRITERIA_PATH,
    CRITERIA_FILENAME,
#ifdef ATTR_INDEX_type
    CRITERIA_TYPE,
#endif
    CRITERIA_OWNER,
    CRITERIA_GROUP,
    CRITERIA_SIZE,
    CRITERIA_LAST_ACCESS,
    CRITERIA_LAST_MOD,
    CRITERIA_DEPTH,
#ifdef ATTR_INDEX_dircount
    CRITERIA_DIRCOUNT,
#endif
#ifdef ATTR_INDEX_last_restore
    CRITERIA_LAST_RESTORE,
#endif
#ifdef ATTR_INDEX_last_archive
    CRITERIA_LAST_ARCHIVE,
#endif
#ifdef ATTR_INDEX_creation_time
    CRITERIA_CREATION,
#endif
    CRITERIA_POOL,
    CRITERIA_OST,
    CRITERIA_XATTR,
    CRITERIA_CUSTOM_CMD
} compare_criteria_t;


typedef enum
{
    BOOL_ERR = 0,
    BOOL_NOT,
    BOOL_OR,
    BOOL_AND
} bool_op_t;


typedef enum
{ TYPE_NONE = 0, TYPE_LINK, TYPE_DIR, TYPE_FILE, TYPE_CHR, TYPE_BLK, TYPE_FIFO, TYPE_SOCK
} obj_type_t;


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
const char * Policy2ListMgrType( obj_type_t type );
obj_type_t ListMgr2PolicyType( const char * str_type );

typedef union
{
    char           str[RBH_PATH_MAX];                    /* for all conditions based on a string */
    unsigned long long size;                     /* for size-based conditions */
    unsigned int   integer;                      /* for int base conditions */
    time_t         duration;                     /* for last access and last mod condition */
    obj_type_t     type;                         /* for conditions based on object type */
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
typedef enum
{
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
    int            attr_mask;                    /**< summary of attributes involved in boolean expression */
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

    int            global_attr_mask;             /**< minimum set of attributes for checking all rules */
} rmdir_policy_t;

#define NO_DIR_POLICY( p_pol ) (((p_pol)->whitelist_count + (p_pol)->recursive_rmdir_count == 0) && ((p_pol)->age_rm_empty_dirs == 0))


#endif

#define POLICY_ID_LEN   128
#define FILESET_ID_LEN  128
#define HINTS_LEN       4096

/* special fileclass name for ignored entries (in database) */
#define CLASS_IGNORED "@ignore@"
#define CLASS_DEFAULT "@default@"

/* fileset definition */
typedef struct fileset_item_t
{
    char           fileset_id[FILESET_ID_LEN];

    /** condition for files to be in this fileset */
    bool_node_t    definition;
    /** summary of attributes involved in boolean expression */
    int            attr_mask;

#ifdef HAVE_PURGE_POLICY
    /* flags for internal management */
    unsigned int   has_purge_policy:1;           /* is the fileset referenced in a purge policy? */
#endif
#ifdef HAVE_MIGR_POLICY
    unsigned int   has_migration_policy:1;       /* is the fileset referenced in a migration policy? */

    /* fileset specific hints for migration */
    char           migration_hints[HINTS_LEN];
#endif

#ifdef _LUSTRE_HSM
    unsigned int archive_id;
#endif

    /** @TODO aggregation policy */

} fileset_item_t;

typedef struct fileset_list_t
{
    fileset_item_t *fileset_list;
    unsigned int   fileset_count;

    int            global_attr_mask;   /**< minimum set of attributes for checking all rules */

} fileset_list_t;

/* migration/purge policy definition */

typedef struct policy_item_t
{
    char           policy_id[POLICY_ID_LEN];

    fileset_item_t **target_list;
    unsigned int   target_count;

    /** condition for purging/migrating files */
    bool_node_t    condition;
    /** summary of attributes involved in boolean expression */
    int            attr_mask;

    char           hints[HINTS_LEN];             /* for migration only */
#ifdef _LUSTRE_HSM
    unsigned int   archive_id;
#endif

} policy_item_t;

/* policy lists*/

typedef struct policy_list_t
{
    whitelist_item_t *whitelist_rules;
    unsigned int   whitelist_count;

    fileset_item_t **ignore_list;
    unsigned int   ignore_count;

    policy_item_t *policy_list;                  /* one of them can be the default policy */
    unsigned int   policy_count;

    /* minimum set of attributes for checking all rules */
    int            global_attr_mask;

} policy_list_t;

#define NO_POLICY( p_list ) ( ((p_list)->whitelist_count + (p_list)->ignore_count + (p_list)->policy_count) == 0 )

static int inline has_default_policy(policy_list_t *list)
{
    int i;
    for (i = 0; i < list->policy_count; i++)
    {
        if (!strcasecmp(list->policy_list[i].policy_id, "default"))
            return TRUE;
    }
    return FALSE;
}

#ifdef HAVE_RM_POLICY
typedef struct unlink_policy
{
    int     hsm_remove; /* feature enabled? */
    time_t  deferred_remove_delay; /* 0=ASAP */
} unlink_policy_t;
#endif

/**
 * path and metadata update strategies
 */
enum updt_policy {
    UPDT_NEVER,     /* get info once, and never refresh it */
    UPDT_ALWAYS,    /* always update info when processing an entry */
    UPDT_ON_EVENT,  /* get info on related event */
    UPDT_ON_EVENT_PERIODIC,  /* default: get info on related event, with a
                              * min interval + periodic update (max interval) */
    UPDT_PERIODIC   /* update info periodically */
};

/**
 * update policy item
 */
typedef struct updt_policy_item_t
{
    enum updt_policy    policy;
    unsigned int        period_min; /* 0=no min */
    unsigned int        period_max; /* 0=no periodic update */
} updt_policy_item_t;

/**
 *  update policy
 */
typedef struct updt_policy_t
{
    updt_policy_item_t  md;
#ifdef _HAVE_FID
    updt_policy_item_t  path;
#endif
    updt_policy_item_t  fileclass; /* only never/always/periodic allowed */
} updt_policy_t;


/* ======================================================================
 * Function for managing all policy configuration (migration, purge, unlink)
 * ======================================================================*/

int            SetDefault_Policies( void *module_config, char *msg_out );
int            Read_Policies( config_file_t config, void *module_config,
                              char *msg_out, int for_reload );
int            Reload_Policies( void *module_config );
int            Write_Policy_Template( FILE * output );
int            Write_Policy_Default( FILE * output );

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
        return TRUE;
#endif
#ifdef HAVE_MIGR_POLICY
   if ( !NO_POLICY( &policies.migr_policies ) )
        return TRUE;
#endif

    return FALSE;
}

static inline int is_dir_class_defined(void)
{
#ifdef HAVE_RMDIR_POLICY
    if ( !NO_DIR_POLICY( &policies.rmdir_policy) )
        return TRUE;
#endif
    return FALSE;
}

/* return values for matching */
typedef enum
{ POLICY_MATCH, POLICY_NO_MATCH, POLICY_MISSING_ATTR, POLICY_ERR } policy_match_t;

/* type of policy */
typedef enum
{
    PURGE_POLICY,
    MIGR_POLICY,                                 /* for HSM purpose only */
    RMDIR_POLICY                                 /* not for lustre HSM */
} policy_type_t;

/** policy modifier */
typedef struct policy_modifier
{
   double time_factor;
   time_t time_min;
} policy_modifier_t;

/** retrieve fileset structure from its name */
fileset_item_t *GetFilesetByName( fileset_list_t * filesets, const char *name );

/** get the first matching policy case for the given file
 *  \param pp_fileset is set to the matching fileset
 *         or NULL for the default policy case
 */
policy_item_t *GetPolicyCase( const entry_id_t * p_entry_id,
                              const attr_set_t * p_entry_attr, policy_type_t policy_type,
                              fileset_item_t ** pp_fileset );

/** get the policy case for the given fileclass.
 *  \param pp_fileset is set to the matching fileset
 *         or NULL for the default policy case
 */
policy_item_t * GetPolicyCaseByClass( const char * class_id,
                                      policy_type_t policy_type,
                                      fileset_item_t ** pp_fileset );

/**
 * Check if an entry has a chance to be matched in any policy condition.
 * (does not report warnings if attrs are missing).
 * p_entry_attr is not read-only because the function may set the matched
 * fileclass.
 */
policy_match_t PolicyMatchAllConditions( const entry_id_t * p_entry_id,
                                         attr_set_t * p_entry_attr,
                                         policy_type_t policy_type,
                                         const policy_modifier_t * p_pol_mod );

#ifdef HAVE_MIGR_POLICY
char          *build_migration_hints( const policy_item_t * policy,
                                      const fileset_item_t * fileset,
                                      const entry_id_t * p_entry_id,
                                      const attr_set_t * p_entry_attr );
void           free_migration_hints( char *hints );
#endif

/* check if an entry is whitelisted for the given policy type,
 * entry attr can be modified to set the matched fileclass.
 */
policy_match_t IsWhitelisted( const entry_id_t * p_entry_id,
                              attr_set_t * p_entry_attr, policy_type_t policy_type );

/** determine if a class is whitelisted for the given policy */
int WhitelistedClass( const char * class_id, policy_type_t policy_type );

/* check if entry matches a boolean expression */
policy_match_t EntryMatches( const entry_id_t * p_entry_id, const attr_set_t * p_entry_attr,
                             bool_node_t * p_node, const policy_modifier_t * p_pol_mod );

/**
 * check whitelist condition for file or directory entries
 * optionnally match fileclasses.
 */
int check_policies( const entry_id_t * p_id, attr_set_t * p_attrs_new,
                    attr_set_t * p_attrs_cached,
                    int match_all_fc );

/**
 *  Check if the fileclass needs to be updated
 */
int need_fileclass_update( const attr_set_t * p_attrs, policy_type_t policy_type );

/**
 *  Check if path or metadata needs to be updated
 *  \param p_allow_event [out] if set to TRUE, the path
 *         must be updated on related event.
 */
typedef enum { UPDT_PATH, UPDT_MD } type_info_t;
int need_info_update( const attr_set_t * p_attrs, int * update_on_event,
                      type_info_t type_info );

#define need_path_update( _pa, _pu )    need_info_update( (_pa), (_pu), UPDT_PATH )
#define need_md_update( _pa, _pu )    need_info_update( (_pa), (_pu), UPDT_MD )

/**
 * Compare 2 boolean expressions
 * @return TRUE if expression structure changed.
 * @return FALSE if they have the same structure,
 * @return  -1 on error.
 */
int compare_boolexpr( const bool_node_t * expr1, const bool_node_t * expr2 );


/**
 * Update the numerical values of a boolean expression.
 * /!\ compare_boolexpr() must have returned 0 (else, unguarantied behavior).
 * @param tgt Boolean expression to be updated
 * @param src Boolean expression to take values from.
 * @return TRUE if expression values have been changed
 * @return FALSE if nothing has been changed
 */
int update_boolexpr( const bool_node_t * tgt, const bool_node_t * src );


/**
 * Convert criteria to ListMgr data
 * \param p_comp        IN: the condition to be converted
 * \param p_attr_index  OUT: related attribute index
 * \param p_compar      OUT: listmgr comparator
 * \param db_type_u     OUT: value
 * \param p_must_release OUT: set to TRUE if the db_type_u.val_str string must be released
 * \return -1 if this is not a criteria stored in DB.
 */
int CriteriaToFilter(const compare_triplet_t * p_comp, int * p_attr_index,
                     filter_comparator_t * p_compar, filter_value_t * p_value,
                     int * p_must_release);

#endif
