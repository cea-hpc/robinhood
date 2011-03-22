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
 * \file       list_mgr.h
 * \brief      List Manager Module (interface to database).
 * \addtogroup LIST_MANAGER
 * @{
 */

#ifndef _LISTMGR_H
#define _LISTMGR_H

#include <string.h>
#include <stdint.h>

#ifndef TRUE
#define TRUE (1)
#endif

#ifndef FALSE
#define FALSE (0)
#endif

/* database specific types */
#include "db_types.h"
#include "config_parsing.h"
#include "rbh_const.h"

/* error codes */
#define DB_SUCCESS              0
/* XXX 1: unused */
#define DB_NOT_EXISTS           2
#define DB_ALREADY_EXISTS       3
#define DB_END_OF_LIST          4
#define DB_OUT_OF_DATE          5
#define DB_CONNECT_FAILED       6
#define DB_REQUEST_FAILED       7
#define DB_BUFFER_TOO_SMALL     8
#define DB_NO_MEMORY            9
#define DB_ATTR_MISSING        10
#define DB_NOT_SUPPORTED       11
#define DB_INVALID_ARG         12
#define DB_READ_ONLY_ATTR      13
#define DB_NOT_ALLOWED         14


/* Tag in logfile */
#define LISTMGR_TAG     "ListMgr"

/* stripe info for all kind of FS and applications */

typedef unsigned int storage_unit_id_t;

typedef struct stripe_info_t
{
    uint64_t       stripe_size;
    unsigned int   stripe_count;
    char           pool_name[MAX_POOL_LEN];
} stripe_info_t;

typedef struct stripe_items_t
{
    unsigned int   count;
    storage_unit_id_t *stripe_units;             /* list of storage units used for striping */
} stripe_items_t;

/* access pattern for fields in database */
#define INIT_ONLY    0x00000001 /* set at insert only: stored in an annex table (can't be modified) */
#define ANNEX_INFO   0x00000002 /* annex information, rarely accessed: stored in an annex table */
#define FREQ_ACCESS  0x00000004 /* frequently updated, or used as select filter: stored in the main table */
#define GENERATED    0x10000000 /* field not stored in database: generated in SELECT requests (read-only) */

/** type of fields in database */
typedef enum
{
    DB_STRIPE_INFO,  /**< stripe info */
    DB_STRIPE_ITEMS,  /**< stripe items */
    DB_TEXT,    /**< string/text        */
    DB_INT,     /**< signed integer     */
    DB_UINT,    /**< unsigned integer   */
    DB_BIGINT,  /**< 64 bits integer    */
    DB_BIGUINT, /**< 64 bits unsigned integer */
    DB_BOOL     /**< boolean            */
} db_type_t;

typedef union
{
    const char    *val_str;
    int            val_int;
    unsigned int   val_uint;
    long long      val_bigint;
    unsigned long long val_biguint;
    int            val_bool;
} db_type_u;

/** value from DB with the associated type */
typedef struct db_value_t
{
    db_type_t      type;
    db_type_u      value_u;
} db_value_t;

#define DB_IS_NULL( _p_v ) ( ((_p_v)->type == DB_TEXT) && ((_p_v)->value_u.val_str == NULL) )

/** generic function from generating fields: 1rst parameter points to the field
 * to be generated. 2nd parameter is the source field.
 */
typedef int    ( *gen_func_t ) ( void *, const void * );


/** generic field definition for all applications */

typedef struct field_info_t
{
    char          *field_name;
    db_type_t      db_type;
    unsigned int   db_type_size;                 /**< size for strings */
    int            flags;
    off_t          offset;
    int            gen_index;    /* source attr index for generating this info */
    gen_func_t     gen_func;    /* function for automatic generation */
} field_info_t;


/* update set mask and attr value */
#define ATTR_MASK_INIT( _p_set ) ((_p_set)->attr_mask = 0)
#define ATTR_MASK_SET( _p_set, _attr_name ) ((_p_set)->attr_mask |= ATTR_MASK_##_attr_name)
#define ATTR_MASK_UNSET( _p_set, _attr_name ) ((_p_set)->attr_mask &= ~ATTR_MASK_##_attr_name)
#define ATTR_MASK_TEST( _p_set, _attr_name ) ((_p_set)->attr_mask & ATTR_MASK_##_attr_name)
#define ATTR( _p_set, _attr_name ) ((_p_set)->attr_values._attr_name)


/* application specific types:
 * these includes MUST define:
 * - entry_id_t type
 * - entry_id_equal( entry_id_t * p_id1, entry_id_t * p_id2) macro or function
 * - entry_info_t type
 * - field_info_t field_infos[] array
 */
#ifdef _LUSTRE_HSM
#include "lustre_hsm_types.h"
#elif defined(_TMP_FS_MGR)
#include "tmp_fs_mgr_types.h"
#elif defined(_SHERPA)
#include "sherpa_types.h"
#elif defined(_BACKUP_FS)
#include "backup_fs_types.h"
#else
#error "No application was specified"
#endif

/* Hash of prepared statements
 * key = attrset / table / op type
 * value => statement
 */

/* First prime before 64 */
#define PREP_STMT_HASH_SIZE 61

/** operation type */
typedef enum
{
    OP_INSERT,
    OP_UPDATE,
    OP_SELECT,
    OP_DELETE
} db_op_type_t;

/** table switch */
typedef enum
{
    TAB_MAIN,
    TAB_ANNEX,
    TAB_STRIPE_INFO,
    TAB_STRIPE_ITEMS,
    TAB_VARS,
    TAB_IDMAP
} db_tables_t;

/** Item in the prepared statement cache */
typedef struct stmt_cache_item_t
{
    /* keys */
    db_op_type_t   op_type;
    db_tables_t    db_table;
    int            attr_mask;

    /* value */
    prep_stmt_t    stmt;

    /** for chained list */
    struct stmt_cache_item_t *p_next;

} stmt_cache_item_t;

/** Connection related information for a thread */
typedef struct lmgr_t
{
    db_conn_t      conn;
    unsigned int   last_commit;

    /* flag for forcing commit */
    int            force_commit;

    stmt_cache_item_t *prep_cache[PREP_STMT_HASH_SIZE];
} lmgr_t;

/** List manager configuration */
typedef struct lmgr_config_t
{
    db_config_t    db_config;
    unsigned int   commit_behavior;              /* 0: autocommit, 1: commit every transaction, <n>: commit every <n> transactions */
    unsigned int   connect_retry_min;            /* min retry delay when connection is lost */
    unsigned int   connect_retry_max;            /* max retry delay when connection is lost */


    /* XXXXXX TEMPORAIREMENT valeur des options pour l'accounting */
    int user_acct;
    int group_acct;
} lmgr_config_t;

/**
 * Configuration management routines
 * \addtogroup MODULE_CONFIG_FUNCTIONS
 * @{
 */
int            SetDefaultLmgrConfig( void *module_config, char *msg_out );
int            ReadLmgrConfig( config_file_t config, void *module_config,
                               char *msg_out, int for_reload );
int            ReloadLmgrConfig( void *module_config );
int            WriteLmgrConfigTemplate( FILE * output );
int            WriteLmgrConfigDefault( FILE * output );
/** @} */

/* opaque types */
struct lmgr_iterator_t;
struct lmgr_report_t;
struct lmgr_rm_list_t;

/** Options for iterators */
typedef struct lmgr_iter_opt_t
{
    unsigned int   list_count_max;               /* max entries to be returned by iterator or report */
} lmgr_iter_opt_t;

/** Set of attributes for a FS entry */
typedef struct attr_set_t
{
    /** attributes in the structure */
    int            attr_mask;
    /** associated values */
    entry_info_t   attr_values;

} attr_set_t;

/** comparators for filters */
typedef enum
{
    EQUAL,
    NOTEQUAL,
    LESSTHAN,
    MORETHAN,
    LESSTHAN_STRICT,
    MORETHAN_STRICT,
    LIKE,
    UNLIKE
} filter_comparator_t;

/** filter values associated to db_type field in field_infos array */

typedef db_type_u filter_value_t;


/** simple filter definition */
typedef struct lmgr_simple_filter_t
{
    unsigned int   filter_count;

    int           *filter_flags;
    unsigned int  *filter_index;
    filter_comparator_t *filter_compar;
    filter_value_t *filter_value;

    /** for internal memory management */
    unsigned int   prealloc;
} lmgr_simple_filter_t;

/* needed here for defining filters */
#include "policies.h"

/** generic filter type */
typedef struct lmgr_filter_t
{
    enum
    { FILTER_SIMPLE, FILTER_BOOLEXPR } filter_type;
    union
    {
        lmgr_simple_filter_t simple_filter;
        struct bool_node_t *boolean_expr;        /* not supported yet */
    } filter_u;

} lmgr_filter_t;

/* for cleaner code */
#define filter_simple   filter_u.simple_filter
#define filter_boolexpr filter_u.boolean_expr

/** Sort types */
typedef enum
{
    SORT_NONE, /**< no sorting */
    SORT_ASC,  /**< sort from lower value to higher */
    SORT_DESC  /**< sort from higher value to lower */
} sort_order_t;

/** specifies result order */
typedef struct lmgr_sort_type_t
{
    unsigned int   attr_index;
    sort_order_t   order;
} lmgr_sort_type_t;

/* mask of read-only attributes */
extern int     readonly_attr_set;

/* -------- Main functions -------- */

/** Initialize the List Manager */
int            ListMgr_Init( const lmgr_config_t * p_conf );

/** Create a connection to the database for current thread */
int            ListMgr_InitAccess( lmgr_t * p_mgr );

/** Close a connection to the database */
int            ListMgr_CloseAccess( lmgr_t * p_mgr );

/**
 * Set force commit behavior.
 * Default is FALSE.
 */
void           ListMgr_ForceCommitFlag( lmgr_t * p_mgr, int force_commit );

/**
 * Check if the last operation was really committed
 * @return TRUE if the last operation has been commited,
 * @return FALSE if commit is deferred.
 */
int            ListMgr_GetCommitStatus( lmgr_t * p_mgr );


/** 
 * Tests if this entry exists in the database.
 * @param p_mgr pointer to a DB connection
 * @param p_id pointer to an entry identifier
 * @return 1 if entry exists
 * @return 0 if doesnt exist
 * @return a negative value on error
 */
int            ListMgr_Exists( lmgr_t * p_mgr, const entry_id_t * p_id );

/**
 * Check that validator is matching for the given entry.
 * @param p_mgr pointer to a DB connection
 * @param p_id pointer to an entry identifier (including validator)
 * @return DB_OUT_OF_DATE if stripe doesn't match, and remove stripe info.
 * @return DB_NOT_EXISTS  if there is no stripe info available.
 * @return DB_SUCCESS     if stripe is valid.
 */
int            ListMgr_CheckStripe( lmgr_t * p_mgr, const entry_id_t * p_id );

/**
 * Insert new stripe info if it is not known yet,
 * or in case ListMgr_CheckStripe() returned DB_OUT_OF_DATE.
 */
int            ListMgr_SetStripe( lmgr_t * p_mgr, const entry_id_t * p_id,
                                  stripe_info_t * p_stripe_info, stripe_items_t * p_stripe_items );

/**
 * Retrieves an entry from database.
 */
int            ListMgr_Get( lmgr_t * p_mgr, const entry_id_t * p_id, attr_set_t * p_info );

/**
 * Releases resources of an attr set.
 */
void           ListMgr_FreeAttrs( attr_set_t * p_attrs );

/**
 * Inserts a new entry to the database.
 */
int            ListMgr_Insert( lmgr_t * p_mgr, entry_id_t * p_id,
                               const attr_set_t * p_info,
                               int update_if_exists);

/**
 * Modifies an existing entry in the database.
 */
int            ListMgr_Update( lmgr_t * p_mgr, const entry_id_t * p_id,
                               const attr_set_t * p_update_set );

/**
 * Applies a modification to all entries that match the specified filter.
 */
int            ListMgr_MassUpdate( lmgr_t * p_mgr, const lmgr_filter_t * p_filter,
                                   const attr_set_t * p_attr_set );

/**
 * Removes an entry from the database.
 */
int            ListMgr_Remove( lmgr_t * p_mgr, const entry_id_t * p_id );

#ifdef HAVE_RM_POLICY
/**
 * Soft Rm functions.
 * \addtogroup SOFT_RM_FUNCTIONS
 * @{
 */

/**
 * Remove an entry from the main database, and insert it to secondary table
 * for delayed removal.
 * \param real_remove_time time when the entry must be really removed.
 */
int            ListMgr_SoftRemove( lmgr_t * p_mgr, const entry_id_t * p_id,
                                   const char * last_known_path,
#ifdef _BACKUP_FS
                                   const char * bkpath,
#endif
                                   time_t real_remove_time );

/**
 * Soft remove a set of entries according to a filter.
 */
int            ListMgr_MassSoftRemove( lmgr_t * p_mgr, const lmgr_filter_t * p_filter,
                                       time_t real_remove_time );

/**
 * Definitly remove an entry from the delayed removal table.
 */
int ListMgr_SoftRemove_Discard( lmgr_t * p_mgr, const entry_id_t * p_id );

/**
 * Initialize a list of items removed 'softly', sorted by expiration time.
 * \param expired_only if TRUE, return only items whose real removal time is expired.
 */
struct lmgr_rm_list_t * ListMgr_RmList( lmgr_t * p_mgr, int expired_only, lmgr_filter_t * filter );


/**
 * Get next entry to be removed.
 */
int            ListMgr_GetNextRmEntry( struct lmgr_rm_list_t *p_iter,
                                       entry_id_t * p_id,
                                       char * last_known_path,
#ifdef _BACKUP_FS
                                       char * bkpath,
#endif
                                       time_t * soft_rm_time,
                                       time_t * expiration_time );

/**
 * Releases rmlist resources.
 */
void           ListMgr_CloseRmList( struct lmgr_rm_list_t *p_iter );

/** @} */

#endif

#ifdef _BACKUP_FS

#define RECOV_ATTR_MASK ( ATTR_MASK_fullpath | ATTR_MASK_size | ATTR_MASK_owner | \
                          ATTR_MASK_gr_name | ATTR_MASK_last_mod | ATTR_MASK_backendpath | \
                          ATTR_MASK_status | ATTR_MASK_stripe_info )

#define SOFTRM_MASK ( ATTR_MASK_fullpath | ATTR_MASK_backendpath )

/**
 * Filesystem recovery from backup.
 * \addtogroup RECOVERY_FUNCTIONS
 * @{
 */

typedef enum {
    RS_OK          = 0,
    RS_DELTA       = 1,
    RS_NOBACKUP    = 2,
    RS_ERROR       = 3,
    RS_COUNT
} recov_status_t;

typedef struct _lmgr_recov_stat
{
    unsigned int total;

    /* recovery status count */
    unsigned int status_count[RS_COUNT];
    uint64_t     status_size[RS_COUNT];
} lmgr_recov_stat_t;

/* Filesystem recovery functions  */

/**
 *  Initialize a recovery process.
 *  \retval DB_SUCCESS the recovery process successfully started;
 *          the stats indicate the recovery states we can expect.
 *  \retval DB_ALREADY_EXISTS a recovery process already started
 *          and was not properly completed.
 *  \retval error   another error occured.
 */
int ListMgr_RecovInit( lmgr_t * p_mgr, lmgr_recov_stat_t * p_stats );

/**
 * Clear the recovery table.
 * /!\ all previously unrecovered entry will be lost
 */
int ListMgr_RecovReset( lmgr_t * p_mgr );

/**
 *  Continue a recovery process (returns an iterator on entry list),
 *  possibly using the specified filter.
 *  \retval iterator must be release using ListMgr_CloseIterator()
 */
struct lmgr_iterator_t * ListMgr_RecovResume( lmgr_t * p_mgr,
                                              const char * dir_path,
                                              int retry, /* also retry previously errorneous entries */
                                              const lmgr_iter_opt_t * p_opt );


int ListMgr_RecovGetNext( struct lmgr_iterator_t *p_iter,
                          entry_id_t * p_id,
                          attr_set_t * p_info );

int ListMgr_RecovComplete( lmgr_t * p_mgr, lmgr_recov_stat_t * p_stats );

int ListMgr_RecovStatus( lmgr_t * p_mgr, lmgr_recov_stat_t * p_stats );

int ListMgr_RecovSetState( lmgr_t * p_mgr, const entry_id_t * p_id,
                           recov_status_t status );

/** @} */

#elif defined( HAVE_RM_POLICY )
/* only keep fullpath by default */
#define SOFTRM_MASK ( ATTR_MASK_fullpath )
#endif


/**
 * Removes all entries that match the specified filter.
 */
int            ListMgr_MassRemove( lmgr_t * p_mgr, const lmgr_filter_t * p_filter );

/**
 * Function for handling iterators.
 * 
 * \addtogroup ITERATOR_FUNCTIONS
 * @{
 */
/**
 * Retrieves an iterator on entries that match the given filter.
 */
struct lmgr_iterator_t *ListMgr_Iterator( lmgr_t * p_mgr,
                                          const lmgr_filter_t * p_filter,
                                          const lmgr_sort_type_t *
                                          p_sort_type, const lmgr_iter_opt_t * p_opt );

/**
 * Get next entry from iterator.
 */
int            ListMgr_GetNext( struct lmgr_iterator_t *p_iter,
                                entry_id_t * p_id, attr_set_t * p_info );

/**
 * Release iterator resources.
 */
void           ListMgr_CloseIterator( struct lmgr_iterator_t *p_iter );

/** @} */

/**
 * Types and functions for building reports about FS content.
 *
 * \addtogroup REPORT_FUNCTIONS
 * @{
 */

/** type of report that can be done on each attr */
typedef enum
{
    REPORT_MIN = 1,
    REPORT_MAX,
    REPORT_AVG,
    REPORT_SUM,
    REPORT_COUNT,               /* special: no attribute assigned */
    REPORT_COUNT_DISTINCT,
    REPORT_GROUP_BY
} report_type_t;

/** describe a report field */
typedef struct report_field_descr_t
{
    unsigned int   attr_index;
    report_type_t  report_type;
    sort_order_t   sort_flag;

    int            filter;      /**< is there a filter on this value ? */
    filter_comparator_t filter_compar;
    filter_value_t filter_value;

} report_field_descr_t;


/**
 * Builds a report from database.
 */
struct lmgr_report_t *ListMgr_Report( lmgr_t * p_mgr, report_field_descr_t * report_desc_array,
                                      unsigned int report_descr_count,
                                      const lmgr_filter_t * p_filter,
                                      const lmgr_iter_opt_t * p_opt );

/**
 * Get next report entry.
 * @param p_value_count is IN/OUT parameter. IN: size of output array. OUT: nbr of fields set in array.
 */
int            ListMgr_GetNextReportItem( struct lmgr_report_t *p_iter, db_value_t * p_value,
                                          unsigned int *p_value_count );

/**
 * Releases report resources.
 */
void           ListMgr_CloseReport( struct lmgr_report_t *p_iter );

/** @} */

/**
 * Persistant variable management
 *
 * \addtogroup PERSISTENT_VARS_MGMT
 * @{
 */
/*
 * Name of variables stored in database
 */
#define LAST_SCAN_VAR       "LastScan"
#define LAST_PURGE_TIME     "LastPurgeTime"
#define LAST_PURGE_TARGET   "LastPurgeTarget"
#define LAST_PURGE_STATUS   "LastPurgeStatus"
#define LAST_MIGR_TIME     "LastMigrTime"
#define LAST_MIGR_STATUS   "LastMigrStatus"
#define LAST_MIGR_INFO     "LastMigrInfo"
#define FS_PATH_VAR         "FS_Path"
#define USAGE_MAX_VAR       "MaxUsage"
#define SCAN_INTERVAL_VAR   "ScanInterval"
#define NEXT_MAINT_VAR      "NextMaintenance"


#define MAX_VAR_LEN     1024
/**
 *  Gets variable value.
 *  @param value must be at least of size 1024.
 */
int            ListMgr_GetVar( lmgr_t * p_mgr, const char *varname, char *value );

/**
 *  Sets variable value.
 *  @param value size must not exceed 1024.
 */
int            ListMgr_SetVar( lmgr_t * p_mgr, const char *varname, const char *value );

/** @} */


/**
 *  Functions for handling filters
 *
 * \addtogroup FILTER_FUNCTIONS
 * @{
 */

/** Initialize a simple filter structure */
int            lmgr_simple_filter_init( lmgr_filter_t * p_filter );

#define FILTER_FLAG_NOT           0x00000001 /* negation of the test */
#define FILTER_FLAG_OR            0x00000002 /* use OR instead of AND (which is default) */
#define FILTER_FLAG_BEGIN         0x00000004 /* start a section with parenthesis */
#define FILTER_FLAG_END           0x00000008 /* ends a section with parenthesis */
#define FILTER_FLAG_ALLOW_NULL    0x00000010 /* null value is allowed to match the condition */

#define FILTER_FLAG_ALLOC_STR     0x10000000 /* for internal usage */

/** Add a criteria to a simple filter */
int            lmgr_simple_filter_add( lmgr_filter_t * p_filter,
                                       unsigned int attr_index,
                                       filter_comparator_t comparator,
                                       filter_value_t value,
                                       int flag );

/** Add a criteria to a simple filter or modify it if it already exists in the filter */
int            lmgr_simple_filter_add_or_replace( lmgr_filter_t * p_filter,
                                                  unsigned int attr_index,
                                                  filter_comparator_t comparator,
                                                  filter_value_t value,
                                                  int flag );

/** Add a criteria to a simple filter if it does not already exist in the filter */
int lmgr_simple_filter_add_if_not_exist( lmgr_filter_t * p_filter,
                                         unsigned int attr_index,
                                         filter_comparator_t comparator, filter_value_t value,
                                         int flag );

/** release a filter structure */
int            lmgr_simple_filter_free( lmgr_filter_t * p_filter );


/** Convert simple expressions to ListMgr filter (append filter) */
int convert_boolexpr_to_simple_filter( struct bool_node_t * boolexpr, lmgr_filter_t * filter );


/** Set a complex filter structure */
int            lmgr_set_filter_expression( lmgr_filter_t * p_filter, struct bool_node_t *boolexpr );

/** Check that all fields in filter are in the given mask of supported attributes */
int lmgr_check_filter_fields( lmgr_filter_t * p_filter, unsigned int attr_mask );


/** @} */

/**
 * If p_target_attrset attributes are unset,
 * retrieve them from p_source_attrset.
 * \param update if the attribute is set in both src and tgt,
 *        this boolean indicates if it must be updated in the target.
 */
void           ListMgr_MergeAttrSets( attr_set_t * p_target_attrset,
                                      attr_set_t * p_source_attrset,
                                      int update );


/**
 * Stats functions
 */
void           dump_prep_stmt_stats( unsigned int index, lmgr_t * pmgr );

/**
 * Generate fields automatically from already existing fields,
 * and check the target mask is satisfied.
 */
int  ListMgr_GenerateFields( attr_set_t * p_set, int target_mask );

#endif

/** @} */
