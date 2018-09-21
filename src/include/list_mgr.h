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
#include <stdbool.h>
#include <glib.h>

/* database specific types */
#include "db_types.h"
#include "rbh_const.h"
#include "config_parsing.h"
#include "assert.h"

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
#define DB_TRG_NOT_EXISTS      15
#define DB_DEADLOCK            16
#define DB_BAD_SCHEMA          17
#define DB_NEED_ALTER          18
#define DB_RBH_SIG_SHUTDOWN    19

static inline const char *lmgr_err2str(int err)
{
    switch (err) {
    case DB_SUCCESS:
        return "success";
    case DB_NOT_EXISTS:
        return "entry doesn't exist";
    case DB_ALREADY_EXISTS:
        return "entry already exists";
    case DB_END_OF_LIST:
        return "end of list";
    case DB_OUT_OF_DATE:
        return "obsolete info";
    case DB_CONNECT_FAILED:
        return "connection failure";
    case DB_REQUEST_FAILED:
        return "request error";
    case DB_BUFFER_TOO_SMALL:
        return "buffer is too small";
    case DB_NO_MEMORY:
        return "out of memory";
    case DB_ATTR_MISSING:
        return "missing attribute";
    case DB_NOT_SUPPORTED:
        return "operation not supported";
    case DB_INVALID_ARG:
        return "invalid argument";
    case DB_READ_ONLY_ATTR:
        return "read-only attribute";
    case DB_NOT_ALLOWED:
        return "operation not allowed";
    case DB_TRG_NOT_EXISTS:
        return "trigger doesn't exist";
    case DB_DEADLOCK:
        return "deadlock or timeout";
    case DB_BAD_SCHEMA:
        return "invalid DB schema";
    case DB_NEED_ALTER:
        return "schema needs to be altered";
    case DB_RBH_SIG_SHUTDOWN:
        return "robinhood signal shutdown";
    default:
        return "unknown error";
    }
}

/* Tag in logfile */
#define LISTMGR_TAG     "ListMgr"

typedef enum {
    RS_FILE_OK      = 0, /* non-empty file/symlink can be recovered */
    RS_FILE_DELTA   = 1,  /* file recovered at previous version */
    RS_FILE_EMPTY   = 2,  /* empty file recovered */
    RS_NON_FILE     = 3,    /* non-file recovered */
    RS_NOBACKUP     = 4,    /* entry can't be recovered: no backup */
    RS_ERROR        = 5,   /* recovery error */
    RS_COUNT
} recov_status_t;

#define DB_IS_NULL(_p_v) (((_p_v)->type == DB_TEXT) \
    && ((_p_v)->value_u.val_str == NULL))

#define LIST_SEP_CHAR    '+'
#define LIST_SEP_STR     "+"
/** for use in printf (in case it includes special char) */
#define LIST_SEP_STR_ESC  LIST_SEP_STR

/* String representation in database (not in config file)
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

/* application specific types:
 * these includes MUST define:
 * - entry_id_t type
 * - entry_id_equal( entry_id_t * p_id1, entry_id_t * p_id2) macro or function
 * - entry_info_t type
 * - field_info_t field_infos[] array
 */
#include "db_schema.h"

typedef union {
    const char         *val_str;
    int                 val_int;
    unsigned int        val_uint;
    short               val_short;
    unsigned short      val_ushort;
    long long           val_bigint;
    unsigned long long  val_biguint;
    bool                val_bool;
    entry_id_t          val_id;
} db_type_u;

/** value from DB with the associated type */
typedef struct db_value_t {
    db_type_e  type;
    db_type_u  value_u;
} db_value_t;

/** table switch */
typedef enum {
    TAB_MAIN,
    TAB_ANNEX,
    TAB_STRIPE_INFO,
    TAB_STRIPE_ITEMS,
    TAB_VARS,
    TAB_IDMAP
} db_tables_t;

typedef enum {
    OPIDX_GET,
    OPIDX_INSERT,
    OPIDX_UPDATE,
    OPIDX_RM,

    OPCOUNT
} op_idx_e;

/** Connection related information for a thread */
typedef struct lmgr_t {
    db_conn_t       conn;
    unsigned int    last_commit;   /*< 0 if last operation was committed */
    bool            force_commit;  /*< force commit on next operation */
    unsigned int    retry_delay;   /*< current retry delay */
    unsigned int    retry_count;   /*< nbr of retries */
    struct timeval  first_error; /*< time of first retried error */
    const char     *last_err_func;  /*< function of the last error */
    int             last_err_line;  /*< line of the last error */

    /* operation statistics */
    unsigned int    nbop[OPCOUNT];

} lmgr_t;

/** List manager configuration */
typedef struct lmgr_config_t {
    db_config_t     db_config;
    unsigned int    commit_behavior;   /* 0: autocommit, 1: commit every
                            transaction, <n>: commit every <n> transactions */
    time_t connect_retry_min;   /* min retry delay when connection is lost */
    time_t connect_retry_max;   /* max retry delay when connection is lost */

    /** enable accounting */
    bool            acct;
} lmgr_config_t;

/** config handlers */
extern mod_cfg_funcs_t lmgr_cfg_hdlr;

/** Cancel SQL retries flag.
 * Set in SIGTERM handler.
 */
extern volatile bool lmgr_cancel_retry;

/** indicate if batched requests can be done simultaneously
 * (risk of deadlock on ACCT table).
 */
bool lmgr_parallel_batches(void);

/** Container to associate an ID with its pathname. */
typedef struct wagon {
    entry_id_t   id;
    char        *fullname;
} wagon_t;

/* opaque types */
struct lmgr_iterator_t;
struct lmgr_report_t;
struct lmgr_profile_t;
struct lmgr_rm_list_t;

/** Options for iterators */
typedef struct lmgr_iter_opt_t {
    unsigned int list_count_max;    /* max entries to be returned by iterator or
                                       report */
    unsigned int force_no_acct:1;   /* don't use acct table for reports */
    unsigned int allow_no_attr:1;   /* allow returning entries if no attr is
                                       available */
} lmgr_iter_opt_t;

#define LMGR_ITER_OPT_INIT {.list_count_max = 0, .force_no_acct = 0, \
                            .allow_no_attr = 0}

typedef struct attr_mask {
    uint32_t std;     /**< standard attribute mask */
    uint32_t status;  /**< status attribute mask */
    uint64_t sm_info; /**< attribute mask of status managers info */
} attr_mask_t;

/** helper to display masks */
#define DMASK "%#"PRIX32"/%#"PRIX32"/%#"PRIX64
#define PMASK(_pm) (_pm)->std, (_pm)->status, (_pm)->sm_info

static const attr_mask_t null_mask = { 0 };

/** indicate if no bit is set in the attr mask
 * get the whole mask structure, as it can be convenient
 * to test the output of attr_mask_and.
 */
static inline bool attr_mask_is_null(const attr_mask_t mask)
{
    return (mask.std == 0 && mask.status == 0 && mask.sm_info == 0);
}

/** indicate attrs mask equals */
static inline bool attr_mask_equal(const attr_mask_t *mask1,
                                   const attr_mask_t *mask2)
{
    return (mask1->std == mask2->std) && (mask1->status == mask2->status)
        && (mask1->sm_info == mask2->sm_info);
}

/** logical AND of 2 masks */
static inline attr_mask_t attr_mask_and(const attr_mask_t *mask1,
                                        const attr_mask_t *mask2)
{
    attr_mask_t mask_out;

    mask_out.std = mask1->std & mask2->std;
    mask_out.status = mask1->status & mask2->status;
    mask_out.sm_info = mask1->sm_info & mask2->sm_info;

    return mask_out;
}

/** remove bits from second mask */
static inline attr_mask_t attr_mask_and_not(const attr_mask_t *mask1,
                                            const attr_mask_t *mask2)
{
    attr_mask_t mask_out;

    mask_out.std = mask1->std & ~mask2->std;
    mask_out.status = mask1->status & ~mask2->status;
    mask_out.sm_info = mask1->sm_info & ~mask2->sm_info;

    return mask_out;
}

/** add missing bits from second mask */
static inline attr_mask_t attr_mask_or_not(const attr_mask_t *mask1,
                                           const attr_mask_t *mask2)
{
    attr_mask_t mask_out;

    mask_out.std = mask1->std | ~mask2->std;
    mask_out.status = mask1->status | ~mask2->status;
    mask_out.sm_info = mask1->sm_info | ~mask2->sm_info;

    return mask_out;
}

/** logical OR of 2 masks */
static inline attr_mask_t attr_mask_or(const attr_mask_t *mask1,
                                       const attr_mask_t *mask2)
{
    attr_mask_t mask_out;

    mask_out.std = mask1->std | mask2->std;
    mask_out.status = mask1->status | mask2->status;
    mask_out.sm_info = mask1->sm_info | mask2->sm_info;

    return mask_out;
}

/** part of attr index that contains flags */
#define ATTR_INDEX_FLG_MASK     0xFF000000
/** flags included to index value for status and sm_info */
#define ATTR_INDEX_FLG_STATUS   0x01000000
#define ATTR_INDEX_FLG_SMINFO   0x02000000
/** specific value for getting entry count report */
#define ATTR_INDEX_FLG_COUNT    0x04000000
/** unspecified attribute index */
#define ATTR_INDEX_FLG_UNSPEC   0x08000000

/** convert an attribute index to the index in status array */
static inline unsigned int attr2status_index(unsigned int index)
{
    assert(index & ATTR_INDEX_FLG_STATUS);
    return index & ~ATTR_INDEX_FLG_STATUS;
}

/** convert an attribute index to the index in sm_info array */
static inline unsigned int attr2sminfo_index(unsigned int index)
{
    assert(index & ATTR_INDEX_FLG_SMINFO);
    return index & ~ATTR_INDEX_FLG_SMINFO;
}

/** Set of attributes for a FS entry */
typedef struct attr_set_t {
    /** attributes in the structure */
    attr_mask_t     attr_mask;
    /** associated values */
    entry_info_t    attr_values;
} attr_set_t;

/** static attr_set_t initializer.
 * First item is a structure.
 */
#define ATTR_SET_INIT { {0} }

/** initialize attr mask */
static inline void ATTR_MASK_INIT(attr_set_t *p_set)
{
    memset(&p_set->attr_mask, 0, sizeof(p_set->attr_mask));
}

/** callback function for 'attrs_for_each'
 * the iteration stops if callback function returns < 0
 */
typedef int (*attr_cb_t) (unsigned int attr_index, void *args);

/** iterate on all attributes */
int attrs_for_each(attr_cb_t func, void *args);

/** iterator on attr indexes
 * @param init   initial value of the iteration.
 * @param cookie must initially store -1
 * @return next iterator value, -1 when the loop ends.
 */
int attr_index_iter(unsigned int init, int *cookie);

extern unsigned int sm_inst_count;  /* defined in 'status_manager.c' */
extern unsigned int sm_attr_count;  /* defined in 'status_manager.c' */

static inline bool attr_mask_test_index(const attr_mask_t *p_mask,
                                        unsigned int index)
{
    if (index & ATTR_INDEX_FLG_STATUS) {
        assert(attr2status_index(index) < sm_inst_count);

        /* remove the bit and test in status mask */
        return p_mask->status & (1 << attr2status_index(index));
    } else if (index & ATTR_INDEX_FLG_SMINFO) {
        assert(attr2sminfo_index(index) < sm_attr_count);

        /* remove the bit and test in sm_info mask */
        return p_mask->sm_info & (1LL << attr2sminfo_index(index));
    } else {
        assert(index < (sizeof(p_mask->std) * CHAR_BIT));
        /* test standard mask */
        return p_mask->std & (1 << index);
    }
}

#include <inttypes.h>
static inline void attr_mask_set_index(attr_mask_t *mask, unsigned int index)
{
    if (index & ATTR_INDEX_FLG_STATUS) {
        assert(attr2status_index(index) < sm_inst_count);

        /* remove the flag and set bit in status mask */
        mask->status |= (1 << attr2status_index(index));
    } else if (index & ATTR_INDEX_FLG_SMINFO) {
        assert(attr2sminfo_index(index) < sm_attr_count);

        /* remove the flag and set bit in sm_info mask */
        mask->sm_info |= (1LL << attr2sminfo_index(index));
    } else {
        assert(index < (sizeof(mask->std) * CHAR_BIT));

        /* set standard mask */
        mask->std |= (1 << index);
    }
}

static inline void attr_mask_unset_index(attr_mask_t *mask, unsigned int index)
{
    if (index & ATTR_INDEX_FLG_STATUS) {
        assert(attr2status_index(index) < sm_inst_count);

        /* remove the flag and unset bit in status mask */
        mask->status &= ~(1 << attr2status_index(index));
    } else if (index & ATTR_INDEX_FLG_SMINFO) {
        assert(attr2sminfo_index(index) < sm_attr_count);

        /* remove the flag and unset bit in sm_info mask */
        mask->sm_info &= ~(1LL << attr2sminfo_index(index));
    } else {
        assert(index < (sizeof(mask->std) * CHAR_BIT));

        /* set standard mask */
        mask->std &= ~(1 << index);
    }
}

#define ATTR_MASK_SET(_p_set, _attr_name) \
        ((_p_set)->attr_mask.std |= ATTR_MASK_##_attr_name)
#define ATTR_MASK_UNSET(_p_set, _attr_name) \
        ((_p_set)->attr_mask.std &= ~ATTR_MASK_##_attr_name)
#define ATTR_MASK_TEST(_p_set, _attr_name) \
        !!((_p_set)->attr_mask.std & ATTR_MASK_##_attr_name)
#define ATTR(_p_set, _attr_name) ((_p_set)->attr_values._attr_name)

/* status mask is in a dedicated mask */
#define SMI_MASK(_smi_idx)  (1 << (_smi_idx))
#define ATTR_MASK_STATUS_SET(_p_set, _smi_idx) \
        ((_p_set)->attr_mask.status |= SMI_MASK(_smi_idx))
#define ATTR_MASK_STATUS_UNSET(_p_set, _smi_idx) \
        ((_p_set)->attr_mask.status &= ~ SMI_MASK(_smi_idx))
#define ATTR_MASK_STATUS_TEST(_p_set, _smi_idx) \
        !!((_p_set)->attr_mask.status & SMI_MASK(_smi_idx))
#define STATUS_ATTR(_p_set, _smi_idx) \
        ((_p_set)->attr_values.sm_status[(_smi_idx)])

/* policy specific attributes are in a dedicated mask */
#define ATTR_MASK_INFO_SET(_p_set, _smi, _attr_idx) \
    ((_p_set)->attr_mask.sm_info |= smi_info_bit((_smi), (_attr_idx)))
#define ATTR_MASK_INFO_UNSET(_p_set, _smi, _attr_idx) \
    ((_p_set)->attr_mask.sm_info &= ~smi_info_bit((_smi), (_attr_idx)))
#define ATTR_MASK_INFO_TEST(_p_set, _smi, _attr_idx) \
    !!((_p_set)->attr_mask.sm_info & smi_info_bit((_smi), (_attr_idx)))
#define SMI_INFO(_p_set, _smi, _attr_idx) \
    ((_p_set)->attr_values.sm_info[(_smi)->sm_info_offset+(_attr_idx)])

#define POSIX_ATTR_MASK (ATTR_MASK_size | ATTR_MASK_blocks | ATTR_MASK_uid \
                         | ATTR_MASK_gid | ATTR_MASK_last_access \
                         | ATTR_MASK_last_mod | ATTR_MASK_type \
                         | ATTR_MASK_mode | ATTR_MASK_nlink \
                         | ATTR_MASK_last_mdchange)

/** unset read-only attributes from mask */
void attr_mask_unset_readonly(attr_mask_t *mask);

/** comparators for filters */
typedef enum {
    EQUAL,
    NOTEQUAL,
    LESSTHAN,
    MORETHAN,
    LESSTHAN_STRICT,
    MORETHAN_STRICT,
    LIKE,
    UNLIKE,
    RLIKE,
    ILIKE,      /* case insensitive */
    IUNLIKE,    /* case insensitive */
    IN,
    NOTIN,
    ISNULL,
    NOTNULL,
} filter_comparator_t;

/** filter values associated to db_type field in field_infos array */
typedef struct value_list {
    unsigned int    count;
    db_type_u      *values;
} value_list_t;

typedef union filter_value {
    db_type_u       value;
    value_list_t    list;
} filter_value_t;

#define FV_NULL { {NULL} }

/** simple filter definition */
typedef struct lmgr_simple_filter_t {
    unsigned int         filter_count;

    int                 *filter_flags;
    unsigned int        *filter_index;
    filter_comparator_t *filter_compar;
    filter_value_t      *filter_value;

    /** for internal memory management */
    unsigned int         prealloc;
} lmgr_simple_filter_t;

/* needed here for defining filters, obj_type_t... */
#include "policy_rules.h"

/** string representation in DB */
static const char *type_db_name[] = {
    NULL,
    STR_TYPE_LINK,
    STR_TYPE_DIR,
    STR_TYPE_FILE,
    STR_TYPE_CHR,
    STR_TYPE_BLK,
    STR_TYPE_FIFO,
    STR_TYPE_SOCK
};

static const inline char *type2db(obj_type_t type)
{
    if (type > TYPE_SOCK)
        return type_db_name[TYPE_NONE];

    return type_db_name[type];
}

static inline obj_type_t db2type(const char *str)
{
    obj_type_t i;

    for (i = TYPE_NONE + 1; i <= TYPE_SOCK; i++) {
        if (!strcasecmp(str, type_db_name[i]))
            return i;
    }
    return TYPE_NONE;
}

/** generic filter type */
typedef struct lmgr_filter_t {
    enum { FILTER_SIMPLE, FILTER_BOOLEXPR } filter_type;
    union {
        lmgr_simple_filter_t simple_filter;
        struct bool_node_t *boolean_expr;   /* not supported yet */
    } filter_u;

} lmgr_filter_t;

/* for cleaner code */
#define filter_simple   filter_u.simple_filter
#define filter_boolexpr filter_u.boolean_expr

/** Sort types */
typedef enum {
    SORT_NONE, /**< no sorting */
    SORT_ASC,  /**< sort from lower value to higher */
    SORT_DESC  /**< sort from higher value to lower */
} sort_order_t;

/** specifies result order */
typedef struct lmgr_sort_type_t {
    unsigned int    attr_index;
    sort_order_t    order;
} lmgr_sort_type_t;

/* -------- Main functions -------- */

enum lmgr_init_flags {
    LIF_REPORT_ONLY = (1 << 0), /**< report only, no action on DB schema */
    LIF_ALTER_DB    = (1 << 1), /**< allow altering DB (insert/drop fields) */
    LIF_ALTER_NODISP = (1 << 2), /**< INTERNAL USE ONLY */
};

/** Initialize the List Manager */
int ListMgr_Init(enum lmgr_init_flags flags);

/** Create a connection to the database for current thread */
int ListMgr_InitAccess(lmgr_t *p_mgr);

/** Close a connection to the database */
int ListMgr_CloseAccess(lmgr_t *p_mgr);

/**
 * Set force commit behavior.
 * Default is false;
 */
void ListMgr_ForceCommitFlag(lmgr_t *p_mgr, bool force_commit);

/**
 * Check if the last operation was really committed
 * @return true if the last operation has been committed,
 * @return false if commit is deferred.
 */
bool ListMgr_GetCommitStatus(lmgr_t *p_mgr);

/**
 * Tests if this entry exists in the database.
 * @param p_mgr pointer to a DB connection
 * @param p_id pointer to an entry identifier
 * @return 1 if entry exists
 * @return 0 if doesn't exist
 * @return a negative value on error
 */
int ListMgr_Exists(lmgr_t *p_mgr, const entry_id_t *p_id);

#ifdef _LUSTRE
/**
 * Check that validator is matching for the given entry.
 * @param p_mgr pointer to a DB connection
 * @param p_id pointer to an entry identifier (including validator)
 * @param validator VALID_EXISTS, VALID_NOSTRIPE, or validator value.
 * @return DB_OUT_OF_DATE if stripe doesn't match, and remove stripe info.
 * @return DB_NOT_EXISTS  if there is no stripe info available.
 * @return DB_SUCCESS     if stripe is valid.
 */
#define VALID_EXISTS -2 /* check if the stripe exists */
#define VALID_NOSTRIPE -1   /* check if the stripe is empty */
int ListMgr_CheckStripe(lmgr_t *p_mgr, const entry_id_t *p_id, int validator);

void free_stripe_items(stripe_items_t *p_stripe_items);

#endif

/**
 * Retrieves an entry from database.
 */
int ListMgr_Get(lmgr_t *p_mgr, const entry_id_t *p_id, attr_set_t *p_info);

/**
 * Retrieve the FID from the database given the parent FID and the
 * file name.
 */
int ListMgr_Get_FID_from_Path(lmgr_t *p_mgr, const entry_id_t *parent_fid,
                              const char *name, entry_id_t *fid);

/**
 * Releases resources of an attr set.
 */
void ListMgr_FreeAttrs(attr_set_t *p_attrs);

/**
 * Inserts a new entry to the database.
 */
int ListMgr_Insert(lmgr_t *p_mgr, entry_id_t *p_id,
                   attr_set_t *p_info, bool update_if_exists);
/**
 * Insert a batch of entries into the database.
 * All entries must have the same attr mask.
 */
int ListMgr_BatchInsert(lmgr_t *p_mgr, entry_id_t **p_ids,
                        attr_set_t **p_attrs, unsigned int count,
                        bool update_if_exists);

/**
 * Modifies an existing entry in the database.
 */
int ListMgr_Update(lmgr_t *p_mgr, const entry_id_t *p_id,
                   const attr_set_t *p_update_set);

/**
 * Applies a modification to all entries that match the specified filter.
 */
int ListMgr_MassUpdate(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                       const attr_set_t *p_attr_set);

/** remove callback function */
typedef void (*rm_cb_func_t) (const entry_id_t *);

/**
 * Removes a name from the database. Remove the entry if last is true.
 */
int ListMgr_Remove(lmgr_t *p_mgr, const entry_id_t *p_id,
                   const attr_set_t *p_attr_set, bool last);

/**
 * Removes all entries that match the specified filter.
 */
int ListMgr_MassRemove(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                       rm_cb_func_t);

/**
 * Atomically replace an entry with another, and relink childs in the namespace if needed.
 */
int ListMgr_Replace(lmgr_t *p_mgr, entry_id_t *old_id, attr_set_t *old_attrs,
                    entry_id_t *new_id, attr_set_t *new_attrs,
                    bool src_is_last, bool update_target_if_exists);

/**
 * Soft Rm functions.
 * \addtogroup SOFT_RM_FUNCTIONS
 * @{
 */

/**
 * Remove an entry from the main database, and insert it to secondary table
 * for delayed removal.
 * \param p_old_attrs contains rm_time
 */
int ListMgr_SoftRemove(lmgr_t *p_mgr, const entry_id_t *p_id,
                       attr_set_t *p_old_attrs);

/**
 * Soft remove a set of entries according to a filter.
 */
int ListMgr_MassSoftRemove(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                           time_t rm_time, rm_cb_func_t);

/**
 * Definitely remove an entry from the delayed removal table.
 */
int ListMgr_SoftRemove_Discard(lmgr_t *p_mgr, const entry_id_t *p_id);

/**
 * Initialize a list of items removed 'softly', sorted by expiration time.
 * Selecting 'expired' entries is done using an rm_time criteria in p_filter
 */
struct lmgr_rm_list_t *ListMgr_RmList(lmgr_t *p_mgr, lmgr_filter_t *filter,
                                      const lmgr_sort_type_t *p_sort_type);

/**
 * Get next entry to be removed.
 */
int ListMgr_GetNextRmEntry(struct lmgr_rm_list_t *p_iter,
                           entry_id_t *p_id, attr_set_t *p_attrs);

/**
 * Releases rmlist resources.
 */
void ListMgr_CloseRmList(struct lmgr_rm_list_t *p_iter);

/**
 * Get entry to be removed from its fid.
 */
int ListMgr_GetRmEntry(lmgr_t *p_mgr, const entry_id_t *p_id,
                       attr_set_t *p_attrs);

/** @} */

/**
 * Create a (persitent) table to tag entries.
 * \param filter indicate this applies to a restricted set of entries.
 * \param reset indicate if the table is cleaned in case it already exists.
 */
int ListMgr_CreateTag(lmgr_t *p_mgr, const char *tag_name,
                      lmgr_filter_t *p_filter, bool reset);
/** destroy a tag */
int ListMgr_DestroyTag(lmgr_t *p_mgr, const char *tag_name);

/**
 * Tag an entry (in the set specified by CreateTag filter)
 */
int ListMgr_TagEntry(lmgr_t *p_mgr, const char *tag_name,
                     const entry_id_t *p_id);
/**
 * Return an iterator on non-tagged entries (in the set specified by CreateTag filter)
 */
struct lmgr_iterator_t *ListMgr_ListUntagged(lmgr_t *p_mgr,
                                             const char *tag_name,
                                             const lmgr_iter_opt_t *p_opt);

#ifdef _HSM_LITE

/**
 * Filesystem recovery from backup.
 * \addtogroup RECOVERY_FUNCTIONS
 * @{
 */

typedef struct _lmgr_recov_stat {
    unsigned long long  total;

    /* recovery status count */
    unsigned long long  status_count[RS_COUNT];
    uint64_t            status_size[RS_COUNT];
} lmgr_recov_stat_t;

/* Filesystem recovery functions  */

/**
 *  Initialize a recovery process.
 *  \param p_filter[in] (optional) filter partial filesystem recovery
 *  \retval DB_SUCCESS the recovery process successfully started;
 *          the stats indicate the recovery states we can expect.
 *  \retval DB_ALREADY_EXISTS a recovery process already started
 *          and was not properly completed.
 *  \retval error   another error occurred.
 */
int ListMgr_RecovInit(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                      lmgr_recov_stat_t *p_stats);

/**
 * Clear the recovery table.
 * /!\ all previously unrecovered entries will be lost
 */
int ListMgr_RecovReset(lmgr_t *p_mgr);

/**
 *  List entries by recovery status.
 *  \param st type of entries to be listed
 *  (done, failed, to be done, all)
 */
typedef enum { RT_ALL, RT_TODO, RT_DONE, RT_FAILED } recov_type_e;
struct lmgr_iterator_t *ListMgr_RecovList(lmgr_t *p_mgr, recov_type_e st);

/**
 *  Continue a recovery process (returns an iterator on entry list),
 *  possibly using the specified filter.
 *  \retval iterator must be release using ListMgr_CloseIterator()
 */
struct lmgr_iterator_t *ListMgr_RecovResume(lmgr_t *p_mgr, const char *dir_path,
                       bool retry, /* also retry previously erroneous entries */
                       const lmgr_iter_opt_t *p_opt);

/** @param last_status last status of the entry (-1: not processed yet) */
int ListMgr_RecovGetNext(struct lmgr_iterator_t *p_iter, entry_id_t *p_id,
                         attr_set_t *p_info, recov_status_t *last_status);

int ListMgr_RecovComplete(lmgr_t *p_mgr, lmgr_recov_stat_t *p_stats);

int ListMgr_RecovStatus(lmgr_t *p_mgr, lmgr_recov_stat_t *p_stats);

int ListMgr_RecovSetState(lmgr_t *p_mgr, const entry_id_t *p_id,
                          recov_status_t status);

/** @} */

#elif defined(HAVE_RM_POLICY)
/* only keep fullpath by default */
#if 0
#define SOFTRM_MASK (POSIX_ATTR_MASK | ATTR_MASK_fullpath | ATTR_MASK_rm_time)
POSIX, fullpath, fields with REMOVED flag, fields asked by status manager.
#endif
#endif
/**
 * Function for handling iterators.
 *
 * \addtogroup ITERATOR_FUNCTIONS
 * @{
 */
/**
 * Retrieves an iterator on entries that match the given filter.
 */
struct lmgr_iterator_t *ListMgr_Iterator(lmgr_t *p_mgr,
                                         const lmgr_filter_t *p_filter,
                                         const lmgr_sort_type_t *p_sort_type,
                                         const lmgr_iter_opt_t *p_opt);
/**
 * Get next entry from iterator.
 */
int ListMgr_GetNext(struct lmgr_iterator_t *p_iter,
                    entry_id_t *p_id, attr_set_t *p_info);

/**
 * Release iterator resources.
 */
void ListMgr_CloseIterator(struct lmgr_iterator_t *p_iter);

/** @} */

/**
 * Function for handling namespace and child entries
 *
 * \addtogroup NAMESPACE_FUNCTIONS
 * @{
 */
/**
 * Get the list of children of a given parent (or list of parents).
 * \param parent_list       [in]  list of parents to get the child of
 * \param parent_count      [in]  number of ids in parent list
 * \param child_id_list     [out] array of child ids
 * \param child_attr_list   [out] array of child attrs
 * \param child_count       [out] number of returned children
 *
 * ListMgr_FreeAttrs() must be called on each child attribute
 * and child_id_list and child_attr_list must be freed with MemFree()
 */
int ListMgr_GetChild(lmgr_t *p_mgr, const lmgr_filter_t *p_filter,
                     const wagon_t *parent_list, unsigned int parent_count,
                     attr_mask_t attr_mask,
                     wagon_t **child, attr_set_t **child_attr_list,
                     unsigned int *child_count);

/** @} */

/**
 * Types and functions for building reports about FS content.
 *
 * \addtogroup REPORT_FUNCTIONS
 * @{
 */

/** type of report that can be done on each attr */
typedef enum {
    /* 0 = no specific operation */
    REPORT_MIN = 1,
    REPORT_MAX,
    REPORT_AVG,
    REPORT_SUM,
    REPORT_COUNT,   /* special: no attribute assigned */
    REPORT_COUNT_DISTINCT,
    REPORT_GROUP_BY
} report_type_t;

/** describe a report field */
typedef struct report_field_descr_t {
    int                 attr_index; /* ATTR_INDEX_FLG_COUNT for count */
    report_type_t       report_type;
    sort_order_t        sort_flag;

    bool                filter;   /**< is there a filter on this value ? */
    filter_comparator_t filter_compar;
    filter_value_t      filter_value;
} report_field_descr_t;

/* profile is based on LOG2(size)
 * -> FLOOR(LOG2(size)/5)
 */
#define SZ_PROFIL_COUNT     10
#define SZ_MIN_BY_INDEX(_i) (((_i) == 0) ? 0 : (1LL << (((_i) - 1) * 5)))
/** size profile descriptor */
typedef struct size_range__ {
    uint64_t  min_size;  /* max_size[i] is min_size[i+1] */
    char     *title;
} size_range_t;

static const __attribute__ ((__unused__))
size_range_t size_range[SZ_PROFIL_COUNT] = {
    {SZ_MIN_BY_INDEX(0), "0"},
    {SZ_MIN_BY_INDEX(1), "1~31"},
    {SZ_MIN_BY_INDEX(2), "32~1K-"},
    {SZ_MIN_BY_INDEX(3), "1K~31K"},
    {SZ_MIN_BY_INDEX(4), "32K~1M-"},
    {SZ_MIN_BY_INDEX(5), "1M~31M"},
    {SZ_MIN_BY_INDEX(6), "32M~1G-"},
    {SZ_MIN_BY_INDEX(7), "1G~31G"},
    {SZ_MIN_BY_INDEX(8), "32G~1T-"},
    {SZ_MIN_BY_INDEX(9), "+1T"}
};

/** size profile values */
typedef struct size_profile__ {
    uint64_t    file_count[SZ_PROFIL_COUNT];
} size_profile_t;

typedef union {
    size_profile_t  size;
    /* TODO mtime, ... */
} profile_u;

/** describe a profile field */
typedef struct profile_field_descr_t {
    unsigned int attr_index;

    /* sort range ratio */
    unsigned int range_ratio_start; /* index of selected range ratio */
    unsigned int range_ratio_len;   /* nbr of ranges in the selected range
                                       (0=none) */
    sort_order_t range_ratio_sort;  /* sort order for this range */
} profile_field_descr_t;

/**
 * Builds a report from database.
 */
struct lmgr_report_t *ListMgr_Report(lmgr_t *p_mgr,
        const report_field_descr_t *report_desc_array,
        unsigned int report_descr_count,
        const profile_field_descr_t *profile_desc,   /* optional */
        const lmgr_filter_t *p_filter,
        const lmgr_iter_opt_t *p_opt);

/**
 * Get next report entry.
 * @param p_value_count is IN/OUT parameter. IN: size of output array. OUT: nbr
 * of fields set in array.
 */
int ListMgr_GetNextReportItem(struct lmgr_report_t *p_iter,
                              db_value_t *p_value, unsigned int *p_value_count,
                              profile_u * p_profile);

/**
 * Releases report resources.
 */
void ListMgr_CloseReport(struct lmgr_report_t *p_iter);

/**
 * Get the number of entries in DB.
 */
int ListMgr_EntryCount(lmgr_t *p_mgr, uint64_t *count);

/**
 * Retrieve profile (on size, atime, mtime, ...)
 * (by status, by user, by group, ...)
 * @param profile_descr information about the attribute to be profiled
 * @param report_descr  information about other fields of the report
                        (field to group on and field to sort on)
 * @param report_descr_count number of items in report_descr
 */
struct lmgr_profile_t *ListMgr_Profile(lmgr_t *p_mgr,
                                       const profile_field_descr_t *
                                       profile_descr,
                                       const report_field_descr_t *
                                       report_descr,
                                       unsigned int report_descr_count,
                                       const lmgr_filter_t *p_filter,
                                       const lmgr_iter_opt_t *p_opt);
/**
 * Get next profile entry.
 * @param p_profile the profile structure
 * @param p_value array of values of report_descr
 * @param p_value_count is IN/OUT parameter. IN: size of output array. OUT: nbr of fields set in array.
 */
int ListMgr_GetNextProfile(struct lmgr_profile_t *p_iter,
                           profile_u * p_profile,
                           db_value_t *p_value, unsigned int *p_value_count);

/**
 * Releases profile resources.
 */
void ListMgr_CloseProfile(struct lmgr_profile_t *p_iter);

/** @} */

/**
 * Persistent variable management
 *
 * \addtogroup PERSISTENT_VARS_MGMT
 * @{
 */
/*
 * Name of variables stored in database
 */
#define LAST_POLICY_START_SUFFIX   "_start"
#define LAST_POLICY_END_SUFFIX     "_end"
#define LAST_POLICY_TRIGGER_SUFFIX "_trigger"  /* trigger type and target */
#define LAST_POLICY_STATUS_SUFFIX  "_status" /* status & stats about last run */
#define CURR_POLICY_START_SUFFIX   "_start_current"  /* start of current run */
#define CURR_POLICY_TRIGGER_SUFFIX "_trigger_current" /* trigger of current run
                                                       */

#define FS_PATH_VAR         "FS_Path"
#define ROOT_ID_VAR         "RootId"
#define USAGE_MAX_VAR       "MaxUsage"
#define SCAN_INTERVAL_VAR   "ScanInterval"
#define NEXT_MAINT_VAR      "NextMaintenance"

// Scan statistics
#define LAST_SCAN_START_TIME  "LastScanStartTime"
#define LAST_SCAN_END_TIME    "LastScanEndTime"
#define LAST_SCAN_PROCESSING_END_TIME "LastScanProcessingEndTime"
#define LAST_SCAN_STATUS      "LastScanStatus"
#define LAST_SCAN_LAST_ACTION_TIME "LastScanLastActionTime"
#define LAST_SCAN_ENTRIES_SCANNED  "LastScanEntriesScanned"
#define LAST_SCAN_ERRORS      "LastScanErrors"
#define LAST_SCAN_TIMEOUTS    "LastScanTimeouts"
#define LAST_SCAN_AVGMSPE     "LastScanAvgMsPerEntry"
#define LAST_SCAN_CURMSPE     "LastScanCurMsPerEntry"
#define LAST_SCAN_NB_THREADS  "LastScanNbThreads"

#define PREV_SCAN_START_TIME  "PrevScanStartTime"
#define PREV_SCAN_END_TIME    "PrevScanEndTime"

#define SCAN_STATUS_DONE       "done"
#define SCAN_STATUS_RUNNING    "running"
#define SCAN_STATUS_ABORTED    "aborted"
#define SCAN_STATUS_INCOMPLETE "incomplete"
#define SCAN_STATUS_PARTIAL    "partial"

/* Old changelog statitics */
#define CL_LAST_READ_REC_ID_OLD   "ChangelogLastId"
#define CL_LAST_READ_REC_TIME_OLD "ChangelogLastRecTime"
#define CL_LAST_READ_TIME_OLD     "ChangelogLastTime"
#define CL_DIFF_INTERVAL_OLD      "ChangelogDiffInt"
/* Old CL counters: <prefix>_<event_name> */
#define CL_COUNT_PREFIX_OLD       "ChangelogCount"
#define CL_DIFF_PREFIX_OLD        "ChangelogDiff"
/* format for this one was <prefix>_<mdt_name> */
#define CL_LAST_COMMITTED_OLD     "ChangelogLastCommit"

/* New changelog statitics.
 * Variable name is <name>_<mdt_name>
 * Format of value is rec_id:rec_time(epoch.us):step_time(epoch.us)
 */
#define CL_LAST_READ_REC        "CL_LastRead"
#define CL_LAST_PUSHED_REC      "CL_LastPushed"
#define CL_LAST_COMMITTED_REC   "CL_LastCommit"
#define CL_LAST_CLEARED_REC     "CL_LastCleared"
#define CL_DIFF_INTERVAL        "CL_DiffInt"

/* new CL counters:  <prefix>_<mdt_name>_<event_name> */
#define CL_COUNT_PREFIX         "CL_Count"
#define CL_DIFF_PREFIX          "CL_Diff"

#define MAX_VAR_LEN     1024
/**
 *  Gets variable value.
 */
int ListMgr_GetVar(lmgr_t *p_mgr, const char *varname, char *value,
                   int bufsize);

/**
 *  Sets variable value.
 *  @param value size must not exceed 1024 (size of DB field).
 */
int ListMgr_SetVar(lmgr_t *p_mgr, const char *varname, const char *value);

/** @} */

/**
 *  Functions for handling filters
 *
 * \addtogroup FILTER_FUNCTIONS
 * @{
 */

/** Initialize a simple filter structure */
int lmgr_simple_filter_init(lmgr_filter_t *p_filter);

enum filter_flags {
    FILTER_FLAG_NOT     = (1 << 0), /**< negation of the current test */
    FILTER_FLAG_OR      = (1 << 1), /**< use OR instead of AND
                                         (which is the default) */
    FILTER_FLAG_BEGIN   = (1 << 2), /**<  start a section with parenthesis */
    FILTER_FLAG_END     = (1 << 3), /**< ends a section with parenthesis */
    FILTER_FLAG_NOT_BEGIN = (1 << 4), /**< negate the whole expression until
                                           NOT_END.
                     * NOT_BEGIN is for expressions like: NOT ( <x> ...
                     * and is to be terminated by NOT_END.
                     * whereas BEGIN + NOT will result in (NOT (<x>) ...
                     */
    FILTER_FLAG_NOT_END    = (1 << 5), /**< terminates a NOT_BEGIN */
    FILTER_FLAG_ALLOW_NULL = (1 << 6), /** null value is allowed to match
                                           the condition */

    FILTER_FLAG_ALLOC_STR  = (1 << 7), /** for internal usage: string in filter
                                           is allocated */
    FILTER_FLAG_ALLOC_LIST = (1 << 8), /** for internal usage: list in filter
                                           is allocated */
    FILTER_FLAG_BEGIN_BLOCK = (1 << 9), /**< start a section with parenthesis */
    FILTER_FLAG_END_BLOCK   = (1 << 10), /**< ends a section with parenthesis */
};

/** Add a criteria to a simple filter */
int lmgr_simple_filter_add(lmgr_filter_t *p_filter,
                           unsigned int attr_index,
                           filter_comparator_t comparator,
                           filter_value_t value, enum filter_flags flag);

/* check if the given attribute is part of a filter */
int lmgr_filter_check_field(const lmgr_filter_t *p_filter,
                            unsigned int attr_index);

/**
 * Add a criteria to a simple filter or modify it if it already exists in the
 * filter
 */
int lmgr_simple_filter_add_or_replace(lmgr_filter_t *p_filter,
                                      unsigned int attr_index,
                                      filter_comparator_t comparator,
                                      filter_value_t value,
                                      enum filter_flags flag);

/**
 * Add a criteria to a simple filter if it does not already exist in the filter
 */
int lmgr_simple_filter_add_if_not_exist(lmgr_filter_t *p_filter,
                                        unsigned int attr_index,
                                        filter_comparator_t comparator,
                                        filter_value_t value,
                                        enum filter_flags flag);

/** release a filter structure */
int lmgr_simple_filter_free(lmgr_filter_t *p_filter);

struct sm_instance;
struct time_modifier;

/**
 * Convert simple expressions to ListMgr filter (append filter).
 * Imbrications of AND and OR filters produced by
 * convert_boolexpr_to_simple_filter() are only supported by listmgr_iterators.
 * Callers that use convert_boolexpr_to_simple_filter() must take care not using
 * "OR" expression if they are using other listmgr calls.
 * @param[in]     boolexpr  the boolean expression to be converted.
 * @param[in,out] filter    the output filter to be appended.
 * @param[in]     smi       the current status manager (if any).
 * @param[in]     time_mod  time modifier for maintenance mode.
 * @param[in]     flags     filter flags
 * @param[in]     op_ctx    default boolean operation
 */
int convert_boolexpr_to_simple_filter(struct bool_node_t *boolexpr,
                                      lmgr_filter_t *filter,
                                      const struct sm_instance *smi,
                                      const struct time_modifier *time_mod,
                                      enum filter_flags flags,
                                      bool_op_t op_ctx);

/** Set a complex filter structure */
int lmgr_set_filter_expression(lmgr_filter_t *p_filter,
                               struct bool_node_t *boolexpr);

/**
 * Check that all fields in filter are in the given mask of supported attributes
 * @param index if not NULL, it is set to the index of the unsupported filter.
 *              and -1 for other errors.
 */
int lmgr_check_filter_fields(lmgr_filter_t *p_filter, attr_mask_t attr_mask,
                             int *index);

/** Convert a set notation (eg. "3,5-8,12") to a list of values
 * \param type[in] the type of output array (DB_INT, DB_UINT, ...)
 * \param p_list[out] list of values (the function allocates a buffer for
 *                    p_list->values)
 */
int lmgr_range2list(const char *set, db_type_e type, value_list_t *p_list);

/** @} */

/**
 * If p_target_attrset attributes are unset,
 * retrieve them from p_source_attrset.
 * \param update if the attribute is set in both src and tgt,
 *        this boolean indicates if it must be updated in the target.
 */
void ListMgr_MergeAttrSets(attr_set_t *p_target_attrset,
                           const attr_set_t *p_source_attrset, bool update);

/** return the mask of attributes that differ */
attr_mask_t ListMgr_WhatDiff(const attr_set_t *p_tgt,
                             const attr_set_t *p_src);

/** print attribute value to display to the user
 * @param[in,out] str  Allocated GString to be appended.
 * @param quote string to quote string types (eg. "'").
 * @return 0 on success, a negative value on error.
 */
int ListMgr_PrintAttr(GString *str, db_type_e type,
                      const db_type_u *value_ptr, const char *quote);

/**
 * Same as ListMgr_PrintAttr, except that the value is passed by pointer
 * instead of db_type_u.
 * @param[in,out] str  Allocated GString to be appended.
 * @return 0 on success, a negative value on error.
 */
int ListMgr_PrintAttrPtr(GString *str, db_type_e type, void *value_ptr,
                         const char *quote);

/**
 * Generate fields automatically from already existing fields,
 * and check the target mask is satisfied.
 */
int ListMgr_GenerateFields(attr_set_t *p_set, attr_mask_t target_mask);

/** Check mask compatibility for request batching. */
bool lmgr_batch_compat(attr_mask_t m1, attr_mask_t m2);

/** Add begin or end block. */
int lmgr_simple_filter_add_block(lmgr_filter_t *, enum filter_flags);

/**
 * Check if conditions can be translated to SQL statement for DB query
 */
bool cond2sql_ok(bool_node_t *boolexpr,
                      const struct sm_instance *smi,
                      const struct time_modifier *time_mod);

#endif

/** @} */
