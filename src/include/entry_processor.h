/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008-2015 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file   entry_processor.h
 * \author Th. Leibovici
 * \brief  This file describes entry processor types and calls.
 */
/**
 * \addtogroup ENTRY_PROCESSOR
 * @{
 */

#ifndef _ENTRY_PROC_H
#define _ENTRY_PROC_H

#include "list.h"
#include "config_parsing.h"
#include "rbh_boolexpr.h"
#include <stdint.h>
#include "list_mgr.h"

/* Tag for Logs */
#define ENTRYPROC_TAG   "EntryProc"

/* === pipeline stage flags ===  */

/* parallelism */
#define STAGE_FLAG_SEQUENTIAL    0x00000001
#define STAGE_FLAG_FORCE_SEQ     0x00000002 /* temporarily force sequential */
#define STAGE_FLAG_MAX_THREADS   0x00000004 /* hard coded limit */
#define STAGE_FLAG_PARALLEL      0x00000008 /* if set, refer to max threads in
                                               configuration */

/* synchronism */
#define STAGE_FLAG_SYNC          0x00000010
#define STAGE_FLAG_ASYNC         0x00000020

/* constraint for entries with same ID */
#define STAGE_FLAG_ID_CONSTRAINT 0x00000100

/* === common types === */

/* forward declaration */
struct entry_proc_op_t;

/**
 * Definition of pipeline stage functions
 */
typedef int (*step_function_t) (struct entry_proc_op_t *, lmgr_t *lmgr);
typedef int (*step_batch_function_t) (struct entry_proc_op_t **, int count,
                                      lmgr_t *lmgr);

/**
 * Callback to check if 2 operations can be batched together.
 */
typedef bool(*test_batchable_func_t) (struct entry_proc_op_t *,
                                      struct entry_proc_op_t *,
                                      attr_mask_t *full_mask);

/**
 * Definition of a pipeline stage
 */
typedef struct pipeline_stage_t {
    unsigned int        stage_index;      /**< index of this puipeline stage */
    const char         *stage_name;       /**< name of this pipeline stage */
    step_function_t     stage_function;   /**< function for performing the stage */
    step_batch_function_t stage_batch_function;  /**< function for performing
                                            the stage with operation batching */
    test_batchable_func_t test_batchable; /**< function to check if 2 operations
                                             are batchable for the batch step */
    int                 stage_flags;      /**< stage qualifiers */
    unsigned int        max_thread_count; /**< 0 = UNLIMITED */

} pipeline_stage_t;

/**
 * This structure indicates pipeline steps index and limits
 */
typedef struct pipeline_descr_t {
    unsigned int    stage_count;
    unsigned int    GET_ID;
    unsigned int    GET_INFO_DB;
    unsigned int    GET_INFO_FS;
    unsigned int    GC_OLDENT;
    unsigned int    DB_APPLY;
} pipeline_descr_t;

/* pipeline currently in operation */
extern pipeline_stage_t *entry_proc_pipeline;
extern pipeline_descr_t  entry_proc_descr;
extern void             *entry_proc_arg;    /* pipeline specific arguments */

/* include purpose specific pipeline definitions:
 * These includes MUST define:
 * - pipeline_stage_t entry_proc_pipeline[] array
 * - PIPELINE_STAGE_COUNT
 * - op_extra_info_t type
 */
#include "pipeline_types.h"

/**
 * callback function definition
 * @param struct entry_proc_op_t * : the structure associated to the entry
 * @param void * : InfoCollector specific parameter
 */
typedef int (*callback_func_t) (lmgr_t *, struct entry_proc_op_t *, void *);

/**
 * function prototype for freeing  extra_info
 */
typedef void (*free_func_t) (void *);

/** operation submitted to the pipeline */
typedef struct entry_proc_op_t {
    /** current stage in pipeline */
    unsigned int    pipeline_stage;

    /* what is set in this structure ? */
    unsigned int    entry_id_is_set:1;
    unsigned int    db_attr_is_set:1;
    unsigned int    fs_attr_is_set:1;
    unsigned int    extra_info_is_set:1;

    /* entry exists in db */
    unsigned int    db_exists:1;

    /* internal flag for pipeline management */
    unsigned int    being_processed:1;
    unsigned int    id_is_referenced:1;
    unsigned int    name_is_referenced:1;

    /* fid needs to be retrieved from db. This is a workaround for
     * Lustre servers that do not have LU-543. */
    unsigned int    get_fid_from_db:1;

    /* for changelog unlink record only, determine in pipeline if file
     * is last and must be completely removed from DB. This is a
     * workaround for Lustre servers that do not have LU-1331
     * (extended records/CLF_RENAME_LAST). */
    unsigned int    check_if_last_entry:1;

    /* for pipeline flush: indicate if not seen entries must be cleaned */
    unsigned int    gc_entries:1;
    /* for pipeline flush: indicate if not seen paths must be cleaned
     * (preserve entries). Used for partial scans. */
    unsigned int    gc_names:1;

    operation_type_e db_op_type;
    callback_func_t callback_func;
    void           *callback_param;

    /* === Entry information === */
    entry_id_t      entry_id;

    /* list of attrs to be retrieved from DB */
    attr_mask_t     db_attr_need;
    /* list of attrs to be retrieved from FS */
    attr_mask_t     fs_attr_need;

    /* attrs from DB (cached) */
    attr_set_t      db_attrs;
    /* attrs from FS (new) */
    attr_set_t      fs_attrs;
    /* true if the striping in DB is up-to-date (do not require a DB update)*/
    bool            db_stripe_ok;

    op_extra_info_t extra_info;
    free_func_t     extra_info_free_func;

    /* ========================= */

    union {
        time_t      changelog_inserted;  /* used by changelog reader */
        struct      timeval start_processing_time;   /* used by pipeline */
    } timestamp;

    /* double chained list for pipeline */
    struct rh_list_head list;

    /* double chained list for hash storage (used by constraint on id) */
    struct rh_list_head id_hash_list;

    /* double chained list for hash storage (used by constraint on parent/name)
     */
    struct rh_list_head name_hash_list;

} entry_proc_op_t;

/* test attribute from filesystem, or else from DB */
#define ATTR_FSorDB_TEST(_entry_op_p, _attr) \
        (ATTR_MASK_TEST(&(_entry_op_p)->fs_attrs, _attr) || \
         ATTR_MASK_TEST(&(_entry_op_p)->db_attrs, _attr))

/* get attribute from filesystem, or else from DB */
#define ATTR_FSorDB(_entry_op_p, _attr) \
        (ATTR_MASK_TEST(&(_entry_op_p)->fs_attrs, _attr) ? \
         ATTR(&(_entry_op_p)->fs_attrs, _attr) :           \
         ATTR(&(_entry_op_p)->db_attrs, _attr))

#define NEED_ANYSTATUS(_op) ((_op)->fs_attr_need.status != 0)
#define NEED_GETSTATUS(_op, _i) ((_op)->fs_attr_need.status & SMI_MASK(_i))
#define NEED_GETSTRIPE(_op) ((_op)->fs_attr_need.std & \
                             (ATTR_MASK_stripe_info | ATTR_MASK_stripe_items))
#define NEED_GETPATH(_op) ((_op)->fs_attr_need.std & \
                            (ATTR_MASK_fullpath | ATTR_MASK_name | \
                             ATTR_MASK_parent_id | ATTR_MASK_depth))
#define NEED_GETATTR(_op) ((_op)->fs_attr_need.std & POSIX_ATTR_MASK)
#define NEED_READLINK(_op) ((_op)->fs_attr_need.std & ATTR_MASK_link)

/** config handlers */
extern mod_cfg_funcs_t entry_proc_cfg_hdlr;

/* ===== entry processor calls ===== */

/**
 *  Initialize entry processor pipeline
 */
int EntryProcessor_Init(pipeline_flavor_e flavor, run_flags_t flags, void *arg);

/**
 * Terminate EntryProcessor
 * \param flush_ops: wait the queue to be flushed
 */
int EntryProcessor_Terminate(bool flush_ops);

/**
 * This function adds a new operation to the queue
 */
void EntryProcessor_Push(entry_proc_op_t *p_entry);

/**
 * Advise that the entry is ready for next step of the pipeline.
 * @param next_stage The next stage to be performed for this entry
 * @param remove This flag indicates that the entry must be removed
 *        from pipeline (basically after the last step).
 */
int EntryProcessor_Acknowledge(entry_proc_op_t *p_op,
                               unsigned int next_stage, bool remove);

/** Acknowledge a batch of operations */
int EntryProcessor_AcknowledgeBatch(entry_proc_op_t **p_op, unsigned int count,
                                    unsigned int next_stage, bool remove);

/**
 * Set entry id.
 */
static void inline EntryProcessor_SetEntryId(entry_proc_op_t *p_op,
                                             const entry_id_t *p_id)
{
    p_op->entry_id_is_set = 1;
    p_op->entry_id = *p_id;

    /* @TODO: remember this reference about this entry id (to check id
     * constraints) */
}

/**
 *  Returns a clean new entry_proc_op_t structure.
 */
entry_proc_op_t *EntryProcessor_Get(void);

/**
 * Release an entry op.
 */
void EntryProcessor_Release(entry_proc_op_t *p_op);

/**
 * Dump info about pipeline stages
 */
void EntryProcessor_DumpCurrentStages(void);

/**
 * Unblock processing in a stage.
 */
void EntryProcessor_Unblock(int stage);

#endif
/**
 * @}
 */
