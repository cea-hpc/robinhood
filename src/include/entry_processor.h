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

#include "list_mgr.h"
#include "config_parsing.h"

/* Tag for Logs */
#define ENTRYPROC_TAG   "EntryProc"

#define ALERT_TITLE_MAX 256
typedef struct alert_item_t
{
    char           title[ALERT_TITLE_MAX];
    bool_node_t    boolexpr;
    int            attr_mask;
} alert_item_t;

typedef struct entry_proc_config_t
{
    unsigned int   nb_thread;
    unsigned int   max_pending_operations;

    alert_item_t  *alert_list;
    unsigned int   alert_count;
    int            alert_attr_mask;

    unsigned int   match_file_classes:1;
    unsigned int   match_dir_classes:1;
#ifdef ATTR_INDEX_creation_time
    /* fake mtime in the past causes higher
     * migration priority */
    unsigned int   detect_fake_mtime:1;
#endif
    int            diff_mask;
} entry_proc_config_t;

/* === pipeline stage flags ===  */

/* paralellism */
#define STAGE_FLAG_SEQUENTIAL    0x00000001
#define STAGE_FLAG_MAX_THREADS   0x00000002     /* hard coded limit */
#define STAGE_FLAG_PARALLEL      0x00000004     /* if set, refer to max threads in configuration */

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
typedef int    ( *step_function_t ) ( struct entry_proc_op_t *, lmgr_t * lmgr );

/**
 * Definition of a pipeline stage
 */
typedef struct pipeline_stage_t
{
    unsigned int   stage_index;                  /**< index of this puipeline stage */
    const char    *stage_name;                   /**< name of this pipeline stage */
    step_function_t stage_function;              /**< function for performing the stage */
    int            stage_flags;                  /**< stage qualifiers */
    unsigned int   max_thread_count;             /**< 0 = UNLIMITED */

} pipeline_stage_t;

/**
 * This structure indicates pipeline steps index and limits
 */
typedef struct pipeline_descr_t
{
   unsigned int stage_count;
   unsigned int GET_ID;
   unsigned int GET_INFO_DB;
   unsigned int GET_INFO_FS;
   unsigned int GC_OLDENT;
} pipeline_descr_t;

/* pipeline currently in operation */
extern pipeline_stage_t * entry_proc_pipeline;
extern pipeline_descr_t   entry_proc_descr;
extern const char * entry_proc_db_tag; /* tag for ListMgr tag operations */

/* include purpose specific pipeline definitions:
 * These includes MUST define:
 * - pipeline_stage_t entry_proc_pipeline[] array
 * - PIPELINE_STAGE_COUNT
 * - op_extra_info_t type
 */
#include "pipeline_types.h"

/** type of operation to be performed on database */
typedef enum operation_type_t
{
    OP_TYPE_NONE = 0,
    OP_TYPE_INSERT,
    OP_TYPE_UPDATE,
    OP_TYPE_REMOVE,
    OP_TYPE_SOFT_REMOVE
} operation_type_t;

/**
 * callback function definition
 * @param struct entry_proc_op_t * : the structure associated to the entry
 * @param void * : InfoCollector specific parameter
 */
typedef int    ( *callback_func_t ) ( lmgr_t *, struct entry_proc_op_t *, void * );

/**
 * function prototype for freeing  extra_info
 */
typedef void   ( *free_func_t ) ( void * );

/** operation submitted to the pipeline */
typedef struct entry_proc_op_t
{
    /** current stage in pipeline */
    unsigned int   pipeline_stage;

    /* what is set in this structure ? */
    unsigned int      entry_id_is_set:1;
    unsigned int      db_attr_is_set:1;
    unsigned int      fs_attr_is_set:1;
    unsigned int      extra_info_is_set:1;

    /* entry exists in db */
    unsigned int      db_exists:1;

    /* internal flag for pipeline management */
    unsigned int      being_processed:1;
    unsigned int      id_is_referenced:1;

    operation_type_t db_op_type;
    callback_func_t callback_func;
    void          *callback_param;

    /* === Entry information === */
    entry_id_t     entry_id;

    /* list of attrs to be retrieved from DB */
    int            db_attr_need;
    /* list of attrs to be retrieved from FS */
    int            fs_attr_need;

    /* attrs from DB (cached) */
    attr_set_t     db_attrs;
    /* attrs from FS (new) */
    attr_set_t     fs_attrs;

    op_extra_info_t extra_info;
    free_func_t     extra_info_free_func;

    /* ========================= */

    /**
     * internal entry lock for pipeline management:
     * lock for entry status (being_processed) and pipeline stage
     */
    pthread_mutex_t entry_lock;

    struct timeval start_processing_time;

    /** lock on entry_id_is_set and entry_id */
    pthread_mutex_t entry_id_lock;

    /* double chained list for FIFO */
    struct entry_proc_op_t *p_next;
    struct entry_proc_op_t *p_prev;

} entry_proc_op_t;

/* test attribute from filesystem, or else from DB */
#define ATTR_FSorDB_TEST(_entry_op_p, _attr) \
        (ATTR_MASK_TEST(&(_entry_op_p)->fs_attrs, _attr) || \
         ATTR_MASK_TEST(&(_entry_op_p)->db_attrs, _attr))

/* get attribute from filesystem, or else from DB */
#define ATTR_FSorDB(_entry_op_p, _attr) \
        (ATTR_MASK_TEST(&(_entry_op_p)->fs_attrs, _attr)? \
         ATTR(&(_entry_op_p)->fs_attrs, _attr):           \
         ATTR(&(_entry_op_p)->db_attrs, _attr))

#define POSIX_ATTR_MASK (ATTR_MASK_size | ATTR_MASK_blocks | ATTR_MASK_owner \
                         | ATTR_MASK_gr_name | ATTR_MASK_last_access \
                         | ATTR_MASK_last_mod | ATTR_MASK_type | ATTR_MASK_mode)

#define NEED_GETSTATUS(_op) ((_op)->fs_attr_need & ATTR_MASK_status)
#define NEED_GETSTRIPE(_op) ((_op)->fs_attr_need & (ATTR_MASK_stripe_info | ATTR_MASK_stripe_items ))
#define NEED_GETPATH(_op) ((_op)->fs_attr_need & (ATTR_MASK_fullpath | ATTR_MASK_name | ATTR_MASK_depth))
#define NEED_GETATTR(_op) ((_op)->fs_attr_need & POSIX_ATTR_MASK )


/* ===== entry processor calls ===== */

/**
 * \addtogroup MODULE_CONFIG_FUNCTIONS
 * @{
 */
int            SetDefault_EntryProc_Config( void *module_config, char *msg_out );
int            Read_EntryProc_Config( config_file_t config,
                                      void *module_config, char *msg_out, int for_reload );
int            Reload_EntryProc_Config( void *module_config );
int            Write_EntryProc_ConfigTemplate( FILE * output );
int            Write_EntryProc_ConfigDefault( FILE * output );
/**
 * @}
 */

/**
 *  Initialize entry processor pipeline
 */
int EntryProcessor_Init( const entry_proc_config_t * p_conf,
                         pipeline_flavor_e flavor, int flags,
                         const char * db_tag );

/**
 * Terminate EntryProcessor
 * \param flush_ops: wait the queue to be flushed
 */
int EntryProcessor_Terminate( int flush_ops );

/**
 * This function adds a new operation to the queue
 */
int            EntryProcessor_Push( const entry_proc_op_t * p_new_op );

/**
 * Advise that the entry is ready for next step of the pipeline.
 * @param next_stage The next stage to be performed for this entry
 * @param remove This flag indicates that the entry must be removed
 *        from pipeline (basically after the last step).
 */
int            EntryProcessor_Acknowledge( entry_proc_op_t * p_op,
                                           unsigned int next_stage, int remove );

/**
 * Set entry id.
 */
int            EntryProcessor_SetEntryId( entry_proc_op_t * p_op, const entry_id_t * p_id );

/**
 *  Initialize a entry_proc_op_t structure.
 */
void           InitEntryProc_op( entry_proc_op_t * p_op );

/**
 * Dump info about pipeline stages 
 */
void           EntryProcessor_DumpCurrentStages(  );

#endif
/**
 * @}
 */
