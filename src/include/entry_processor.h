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
    int   match_classes;

    alert_item_t  *alert_list;
    unsigned int   alert_count;
    int            alert_attr_mask;
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

/* include purpose specific pipeline definitions:
 * These includes MUST define:
 * - pipeline_stage_t entry_proc_pipeline[] array
 * - PIPELINE_STAGE_COUNT
 * - op_extra_info_t type
 */
#ifdef _LUSTRE_HSM
#include "lustre_hsm_pipeline.h"
#elif defined(_TMP_FS_MGR)
#include "tmp_fs_mgr_pipeline.h"
#elif defined(_SHERPA)
#include "sherpa_pipeline.h"
#elif defined(_BACKUP_FS)
#include "backup_fs_pipeline.h"
#else
#error "No application was specified"
#endif


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
typedef int    ( *callback_func_t ) ( struct entry_proc_op_t *, void * );

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
    int            entry_id_is_set:1;
    int            entry_attr_is_set:1;
    int            extra_info_is_set:1;

    /* entry exists in db */
    int            db_exists:1;

    /* internal flag for pipeline management */
    int            being_processed:1;
    int            id_is_referenced:1;

    operation_type_t db_op_type;
    callback_func_t callback_func;
    void          *callback_param;
    entry_id_t     entry_id;
    attr_set_t     entry_attr;

    op_extra_info_t extra_info;
    free_func_t     extra_info_free_func;

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
int            EntryProcessor_Init( const entry_proc_config_t * p_conf, int flags );

/**
 * Terminate EntryProcessor (after the queue has been flushed)
 */
int            EntryProcessor_Terminate(  );

/**
 * This function adds a new operation to the queue
 */
int            EntryProcessor_Push( const entry_proc_op_t * p_new_op );

/**
 * This function returns the next operation to be processed
 * according to pipeline stage/ordering constrains.
 */
entry_proc_op_t *EntryProcessor_GetNextOp(  );

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
