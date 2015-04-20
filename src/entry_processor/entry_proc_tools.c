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
 * Misc tools for managing entry processor pipeline
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "entry_proc_tools.h"
#include "entry_proc_hash.h"
#include "Memory.h"
#include "RobinhoodLogs.h"
#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

/* configuration for this module */
entry_proc_config_t entry_proc_conf;
int                 pipeline_flags = 0;

/* default max pending operation is 10k */
#define ID_HASH_SIZE 16007
/* hash table for storing references to ids */
static struct id_hash *id_hash;
/* hash table for storing references to parent_id/name */
static struct id_hash *name_hash;

/** initialize id constraint manager */
int id_constraint_init( void )
{
    id_hash = id_hash_init(ID_HASH_SIZE, TRUE);
    name_hash = id_hash_init(ID_HASH_SIZE, TRUE);
    /* exiting the process releases hash resources */
    return (id_hash == NULL || name_hash == NULL)? -1 : 0;
}

/**
 * This is called to register the operation (with the ordering of pipeline)
 * Normal operation is to register at the tail.
 * @return ID_OK if the entry can be processed.
 *         ID_MISSING if the ID is not set in p_op structure
 *         ID_ALREADY if the op_structure has already been registered
 */
int id_constraint_register(entry_proc_op_t * p_op, int at_head)
{
    struct id_hash_slot *slot;

    if (!p_op->entry_id_is_set)
        return ID_MISSING;

    /* compute id hash value */
    slot = get_hash_slot(id_hash, &p_op->entry_id);

    P(slot->lock);

    if (at_head)
        rh_list_add(&p_op->id_hash_list, &slot->list);
    else
        rh_list_add_tail(&p_op->id_hash_list, &slot->list);

    slot->count++;
    p_op->id_is_referenced = TRUE;

    V(slot->lock);

    /* also lock parent_id/name */
    if (ATTR_MASK_TEST(&p_op->fs_attrs, parent_id) &&
        ATTR_MASK_TEST(&p_op->fs_attrs, name))
    {
        slot = get_name_hash_slot(name_hash, &ATTR(&p_op->fs_attrs, parent_id),
                                  ATTR(&p_op->fs_attrs, name));
        P(slot->lock);

        if (at_head)
            rh_list_add(&p_op->name_hash_list, &slot->list);
        else
            rh_list_add_tail(&p_op->name_hash_list, &slot->list);

        slot->count++;
        p_op->name_is_referenced = TRUE;

        V(slot->lock);
    }

    return ID_OK;
}

#ifdef HAVE_CHANGELOGS
    #define op_name(_op)    ((_op)->extra_info.is_changelog_record ? \
                             changelog_type2str((_op)->extra_info.log_record.p_log_rec->cr_type): \
                             "scan_op")
#else
    #define op_name(_op)    "scan_op"
#endif

/**
 * Get the first operation for a given id or parent/name.
 * @return an operation to be processed when it is possible.
 *         NULL else.
 */
int id_constraint_is_first_op(entry_proc_op_t *p_op_in)
{
    entry_proc_op_t *op;
    struct id_hash_slot *slot;
    int is_first = -1; /* not set */

    /* compute id hash value */
    slot = get_hash_slot(id_hash, &p_op_in->entry_id);

    P(slot->lock);
    rh_list_for_each_entry(op, &slot->list, id_hash_list)
    {
        if (entry_id_equal(&p_op_in->entry_id, &op->entry_id))
        {
            /* found an operation with the given id */
            if (op == p_op_in)
                is_first = 1;
            else
            {
                is_first = 0;
                DisplayLog(LVL_FULL, "IdConstraint", "Pending operation with the same id: "DFID" (%s). next op: %s",
                           PFID(&op->entry_id), op_name(op), op_name(p_op_in));
            }
            break;
        }
    }
    V(slot->lock);

    if (is_first == 0)
        /* for sure, there is another operation on the same id before this one */
        return FALSE;

    /* sanity check: registered operation was not found??? */
    if ((is_first == -1) && (p_op_in->id_is_referenced))
        RBH_BUG("Registered operation was not found in id_constraint hash");

    /* Entry may be the first (or is not registered).
     * Additional check of parent/name constraint: */
    if (ATTR_MASK_TEST(&p_op_in->fs_attrs, parent_id) &&
        ATTR_MASK_TEST(&p_op_in->fs_attrs, name))
    {
        slot = get_name_hash_slot(name_hash, &ATTR(&p_op_in->fs_attrs, parent_id),
                                  ATTR(&p_op_in->fs_attrs, name));
        P(slot->lock);
        rh_list_for_each_entry(op, &slot->list, name_hash_list)
        {
            if (entry_id_equal(&ATTR(&p_op_in->fs_attrs, parent_id), &ATTR(&op->fs_attrs, parent_id))
                && !strcmp(ATTR(&p_op_in->fs_attrs, name), ATTR(&op->fs_attrs, name)))
            {
                if (op == p_op_in)
                    is_first = 1;
                else
                {
                    is_first = 0;
                    DisplayLog(LVL_FULL, "IdConstraint", "Pending operation with the same parent/name: "DFID"/%s (%s). next op: %s",
                            PFID(&ATTR(&p_op_in->fs_attrs, parent_id)),
                            ATTR(&p_op_in->fs_attrs, name), op_name(op),
                            op_name(p_op_in));
                }
                break;
            }
        }
        V(slot->lock);
    }

    /* if is_first = 0 => not first */
    /* if is_first = -1: not found => not first */
    /* just return TRUE if is_first = 1 */
    return (is_first == 1);
}



/**
 * This removes the current reference to an id when the operation is removed.
 */
int id_constraint_unregister( entry_proc_op_t * p_op )
{
    struct id_hash_slot *slot;

    if ( !p_op->entry_id_is_set )
        return ID_MISSING;

    if ( !p_op->id_is_referenced )
        return ID_NOT_EXISTS;

    slot = get_hash_slot(id_hash, &p_op->entry_id);

    /* Remove the entry */
    P( slot->lock );

    rh_list_del(&p_op->id_hash_list);
    p_op->id_is_referenced = FALSE;
    slot->count--;

    V( slot->lock );

    if (p_op->name_is_referenced)
    {
        if (ATTR_MASK_TEST(&p_op->fs_attrs, parent_id) &&
            ATTR_MASK_TEST(&p_op->fs_attrs, name))
        {
            slot = get_name_hash_slot(name_hash, &ATTR(&p_op->fs_attrs, parent_id),
                                      ATTR(&p_op->fs_attrs, name));
            /* Remove the entry */
            P(slot->lock);

            rh_list_del(&p_op->name_hash_list);
            p_op->name_is_referenced = FALSE;
            slot->count--;

            V(slot->lock);
        }
        else
        {
            DisplayLog(LVL_MAJOR, "IdConstraint", "WARNING: cannot unregister "
                       "entry with no parent/name but with a registered name!");
        }
    }

    return ID_OK;
}


void id_constraint_stats(void)
{
    id_hash_stats(id_hash, "Id constraints count");
    id_hash_stats(name_hash, "Name constraints count");
}

void id_constraint_dump(void)
{
    id_hash_dump(id_hash, FALSE);
    id_hash_dump(name_hash, TRUE);
}

/* ------------ Config management functions --------------- */

#define ENTRYPROC_CONFIG_BLOCK  "EntryProcessor"
#define ALERT_BLOCK "Alert"

int SetDefault_EntryProc_Config( void *module_config, char *msg_out )
{
    entry_proc_config_t *conf = ( entry_proc_config_t * ) module_config;
    msg_out[0] = '\0';

    conf->nb_thread = 16;
    conf->max_pending_operations = 10000; /* for efficient batching of 1000 ops */
    conf->max_batch_size = 1000;
    conf->match_file_classes = TRUE;
#ifdef HAVE_RMDIR_POLICY
    conf->match_dir_classes = TRUE;
#else
    conf->match_dir_classes = FALSE;
#endif
#ifdef ATTR_INDEX_creation_time
    conf->detect_fake_mtime = FALSE;
#endif

    conf->alert_list = NULL;
    conf->alert_count = 0;
    conf->alert_attr_mask = 0;

    conf->diff_mask = 0;

    return 0;
}

int Write_EntryProc_ConfigDefault(FILE * output)
{
    print_begin_block(output, 0, ENTRYPROC_CONFIG_BLOCK, NULL);
    print_line(output, 1, "nb_threads             :  16");
    print_line(output, 1, "max_pending_operations :  10000");
    print_line(output, 1, "max_batch_size         :  1000");
    print_line(output, 1, "match_classes          :  TRUE");
#ifdef ATTR_INDEX_creation_time
    print_line(output, 1, "detect_fake_mtime      :  FALSE");
#endif
    print_line(output, 1, "alert                  :  NONE");
    print_end_block(output, 0);
    return 0;
}





#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                                        sprintf( msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        return EFAULT; \
                                    }\
                                } while (0)


/** set expected values for the std pipeline
 * \return the number of variables added to array
 */
static int std_pipeline_arg_names(char **list, char *buffer)
{
    int i, c;
    char *curr_buf = buffer;
    unsigned int w;
    c = 0;
    for (i = 0; i < std_pipeline_descr.stage_count; i++)
    {
        w = sprintf(curr_buf, "%s_threads_max", std_pipeline[i].stage_name);
        list[i] = curr_buf;
        curr_buf += w + 1; /* written bytes + final null char */
        c++;
    }
    return c;
}


static int load_pipeline_config(const pipeline_descr_t * descr, pipeline_stage_t * p,
                                const entry_proc_config_t *conf,
                                config_item_t  entryproc_block, char *msg_out)
{
    int i, rc, tmpval;

    for (i = 0; i < descr->stage_count; i++)
    {
        char           varname[256];

        snprintf( varname, 256, "%s_threads_max", p[i].stage_name );

        rc = GetIntParam( entryproc_block, ENTRYPROC_CONFIG_BLOCK, varname,
                          INT_PARAM_POSITIVE, &tmpval, NULL, NULL, msg_out );

        if ( ( rc != 0 ) && ( rc != ENOENT ) )
            return rc;
        else if ( ( rc != ENOENT ) && ( tmpval > 0 ) )  /* 0: keep default */
        {

            if (p[i].stage_flags & STAGE_FLAG_PARALLEL)
            {
                /* the stage is no longer parallel, it has a limited number of threads */
                p[i].stage_flags &= ~STAGE_FLAG_PARALLEL;
                p[i].stage_flags |= STAGE_FLAG_MAX_THREADS;
                p[i].max_thread_count = conf->nb_thread;
            }
            if (p[i].stage_flags & STAGE_FLAG_MAX_THREADS)
            {
                if ((i == descr->DB_APPLY) && (conf->nb_thread > 1))
                    /* don't starve other steps: max is nb_thread-1 (except if nb_thread = 1)*/
                    p[i].max_thread_count = MIN2(conf->nb_thread - 1, tmpval);
                else
                    /* nb_thread at max */
                    p[i].max_thread_count = MIN2(conf->nb_thread, tmpval);
            }
            else if ( ( p[i].stage_flags & STAGE_FLAG_SEQUENTIAL )
                      && ( tmpval != 1 ) )
            {
                sprintf( msg_out, "%s is sequential. Cannot use %u threads at this stage.",
                         p[i].stage_name, tmpval );
                return EINVAL;
            }
        }
    }

    return 0;
}

int Read_EntryProc_Config( config_file_t config, void *module_config,
                           char *msg_out, int for_reload )
{
    int            rc, blc_index, i;
    int            tmpval;
    entry_proc_config_t *conf = ( entry_proc_config_t * ) module_config;
    unsigned int next_idx = 0;

    /* buffer to store arg names */
    char           *pipeline_names = NULL;
    /* max size is max pipeline steps (<10) + other args (<6) */
#define MAX_ENTRYPROC_ARGS 16
    char           *entry_proc_allowed[MAX_ENTRYPROC_ARGS];

    /* get EntryProcessor block */

    config_item_t  entryproc_block = rh_config_FindItemByName( config, ENTRYPROC_CONFIG_BLOCK );

    if ( entryproc_block == NULL )
    {
        strcpy( msg_out, "Missing configuration block '" ENTRYPROC_CONFIG_BLOCK "'" );
        /* No error because no parameter is mandatory  */
        return 0;
    }

    if ( rh_config_ItemType( entryproc_block ) != CONFIG_ITEM_BLOCK )
    {
        strcpy( msg_out, "A block is expected for '" ENTRYPROC_CONFIG_BLOCK "' item" );
        return EINVAL;
    }

    /* retrieve parameters */

    rc = GetIntParam( entryproc_block, ENTRYPROC_CONFIG_BLOCK, "nb_threads",
                      INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                      ( int * ) &conf->nb_thread, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    /* should have at least 2 threads! */
    if (conf->nb_thread == 1)
        DisplayLog(LVL_MAJOR, "EntryProc_Config", "WARNING: "ENTRYPROC_CONFIG_BLOCK" should have at least 2 threads to avoid pipeline step starvation!");

    rc = GetIntParam( entryproc_block, ENTRYPROC_CONFIG_BLOCK, "max_pending_operations",
                      INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                      ( int * ) &conf->max_pending_operations, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;

    rc = GetIntParam(entryproc_block, ENTRYPROC_CONFIG_BLOCK, "max_batch_size",
                     INT_PARAM_POSITIVE | INT_PARAM_NOT_NULL,
                     (int *)&conf->max_batch_size, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;

    rc = GetBoolParam( entryproc_block, ENTRYPROC_CONFIG_BLOCK, "match_classes",
                       0, &tmpval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if (rc == 0)
#ifdef HAVE_RMDIR_POLICY
        conf->match_file_classes = conf->match_dir_classes = tmpval;
#else
        conf->match_file_classes = tmpval;
#endif

#ifdef ATTR_INDEX_creation_time
    rc = GetBoolParam( entryproc_block, ENTRYPROC_CONFIG_BLOCK, "detect_fake_mtime",
                       0, &tmpval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if (rc == 0)
        conf->detect_fake_mtime = tmpval;
#endif

    /* look for '<stage>_thread_max' parameters (for all pipelines) */
    if (!for_reload)
    {
        rc = load_pipeline_config(&std_pipeline_descr, std_pipeline, conf,
                                  entryproc_block, msg_out);
        if (rc)
            return rc;

        // TODO load_pipeline_config(&diff_pipeline_descr, &diff_pipeline);
    }

    /* Find and parse "Alert" blocks */
    for ( blc_index = 0; blc_index < rh_config_GetNbItems( entryproc_block ); blc_index++ )
    {
        char          *block_name;
        config_item_t  curr_item = rh_config_GetItemByIndex( entryproc_block, blc_index );
        critical_err_check( curr_item, ENTRYPROC_CONFIG_BLOCK );

        if ( rh_config_ItemType( curr_item ) != CONFIG_ITEM_BLOCK )
            continue;

        block_name = rh_config_GetBlockName( curr_item );
        critical_err_check( curr_item, ENTRYPROC_CONFIG_BLOCK );

        if ( !strcasecmp( block_name, ALERT_BLOCK ) )
        {
            char * alert_title = NULL;

            if ( conf->alert_count == 0 )
                conf->alert_list = ( alert_item_t * ) malloc( sizeof( alert_item_t ) );
            else
                conf->alert_list =
                    ( alert_item_t * ) realloc( conf->alert_list,
                                                ( conf->alert_count + 1 )
                                                * sizeof( alert_item_t ) );

            conf->alert_count++;

            alert_title = rh_config_GetBlockId( curr_item );
            if ( alert_title != NULL )
                rh_strncpy(conf->alert_list[conf->alert_count - 1].title,
                         alert_title, ALERT_TITLE_MAX );
            else
                conf->alert_list[conf->alert_count - 1].title[0] = '\0';

            /* analyze boolean expression */
            rc = GetBoolExpr( curr_item, block_name,
                              &conf->alert_list[conf->alert_count - 1].boolexpr,
                              &conf->alert_list[conf->alert_count - 1].attr_mask, msg_out );

            if ( rc )
                return rc;

            conf->alert_attr_mask |= conf->alert_list[conf->alert_count - 1].attr_mask;
        }
    }                           /* Loop on subblocks */


    /* prepare the list of allowed variables to display a warning for others */
    for (i = 0; i < MAX_ENTRYPROC_ARGS; i++)
        entry_proc_allowed[i] = NULL;

    entry_proc_allowed[0] = "nb_threads";
    entry_proc_allowed[1] = "max_pending_operations";
    entry_proc_allowed[2] = "max_batch_size";
    entry_proc_allowed[3] = "match_classes";
    entry_proc_allowed[4] = ALERT_BLOCK;
#ifdef ATTR_INDEX_creation_time
    entry_proc_allowed[5] = "detect_fake_mtime";
    next_idx = 6;
#else
    next_idx = 5;
#endif

    pipeline_names = malloc(16*256); /* max 16 strings of 256 (oversized) */
    if (!pipeline_names)
        return ENOMEM;

    /* fill arg list with pipeline step names */
    next_idx += std_pipeline_arg_names(entry_proc_allowed + next_idx, pipeline_names);
    //TODO
    //next_idx += diff_pipeline_arg_names(entry_proc_allowed + next_idx, pipeline_names + XXX?);

    CheckUnknownParameters( entryproc_block, ENTRYPROC_CONFIG_BLOCK,
                            ( const char ** ) entry_proc_allowed );
    free(pipeline_names);

    return 0;
}

/** check alert rules and update values */
static void update_alerts( alert_item_t * old_items, unsigned int old_count,
                           alert_item_t * new_items, unsigned int new_count,
                           const char *block_name )
{
    unsigned int   i;

    if ( old_count != new_count )
    {
        DisplayLog( LVL_MAJOR, "EntryProc_Config",
                    "Alert rules count changed in block '%s' but cannot be modified dynamically: alert update cancelled",
                    block_name );
        return;
    }

    /* compare alert boolean expression structure */
    for ( i = 0; i < new_count; i++ )
    {
        if ( ( old_items[i].attr_mask != new_items[i].attr_mask )
             || compare_boolexpr( &old_items[i].boolexpr, &new_items[i].boolexpr ) )
        {
            DisplayLog( LVL_MAJOR, "EntryProc_Config",
                        "Alert expression #%u changed in block '%s'. Only numerical values can be modified dynamically. Alert update cancelled",
                        i, block_name );
            return;
        }
    }

    /* if they are all the same, update/check their values */

    for ( i = 0; i < new_count; i++ )
    {
        if ( update_boolexpr( &old_items[i].boolexpr, &new_items[i].boolexpr ) )
        {
            char           criteriastr[2048];
            BoolExpr2str( &old_items[i].boolexpr, criteriastr, 2048 );
            DisplayLog( LVL_EVENT, "EntryProc_Config",
                        "Alert expression #%u in block '%s' has been updated and is now: %s", i,
                        block_name, criteriastr );
        }
    }

    /* XXX attr_mask is unchanged, since we keep the same expression structures */

}

static void free_alert( alert_item_t * p_items, unsigned int count )
{
    unsigned int   i;

    for ( i = 0; i < count; i++ )
        FreeBoolExpr( &p_items[i].boolexpr, FALSE );

    if ( ( count > 0 ) && ( p_items != NULL ) )
        free( p_items );
}


int Reload_EntryProc_Config( void *module_config )
{
    entry_proc_config_t *conf = ( entry_proc_config_t * ) module_config;

    if ( conf->nb_thread != entry_proc_conf.nb_thread )
        DisplayLog( LVL_MAJOR, "EntryProc_Config",
                    ENTRYPROC_CONFIG_BLOCK
                    "::nb_threads changed in config file, but cannot be modified dynamically" );

    if ( conf->max_pending_operations != entry_proc_conf.max_pending_operations )
        DisplayLog( LVL_MAJOR, "EntryProc_Config",
                    ENTRYPROC_CONFIG_BLOCK
                    "::max_pending_operations changed in config file, but cannot be modified dynamically" );

    if (conf->max_batch_size != entry_proc_conf.max_batch_size)
    {
        DisplayLog(LVL_MAJOR, "EntryProc_Config",
                   ENTRYPROC_CONFIG_BLOCK"::max_batch_size updated: '%u'->'%u'",
                   entry_proc_conf.max_batch_size, conf->max_batch_size);
        entry_proc_conf.max_batch_size = conf->max_batch_size;
    }

    if ( conf->match_file_classes != entry_proc_conf.match_file_classes)
    {
        DisplayLog( LVL_MAJOR, "EntryProc_Config",
                    ENTRYPROC_CONFIG_BLOCK"::match_classes (files) updated: '%s'->'%s'",
                    bool2str(entry_proc_conf.match_file_classes), bool2str(conf->match_file_classes) );
        entry_proc_conf.match_file_classes = conf->match_file_classes;
    }

#ifdef HAVE_RMDIR_POLICY
    if ( conf->match_dir_classes != entry_proc_conf.match_dir_classes)
    {
        DisplayLog( LVL_MAJOR, "EntryProc_Config",
                    ENTRYPROC_CONFIG_BLOCK"::match_classes (dirs) updated: '%s'->'%s'",
                    bool2str(entry_proc_conf.match_dir_classes), bool2str(conf->match_dir_classes) );
        entry_proc_conf.match_dir_classes = conf->match_dir_classes;
    }
#endif


#ifdef ATTR_INDEX_creation_time
    if ( conf->detect_fake_mtime != entry_proc_conf.detect_fake_mtime )
    {
        DisplayLog( LVL_MAJOR, "EntryProc_Config",
                    ENTRYPROC_CONFIG_BLOCK"::detect_fake_mtime updated: '%s'->'%s'",
                    bool2str(entry_proc_conf.detect_fake_mtime), bool2str(conf->detect_fake_mtime) );
        entry_proc_conf.detect_fake_mtime = conf->detect_fake_mtime;
    }
#endif

    /* Check alert rules  */
    update_alerts( entry_proc_conf.alert_list, entry_proc_conf.alert_count,
                   conf->alert_list, conf->alert_count, ENTRYPROC_CONFIG_BLOCK );


    free_alert( conf->alert_list, conf->alert_count );

    if (entry_proc_conf.match_file_classes && !is_file_class_defined())
    {
        DisplayLog( LVL_EVENT, "EntryProc_Config" , "No class defined in policies, disabling file class matching." );
        entry_proc_conf.match_file_classes = FALSE;
    }
#ifdef HAVE_RMDIR_POLICY
    if (entry_proc_conf.match_dir_classes && !is_dir_class_defined())
    {
        DisplayLog( LVL_EVENT, "EntryProc_Config" , "No class defined in policies, disabling dir class matching." );
        entry_proc_conf.match_dir_classes = FALSE;
    }
#endif

    return 0;
}

int Write_EntryProc_ConfigTemplate( FILE * output )
{
    int            i;

    print_begin_block( output, 0, ENTRYPROC_CONFIG_BLOCK, NULL );

    print_line( output, 1, "# Raise alerts for directories with too many entries" );
    print_begin_block( output, 1, ALERT_BLOCK, "Too_many_entries_in_directory" );
    print_line( output, 2, "type == directory" );
    print_line( output, 2, "and" );
    print_line( output, 2, "dircount > 10000" );
    print_end_block( output, 1 );
    fprintf( output, "\n" );
    print_line( output, 1, "# Raise alerts for large files" );
    print_begin_block( output, 1, ALERT_BLOCK, "Large_file" );
    print_line( output, 2, "type == file" );
    print_line( output, 2, "and" );
    print_line( output, 2, "size > 100GB" );
    print_end_block( output, 1 );
    fprintf( output, "\n" );

    print_line( output, 1, "# nbr of worker threads for processing pipeline tasks" );
    print_line( output, 1, "nb_threads = 16 ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# Max number of operations in the Entry Processor pipeline." );
    print_line( output, 1, "# If the number of pending operations exceeds this limit, " );
    print_line( output, 1, "# info collectors are suspended until this count decreases" );
#ifdef _SQLITE
    print_line( output, 1, "max_pending_operations = 500 ;" );
#else
    print_line( output, 1, "max_pending_operations = 10000 ;" );
#endif
    fprintf(output, "\n");
    print_line(output, 1, "# max batched DB operations (1=no batching)");
    print_line(output, 1, "max_batch_size = 1000;");
    fprintf(output, "\n");

    print_line( output, 1,
                "# Optionnaly specify a maximum thread count for each stage of the pipeline:" );
    print_line( output, 1, "# <stagename>_threads_max = <n> (0: use default)" );
    for ( i = 0; i < std_pipeline_descr.stage_count; i++ )
    {
        if (i == std_pipeline_descr.DB_APPLY)
        {
            print_line(output, 1, "# Disable batching (max_batch_size=1) or accounting (*_acct=no) to allow parallelizing the following step:");
        }

        if ( std_pipeline[i].stage_flags & STAGE_FLAG_PARALLEL )
            print_line( output, 1, "# %s_threads_max\t= 4 ;", std_pipeline[i].stage_name );
        else if (std_pipeline[i].stage_flags & STAGE_FLAG_MAX_THREADS)
            print_line( output, 1, "%s_threads_max\t= %u ;", std_pipeline[i].stage_name,
                        std_pipeline[i].max_thread_count );
    }
    fprintf( output, "\n" );

    print_line( output, 1, "# if set to FALSE, classes will only be matched");
    print_line( output, 1, "# at policy application time (not during a scan or reading changelog)" );
    print_line( output, 1, "match_classes = TRUE;");

#ifdef ATTR_INDEX_creation_time
    fprintf( output, "\n" );
    print_line( output, 1, "# Faking mtime to an old time causes the file to be migrated");
    print_line( output, 1, "# with top priority. Enabling this parameter detect this behavior");
    print_line( output, 1, "# and doesn't allow  mtime < creation_time");
    print_line( output, 1, "detect_fake_mtime = FALSE;");
#endif

    print_end_block( output, 0 );
    return 0;
}

/** try to convert a time_t to a human readable form */
void time2human_helper(time_t t, const char *attr_name, char *str,
                       size_t size, const struct entry_proc_op_t *p_op)
{
    struct tm res;

    /* initialize as 'out of range' */
    strncpy(str, "<out of range>", size);

    if (localtime_r(&t, &res) != NULL)
        strftime(str, size, "%Y/%m/%d %T", &res);
    else if (ATTR_FSorDB_TEST(p_op, fullpath))
        DisplayLog(LVL_MAJOR, ENTRYPROC_TAG, "Invalid or corrupted %s detected for %s: %lu",
                   attr_name, ATTR_FSorDB(p_op, fullpath), t);
    else
        DisplayLog(LVL_MAJOR, ENTRYPROC_TAG, "Invalid or corrupted %s detected for "DFID": %lu",
                   attr_name, PFID(&p_op->entry_id), t);
}

void check_and_warn_fake_mtime(const struct entry_proc_op_t *p_op)
{
    char        mt[128];
    char        ct[128];

    /* check if mtime is before estimated creation time */
    if (ATTR(&p_op->fs_attrs, last_mod) < ATTR_FSorDB(p_op, creation_time))
    {
        time2human_helper(ATTR(&p_op->fs_attrs, last_mod), "mtime", mt,
                          sizeof(mt), p_op);

        time2human_helper(ATTR(&p_op->fs_attrs, creation_time), "crtime", ct,
                          sizeof(ct), p_op);

        if (ATTR_FSorDB_TEST(p_op, fullpath))
            DisplayLog(LVL_VERB, ENTRYPROC_TAG,
                       "Fake mtime detected for '%s': mtime=%s, creation=%s",
                       ATTR_FSorDB(p_op, fullpath), mt, ct);
        else
            DisplayLog(LVL_VERB, ENTRYPROC_TAG,
                       "Fake mtime detected for "DFID": mtime=%s, creation=%s",
                       PFID(&p_op->entry_id), mt, ct);
    }
    /* a 24h delay can be explained by different timezones */
    else if (ATTR(&p_op->fs_attrs, last_mod) > time(NULL) + 86400)
    {
        time2human_helper(ATTR(&p_op->fs_attrs, last_mod), "mtime", mt,
                          sizeof(mt), p_op);

        if (ATTR_FSorDB_TEST(p_op, fullpath))
            DisplayLog(LVL_EVENT, ENTRYPROC_TAG,
                       "Fake mtime detected for '%s': mtime=%s is in the future",
                       ATTR_FSorDB(p_op, fullpath), mt);
        else
            DisplayLog(LVL_EVENT, ENTRYPROC_TAG,
                       "Fake mtime detected for "DFID": mtime=%s is in the future",
                       PFID(&p_op->entry_id), mt);
    }
}

#ifdef _LUSTRE
static void clear_stripe_info(attr_set_t *attrs)
{
    ATTR_MASK_UNSET(attrs, stripe_info);
    if (ATTR_MASK_TEST(attrs, stripe_items))
    {
        /* free stripe structure */
        if (ATTR(attrs, stripe_items).stripe)
            MemFree(ATTR(attrs, stripe_items).stripe);
        ATTR_MASK_UNSET(attrs, stripe_items);
    }
}

void check_stripe_info(struct entry_proc_op_t *p_op, lmgr_t *lmgr)
{
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
                /* Since Lustre2.4, entry striping can change (lfs swap_layouts, lfs hsm_release...)
                 * so scanning must update file stripe information. */
                /* Possible cases:
                 * - File striping is not set in fs_attrs: check it exists in DB
                 *      If not, get stripe info from filesystem
                 * - File striping is set in fs_attrs:
                 *      - Check stripe validator in DB: if OK, don't update DB info
                 *      - if an error is reported, update with the new values.
                 */
                if (!ATTR_MASK_TEST(&p_op->fs_attrs, stripe_info))
                {
#endif
                    /* check it exists in DB */
                    if (ListMgr_CheckStripe(lmgr, &p_op->entry_id, VALID_EXISTS) != DB_SUCCESS)
                    {
                        DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Stripe information is missing/invalid in DB");

                        /* don't need to get stripe if we already have fresh stripe info from FS */
                        if (!(ATTR_MASK_TEST(&p_op->fs_attrs, stripe_info)
                              && ATTR_MASK_TEST(&p_op->fs_attrs, stripe_items)))
                        {
                                p_op->fs_attr_need |= ATTR_MASK_stripe_info | ATTR_MASK_stripe_items;
                        }
                    }
                    else /* stripe is OK, don't update stripe items */
                        clear_stripe_info(&p_op->fs_attrs);

#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
                }
                else if (ListMgr_CheckStripe(lmgr, &p_op->entry_id,
                                   ATTR(&p_op->fs_attrs, stripe_info).validator)
                         == DB_SUCCESS)
                {
                    /* don't update */
                    clear_stripe_info(&p_op->fs_attrs);
                }
                else /* keep stripe info in fs_attrs, as it must be updated */
                    DisplayLog(LVL_DEBUG, ENTRYPROC_TAG, "Stripe information has changed");
#endif
}
#endif



