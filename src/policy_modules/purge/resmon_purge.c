/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "RobinhoodConfig.h"
#include "resource_monitor.h"
#include "resmon_purge.h"
#include "queue.h"
#include "list_mgr.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "Memory.h"
#include "xplatform_print.h"

#ifdef _HSM_LITE
#include "backend_mgr.h"
#include "backend_ext.h"
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

#define PURGE_TAG   "Purge"
#define CHECK_PURGE_INTERVAL    1

static int resmon_flags = 0;
static int purge_abort = FALSE;

#define ignore_policies (resmon_flags & FLAG_IGNORE_POL)
#define dry_run (resmon_flags & FLAG_DRY_RUN)

/* queue of entries to be checked/purged */
entry_queue_t  purge_queue;

/**
 * Purge helpers (depending on purpose)
 * @return posix error code (from errno)
 */

#if defined(_LUSTRE_HSM) || defined(_HSM_LITE)
/** purge by FID in Lustre-HSM and HSMLITE */
static int PurgeEntry_ByFid( const entry_id_t * p_entry_id,
                             attr_set_t * p_attrs )
{
    DisplayLog( LVL_EVENT, PURGE_TAG, "Releasing(" DFID_NOBRACE ": %s)", PFID( p_entry_id ),
                (p_attrs && ATTR_MASK_TEST(p_attrs, fullpath))?ATTR(p_attrs, fullpath):"" );

    if ( dry_run )
        return 0;

#ifdef _LUSTRE_HSM
    return LustreHSM_Action( HUA_RELEASE, p_entry_id, NULL, 0 );
#elif defined(HAVE_SHOOK)
    return rbhext_release(p_entry_id, p_attrs);
#endif
}
#else
/** purge by path for other purpose */
static int PurgeEntry(const entry_id_t *id, const char *entry_path)
{
    if (EMPTY_STRING(resmon_config.purge_command))
    {
        /* no custom purge command => unlink */
        DisplayLog(LVL_DEBUG, PURGE_TAG, "%sunlink(%s)", dry_run ? "(dry-run) " : "",
                   entry_path);
        if (!dry_run)
        {
            if (unlink( entry_path) != 0)
                return errno;
        }
        return 0;
    }
    else
    {
        /* execute custom action */
        char strfid[128];
        sprintf(strfid, DFID, PFID(id));

        const char *vars[] = {
            "path", entry_path,
            "fsname", get_fsname(),
            "fid", strfid,
            NULL, NULL
        };

        char *cmd = replace_cmd_parameters(resmon_config.purge_command, vars);
        if (cmd)
        {
            int rc = 0;
            /* call custom purge command instead of unlink() */
            DisplayLog(LVL_DEBUG, PURGE_TAG, "%scmd(%s)", dry_run ? "(dry-run) " : "", cmd);
            if (!dry_run)
                rc =  execute_shell_command(TRUE, cmd, 0);
            free(cmd);
            return rc;
        }
        else
            return errno;
        /* @TODO handle other hardlinks to the same entry */
    }
}
#endif

typedef struct purge_item__
{
    entry_id_t     entry_id;
    attr_set_t     entry_attr;
    unsigned long  specific_blocks;
} purge_item_t;

/**
 *  Alloc a new purge item so it can be pushed to the purge queue.
 */
static void   *Entry2PurgeItem( entry_id_t * p_entry_id, attr_set_t * p_attr_set,
                                unsigned long specific_blocks )
{
    purge_item_t  *new_entry;

    new_entry = ( purge_item_t * ) MemAlloc( sizeof( purge_item_t ) );
    if ( !new_entry )
        return NULL;

    new_entry->entry_id = *p_entry_id;
    new_entry->entry_attr = *p_attr_set;
    new_entry->specific_blocks = specific_blocks;

    return new_entry;
}

/**
 * Free a Purge Item (and the resources of entry_attr).
 */
static void FreePurgeItem( purge_item_t * item )
{
    ListMgr_FreeAttrs( &item->entry_attr );
    MemFree( item );
}

static int heuristic_end_of_list( time_t last_access_time )
{
    entry_id_t     void_id;
    attr_set_t     void_attr;

    if (!resmon_config.sort)
        return FALSE;

    /* list all files if policies are ignored */
    if ( ignore_policies )
        return FALSE;

    /* XXX Tip for optimization:
     * we build a void entry with last_access = last_access_time
     * and last_restore_time = last_access_time = creation_time.
     * If it doesn't match any policy, next entries won't match too
     * because entries are sorted by last access time,
     * so it is not necessary to continue.
     * Note we have last_restore_time < last_access_time
     * (because entries are not purged), and of course
     * creation_time < last_access_time.
     */
    memset( &void_id, 0, sizeof( entry_id_t ) );
    memset( &void_attr, 0, sizeof( attr_set_t ) );

    ATTR_MASK_INIT( &void_attr );
    ATTR_MASK_SET( &void_attr, last_access );
    ATTR( &void_attr, last_access ) = last_access_time;
#ifdef ATTR_INDEX_last_restore
    ATTR_MASK_SET( &void_attr, last_restore );
    ATTR( &void_attr, last_restore ) = last_access_time;
#endif
#ifdef ATTR_INDEX_creation_time
    ATTR_MASK_SET( &void_attr, creation_time );
    ATTR( &void_attr, creation_time ) = last_access_time;
#endif

    if ( PolicyMatchAllConditions( &void_id, &void_attr, PURGE_POLICY, NULL ) == POLICY_NO_MATCH )
    {
        DisplayLog( LVL_DEBUG, PURGE_TAG,
                    "Optimization: entries with access time later than %lu"
                    " cannot match any policy condition. Stop retrieving DB entries.",
                    last_access_time );
        return TRUE;
    }
    else
        return FALSE;
}


/**
 *  Sum the number of acks from a status tab
 */
static inline unsigned int ack_count( const unsigned int *status_tab )
{
    unsigned int   i, sum;
    sum = 0;

    for ( i = 0; i < PURGE_ST_COUNT; i++ )
        sum += status_tab[i];

    return sum;
}


/**
 * build a filter from policies, to optimize DB queries.
 */
static int set_purge_optimization_filters(lmgr_filter_t * p_filter)
{
    /** @TODO build a filter for getting the union of all filesets/conditions */

    /* If there is a single policy, try to convert its condition
     * to a simple filter.
     */
    if ( policies.purge_policies.policy_count == 1 )
    {
        if ( convert_boolexpr_to_simple_filter( &policies.purge_policies.policy_list[0].condition,
             p_filter ) )
        {
            DisplayLog( LVL_FULL, PURGE_TAG, "Could not convert purge policy '%s' to simple filter.",
                policies.purge_policies.policy_list[0].policy_id );
        }
    }

    if (!resmon_config.recheck_ignored_classes)
    {
        int i;
        filter_value_t fval;

        /* don't select files in ignored classes */
        for (i = 0; i < policies.purge_policies.ignore_count; i++)
        {
            int flags = 0;
            fval.value.val_str = policies.purge_policies.ignore_list[i]->fileset_id;
            if (i == 0)
                flags = FILTER_FLAG_NOT | FILTER_FLAG_ALLOW_NULL;
            else
                flags = FILTER_FLAG_NOT;
            lmgr_simple_filter_add( p_filter, ATTR_INDEX_release_class, EQUAL, fval, flags );
        }
    }

    return 0;
}

/** wait until the queue is empty or migrations timed-out.
 * \return 0 when the queue is empty
 *         ETIME on timeout.
 */
static int wait_queue_empty( unsigned int nb_submitted,
                             const unsigned long long * feedback_init,
                             const unsigned int * status_tab_init,
                             unsigned long long * feedback_after,
                             int long_sleep )
{
    unsigned int nb_in_queue, nb_purge_pending;
    unsigned int   status_tab[PURGE_ST_COUNT];

    /* Wait for end of purge pass */
    do
    {
        time_t last_push, last_pop, last_ack, last_activity;
        last_push = last_pop = last_ack = last_activity = 0;

        RetrieveQueueStats( &purge_queue, NULL, &nb_in_queue,
                            &last_push, &last_pop, &last_ack,
                            status_tab, feedback_after );

        /* the last time a request was pushed/poped/acknowledged */
        last_activity = MAX3( last_push, last_pop, last_ack );

        /* nb of purge operation pending
            = nb_enqueued - ( nb ack after - nb ack before ) */
        nb_purge_pending = nb_submitted + ack_count( status_tab_init )
                            - ack_count( status_tab );

        if ( ( nb_in_queue != 0 ) || ( nb_purge_pending != 0 ) )
        {
            DisplayLog( LVL_DEBUG, PURGE_TAG,
                        "Waiting for the end of this purge pass: "
                        "still %u files to be purged "
                        "(%u in queue, %u beeing processed). "
                        "Last action %us ago.",
                        nb_purge_pending, nb_in_queue,
                        nb_purge_pending - nb_in_queue,
                        (unsigned int)(time(NULL) - last_activity));

            if ( long_sleep )
                rh_sleep( CHECK_PURGE_INTERVAL );
            else
                rh_usleep( 1000 );
        }
        else
            DisplayLog( LVL_DEBUG, PURGE_TAG, "End of this purge pass" );

    }
    while ( ( nb_in_queue != 0 ) || ( nb_purge_pending != 0 ) );

    return 0;
}

/* indicates attributes to be retrieved from db */
static int init_db_attr_mask( attr_set_t * p_attr_set )
{
#ifdef _HSM_LITE
    int rc;
    unsigned int allow_cached_attrs = 0;
    unsigned int need_fresh_attrs = 0;
#endif

    ATTR_MASK_INIT( p_attr_set );

    /* Retrieve at least: fullpath, last_access, size, blcks
     * because they are used for checking if info changed.
     * Retrieve last_mod and stripe_info for logs and reports.
     * Also retrieve info needed for blacklist/whitelist rules.
     */
/* need parent_id and name for ListMgr_Remove() prototype */
    ATTR_MASK_SET( p_attr_set, name );
    ATTR_MASK_SET( p_attr_set, parent_id );

    ATTR_MASK_SET( p_attr_set, fullpath );
    ATTR_MASK_SET( p_attr_set, path_update );
    ATTR_MASK_SET( p_attr_set, last_access );
    ATTR_MASK_SET( p_attr_set, size );
    ATTR_MASK_SET( p_attr_set, blocks );
    ATTR_MASK_SET( p_attr_set, last_mod );
    ATTR_MASK_SET( p_attr_set, stripe_info );
    ATTR_MASK_SET( p_attr_set, stripe_items );
#ifdef ATTR_INDEX_status
    ATTR_MASK_SET( p_attr_set, status );
#endif
    ATTR_MASK_SET( p_attr_set, release_class );
    ATTR_MASK_SET( p_attr_set, rel_cl_update );
    p_attr_set->attr_mask |= policies.purge_policies.global_attr_mask;

#ifdef _HSM_LITE
    ATTR_MASK_SET( p_attr_set, type );

    /* what information the backend needs from DB? */
    rc = rbhext_status_needs( TYPE_NONE, &allow_cached_attrs, &need_fresh_attrs );
    if (rc != 0)
    {
        DisplayLog(LVL_MAJOR, PURGE_TAG, "Unexpected error from rbhext_status_needs(), in %s line %u: %d",
                   __FUNCTION__, __LINE__, rc );
        return rc;
    }
    p_attr_set->attr_mask |= allow_cached_attrs;
#endif

    return 0;
}


/*
 *  Return ENOENT if no list is available
 */
int perform_purge(lmgr_t *lmgr, purge_param_t *p_purge_param,
                  unsigned long long *p_blks_purged,
                  unsigned long long *p_nb_specific)
{
    int            rc;
    struct lmgr_iterator_t *it = NULL;

    lmgr_filter_t  filter;
    filter_value_t fval;
    lmgr_sort_type_t sort_type;

    attr_set_t     attr_set;
    entry_id_t     entry_id;

    unsigned long long feedback_before[PURGE_FDBK_COUNT];
    unsigned long long feedback_after[PURGE_FDBK_COUNT];

    unsigned int   status_tab[PURGE_ST_COUNT];

    unsigned int   nb_submitted;
    unsigned long long submitted_amount, purged_amount, purged_vol, target;

    enum { TGT_BLOCK, TGT_COUNT, TGT_ALL } target_type;

    int            last_entry_access = 0;
    time_t         last_request_time = 0;
    int            attr_mask_sav;
    int            end_of_list = FALSE;
    char           timestamp[1024];

    unsigned int   nb_returned, total_returned;

    lmgr_iter_opt_t opt;

    if ( !p_purge_param )
        return EFAULT;

    resmon_flags = p_purge_param->flags;

    if (p_blks_purged)
        *p_blks_purged = 0;
    if (p_nb_specific)
        *p_nb_specific = 0;

    if ( p_purge_param->nb_blocks != 0 )
    {
        target_type = TGT_BLOCK;
        target = p_purge_param->nb_blocks;
    }
    else if ( p_purge_param->nb_inodes != 0 )
    {
        target_type = TGT_COUNT;
        target = p_purge_param->nb_inodes;
    }
    else if ( (p_purge_param->type == PURGE_BY_CLASS)
             || (p_purge_param->type == PURGE_ALL) )
    {
        target_type = TGT_ALL;
        target = 0;
    }
    else
    {
        DisplayLog( LVL_CRIT, PURGE_TAG,
                    "Unexpected purge parameter in %s(): nb_blocks=%lu, nb_inodes=%Lu, purge type=%u",
                    __FUNCTION__, p_purge_param->nb_blocks, p_purge_param->nb_inodes, p_purge_param->type );
        return EINVAL;
    }

    /**
     * Do nothing if no previous scan was done.
     */
    if ( ListMgr_GetVar( lmgr, LAST_SCAN_END_TIME, timestamp ) != DB_SUCCESS )
    {
        DisplayLog( LVL_MAJOR, PURGE_TAG,
                    "Full FS Scan has never been done. Cannot build a purge list." );
        return ENOENT;
    }
    else
    {
        time_t         last_scan = atoi( timestamp );
        struct tm      date;


        localtime_r( &last_scan, &date );
        DisplayLog( LVL_EVENT, PURGE_TAG,
#ifdef HAVE_CHANGELOGS
                    "Building a purge list - last full FS Scan: "
#else
                    "Building a purge list from last full FS Scan: "
#endif
                    "%.4d/%.2d/%.2d %.2d:%.2d:%.2d",
                    1900 + date.tm_year, date.tm_mon + 1, date.tm_mday,
                    date.tm_hour, date.tm_min, date.tm_sec );
    }


    rc = init_db_attr_mask( &attr_set );
    if (rc)
        return rc;

    /* sort by last access (only if 'sort' is specified) */
    sort_type.attr_index = ATTR_INDEX_last_access;
    sort_type.order = (resmon_config.sort ? SORT_ASC : SORT_NONE);

    rc = lmgr_simple_filter_init( &filter );
    if ( rc )
        return rc;

#ifdef ATTR_INDEX_invalid
    /* do not retrieve 'invalid' entries */
    fval.value.val_bool = FALSE;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_invalid, EQUAL, fval,
                                 FILTER_FLAG_ALLOW_NULL );
    if ( rc )
        return rc;
#endif

#ifdef ATTR_INDEX_no_release
    /* do not retrieve entries with 'no_release' tag = 1 */
    fval.value.val_bool = TRUE;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_no_release, NOTEQUAL,
                                 fval, FILTER_FLAG_ALLOW_NULL );
    if ( rc )
        return rc;
#endif

#ifdef ATTR_INDEX_status
    /* only get entries with HSM state SYNCHRO */
    fval.value.val_int = STATUS_SYNCHRO;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL, fval, 0 );
    if ( rc )
        return rc;
#endif

    /* do not consider directories, if they are stored in DB */
#ifdef ATTR_INDEX_type
    if ( (field_infos[ATTR_INDEX_type].flags & GENERATED) == 0 )
    {

#if defined(_LUSTRE_HSM) || defined(_HSM_LITE)
        /* only retrieve files */
        fval.value.val_str = STR_TYPE_FILE;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fval, 0 );
#else
        /* do not retrieve directories */
        fval.value.val_str = STR_TYPE_DIR;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_type, NOTEQUAL, fval, 0 );
#endif
        if ( rc )
            return rc;
    }
#endif

    switch ( p_purge_param->type )
    {
    case PURGE_FS:
    case PURGE_ALL:
        DisplayLog(LVL_MAJOR, PURGE_TAG, "Starting purge");

        /* We must retrieve all files sorted by atime: no extra filter */
        break;

    case PURGE_BY_OST:
        DisplayLog(LVL_MAJOR, PURGE_TAG, "Starting purge on OST #%u",
                   p_purge_param->param_u.ost_index);

        /* retrieve stripe info and stripe items */
        ATTR_MASK_SET( &attr_set, stripe_info );
        ATTR_MASK_SET( &attr_set, stripe_items );

        /* We must retrieve files from this OST, sorted by atime */

        fval.value.val_uint = p_purge_param->param_u.ost_index;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_stripe_items,
                                     EQUAL, fval, 0 );
        if ( rc )
            return rc;
        break;

    case PURGE_BY_POOL:
        DisplayLog(LVL_MAJOR, PURGE_TAG, "Starting purge of pool '%s'",
                   p_purge_param->param_u.pool_name);

        /** @TODO must retrieve files stored on current pool definition */

        /* retrieve stripe info and stripe items */
        ATTR_MASK_SET( &attr_set, stripe_info );
        ATTR_MASK_SET( &attr_set, stripe_items );

        /* We must retrieve files from these OSTs, sorted by atime */

        fval.value.val_str = p_purge_param->param_u.pool_name;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_stripe_info,
                                     EQUAL, fval, 0 );

        if ( rc )
            return rc;
        break;

    case PURGE_BY_USER:
        DisplayLog(LVL_MAJOR, PURGE_TAG, "Starting purge of '%s' user files",
                   p_purge_param->param_u.user_name);

        /* We must retrieve files for this user, sorted by atime */

        fval.value.val_str = p_purge_param->param_u.user_name;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_owner, EQUAL, fval, 0 );

        if ( rc )
            return rc;
        break;

    case PURGE_BY_GROUP:
        DisplayLog(LVL_MAJOR, PURGE_TAG, "Starting purge of '%s' group files",
                   p_purge_param->param_u.group_name);

        /* We must retrieve files for this group, sorted by atime */

        fval.value.val_str = p_purge_param->param_u.group_name;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_gr_name, EQUAL, fval, 0 );
        if ( rc )
            return rc;
        break;

    case PURGE_BY_CLASS:
        DisplayLog(LVL_MAJOR, PURGE_TAG, "Starting purge of fileclass(es) '%s'",
                   p_purge_param->param_u.class_name);

        if (!strcasecmp( p_purge_param->param_u.class_name, "default"))
            fval.value.val_str = CLASS_DEFAULT;
        else if ( !strcasecmp( p_purge_param->param_u.class_name, "ignored"))
            fval.value.val_str = CLASS_IGNORED;
        else
            fval.value.val_str = p_purge_param->param_u.class_name;

        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_release_class, LIKE,
                                     fval, 0 );
        /* @TODO add FILTER_FLAG_ALLOW_NULL when we will post-check entry attributes */

        if ( rc )
            return rc;
        break;

    default:
        DisplayLog( LVL_CRIT, PURGE_TAG, "Unknown purge type %d", p_purge_param->type );
        return EINVAL;
    }

    /* Flushing messages before performing DB query */
    FlushLogs(  );

    /* add optimisation filters based on policies */
    if ( !ignore_policies )
        set_purge_optimization_filters(&filter);

    /* optimization: do not retrieve whitelisted entries
    * that do not need fileclass update. Skip this optimization
     * if there is no 'ignore' statement.
     */
    if ( policies.purge_policies.whitelist_count > 0 )
    {
        if ( policies.updt_policy.fileclass.policy == UPDT_NEVER )
        {
            /* filter: release class != ignored */
            fval.value.val_str = CLASS_IGNORED;
            rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_release_class,
                                         NOTEQUAL, fval, FILTER_FLAG_ALLOW_NULL );
            if ( rc )
                return rc;
        }
        else if ( policies.updt_policy.fileclass.policy == UPDT_PERIODIC )
        {
            /* filter: release class != ignored OR update <= now - period */
            fval.value.val_str = CLASS_IGNORED;
            rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_release_class,
                                         NOTEQUAL, fval, FILTER_FLAG_ALLOW_NULL
                                                        | FILTER_FLAG_BEGIN );
            fval.value.val_uint = time(NULL) - policies.updt_policy.fileclass.period_max;
            rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_rel_cl_update,
                                         LESSTHAN, fval, FILTER_FLAG_ALLOW_NULL
                                                        | FILTER_FLAG_OR
                                                        | FILTER_FLAG_END );
        }
    }

    /* start with a limited count of entries, to save memory */
    opt.list_count_max = resmon_config.db_request_limit;
    opt.force_no_acct = FALSE;
    opt.allow_no_attr = FALSE;
    nb_returned = 0;
    total_returned = 0;

    it = ListMgr_Iterator( lmgr, &filter, &sort_type, &opt );

    if ( it == NULL )
    {
        lmgr_simple_filter_free( &filter );
        DisplayLog( LVL_CRIT, PURGE_TAG,
                    "Error retrieving list of candidates from database. "
                    "Purge cancelled." );
        return -1;
    }

    attr_mask_sav = attr_set.attr_mask;

    last_request_time = time( NULL );

    purged_amount = 0;
    purged_vol = 0;

    /* loop on all purge passes */
    do
    {

        /* Retrieve stats before starting purge,
         * for computing a delta later.
         */
        RetrieveQueueStats( &purge_queue, NULL, NULL, NULL, NULL, NULL,
                            status_tab, feedback_before );

        submitted_amount = 0;
        nb_submitted = 0;

        /* List entries for purge */
        do
        {
            unsigned long  tgt_count;

            /* reset attr_mask, if it was altered by last ListMgr_GetNext() call */
            attr_set.attr_mask = attr_mask_sav;

            memset( &entry_id, 0, sizeof( entry_id_t ) );
            rc = ListMgr_GetNext( it, &entry_id, &attr_set );

            if ( purge_abort )
            {
                /* free the last returned entry */
                if (rc == 0)
                    ListMgr_FreeAttrs(&attr_set);

                DisplayLog( LVL_MAJOR, PURGE_TAG, "Purge aborted, stop enqueuing "
                            "purge requests." );
                rc = DB_END_OF_LIST;
                break;
            }
            else if ( rc == DB_END_OF_LIST )
            {
                total_returned += nb_returned;

                /* if limit = inifinite => END OF LIST */
                if ( ( nb_returned == 0 )
                     || ( ( opt.list_count_max > 0 ) &&
                        ( nb_returned < opt.list_count_max ) ) )
                {
                    DisplayLog( LVL_FULL, PURGE_TAG, "End of LRU "
                                "(%u entries returned)", total_returned );
                    end_of_list = TRUE;
                    break;
                }

                /* no new useless request */
                if ( heuristic_end_of_list( last_entry_access ) )
                {
                    end_of_list = TRUE;
                    break;
                }

                /* Free previous iterator */
                ListMgr_CloseIterator( it );

                /* we must wait that migr. queue is empty,
                 * to avoid race conditions (by processing the same
                 * entry twice */
                wait_queue_empty( nb_submitted, feedback_before,
                                  status_tab, feedback_after, FALSE );

                /* perform a new request with next entries */

                /* /!\ if there is already a filter on last_ccess or md_update
                 * only replace it, do not add a new filter.
                 */

                /* don't retrieve just-updated entries
                 * (update>=last_request_time) */
                fval.value.val_int = last_request_time;
                rc = lmgr_simple_filter_add_or_replace( &filter,
                                                        ATTR_INDEX_md_update,
                                                        LESSTHAN_STRICT, fval,
                                                        FILTER_FLAG_ALLOW_NULL);
                if ( rc )
                    return rc;

                if (resmon_config.sort)
                {
                    /* filter on access time */
                    fval.value.val_int = last_entry_access;
                    rc = lmgr_simple_filter_add_or_replace( &filter,
                                                            ATTR_INDEX_last_access,
                                                            MORETHAN, fval,
                                                            FILTER_FLAG_ALLOW_NULL);
                    if ( rc )
                        return rc;
                }

                if (!resmon_config.sort)
                    DisplayLog(LVL_DEBUG, PURGE_TAG,
                               "Performing new request with a limit of %u entries"
                               " and md_update < %ld ",
                               opt.list_count_max, last_request_time);
                else
                     DisplayLog(LVL_DEBUG, PURGE_TAG,
                               "Performing new request with a limit of %u entries"
                               " and access >= %d and md_update < %ld ",
                               opt.list_count_max, last_entry_access,
                               last_request_time);

                nb_returned = 0;
                it = ListMgr_Iterator( lmgr, &filter, &sort_type, &opt );

                if ( it == NULL )
                {
                    lmgr_simple_filter_free( &filter );
                    DisplayLog( LVL_CRIT, PURGE_TAG,
                                "Error retrieving list of candidates from "
                                "database. Purge cancelled." );
                    return -1;
                }
                last_request_time = time( NULL );

                continue;
            }
            else if ( rc != 0 )
            {
                DisplayLog( LVL_CRIT, PURGE_TAG, "Error %d getting next entry of iterator", rc );
                break;
            }

            nb_returned++;

            if ( ATTR_MASK_TEST( &attr_set, last_access ) )
                last_entry_access = ATTR( &attr_set, last_access );

            if ( target_type == TGT_BLOCK )
            {
                if ( p_purge_param->type == PURGE_BY_OST )
                {
#ifdef _LUSTRE
                    unsigned int strp_cnt = ATTR(&attr_set, stripe_info).stripe_count;

                    /* if stripe_count is 0, assume that blk count is 0 too */
                    if ( strp_cnt == 0 )
                        tgt_count = 0;
                    else
                    {
                        tgt_count = ATTR(&attr_set, blocks)/strp_cnt;
                        /* if block count is not multiple of stripe count,
                         * take round.sup of block/OST
                         */
                        if ( (ATTR(&attr_set, blocks) % strp_cnt) != 0 )
                            tgt_count++;
                    }
#else
                    DisplayLog( LVL_CRIT, PURGE_TAG, "Purge by OST is not supported in this mode" );
                    return -ENOTSUP;
#endif
                }
                else
                    tgt_count = ATTR( &attr_set, blocks );
            }
            else
                tgt_count = 1;

            /* Insert candidate */
            rc = Queue_Insert( &purge_queue,
                    Entry2PurgeItem( &entry_id, &attr_set, tgt_count ));
            if ( rc )
                return rc;

            submitted_amount += tgt_count;
            nb_submitted++;

        }
        while ( (submitted_amount + purged_amount < target) || (target_type == TGT_ALL) );

        /* Wait for end of purge pass */
        wait_queue_empty( nb_submitted, feedback_before, status_tab,
                          feedback_after, TRUE );
        /* how much blocks have been purged? (must be count for TGT_COUNT) */
        purged_amount += feedback_after[PURGE_SPECIFIC_COUNT] - feedback_before[PURGE_SPECIFIC_COUNT];
        purged_vol += feedback_after[PURGE_FDBK_BLOCKS] - feedback_before[PURGE_FDBK_BLOCKS];

        /* if getnext returned an error */
        if (rc)
            break;
    }
    while ( ( !end_of_list ) &&
            ( (purged_amount < target) || (target_type == TGT_ALL) ));

    lmgr_simple_filter_free( &filter );
    ListMgr_CloseIterator( it );

    if (p_blks_purged)
        *p_blks_purged = purged_vol;
    if (p_nb_specific)
        *p_nb_specific = purged_amount;

    return (purge_abort?ECANCELED:0);
}

#ifndef _HAVE_FID               /* if entries are accessed by FID, we can always get their status */
inline static int invalidate_entry( lmgr_t * lmgr, entry_id_t * p_entry_id )
{
    attr_set_t     new_attr_set;
    int            rc;

    ATTR_MASK_INIT( &new_attr_set );
    ATTR_MASK_SET( &new_attr_set, invalid );
    ATTR( &new_attr_set, invalid ) = TRUE;

    /* update the entry */
    rc = ListMgr_Update( lmgr, p_entry_id, &new_attr_set );
    if ( rc )
        DisplayLog( LVL_CRIT, PURGE_TAG, "Error %d tagging entry as invalid in database.", rc );

    return rc;
}
#endif

/* declaration from listmgr_common.c */
extern int     readonly_attr_set;

inline static int update_entry( lmgr_t * lmgr, entry_id_t * p_entry_id, attr_set_t * p_attr_set )
{
    int            rc;
    attr_set_t     tmp_attrset = *p_attr_set;

    /* /!\ do not update stripe info */
    /* @TODO actually, the best operation would be to update only attributes that changed */
    ATTR_MASK_UNSET( &tmp_attrset, stripe_info );
    ATTR_MASK_UNSET( &tmp_attrset, stripe_items );

    /* also unset read only attrs */
    tmp_attrset.attr_mask &= ~readonly_attr_set;
#ifdef ATTR_INDEX_creation_time
    /* never update creation time */
    ATTR_MASK_UNSET( &tmp_attrset, creation_time );
#endif

    /* update DB and skip the entry */
    rc = ListMgr_Update( lmgr, p_entry_id, &tmp_attrset );
    if ( rc )
        DisplayLog( LVL_CRIT, PURGE_TAG, "Error %d updating entry in database.", rc );

    return rc;
}

#ifdef _HAVE_FID
/**
 * Check that entry exists
 * @param fill entry MD if entry is valid
 */
static int check_entry( lmgr_t * lmgr, purge_item_t * p_item, attr_set_t * new_attr_set )
{
    char           fid_path[RBH_PATH_MAX];
    struct stat    entry_md;
    int            rc;

    DisplayLog( LVL_FULL, PURGE_TAG, "Considering entry " DFID, PFID( &p_item->entry_id ) );

    /* 1) Build fid path */
    BuildFidPath( &p_item->entry_id, fid_path );

    /* 2) Perform lstat on entry (size is need!) */

    if ( lstat( fid_path, &entry_md ) != 0 )
    {
        rc = errno;

        /* If lstat returns an error, invalidate the entry */
        DisplayLog( LVL_DEBUG, PURGE_TAG, "lstat() failed on %s. Skipping it.", fid_path );

        /* This entry has been processed and has probably removed */
        if ( rc == ENOENT )
            /** @TODO remove entry from DB if errno = ENOENT ? */
            return PURGE_ENTRY_MOVED;
        else
            return PURGE_STAT_FAILURE;
    }

    /* convert posix attributes to attr structure */
    PosixStat2EntryAttr( &entry_md, new_attr_set, TRUE );

    /* set update time of the stucture */
    ATTR_MASK_SET( new_attr_set, md_update );
    ATTR( new_attr_set, md_update ) = time( NULL );

    /* get fullpath or name, if they are needed for applying policy
     * and if it is expired in DB */
    if (((policies.purge_policies.global_attr_mask & ATTR_MASK_fullpath)
          || (policies.purge_policies.global_attr_mask & ATTR_MASK_name)) &&
        need_path_update(&p_item->entry_attr, NULL))
    {
        path_check_update(&p_item->entry_id, fid_path, new_attr_set,
                          policies.purge_policies.global_attr_mask);
    }

#ifdef ATTR_INDEX_status
#ifdef _LUSTRE_HSM
    /* For Lustre-HSM, don't care about fresh status because 'release'
     * is safe (Lustre locking + check of open/modified files)
     */
    /* is status known? */
    if ( !ATTR_MASK_TEST( &p_item->entry_attr, status )
        || ATTR(&p_item->entry_attr, status) == STATUS_UNKNOWN )
    {
        DisplayLog( LVL_FULL, PURGE_TAG, "Update of HSM state (not known in DB)" );
        rc = LustreHSM_GetStatus( fid_path, &ATTR( new_attr_set, status ),
                                  &ATTR( new_attr_set, no_release ),
                                  &ATTR( new_attr_set, no_archive ) );
        if ( !rc )
        {
            ATTR_MASK_SET( new_attr_set, status );
            ATTR_MASK_SET( new_attr_set, no_release );
            ATTR_MASK_SET( new_attr_set, no_archive );
        }
    }

#elif defined(_HSM_LITE)
    /* always update status for _HSM_LITE */
    rc = rbhext_get_status( &p_item->entry_id, new_attr_set, new_attr_set );
    if ( rc )
        return PURGE_ERROR;
#endif
#endif

    /* entry is valid */
    return PURGE_OK;
}
#else /* no FID */
/**
 * Check that entry exists with the good path and its id is consistent.
 * @param fill entry MD if entry is valid
 */
static int check_entry( lmgr_t * lmgr, purge_item_t * p_item, attr_set_t * new_attr_set )
{
    struct stat    entry_md;
    char * stat_path;
#ifdef _HAVE_FID
    char fid_path[1024];
#endif

#ifndef _HAVE_FID
    /* 1) Check if fullpath is set (if no fid support) */
    if ( !ATTR_MASK_TEST( &p_item->entry_attr, fullpath ) )
    {
        DisplayLog( LVL_DEBUG, PURGE_TAG, "Warning: entry fullpath is not set. Tagging it invalid." );
        invalidate_entry( lmgr, &p_item->entry_id );

        /* not enough metadata */
        return PURGE_PARTIAL_MD;
    }
    stat_path = ATTR(  &p_item->entry_attr, fullpath );
#else
    BuildFidPath( &p_item->entry_id, fid_path );
    stat_path = fid_path;
#endif

if ( ATTR_MASK_TEST( &p_item->entry_attr, fullpath ) )
    DisplayLog( LVL_FULL, PURGE_TAG, "Considering entry %s", ATTR(  &p_item->entry_attr, fullpath ) );
#ifdef _HAVE_FID
else
    DisplayLog( LVL_FULL, PURGE_TAG, "Considering entry with fid="DFID, PFID(&p_item->entry_id) );
#endif

    /* 2) Perform lstat on entry */

    if ( lstat( stat_path, &entry_md ) != 0 )
    {
        /* If lstat returns an error, invalidate the entry */
        DisplayLog( LVL_DEBUG, PURGE_TAG, "lstat() failed on %s. Tagging it invalid.",
                    stat_path );
        invalidate_entry( lmgr, &p_item->entry_id );

        /* This entry has been processed and has probably moved */
        return PURGE_ENTRY_MOVED;
    }

    /* 3) check entry id and fskey */

    if ( ( entry_md.st_ino != p_item->entry_id.inode )
         || ( get_fskey() != p_item->entry_id.fs_key ) )
    {
        /* If it has changed, invalidate the entry (fullpath does not match entry_id, it will be updated or removed at next FS scan). */
        DisplayLog( LVL_DEBUG, PURGE_TAG, "Inode of %s changed: old=<%llu,%llu>, "
                    "new=<%llu,%llu>. Tagging it invalid.",
                    ATTR(  &p_item->entry_attr, fullpath ),
                    ( unsigned long long ) p_item->entry_id.inode,
                    ( unsigned long long ) p_item->entry_id.fs_key,
                    ( unsigned long long ) entry_md.st_ino,
                    ( unsigned long long ) get_fskey() );

        invalidate_entry( lmgr, &p_item->entry_id );

        /* This entry has been processed and has probably moved */
        return PURGE_ENTRY_MOVED;
    }

    /* convert posix attributes to attr structure */
    PosixStat2EntryAttr( &entry_md, new_attr_set, TRUE );

    /* set update time of the stucture */
    ATTR_MASK_SET( new_attr_set, md_update );
    ATTR( new_attr_set, md_update ) = time( NULL );

    /* entry is valid */
    return PURGE_OK;
}
#endif


/**
 * Manage an entry by path or by fid, depending on FS
 */
static void ManageEntry( lmgr_t * lmgr, purge_item_t * p_item )
{
    attr_set_t     new_attr_set;
    unsigned long long feedback[PURGE_FDBK_COUNT];
    int            rc;

    policy_match_t match;
    policy_item_t *policy_case;
    fileset_item_t *p_fileset;
    unsigned long   blk_sav = 0;

    int update_fileclass = -1; /* not set */
    int lastrm;

/* acknowledging helper */
#define Acknowledge( _q, _status, _fdbk1, _fdbk2 )  do {                \
                               memset(feedback, 0, sizeof(feedback));   \
                               feedback[PURGE_FDBK_BLOCKS] = _fdbk1;    \
                               feedback[PURGE_SPECIFIC_COUNT] = _fdbk2; \
                               Queue_Acknowledge( _q, _status, feedback, PURGE_FDBK_COUNT ); \
                            } while(0)

    if ( purge_abort )
    {
       /* migration aborted by a signal, doesn't submit new migrations */
       DisplayLog( LVL_FULL, PURGE_TAG, "Purge aborted: threads skipping purge requests" );
       Acknowledge( &purge_queue, PURGE_ABORT, 0, 0 );
       rc = PURGE_ABORT;
       goto end;
    }

    DisplayLog( LVL_FULL, PURGE_TAG,
                "Checking if entry %s can be released", ATTR( &p_item->entry_attr, fullpath ) );

    ATTR_MASK_INIT( &new_attr_set );

    rc = check_entry( lmgr, p_item, &new_attr_set );
    if ( rc != PURGE_OK )
    {
        Acknowledge( &purge_queue, rc, 0, 0 );
        goto end;
    }

    /* Merge with missing attrs from database */
    ListMgr_MergeAttrSets( &new_attr_set, &p_item->entry_attr, FALSE );

#ifdef ATTR_INDEX_invalid
    /* From here, assume that entry is valid */
    ATTR_MASK_SET( &new_attr_set, invalid );
    ATTR( &new_attr_set, invalid ) = FALSE;
#endif

#ifdef ATTR_INDEX_no_release
    /* check that the entry has the expected status */
    if ( ATTR_MASK_TEST( &new_attr_set, no_release ) && ATTR( &new_attr_set, no_release ) )
    {
        /* this entry is now tagged 'no_release' */
        DisplayLog( LVL_MAJOR, PURGE_TAG,
                    "Entry " DFID_NOBRACE " is now tagged 'no_release', skipping it.",
                    PFID( &p_item->entry_id ) );

        /* update DB and skip the entry */
        update_entry( lmgr, &p_item->entry_id, &new_attr_set );

        /* Notify that this entry is whitelisted */
        Acknowledge( &purge_queue, PURGE_ENTRY_WHITELISTED, 0, 0 );

        goto end;
    }
#endif

#ifdef ATTR_INDEX_status
    if ( !ATTR_MASK_TEST( &new_attr_set, status ) )
    {
        DisplayLog( LVL_MAJOR, PURGE_TAG, "Warning: entry status should be set at this point");
        Acknowledge( &purge_queue, PURGE_PARTIAL_MD, 0, 0 );
        goto end;
    }
    else if ( ATTR( &new_attr_set, status ) != STATUS_SYNCHRO )
    {
        /* status changed */
        DisplayLog( LVL_MAJOR, PURGE_TAG,
                    "%s: entry status recently changed (%s): skipping entry.",
                    ATTR(&new_attr_set,fullpath),
                    db_status2str(ATTR(&new_attr_set, status),1));

        /* update DB and skip the entry */
        update_entry( lmgr, &p_item->entry_id, &new_attr_set );

        /* Notify that this entry is whitelisted */
        Acknowledge( &purge_queue, PURGE_STATUS_CHGD, 0, 0 );
        goto end;
    }
#endif

    update_fileclass = need_fileclass_update( &new_attr_set, PURGE_POLICY );
    if ( update_fileclass == -1 )
    {
        Acknowledge( &purge_queue, PURGE_ERROR, 0, 0 );
        goto end;
    }

    /* if ignore-policies flag is specified:
     * - don't check rules
     * - don't care about recent atime etc...
     */
    if ( !ignore_policies )
    {

        /* 4) check whitelist rules */

        if ( update_fileclass )
            match = IsWhitelisted( &p_item->entry_id, &new_attr_set,
                                   PURGE_POLICY );
        else
        {
            /* fileclass is still valid, check if it is whitelisted */
            if ( !strcmp( ATTR(&new_attr_set, release_class ), CLASS_IGNORED ) )
                match = POLICY_MATCH;
            else if ( WhitelistedClass( ATTR(&new_attr_set, release_class ),
                                        PURGE_POLICY ) )
                match = POLICY_MATCH;
            else
                match = POLICY_NO_MATCH;
        }

        if ( match == POLICY_MATCH )
        {
            /* update DB and skip the entry */
            update_entry( lmgr, &p_item->entry_id, &new_attr_set );

            /* Notify that this entry is whitelisted */
            Acknowledge( &purge_queue, PURGE_ENTRY_WHITELISTED, 0, 0 );

            goto end;
        }
        else if ( match != POLICY_NO_MATCH )
        {
            /* Cannot determine if entry is whitelisted: skip it (do nothing in database) */
            DisplayLog( LVL_MAJOR, PURGE_TAG,
                        "Warning: cannot determine if entry %s is whitelisted: skipping it.",
                        ATTR( &p_item->entry_attr, fullpath ) );

            /* Notify error */
            Acknowledge( &purge_queue, PURGE_PARTIAL_MD, 0, 0 );

            goto end;
        }

        /* 5) check that entry has not been accessed.
         *   - If is has been accessed, update its info.
         *   - Else, perform purge, and remove entry from database /!\ nlink ?.
         */

        int atime_check = TRUE;

        /* for directories or links, don't check access time as it is modified
         * by robinhood itself will collecting info about entry.
         */
        if (ATTR_MASK_TEST(&p_item->entry_attr, type) &&
            (!strcmp(ATTR(&p_item->entry_attr, type), STR_TYPE_LINK)
             || !strcmp(ATTR(&p_item->entry_attr, type), STR_TYPE_DIR)))
            atime_check = FALSE;

        if ((atime_check && !ATTR_MASK_TEST( &p_item->entry_attr, last_access ))
             || !ATTR_MASK_TEST( &p_item->entry_attr, size ) )
        {
            /* cannot determine if entry has been accessed: update and skip it */
            DisplayLog( LVL_MAJOR, PURGE_TAG,
                        "Warning: previous value of 'last_access' or 'size' "
                        "is not available: cannot determine if entry has been "
                        "accessed. Skipping entry." );
            update_entry( lmgr, &p_item->entry_id, &new_attr_set );

            /* Notify error */
            Acknowledge( &purge_queue, PURGE_PARTIAL_MD, 0, 0 );

            goto end;
        }

        if ( (atime_check && (ATTR( &p_item->entry_attr, last_access ) < ATTR( &new_attr_set, last_access )))
             || ( ATTR( &p_item->entry_attr, size ) != ATTR( &new_attr_set, size ) ) )
        {
            DisplayLog( LVL_DEBUG, PURGE_TAG,
                        "%s has been accessed or modified since it was updated. Skipping entry.",
                        ATTR( &p_item->entry_attr, fullpath ) );
            DisplayLog( LVL_FULL, PURGE_TAG,
                        "atime before=%d, after=%d | size before=%llu, after=%llu",
                        ( int ) ATTR( &p_item->entry_attr, last_access ),
                        ( int ) ATTR( &new_attr_set, last_access ),
                        ( unsigned long long ) ATTR( &p_item->entry_attr, size ),
                        ( unsigned long long ) ATTR( &new_attr_set, size ) );

            update_entry( lmgr, &p_item->entry_id, &new_attr_set );

            Acknowledge( &purge_queue, PURGE_ENTRY_ACCESSED, 0, 0 );

            goto end;
        }

    } /* end if 'don't ignore policies' */

    /* check purge policy for the entry */

    /* if fileclass is still valid, retrieve the related policy case */
    if ( !update_fileclass )
    {
        policy_case = GetPolicyCaseByClass( ATTR(&new_attr_set, release_class ),
                                            PURGE_POLICY, &p_fileset );
        /* update if GetPolicyCaseByClass returned NULL */
        if ( policy_case == NULL )
            update_fileclass = TRUE;
    }

    if ( update_fileclass )
        policy_case = GetPolicyCase( &p_item->entry_id, &new_attr_set,
                                    PURGE_POLICY, &p_fileset );

    if ( !policy_case )
    {
        update_entry( lmgr, &p_item->entry_id, &new_attr_set );
        Acknowledge( &purge_queue, PURGE_NO_POLICY, 0, 0 );
        goto end;
    }
    else
    {
        /* remember the matched fileclass */
        if ( p_fileset )
            strcpy( ATTR( &new_attr_set, release_class ), p_fileset->fileset_id );
        else
            strcpy( ATTR( &new_attr_set, release_class ), CLASS_DEFAULT );
        ATTR_MASK_SET( &new_attr_set, release_class );
        ATTR( &new_attr_set, rel_cl_update ) = time(NULL);
        ATTR_MASK_SET( &new_attr_set, rel_cl_update );
    }


    /* don't care about policy condition if 'ignore-policies' flag is specified */
    if ( !ignore_policies )
    {

        /* check if the entry matches the policy condition */
        switch ( EntryMatches( &p_item->entry_id, &new_attr_set, &policy_case->condition, NULL ) )
        {
        case POLICY_NO_MATCH:
            /* entry is not eligible now */
            update_entry( lmgr, &p_item->entry_id, &new_attr_set );
            Acknowledge( &purge_queue, PURGE_ENTRY_WHITELISTED, 0, 0 );
            goto end;
            break;
        case POLICY_MATCH:
            /* OK, can be purged */
            DisplayLog( LVL_DEBUG, PURGE_TAG,
                        "Entry %s matches the condition for policy '%s'.",
                        ATTR( &p_item->entry_attr, fullpath ), policy_case->policy_id );
            break;
        default:
            /* Cannot determine if entry matches the policy condition */
            DisplayLog( LVL_MAJOR, PURGE_TAG,
                        "Warning: cannot determine if entry %s matches the condition for policy '%s': skipping it.",
                        ATTR( &p_item->entry_attr, fullpath ), policy_case->policy_id );

            /* Notify error */
            Acknowledge( &purge_queue, PURGE_PARTIAL_MD, 0, 0 );

            goto end;
        }
    }

    /* save block count before purging */
    if (!ATTR_MASK_TEST(&new_attr_set, blocks))
        DisplayLog( LVL_MAJOR, PURGE_TAG, "Warning: no block count available for computing purged amount");
    else
        blk_sav = ATTR(&new_attr_set, blocks);

    /* Perform purge operation! */

#ifdef _LUSTRE_HSM
    rc = PurgeEntry_ByFid( &p_item->entry_id, &new_attr_set );

    if ( rc == 0 )
    {
        /* new status is release pending */
        ATTR_MASK_SET( &new_attr_set, status );
        ATTR( &new_attr_set, status ) = STATUS_RELEASE_PENDING;
    }
    else
    {
        char fid_path[RBH_PATH_MAX];
        BuildFidPath( &p_item->entry_id, fid_path );

        /* we probably have a wrong status for this entry: refresh it */

        rc = LustreHSM_GetStatus( fid_path, &ATTR( &new_attr_set, status ),
                                  &ATTR( &new_attr_set, no_release ),
                                  &ATTR( &new_attr_set, no_archive ) );
        if ( !rc )
        {
            ATTR_MASK_SET( &new_attr_set, status );
            ATTR_MASK_SET( &new_attr_set, no_release );
            ATTR_MASK_SET( &new_attr_set, no_archive );
        }
    }
#elif defined(_HSM_LITE)
    rc = PurgeEntry_ByFid( &p_item->entry_id, &new_attr_set );

    if ( rc == 0 )
    {
        /* new status is released */
        ATTR_MASK_SET( &new_attr_set, status );
        ATTR( &new_attr_set, status ) = STATUS_RELEASED;
    }
#else
    /* FIXME should remove all paths to the object */
    rc = PurgeEntry(&p_item->entry_id, ATTR(&new_attr_set, fullpath));
#endif
    if ( rc )
    {
        DisplayLog( LVL_DEBUG, PURGE_TAG, "Error purging entry %s: %s",
                    ATTR( &new_attr_set, fullpath ), strerror( abs(rc) ) );

        update_entry( lmgr, &p_item->entry_id, &new_attr_set );

        Acknowledge( &purge_queue, PURGE_ERROR, 0, 0 );
    }
    else
    {
        char           straccess[256];
        char           strsize[256];
#ifdef _LUSTRE
        char           strstorage[24576];
#endif

        /* Entry has been successfully purged */

        /* report messages */

        FormatDurationFloat( straccess, 256, time( NULL ) - ATTR( &new_attr_set, last_access ) );
        FormatFileSize( strsize, 256, ATTR( &new_attr_set, size ) );

#ifdef _LUSTRE
        if ( ATTR_MASK_TEST( &p_item->entry_attr, stripe_items ) )
            FormatStripeList( strstorage, sizeof(strstorage), &ATTR( &p_item->entry_attr, stripe_items ), 0 );
        else
            strcpy( strstorage, "(none)" );
#endif

#ifdef _LUSTRE_HSM
#define ACTION_ED "Released"
#else
#define ACTION_ED "Purged"
#endif

        DisplayLog( LVL_DEBUG, PURGE_TAG,
                    ACTION_ED " '%s' using policy '%s', last access %s ago, size=%s"
#ifdef _LUSTRE
                    ", stored on %s"
#endif
                    , ATTR( &p_item->entry_attr, fullpath ), policy_case->policy_id, straccess,
                    strsize
#ifdef _LUSTRE
                    , strstorage
#endif
 );

        DisplayReport( ACTION_ED " '%s' using policy '%s', last access %s ago | size=%"PRIu64
                       ", last_access=%" PRI_TT ", last_mod=%" PRI_TT
#ifdef _LUSTRE
                       ", osts=%s"
#endif
                       , ATTR( &p_item->entry_attr, fullpath ),
                       policy_case->policy_id, straccess, ATTR( &new_attr_set, size ),
                       (time_t)ATTR( &new_attr_set, last_access ),
                       (time_t)ATTR( &new_attr_set, last_mod )
#ifdef _LUSTRE
                        , strstorage
#endif
                       );

#if defined(_LUSTRE_HSM) || defined(_HSM_LITE)
        /* Lustre-HSM: do not remove the entry from database:
         * a chglog record will confirm that the entry has been
         * release successfully */
        /* Shook: don't remove the entry (need to keep path in backend) */
        update_entry( lmgr, &p_item->entry_id, &new_attr_set );

#else
        /* if nlink is set, check if it the last unlink.
         * else, consider it is (like robinhood version <= 2.4 did).
        */
        if (ATTR( &new_attr_set, nlink))
            lastrm = (ATTR(&new_attr_set, nlink) == 1);
        else
            lastrm = 1;
        /* remove it from database */
        rc = ListMgr_Remove( lmgr, &p_item->entry_id,
                             &p_item->entry_attr, /* must be based on the DB content = old attrs */
                             lastrm );
        if ( rc )
            DisplayLog( LVL_CRIT, PURGE_TAG, "Error %d removing entry from database.", rc );
#endif

        /* ack to queue manager */
        Acknowledge( &purge_queue, PURGE_OK, blk_sav, p_item->specific_blocks );

    }

    /* XXX /!\ what to do if (nlink > 1) ??? /!\ */

  end:
    /* free entry resources */
    FreePurgeItem( p_item );
    return;
}




/**
 *  Main routine of purge pthread
 */
static void   *Thr_Purge( void *arg )
{
    int            rc;
    lmgr_t         lmgr;
    void          *p_queue_entry;

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, PURGE_TAG, "Could not connect to database (error %d). Exiting.", rc );
        exit( rc );
    }

    while ( Queue_Get( &purge_queue, &p_queue_entry ) == 0 )
    {
        ManageEntry( &lmgr, ( purge_item_t * ) p_queue_entry );
    }

    /* Error occured in purge queue management... */
    DisplayLog( LVL_CRIT, PURGE_TAG, "An error occured in purge queue management. Exiting." );
    exit( -1 );
    return NULL;                /* for avoiding compiler warnings */
}

/* array of purge threads */
static pthread_t *thread_ids = NULL;

int start_purge_threads( unsigned int nb_threads )
{
    unsigned int   i;

    thread_ids = ( pthread_t * ) MemCalloc( nb_threads, sizeof( pthread_t ) );
    if ( !thread_ids )
    {
        DisplayLog( LVL_CRIT, PURGE_TAG, "Memory error in %s", __FUNCTION__ );
        return ENOMEM;
    }

    for ( i = 0; i < nb_threads; i++ )
    {
        if ( pthread_create( &thread_ids[i], NULL, Thr_Purge, NULL ) != 0 )
        {
            int            rc = errno;
            DisplayLog( LVL_CRIT, PURGE_TAG, "Error %d creating purge thread in %s: %s", rc,
                        __FUNCTION__, strerror( rc ) );
            return rc;
        }
    }
    return 0;
}

void abort_purge()
{
    purge_abort = TRUE;
}

#ifdef ATTR_INDEX_status
/**
 * Update the status of outstanding purges
 * \param lmgr          [IN] connexion to database
 * \param p_nb_reset    [OUT] number of purge reset
 * \param p_nb_total    [OUT] total number of purge checked
 */
int  check_current_purges( lmgr_t * lmgr, unsigned int *p_nb_reset,
                           unsigned int * p_nb_total,
                           time_t timeout )
{
    int            rc;
    struct lmgr_iterator_t *it = NULL;

    lmgr_filter_t  filter;
    filter_value_t fval;

    purge_item_t purge_item;

    unsigned int nb_returned = 0;
    unsigned int nb_aborted = 0;
    int          attr_mask_sav = 0;
#ifdef _HSM_LITE
    unsigned int allow_cached_attrs = 0;
    unsigned int need_fresh_attrs = 0;
#endif

    /* attributes to be retrieved */
    ATTR_MASK_INIT( &purge_item.entry_attr );
    ATTR_MASK_SET( &purge_item.entry_attr, fullpath );
    ATTR_MASK_SET( &purge_item.entry_attr, path_update );
    /* /!\ don't retrieve status, to force getting it from the filesystem */

#ifdef _HSM_LITE
    ATTR_MASK_SET( &purge_item.entry_attr, type );

    /* what information the backend needs from DB? */
    rc = rbhext_status_needs( TYPE_NONE, &allow_cached_attrs, &need_fresh_attrs );
    if (rc != 0)
    {
        DisplayLog(LVL_MAJOR, PURGE_TAG, "Unexpected error from rbhext_status_needs(), in %s line %u: %d",
                   __FUNCTION__, __LINE__, rc );
        return rc;
    }
    purge_item.entry_attr.attr_mask |= allow_cached_attrs;
#endif

    attr_mask_sav = purge_item.entry_attr.attr_mask;

    rc = lmgr_simple_filter_init( &filter );
    if ( rc )
        return rc;

    /* if timeout is > 0, only select entries whose last update
     * is old enough (last_update <= now - timeout) or NULL*/
    if ( timeout > 0 )
    {
        fval.value.val_int = time(NULL) - timeout;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_md_update, LESSTHAN,
                fval, FILTER_FLAG_ALLOW_NULL );
        if ( rc )
            return rc;
    }

    /* filter on current status RESTORE_RUNNING or RELEASE_PENDING
     * also check values with NULL status */

     /* '( status = RELEASE_PENDING' ... */
    fval.value.val_int = STATUS_RELEASE_PENDING;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL, fval,
                FILTER_FLAG_BEGIN );
    if ( rc )
        return rc;

    /* ...' OR status = RESTORE_RUNNING OR status is NULL )' */
    fval.value.val_int = STATUS_RESTORE_RUNNING;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL, fval,
                FILTER_FLAG_OR | FILTER_FLAG_ALLOW_NULL | FILTER_FLAG_END );
    if ( rc )
        return rc;

#ifdef ATTR_INDEX_invalid
    /* don't retrieve invalid entries (allow entries with invalid == NULL) */
    fval.value.val_int = TRUE;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_invalid, NOTEQUAL, fval,
            FILTER_FLAG_ALLOW_NULL );
    if ( rc )
        return rc;
#endif

    it = ListMgr_Iterator( lmgr, &filter, NULL, NULL );

    if ( it == NULL )
    {
        lmgr_simple_filter_free( &filter );
        DisplayLog( LVL_CRIT, PURGE_TAG,
                    "Error retrieving the list of current purges. Recovery cancelled." );
        return -1;
    }

    memset( &purge_item, 0, sizeof(purge_item_t) );
    purge_item.entry_attr.attr_mask = attr_mask_sav;

    while ( (rc = ListMgr_GetNext( it, &purge_item.entry_id, &purge_item.entry_attr ))
                == DB_SUCCESS )
    {
        nb_returned ++;

        if ( ATTR_MASK_TEST(  &purge_item.entry_attr, fullpath ) )
            DisplayLog( LVL_VERB, PURGE_TAG, "Updating status of '%s'...",
                ATTR( &purge_item.entry_attr, fullpath ) );

        /* check entry */
        if ( check_entry( lmgr, &purge_item, &purge_item.entry_attr ) == PURGE_OK )
        {
            /* check new status */
            if (ATTR_MASK_TEST( &purge_item.entry_attr, status)
                 && (ATTR(&purge_item.entry_attr, status) == STATUS_RESTORE_RUNNING))
            {
               DisplayLog( LVL_EVENT, PURGE_TAG, "%s: restore still running",
                           ATTR(&purge_item.entry_attr, fullpath) );
            }
            else if (ATTR_MASK_TEST( &purge_item.entry_attr, status)
                 && (ATTR(&purge_item.entry_attr, status) == STATUS_RELEASE_PENDING))
            {
               DisplayLog( LVL_EVENT, PURGE_TAG, "%s: release still in progress",
                           ATTR(&purge_item.entry_attr, fullpath) );
            }
            else
            {
               DisplayLog( LVL_EVENT, PURGE_TAG, "%s: operation finished\n",
                           ATTR(&purge_item.entry_attr, fullpath) );
               nb_aborted++;
            }
            /* update entry status */
            update_entry( lmgr, &purge_item.entry_id,  &purge_item.entry_attr );
        }
        else
            nb_aborted ++;

        /* reset attr_mask, if it was altered by last ListMgr_GetNext() call */
        memset( &purge_item, 0, sizeof(purge_item_t) );
        purge_item.entry_attr.attr_mask = attr_mask_sav;
    }

    lmgr_simple_filter_free( &filter );
    ListMgr_CloseIterator( it );

    if ( p_nb_total )
        *p_nb_total = nb_returned;

    if ( p_nb_reset )
        *p_nb_reset = nb_aborted;

    /* check rc */
    if ( rc != DB_END_OF_LIST )
    {
        DisplayLog( LVL_CRIT, PURGE_TAG, "Error %d getting next entry of iterator", rc );
        return -1;
    }

    return 0;
}
#endif
