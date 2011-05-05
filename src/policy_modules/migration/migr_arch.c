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

#include "migration.h"
#include "migr_arch.h"
#include "queue.h"
#include "list_mgr.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "RobinhoodConfig.h"
#include "Memory.h"
#include "xplatform_print.h"

#ifdef _SHERPA
#include <SHERPA_CacheAcces.h>
#include <SHERPA_CacheAccesP.h>
#endif
#ifdef _BACKUP_FS
#include "backend_mgr.h"
#include "backend_ext.h"
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

#define MIGR_TAG   "Migr"
#define CHECK_MIGR_INTERVAL    1

/* Module configuration */
migration_config_t migr_config;

static int migr_flags = 0;
static int migr_abort = FALSE;

static const policy_modifier_t * migr_pol_mod = NULL;

#define ignore_policies ( migr_flags & FLAG_IGNORE_POL )
#define dry_run (migr_flags & FLAG_DRY_RUN)
#define no_limit (migr_flags & FLAG_NO_LIMIT)

/* queue of entries to be checked/migrated */
entry_queue_t  migr_queue;

void abort_migration()
{
    migr_abort = TRUE;
}

/** 
 * Migration helpers (depending on purpose)
 * @return posix error code
 */

#ifdef _LUSTRE_HSM
static migr_status_t MigrateEntry_ByFid( const entry_id_t * p_entry_id,
                                         char *hints, unsigned int archive_num )
{
    if ( hints )
        DisplayLog( LVL_EVENT, MIGR_TAG,
            "Start archiving(" DFID_NOBRACE ", hints='%s', archive_num=%u)",
            PFID( p_entry_id ), hints, archive_num );
    else
        DisplayLog( LVL_EVENT, MIGR_TAG,
            "Start archiving(" DFID_NOBRACE ", <no_hints>, archive_num=%u)",
             PFID( p_entry_id ), archive_num );

    if (dry_run)
        return MIGR_OK;

    return LustreHSM_Action( HUA_ARCHIVE, p_entry_id, hints, archive_num );
}
#elif defined(_SHERPA)
static migr_status_t MigrateEntry_ByPath( const char * path, char *hints ) 
{
    char tmp[1024];
    if ( hints )
        DisplayLog( LVL_EVENT, MIGR_TAG,
            "Starting sherpa_majref('%s', hints='%s')", path, hints );
    else
        DisplayLog( LVL_EVENT, MIGR_TAG,
            "Starting sherpa_majref('%s', <no_hints>)", path );

    if (dry_run)
        return MIGR_OK;

    return sherpa_maj_reference_entree(path, NULL, tmp, SHERPA_FLAG_ZAPPER);
}
#elif defined(_BACKUP_FS)

static migr_status_t MigrateEntry( const entry_id_t * p_id,
                                   attr_set_t * p_attrs,
                                   const char * hints )
{
    char fid_str[128];
    const char * entry = "?";
    if ( ATTR_MASK_TEST(p_attrs, fullpath) )
        entry = ATTR( p_attrs, fullpath );
#ifdef _HAVE_FID
    else
    {
        sprintf(fid_str, "fid="DFID, PFID(p_id) );
        entry = fid_str;
    }
#else
    else
    {
        sprintf(fid_str, "[dev=%llu, ino=%llu]", (unsigned long long)p_id->device,
                (unsigned long long)p_id->inode );
        entry = fid_str;
    }
#endif

    if ( hints )
        DisplayLog( LVL_EVENT, MIGR_TAG,
            "Starting backup(%s, hints='%s')", entry, hints );
    else
        DisplayLog( LVL_EVENT, MIGR_TAG,
            "Starting backup(%s, <no_hints>)", entry );

    if (dry_run)
        return MIGR_OK;

    return rbhext_archive( (backend.async_archive ? RBHEXT_ASYNC:RBHEXT_SYNC),
                         p_id, p_attrs, hints );
}
#endif

typedef struct migr_item__
{
    entry_id_t     entry_id;
    attr_set_t     entry_attr;
} migr_item_t;

/**
 *  Alloc a new migr item so it can be pushes to the migr queue.
 */
static void   *Entry2MigrItem( entry_id_t * p_entry_id, attr_set_t * p_attr_set )
{
    migr_item_t   *new_entry;

    new_entry = ( migr_item_t * ) MemAlloc( sizeof( migr_item_t ) );
    if ( !new_entry )
        return NULL;

    new_entry->entry_id = *p_entry_id;
    new_entry->entry_attr = *p_attr_set;

    return new_entry;
}

/**
 * Free a migration Item (and the resources of entry_attr).
 */
static void FreeMigrItem( migr_item_t * item )
{
    ListMgr_FreeAttrs( &item->entry_attr );
    MemFree( item );
}


static int heuristic_end_of_list( time_t last_mod_time )
{
    entry_id_t     void_id;
    attr_set_t     void_attr;

    /* list all files if policies are ignored */
    if ( ignore_policies )
        return FALSE;

    /* XXX Tip for optimization:
     * we build a void entry with last_mod = last_mod_time
     * and last_archive_time = last_mod_time.
     * If it doesn't match any policy, next entries won't match too
     * because entries are sorted by last modification time,
     * so it is not necessary to continue.
     * (Note that we have last_archive_time < last_modification_time (entry is dirty)).
     */
    memset( &void_id, 0, sizeof( entry_id_t ) );
    memset( &void_attr, 0, sizeof( attr_set_t ) );

    ATTR_MASK_INIT( &void_attr );
    ATTR_MASK_SET( &void_attr, last_mod );
    ATTR( &void_attr, last_mod ) = last_mod_time;
#ifdef ATTR_INDEX_last_archive
    ATTR_MASK_SET( &void_attr, last_archive );
    ATTR( &void_attr, last_archive ) = last_mod_time;
#endif

    if ( PolicyMatchAllConditions( &void_id, &void_attr, MIGR_POLICY,
                                   migr_pol_mod ) == POLICY_NO_MATCH )
    {
        DisplayLog( LVL_DEBUG, MIGR_TAG,
                    "Optimization: entries with modification time later than %lu"
                    " cannot match any policy condition. Stop retrieving DB entries.",
                    last_mod_time );
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

    for ( i = 0; i < MIGR_ST_COUNT; i++ )
        sum += status_tab[i];

    return sum;
}

/* return 0 if limit is not reached, a non null value else */
static inline int check_migration_limit( unsigned int count, unsigned long long vol )
{
    if ( no_limit )
        return 0;

    if ( ( migr_config.max_migr_nbr > 0 ) && ( count >= migr_config.max_migr_nbr ) )
    {
        DisplayLog( LVL_EVENT, MIGR_TAG, "max migration count %u is reached.", migr_config.max_migr_nbr);
        return 1;
    }
    if ( ( migr_config.max_migr_vol > 0 ) && ( vol >= migr_config.max_migr_vol ) )
    {
        DisplayLog( LVL_EVENT, MIGR_TAG, "max migration volume %llu is reached.", migr_config.max_migr_vol);
        return 1;
    }
    return 0;
}

/**
 * build a filter from policies, to optimize DB queries.
 */
static int set_migr_optimization_filters(lmgr_filter_t * p_filter)
{
    /** @TODO build a filter for getting the union of all filesets/conditions */

    /* If there is a single policy, try to convert its condition
     * to a simple filter.
     */
    if ( policies.migr_policies.policy_count == 1 )
    {
        if ( convert_boolexpr_to_simple_filter( &policies.migr_policies.policy_list[0].condition,
             p_filter ) )
        {
            DisplayLog( LVL_FULL, MIGR_TAG, "Could not convert migration policy '%s' to simple filter.",
                policies.migr_policies.policy_list[0].policy_id );
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
    unsigned int nb_in_queue, nb_migr_pending;
    unsigned int   status_tab[MIGR_ST_COUNT];

    /* Wait for end of migration pass */
    do
    {
        time_t last_push, last_pop, last_ack, last_activity;
        last_push = last_pop = last_ack = last_activity = 0;

        RetrieveQueueStats( &migr_queue, NULL, &nb_in_queue,
                            &last_push, &last_pop, &last_ack,
                            status_tab, feedback_after );

        /* the last time a request was pushed/poped/acknowledged */
        last_activity = MAX3( last_push, last_pop, last_ack );

        /* nb of migr operation pending = nb_enqueued - ( nb ack after - nb ack before ) */
        nb_migr_pending = nb_submitted + ack_count( status_tab_init )
                            - ack_count( status_tab );

        if ( ( nb_in_queue != 0 ) || ( nb_migr_pending != 0 ) )
        {
            /* abort this migrations pass if the last action was done a too long time ago */
            if ( ( migr_config.migration_timeout != 0 ) &&
                 (time(NULL) - last_activity > migr_config.migration_timeout) )
            {
                DisplayLog( LVL_MAJOR, MIGR_TAG,
                            "Migration pass time-out: %u migrations inactive for %us",
                            nb_migr_pending, (unsigned int) (time(NULL) - last_activity) );
                /* don't wait for current migrations to end, continue with other entries */
                return ETIME;
            }

            DisplayLog( LVL_DEBUG, MIGR_TAG,
                        "Waiting for the end of this migr pass: "
                        "still %u files to be archived "
                        "(%u in queue, %u being processed). "
                        "Last action: %"PRI_TT"s ago.",
                        nb_migr_pending, nb_in_queue,
                        nb_migr_pending - nb_in_queue,
                        time(NULL) - last_activity );

            if ( long_sleep )
                rh_sleep( CHECK_MIGR_INTERVAL );
            else
                rh_usleep( 1000 );
        }
        else
            DisplayLog( LVL_DEBUG, MIGR_TAG, "End of this migration pass" );

    }
    while ( ( nb_in_queue != 0 ) || ( nb_migr_pending != 0 ) );

    return 0;
}

/* indicates attributes to be retrieved from db */
static int init_db_attr_mask( attr_set_t * p_attr_set )
{
#ifdef _BACKUP_FS
    int rc;
    unsigned int allow_cached_attrs = 0;
    unsigned int need_fresh_attrs = 0;
#endif

    ATTR_MASK_INIT( p_attr_set );

    /* Retrieve at least: fullpath, last_mod, last_archive, size
     * for logging or computing statistics */
    ATTR_MASK_SET( p_attr_set, fullpath );
    ATTR_MASK_SET( p_attr_set, path_update );
    ATTR_MASK_SET( p_attr_set, last_mod );
    ATTR_MASK_SET( p_attr_set, size );
#ifdef ATTR_INDEX_last_archive
    ATTR_MASK_SET( p_attr_set, last_archive );
#endif
    ATTR_MASK_SET( p_attr_set, status );
    ATTR_MASK_SET( p_attr_set, archive_class );
    ATTR_MASK_SET( p_attr_set, arch_cl_update );
    p_attr_set->attr_mask |= policies.migr_policies.global_attr_mask;

#ifdef _BACKUP_FS
    ATTR_MASK_SET( p_attr_set, type );

    /* what information the backend needs from DB? */
    rc = rbhext_status_needs( TYPE_NONE, &allow_cached_attrs, &need_fresh_attrs );
    if (rc != 0)
    {
        DisplayLog(LVL_MAJOR, MIGR_TAG, "Unexpected error from rbhext_status_needs(), in %s line %u: %d",
                   __FUNCTION__, __LINE__, rc );
        return rc;
    }
    p_attr_set->attr_mask |= allow_cached_attrs;
#endif
    return 0;
}


int perform_migration( lmgr_t * lmgr, migr_param_t * p_migr_param,
                       unsigned int *p_nb_migr, unsigned long long *p_migr_vol )
{
    int            rc;
    struct lmgr_iterator_t *it = NULL;

    lmgr_filter_t  filter;
    filter_value_t fval;
    lmgr_sort_type_t sort_type;

    attr_set_t     attr_set;
    entry_id_t     entry_id;

    unsigned long long feedback_before[MIGR_FDBK_COUNT];
    unsigned long long feedback_after[MIGR_FDBK_COUNT];

    unsigned int   status_tab[MIGR_ST_COUNT];

    unsigned int   nb_submitted, migr_count;
    unsigned long  submitted_vol, migr_vol;

    int            last_mod_time = 0;
    time_t         last_request_time = 0;

    int            attr_mask_sav;
    int            end_of_list = FALSE;

    unsigned int   nb_returned, total_returned;

    lmgr_iter_opt_t opt;

    if ( !p_migr_param )
        return EFAULT;

    migr_flags = p_migr_param->flags;
    migr_pol_mod = p_migr_param->policy_mod;

    if ( p_nb_migr )
        *p_nb_migr = 0;
    if ( p_migr_vol )
        *p_migr_vol = 0;

    rc = init_db_attr_mask( &attr_set );
    if (rc)
        return rc;

    /* sort by last modification time */
    sort_type.attr_index = ATTR_INDEX_last_mod;
    sort_type.order = SORT_ASC;

    rc = lmgr_simple_filter_init( &filter );
    if ( rc )
        return rc;

#ifdef ATTR_INDEX_no_archive
    /* do not retrieve entries with 'no_archive' tag = 1.
     * get entrie with no_archive == NULL
     */
    fval.val_bool = TRUE;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_no_archive, NOTEQUAL,
                                 fval, FILTER_FLAG_ALLOW_NULL );
    if ( rc )
        return rc;
#endif

#if defined(_LUSTRE_HSM) || defined(_BACKUP_FS)
    if ( migr_config.backup_new_files )
    {
        /* retrieve entries with status MODIFIED or NEW or NULL */

        fval.val_int = STATUS_MODIFIED;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL, fval,
                                     FILTER_FLAG_BEGIN );
        if ( rc )
            return rc;

        fval.val_int = STATUS_NEW;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL, fval,
                                     FILTER_FLAG_OR | FILTER_FLAG_END );
        if ( rc )
            return rc;
    }
    else
    {
#endif
        /* only retrieve entries with status MODIFIED */
        fval.val_int = STATUS_MODIFIED;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL, fval, 0 );
        if ( rc )
            return rc;
#if defined(_LUSTRE_HSM) || defined(_BACKUP_FS)
    }
#endif

    switch ( p_migr_param->type )
    {
    case MIGR_FS:
        DisplayLog( LVL_EVENT, MIGR_TAG, "Starting migration pass" );
        /* We must retrieve all files sorted by mtime: no extra filter */
        break;

    case MIGR_BY_OST:
        DisplayLog( LVL_EVENT, MIGR_TAG, "Starting migration on OST #%u",
                    p_migr_param->param_u.ost_index );

        /* retrieve stripe info and stripe items */
        ATTR_MASK_SET( &attr_set, stripe_info );
        ATTR_MASK_SET( &attr_set, stripe_items );

        /* We must retrieve files from this OST */

        fval.val_uint = p_migr_param->param_u.ost_index;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_stripe_items, EQUAL, fval, 0 );
        if ( rc )
            return rc;
        break;

    case MIGR_BY_USER:
        DisplayLog( LVL_EVENT, MIGR_TAG, "Starting migration of '%s' user files",
                    p_migr_param->param_u.user_name );

        /* We must retrieve files for this user */

        fval.val_str = p_migr_param->param_u.user_name;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_owner, EQUAL, fval, 0 );
        if ( rc )
            return rc;
        break;

    case MIGR_BY_GROUP:
        DisplayLog( LVL_EVENT, MIGR_TAG, "Starting migration of '%s' group files",
                    p_migr_param->param_u.group_name );

        /* We must retrieve files for this group */

        fval.val_str = p_migr_param->param_u.group_name;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_gr_name, EQUAL,
                                     fval, 0 );
        if ( rc )
            return rc;
        break;

    case MIGR_BY_CLASS:
        DisplayLog( LVL_EVENT, MIGR_TAG, "Starting migration on fileclass '%s'",
                    p_migr_param->param_u.class_name );

        if (!strcasecmp( p_migr_param->param_u.class_name, "default"))
            fval.val_str = CLASS_DEFAULT;
        else if ( !strcasecmp( p_migr_param->param_u.class_name, "ignored"))
            fval.val_str = CLASS_IGNORED;
        else
            fval.val_str = p_migr_param->param_u.class_name;

        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_archive_class, LIKE,
                                     fval, 0 );
        /* @TODO add FILTER_FLAG_ALLOW_NULL when we will post-check entry attributes */
        if ( rc )
            return rc;
        break;

    default:
        DisplayLog( LVL_CRIT, MIGR_TAG, "Unknown/unsupported migration type %d",
                    p_migr_param->type );
        return EINVAL;
    }

    /* Flushing messages before performing DB query */
    FlushLogs(  );


    /* add optimisation filters based on policies */
    if ( !ignore_policies )
        set_migr_optimization_filters(&filter);

    /* optimization: do not retrieve whitelisted entries
     * that do not need fileclass update. Skip this optimization
     * if there is no 'ignore' statement.
     */
    if ( policies.migr_policies.whitelist_count > 0 )
    {
        if ( policies.updt_policy.fileclass.policy == UPDT_NEVER )
        {
            /* filter: archive class != ignored */
            fval.val_str = CLASS_IGNORED;
            rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_archive_class,
                                         NOTEQUAL, fval, FILTER_FLAG_ALLOW_NULL );
            if ( rc )
                return rc;
        }
        else if ( policies.updt_policy.fileclass.policy == UPDT_PERIODIC )
        {
            /* filter: archive class != ignored OR update <= now - period */
            fval.val_str = CLASS_IGNORED;
            rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_archive_class,
                                         NOTEQUAL, fval, FILTER_FLAG_ALLOW_NULL
                                                        | FILTER_FLAG_BEGIN );
            fval.val_uint = time(NULL) - policies.updt_policy.fileclass.period_max;
            rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_arch_cl_update,
                                         LESSTHAN, fval, FILTER_FLAG_ALLOW_NULL
                                                        | FILTER_FLAG_OR
                                                        | FILTER_FLAG_END );
        }
    }

    /* start with a limited count of entries, to save memory */
    opt.list_count_max = migr_config.db_request_limit;
    opt.force_no_acct = FALSE;
    nb_returned = 0;
    total_returned = 0;

    it = ListMgr_Iterator( lmgr, &filter, &sort_type, &opt );

    if ( it == NULL )
    {
        lmgr_simple_filter_free( &filter );
        DisplayLog( LVL_CRIT, MIGR_TAG,
                    "Error retrieving list of candidates from database. "
                    "Migration cancelled." );
        return -1;
    }

    attr_mask_sav = attr_set.attr_mask;

    last_request_time = time( NULL );

    migr_count = 0;
    migr_vol = 0;

    /* loop on all migration passes */
    do
    {

        /* Retrieve stats before starting migr,
         * for computing a delta later.
         */
        RetrieveQueueStats( &migr_queue, NULL, NULL, NULL, NULL, NULL,
                            status_tab, feedback_before );

        submitted_vol = 0;
        nb_submitted = 0;

        /* List entries for migration */
        do
        {
            unsigned long long sz;

            /* reset attr_mask, if it was altered by last ListMgr_GetNext() call */
            memset( &attr_set, 0, sizeof(attr_set_t) );
            attr_set.attr_mask = attr_mask_sav;

            memset( &entry_id, 0, sizeof( entry_id_t ) );
            rc = ListMgr_GetNext( it, &entry_id, &attr_set );

            if ( migr_abort )
            {
                DisplayLog( LVL_MAJOR, MIGR_TAG, "Migration aborted, stop enqueuing "
                            "file migration requests." );
                end_of_list = TRUE;
                break;
            }

            if ( rc == DB_END_OF_LIST )
            {
                total_returned += nb_returned; 

                /* if limit = inifinite => END OF LIST */
                if ( ( nb_returned == 0 )
                     || ( ( opt.list_count_max > 0 ) &&
                          ( nb_returned < opt.list_count_max ) ) )
                {
                    DisplayLog( LVL_FULL, MIGR_TAG, "End of list "
                                "(%u entries returned)", total_returned );
                    end_of_list = TRUE;
                    break;
                }

                /* no new useless request */
                if ( heuristic_end_of_list( last_mod_time ) )
                {
                    end_of_list = TRUE;
                    break;
                }

                /* Free previous iterator */
                ListMgr_CloseIterator( it );

                /* we must wait that migr. queue is empty,
                 * to avoid race conditions (by processing the same
                 * entry twice */
                wait_queue_empty( nb_submitted, feedback_before, status_tab,
                                  feedback_after, FALSE );

                /* perform a new request with next entries */

                /* /!\ if there is already a filter on last_mod or md_update
                 * only replace it, do not add a new filter.
                 */

                /* don't retrieve just-updated entries 
                 * (update>=last_request_time),
                 * allow entries with md_update == NULL.
                 */
                fval.val_int = last_request_time;
                rc = lmgr_simple_filter_add_or_replace(&filter,
                                                       ATTR_INDEX_md_update,
                                                       LESSTHAN_STRICT, fval,
                                                       FILTER_FLAG_ALLOW_NULL );
                if ( rc )
                    return rc;

                /* filter on modification time (allow NULL) */
                fval.val_int = last_mod_time;
                rc = lmgr_simple_filter_add_or_replace(&filter,
                                                       ATTR_INDEX_last_mod,
                                                       MORETHAN, fval,
                                                       FILTER_FLAG_ALLOW_NULL );
                if ( rc )
                    return rc;

                DisplayLog( LVL_DEBUG, MIGR_TAG,
                            "Performing new request with a limit of %u entries"
                            " and mod >= %d and md_update < %ld ",
                            opt.list_count_max, last_mod_time,
                            last_request_time );

                nb_returned = 0;
                it = ListMgr_Iterator( lmgr, &filter, &sort_type, &opt );

                if ( it == NULL )
                {
                    lmgr_simple_filter_free( &filter );
                    DisplayLog( LVL_CRIT, MIGR_TAG,
                                "Error retrieving list of candidates from "
                                "database. Migration cancelled." );
                    return -1;
                }
                last_request_time = time( NULL );

                continue;
            }
            else if ( rc != 0 )
            {
                DisplayLog( LVL_CRIT, MIGR_TAG,
                            "Error %d getting next entry of iterator", rc );
                break;
            }

            nb_returned++;

            if ( ATTR_MASK_TEST( &attr_set, last_mod ) )
                last_mod_time = ATTR( &attr_set, last_mod );

            sz = ATTR( &attr_set, size );

            /* Insert candidate */
            rc = Queue_Insert( &migr_queue,
                               Entry2MigrItem( &entry_id, &attr_set ) );
            if ( rc )
                return rc;

            submitted_vol += sz;
            nb_submitted++;

            /* periodically check if we have a chance to have more matching entries */
            if ( nb_submitted % 1000 == 0 )
            {
                if ( heuristic_end_of_list( last_mod_time ) )
                {
                    end_of_list = TRUE;
                    break;
                }
            }

        }
        while ( !check_migration_limit( nb_submitted + migr_count,
                                        submitted_vol + migr_vol ) );

        /* Wait for end of migration pass */
        wait_queue_empty( nb_submitted, feedback_before, status_tab,
                          feedback_after, TRUE );

        /* how much entries have been migrated ? */
        migr_vol += feedback_after[MIGR_FDBK_VOL] - feedback_before[MIGR_FDBK_VOL];
        migr_count += feedback_after[MIGR_FDBK_NBR] - feedback_before[MIGR_FDBK_NBR];

        /* if getnext returned an error */
        if ( rc )
            break;

    }
    while ( ( !end_of_list ) && !check_migration_limit( migr_count, migr_vol ) );

    lmgr_simple_filter_free( &filter );
    ListMgr_CloseIterator( it );

    if ( p_nb_migr )
        *p_nb_migr = migr_count;
    if ( p_migr_vol )
        *p_migr_vol = migr_vol;

    return 0;
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
        DisplayLog( LVL_CRIT, MIGR_TAG, "Error %d tagging entry as invalid in database.", rc );

    return rc;
}
#endif

/* declaration from listmgr_common.c */
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

    /* update DB */
    rc = ListMgr_Update( lmgr, p_entry_id, &tmp_attrset );
    if ( rc )
        DisplayLog( LVL_CRIT, MIGR_TAG, "Error %d updating entry in database.", rc );

    return rc;
}

#ifdef _HAVE_FID

#ifdef _LUSTRE_HSM /* Lustre-HSM + fid support */
/**
 * Check that entry exists
 * @param fill entry MD if entry is valid
 */
static int check_entry( lmgr_t * lmgr, migr_item_t * p_item, attr_set_t * new_attr_set )
{
    char           fid_path[RBH_PATH_MAX];
    struct stat    entry_md;
    int            rc;

    DisplayLog( LVL_FULL, MIGR_TAG, "Considering entry " DFID, PFID( &p_item->entry_id ) );

    /* 1) Build fid path */
    BuildFidPath( &p_item->entry_id, fid_path );

    /* 2) always stat the entry: it's important to respect LRU sort order,
     * fileclass conditions and to check if the entry still exists.
     */
    rc = errno = 0;
#if defined( _MDS_STAT_SUPPORT )
    if ( global_config.direct_mds_stat )
        rc = lustre_mds_stat_by_fid( &p_item->entry_id, &entry_md );
    else
#endif
        if ( lstat( fid_path, &entry_md ) != 0 )
            rc = errno;

    if ( rc != 0 )
    {
        /* If lstat returns an error, invalidate the entry */
        DisplayLog( LVL_DEBUG, MIGR_TAG, "stat() failed on %s. Skipping it.",
                    fid_path );

        /* This entry has been processed and has probably removed */
        if ( rc == ENOENT )
            /** @TODO remove entry from DB if errno = ENOENT ? */
            return MIGR_ENTRY_MOVED;
        else
            return MIGR_STAT_FAILURE;
    }

    /* convert posix attributes to attr structure */
    PosixStat2EntryAttr( &entry_md, new_attr_set, TRUE );

    /* set update time of the stucture */
    ATTR_MASK_SET( new_attr_set, md_update );
    ATTR( new_attr_set, md_update ) = time( NULL );

    /* get fullpath or name, if they are needed for applying policy
     * and if it is expired in DB */
    if ( ( policies.migr_policies.global_attr_mask & ATTR_MASK_fullpath )
         || ( policies.migr_policies.global_attr_mask & ATTR_MASK_name ) )
    {
        if ( need_path_update(&p_item->entry_attr, NULL) )
        {
            if ( Lustre_GetFullPath( &p_item->entry_id,
                                    ATTR( new_attr_set, fullpath ),
                                    1024 ) == 0 )
            {
                char          *curr;
                ATTR_MASK_SET( new_attr_set, fullpath );
                curr = strrchr( ATTR( new_attr_set, fullpath ), '/' );

                /* update path refresh time */
                ATTR_MASK_SET( new_attr_set, path_update );
                ATTR( new_attr_set, path_update ) = time( NULL );

                if ( curr )
                {
                    curr++;
                    strcpy( ATTR( new_attr_set, name ), curr );
                    ATTR_MASK_SET( new_attr_set, name );
                }
            }
        } /* end get path */
    }

    /* is status known? */
    if ( !ATTR_MASK_TEST( &p_item->entry_attr, status ) )
    {
        DisplayLog( LVL_FULL, MIGR_TAG, "Update of HSM state (not known in DB)" );
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

    /* entry is valid */
    return MIGR_OK;
}
#elif defined(_SHERPA) /* sherpa + fid support */

static int check_entry( lmgr_t * lmgr, migr_item_t * p_item, attr_set_t * new_attr_set )
{

    if ( ATTR_MASK_TEST(&p_item->entry_attr, fullpath) )
        DisplayLog( LVL_FULL, MIGR_TAG, "Considering entry %s", ATTR( &p_item->entry_attr, fullpath ) );
    else
        DisplayLog( LVL_FULL, MIGR_TAG, "Considering entry with fid="DFID, PFID(&p_item->entry_id) );

    /* sherpa needs the full path to map with the reference */
    if ( ATTR_MASK_TEST( &p_item->entry_attr, fullpath )
         && !ATTR_MASK_TEST( new_attr_set, fullpath ) )
    {
        strcpy( ATTR( new_attr_set, fullpath ), ATTR( &p_item->entry_attr, fullpath ) );
        ATTR_MASK_SET( new_attr_set, fullpath );
    }

    /* get info about this entry and check policies about the entry.
     * don't check policies now, it will be done later */
    switch( SherpaManageEntry( &p_item->entry_id, new_attr_set, FALSE) )
    {
        case do_skip:
        case do_rm:
            /* entry has been removed? */
            return MIGR_ENTRY_MOVED;
        case do_update:
            /* OK, continue */
            break; 
    }
    /* entry is valid */
    return MIGR_OK;
}

#elif defined(_BACKUP_FS) /* backup with fid support */

static int check_entry( lmgr_t * lmgr, migr_item_t * p_item, attr_set_t * new_attr_set )
{
    int rc;
    unsigned int allow_cached_attrs = 0;
    unsigned int need_fresh_attrs = 0;
    /* path for handling the entry */
    char     fspath[RBH_PATH_MAX] = "";

#ifdef _HAVE_FID
    BuildFidPath( &p_item->entry_id, fspath );
#else
    if ( ATTR_MASK_TEST(&p_item->entry_attr, fullpath) )
        strcpy( fspath, ATTR( &p_item->entry_attr, fullpath ) );
    else
    {
        DisplayLog( LVL_MAJOR, MIGR_TAG, "Missing fullpath info for addressing the entry" );
        invalidate_entry( lmgr, &p_item->entry_id );
        return MIGR_PARTIAL_MD;
    }
#endif
    DisplayLog( LVL_FULL, MIGR_TAG, "Considering entry '%s'", fspath );

    /* what up-to-date information the backend needs? */
    if ( ATTR_MASK_TEST(&p_item->entry_attr, type) )
        rc = rbhext_status_needs( TYPE_NONE, &allow_cached_attrs, &need_fresh_attrs );
    else
        rc = rbhext_status_needs( ListMgr2PolicyType(ATTR(&p_item->entry_attr, type)),
                                  &allow_cached_attrs, &need_fresh_attrs );
    if (rc == -ENOTSUP)
    {
        DisplayLog(LVL_MAJOR, MIGR_TAG, "This type of entry is not supported" );
        return MIGR_BAD_TYPE;
    }
    else if (rc != 0)
    {
        DisplayLog(LVL_MAJOR, MIGR_TAG, "Unexpected error from rbhext_status_needs(), in %s line %u: %d",
                   __FUNCTION__, __LINE__, rc );
        return MIGR_ERROR;
    }

    /* check what cached attributes are missing and what fresh are needed */
    need_fresh_attrs |= (allow_cached_attrs & ~(p_item->entry_attr.attr_mask) );

    if ( need_fresh_attrs & (ATTR_MASK_stripe_info | ATTR_MASK_stripe_items ) )
    {
        /* need stripe */
        rc = File_GetStripeByPath( fspath,
                                   &ATTR( new_attr_set, stripe_info ),
                                   &ATTR( new_attr_set, stripe_items ) );
        if ( rc == 0 )
        {
            ATTR_MASK_SET( new_attr_set, stripe_info );
            ATTR_MASK_SET( new_attr_set, stripe_items );
        }
    }

    if ( (need_fresh_attrs & (ATTR_MASK_fullpath | ATTR_MASK_name | ATTR_MASK_depth ))
         || need_path_update(&p_item->entry_attr, NULL) )
    {
#ifdef _HAVE_FID
        char pathnew[RBH_PATH_MAX];
        /* /!\ Lustre_GetFullPath modifies fullpath even on failure,
         * so, first write to a tmp buffer */
        rc = Lustre_GetFullPath( &p_item->entry_id, pathnew, RBH_PATH_MAX );

        if ( rc == 0 )
        {
            strcpy( ATTR( &p_item->entry_attr, fullpath ), pathnew );
            ATTR_MASK_SET( &p_item->entry_attr, fullpath );
            ATTR_MASK_SET( &p_item->entry_attr, path_update );
            ATTR( &p_item->entry_attr, path_update ) = time( NULL );
        }
#else
        DisplayLog( LVL_MAJOR, MIGR_TAG, "Missing path info for addressing the entry" );
        return MIGR_PARTIAL_MD;
#endif
    }

    if ( (need_fresh_attrs & POSIX_ATTR_MASK)
         || need_md_update( &p_item->entry_attr, NULL ) )
    {
        struct stat entry_md;
        /* need lstat */
        if ( lstat( fspath, &entry_md ) != 0 )
          rc = errno;

        /* get entry attributes */
        if ( rc == 0 )
        {
#if defined( _LUSTRE ) && defined( _HAVE_FID ) && defined( _MDS_STAT_SUPPORT )
            PosixStat2EntryAttr( &entry_md, &p_item->entry_attr, !global_config.direct_mds_stat );
#else
            PosixStat2EntryAttr( &entry_md, &p_item->entry_attr, TRUE );
#endif
            /* set update time of the stucture */
            ATTR_MASK_SET( new_attr_set, md_update );
            ATTR( new_attr_set, md_update ) = time( NULL );
        }
    }

    /* Merge with missing attrs from database */
    ListMgr_MergeAttrSets( new_attr_set, &p_item->entry_attr, FALSE );
    /* autogen fields */
    ListMgr_GenerateFields( new_attr_set, need_fresh_attrs | allow_cached_attrs );

    /* check the status */
    rc = rbhext_get_status( &p_item->entry_id, new_attr_set, new_attr_set );

    if ( rc == -ENOENT || rc == -ESTALE )
    {
        DisplayLog( LVL_FULL, MIGR_TAG, "Entry %s does not exist anymore",
                    fspath );
        return MIGR_ENTRY_MOVED;
    }
    else if ( rc == 0 )
    {
        if ( !ATTR_MASK_TEST( new_attr_set, status ) )
        {
            DisplayLog( LVL_MAJOR, MIGR_TAG,
                        "ERROR: rbhext_get_status() reported success without setting status (entry %s)",
                        fspath );
            return MIGR_ERROR;
        }
        /* success. New status will be checked by caller. */
        return MIGR_OK;
    }
    else if ( rc == -ENOTSUP )
    {
        /* this type of entry is not managed: ignored */
        DisplayLog( LVL_FULL, MIGR_TAG, "Entry %s is not archivable", fspath );
        return MIGR_BAD_TYPE;
    }
    else /* misc error */
            return MIGR_ERROR;
}

#endif /* switch Lustre_HSM/SHERPA */

#else  /* no fid support (SHERPA ONLY) */

#ifndef _SHERPA
#error "FID support must be activated for Lustre-HSM"
#endif

/**
 * Check that entry exists with the good path and its id is consistent.
 * @param fill entry MD if entry is valid
 */
static int check_entry( lmgr_t * lmgr, migr_item_t * p_item,
                        attr_set_t * new_attr_set )
{
    struct stat    entry_md;

    /* 1) Check if fullpath is set */

    if ( !ATTR_MASK_TEST( &p_item->entry_attr, fullpath ) )
    {
        DisplayLog( LVL_DEBUG, MIGR_TAG, "Warning: entry fullpath is not set. Tag it invalid." );
        invalidate_entry( lmgr, &p_item->entry_id );

        /* not enough metadata */
        return MIGR_PARTIAL_MD;
    }

    DisplayLog( LVL_FULL, MIGR_TAG, "Considering entry %s", ATTR( &p_item->entry_attr, fullpath ) );

    /* we must have at least its full path, if we are not using fids */
    if ( ATTR_MASK_TEST( &p_item->entry_attr, fullpath )
         && !ATTR_MASK_TEST( new_attr_set, fullpath ) )
    {
        strcpy( ATTR( new_attr_set, fullpath ), ATTR( &p_item->entry_attr, fullpath ) );
        ATTR_MASK_SET( new_attr_set, fullpath );
    }

    /* get info about this entry and check policies about the entry. */
    switch( SherpaManageEntry( &p_item->entry_id, new_attr_set, FALSE ) )
    {
        case do_skip:
        case do_rm:
            /* invalidate entry if it is not OK */
            invalidate_entry( lmgr, &p_item->entry_id );
            return MIGR_ENTRY_MOVED;
        case do_update:
            /* OK, continue */
            break; 
    }

    /* entry is valid */
    return MIGR_OK;
}
#endif /* no fid support / SHERPA */


/**
 * Manage an entry by path or by fid, depending on FS
 */
static int ManageEntry( lmgr_t * lmgr, migr_item_t * p_item, int no_queue )
{
    attr_set_t     new_attr_set;
    unsigned long long feedback[MIGR_FDBK_COUNT];
    int            rc;

    policy_match_t match;

    policy_item_t *policy_case;
    fileset_item_t *p_fileset;
    char          *hints;

    int update_fileclass = -1; /* not set */

#ifdef _LUSTRE_HSM
    unsigned int archive_num = 0;
#endif

/* acknowledging helper */
#define Acknowledge( _q, _status, _fdbk1, _fdbk2 )  do {            \
                               if (no_queue)                        \
                                 rc = (_status);                    \
                               else {                               \
                                    feedback[MIGR_FDBK_NBR] = _fdbk1;  \
                                    feedback[MIGR_FDBK_VOL] = _fdbk2;  \
                                    Queue_Acknowledge( _q, _status, feedback, MIGR_FDBK_COUNT ); \
                               }                                       \
                            } while(0)

    if ( migr_abort )
    {
       /* migration aborted by a signal, doesn't submit new migrations */
       DisplayLog( LVL_FULL, MIGR_TAG, "Migration aborted: migration thread skipping migration requests" );
       Acknowledge( &migr_queue, MIGR_ABORT, 0, 0 );
       rc = MIGR_ABORT;
       goto end;
    }

    DisplayLog( LVL_FULL, MIGR_TAG,
                "Checking if entry %s can be archived",
                ATTR( &p_item->entry_attr, fullpath ) );

    ATTR_MASK_INIT( &new_attr_set );

    rc = check_entry( lmgr, p_item, &new_attr_set );
    if ( rc != MIGR_OK )
    {
        Acknowledge( &migr_queue, rc, 0, 0 );
        goto end;
    }

    /* Merge with missing attrs from database */
    ListMgr_MergeAttrSets( &new_attr_set, &p_item->entry_attr, FALSE );

#ifdef ATTR_INDEX_invalid
    /* From here, assume that entry is valid */
    ATTR_MASK_SET( &new_attr_set, invalid );
    ATTR( &new_attr_set, invalid ) = FALSE;
#endif

#ifdef ATTR_INDEX_no_archive
    /* check that the entry has the expected status */
    if ( ATTR_MASK_TEST( &new_attr_set, no_archive )
         && ATTR( &new_attr_set, no_archive ) )
    {
        /* this entry is now tagged 'no_archive' */
        DisplayLog( LVL_MAJOR, MIGR_TAG,
                    "Entry " DFID_NOBRACE " is now tagged 'no_archive', skipping it.",
                    PFID( &p_item->entry_id ) );

        /* update DB and skip the entry */
        update_entry( lmgr, &p_item->entry_id, &new_attr_set );

        /* Notify that this entry is whitelisted */
        Acknowledge( &migr_queue, MIGR_ENTRY_WHITELISTED, 0, 0 );

        goto end;
    }
#endif

    if ( !ATTR_MASK_TEST( &new_attr_set, status ) )
    {
        DisplayLog( LVL_MAJOR, MIGR_TAG, "Error: entry status should be set at this point");
        Acknowledge( &migr_queue, MIGR_PARTIAL_MD, 0, 0 );
        goto end;
    }
    else
    {
#if defined( _LUSTRE_HSM ) || defined( _BACKUP_FS )
        if ( (ATTR( &new_attr_set, status ) != STATUS_MODIFIED)
             && ( !migr_config.backup_new_files
                  || ( ATTR( &new_attr_set, status ) != STATUS_NEW )) )
#else /* sherpa */
        if ( ATTR( &new_attr_set, status ) != STATUS_MODIFIED )
#endif
        {
            /* status changed */
            DisplayLog( LVL_MAJOR, MIGR_TAG,
                        "%s: entry status is not 'new' or 'modified' (%#x): skipping entry.",
                        ATTR(&new_attr_set,fullpath),
                        ATTR( &new_attr_set, status ));

            /* update DB and skip the entry */
            update_entry( lmgr, &p_item->entry_id, &new_attr_set );

            /* Notify that this entry is whitelisted */
            Acknowledge( &migr_queue, MIGR_STATUS_CHGD, 0, 0 );
            goto end;
        }
    }

    /* check for periodic fileclass matching */
    update_fileclass = need_fileclass_update( &new_attr_set, MIGR_POLICY );
    if ( update_fileclass == -1 )
    {
        Acknowledge( &migr_queue, MIGR_ERROR, 0, 0 );
        goto end;
    }

    /* only check whitelist rules and acces time
     * if 'ignore-policies' flag is not set
     */
    if ( !ignore_policies )
    {

        /* 4) compute whitelist rules */

        if ( update_fileclass )
            match = IsWhitelisted( &p_item->entry_id, &new_attr_set,
                                   MIGR_POLICY );
        else
        {
            /* fileclass is still valid, check if it is whitelisted */
            if ( !strcmp( ATTR(&new_attr_set, archive_class ), CLASS_IGNORED ) )
                match = POLICY_MATCH;
            else if ( WhitelistedClass( ATTR(&new_attr_set, archive_class ),
                                        MIGR_POLICY ) )
                match = POLICY_MATCH;
            else
                match = POLICY_NO_MATCH;
        }

        if ( match == POLICY_MATCH )
        {
    #ifdef ATTR_INDEX_whitelisted
            /* Entry is whitelisted */
            ATTR_MASK_SET( &new_attr_set, whitelisted );
            ATTR( &new_attr_set, whitelisted ) = TRUE;
    #endif

            /* update DB and skip the entry */
            update_entry( lmgr, &p_item->entry_id, &new_attr_set );

            /* Notify that this entry is whitelisted */
            Acknowledge( &migr_queue, MIGR_ENTRY_WHITELISTED, 0, 0 );

            goto end;
        }
        else if ( match == POLICY_NO_MATCH )
        {
    #ifdef ATTR_INDEX_whitelisted
            /* set whitelisted field as false (for any further update op) */
            ATTR_MASK_SET( &new_attr_set, whitelisted );
            ATTR( &new_attr_set, whitelisted ) = FALSE;
    #endif
        }
        else
        {
            /* Cannot determine if entry is whitelisted: skip it (do nothing in database) */
            DisplayLog( LVL_MAJOR, MIGR_TAG,
                        "Warning: cannot determine if entry %s is whitelisted: skipping it.",
                        ATTR( &p_item->entry_attr, fullpath ) );

            /* Notify error */
            Acknowledge( &migr_queue, MIGR_PARTIAL_MD, 0, 0 );

            goto end;
        }

    } /* end if 'don't ignore policies' */

    /* check migr policy for the entry */

    /* if fileclass is still valid, retrieve the related policy case */
    if ( !update_fileclass )
    {
        policy_case = GetPolicyCaseByClass( ATTR(&new_attr_set, archive_class ),
                                            MIGR_POLICY, &p_fileset );
        /* update if GetPolicyCaseByClass returned NULL */
        if ( policy_case == NULL )
            update_fileclass = TRUE;
    }

    if ( update_fileclass )
        policy_case = GetPolicyCase( &p_item->entry_id, &new_attr_set, MIGR_POLICY,
                                     &p_fileset );

    if ( !policy_case )
    {
        update_entry( lmgr, &p_item->entry_id, &new_attr_set );
        Acknowledge( &migr_queue, MIGR_NO_POLICY, 0, 0 );
        goto end;
    }
    else
    {
        /* remember the matched fileclass */
        if ( p_fileset )
            strcpy( ATTR( &new_attr_set, archive_class ), p_fileset->fileset_id );
        else
            strcpy( ATTR( &new_attr_set, archive_class ), CLASS_DEFAULT );
        ATTR_MASK_SET( &new_attr_set, archive_class );
        ATTR( &new_attr_set, arch_cl_update ) = time(NULL);
        ATTR_MASK_SET( &new_attr_set, arch_cl_update );
    }

    /* @TODO if the command is '--migrate-class', '--migrate-user', '--migrate-group',
     * check that the entry still matches */

    if ( !ignore_policies )
    {
        /* check if the entry matches the policy condition */
        switch ( EntryMatches( &p_item->entry_id, &new_attr_set, &policy_case->condition, migr_pol_mod ) )
        {
        case POLICY_NO_MATCH:
            /* entry is not eligible now */
            update_entry( lmgr, &p_item->entry_id, &new_attr_set );
            Acknowledge( &migr_queue, MIGR_ENTRY_WHITELISTED, 0, 0 );
            goto end;
            break;
        case POLICY_MATCH:
            /* OK, can be migrd */
            DisplayLog( LVL_DEBUG, MIGR_TAG,
                        "Entry %s matches the condition for policy '%s'.",
                        ATTR( &p_item->entry_attr, fullpath ), policy_case->policy_id );
            break;
        default:
            /* Cannot determine if entry matches the policy condition */
            DisplayLog( LVL_MAJOR, MIGR_TAG,
                        "Warning: cannot determine if entry %s matches the condition for policy '%s': skipping it.",
                        ATTR( &p_item->entry_attr, fullpath ), policy_case->policy_id );

            /* Notify error */
            Acknowledge( &migr_queue, MIGR_PARTIAL_MD, 0, 0 );

            goto end;
        }
    } /* end if don't ignore policies */

    /* build hints */
    hints = build_migration_hints( policy_case, p_fileset, &p_item->entry_id, &new_attr_set );

#ifdef _LUSTRE_HSM
    /* what archive num is to be used ? */
    if ( ( p_fileset != NULL ) && (p_fileset->archive_num != 0) )
        archive_num =  p_fileset->archive_num;
    /* policy archive_num overrides fileset archive_num */
    if ( ( policy_case != NULL ) && ( policy_case->archive_num != 0) )
        archive_num = policy_case->archive_num;
#endif

    /* Perform migration operation! */

#ifdef _LUSTRE_HSM
    rc = MigrateEntry_ByFid( &p_item->entry_id, hints, archive_num );

    if ( rc == 0 )
    {
        /* new status is archive running */
        ATTR_MASK_SET( &new_attr_set, status );
        ATTR( &new_attr_set, status ) = STATUS_ARCHIVE_RUNNING;
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
#elif defined( _BACKUP_FS )

    /* if archive is synchronous, set status=archive running before */
    if ( backend.async_archive )
    {
        ATTR_MASK_SET( &new_attr_set, status );
        ATTR( &new_attr_set, status ) = STATUS_ARCHIVE_RUNNING;
        update_entry( lmgr, &p_item->entry_id, &new_attr_set );
    }

    rc = MigrateEntry( &p_item->entry_id, &new_attr_set, hints );

    /* status has been updated by MigrateEntry call, even on failure */

#elif defined(_SHERPA)
    if ( !ATTR_MASK_TEST( &new_attr_set, refpath) )
    {
        DisplayLog( LVL_MAJOR, MIGR_TAG,
            "Warning: entry %s has no reference path: skipping it.",
            ATTR( &p_item->entry_attr, fullpath ) );

        Acknowledge( &migr_queue, MIGR_PARTIAL_MD, 0, 0 );
        goto end;
    }

    /* set entry status = ARCHIVE_RUNNING */
    ATTR_MASK_SET( &new_attr_set, status );
    ATTR( &new_attr_set, status ) = STATUS_ARCHIVE_RUNNING;
    update_entry( lmgr, &p_item->entry_id, &new_attr_set );

    rc = MigrateEntry_ByPath( ATTR( &new_attr_set, refpath), hints );

    if ( rc )
        /* unknown status */
        ATTR_MASK_UNSET( &new_attr_set, status );
    else
    {
        ATTR_MASK_SET( &new_attr_set, status );
        ATTR( &new_attr_set, status ) = STATUS_SYNCHRO;
    }

#endif
    if ( hints )
        free_migration_hints( hints );

    if ( rc != 0 )
    {
        const char * action_str;
        const char * err_str;
#ifdef _LUSTRE_HSM
        /* copy is asynchronous */
        action_str = "starting archive";
        err_str = strerror(-rc);
#elif defined(_SHERPA)
        char buff[1024];
        /* copy is synchronous */
        action_str = "performing migration";
        err_str =  sherpa_cache_message_r(rc, buff, 1024 );
#elif defined(_BACKUP_FS)
        if (backend.async_archive)
            action_str = "starting archive";
        else
            action_str = "performing archive";
        err_str = strerror(-rc);
#else
        #error "Unexpected flavor"
#endif

        DisplayLog( LVL_MAJOR, MIGR_TAG, "Error %s of '%s': %s", action_str,
                    ATTR( &p_item->entry_attr, fullpath ), err_str );

        update_entry( lmgr, &p_item->entry_id, &new_attr_set );

        Acknowledge( &migr_queue, MIGR_ERROR, 0, 0 );
    }
    else
    {
        char           strmod[256];
        char           strarchive[256];
        char           strsize[256];
        char           strstorage[1024]="";

        int            is_copy = TRUE;
        int            is_stor = TRUE;

        const char * action_str;

#ifdef _LUSTRE_HSM
        /* Entry migration has been successfully started */
        action_str = "Start archiving";
#elif defined(_SHERPA)
        /* For SHERPA, migration is synchronous */
        action_str = "Archived";
#elif defined(_BACKUP_FS)
        if (backend.async_archive)
            action_str = "Start archiving";
        else
            action_str = "Archived";
#endif

        /* report messages */
        FormatDurationFloat( strmod, 256, time( NULL ) - ATTR( &new_attr_set, last_mod ) );

#ifdef ATTR_INDEX_last_archive
        if ( ATTR_MASK_TEST( &new_attr_set, last_archive ) )
            FormatDurationFloat( strarchive, 256,
                                 time( NULL ) - ATTR( &new_attr_set, last_archive ) );
        else
#endif
            is_copy = FALSE;

        FormatFileSize( strsize, 256, ATTR( &new_attr_set, size ) );

        if ( ATTR_MASK_TEST( &p_item->entry_attr, stripe_items ) )
            FormatStripeList( strstorage, 1024, &ATTR( &p_item->entry_attr, stripe_items ) );
        else
            is_stor = FALSE;

        DisplayLog( LVL_DEBUG, MIGR_TAG,
                    "%s '%s' using policy '%s', last modified %s ago,"
                    " last archived %s%s,  size=%s%s%s",
                    action_str, ATTR( &p_item->entry_attr, fullpath ),
                    policy_case->policy_id, strmod,
                    ( is_copy ? strarchive : "(unknown)" ), ( is_copy ? " ago" : "" ),
                    strsize, ( is_stor ? "stored on" : "" ), ( is_stor ? strstorage : "" ) );

#ifdef ATTR_INDEX_last_archive
        DisplayReport( "%s '%s' using policy '%s', last mod %s ago | size=%"
                       PRI_STSZ ", last_mod=%" PRI_TT ", last_archive=%" PRI_TT
                       "%s%s", action_str, ATTR( &p_item->entry_attr, fullpath ),
                       policy_case->policy_id, strmod, ATTR( &new_attr_set, size ),
                       (time_t)ATTR( &new_attr_set, last_mod ),
                       is_copy ? (time_t)ATTR( &new_attr_set, last_archive ) : 0,
                       ( is_stor ? ", storage_units=" : "" ), 
                       ( is_stor ? strstorage : "" ) );
#else
   DisplayReport( "%s '%s' using policy '%s', last mod %s ago | size=%"
                       PRI_SZ ", last_mod=%" PRI_TT "%s%s",
                       action_str, ATTR( &p_item->entry_attr, fullpath ),
                       policy_case->policy_id, strmod, ATTR( &new_attr_set, size ),
                       (time_t)ATTR( &new_attr_set, last_mod ),
                       ( is_stor ? ", storage_units=" : "" ),
                       ( is_stor ? strstorage : "" ) );
#endif

        /* update info in database */
        update_entry( lmgr, &p_item->entry_id, &new_attr_set );

        /* ack to queue manager */
        Acknowledge( &migr_queue, MIGR_OK, 1, ATTR( &new_attr_set, size ) );
    }

  end:
    /* free entry resources */
    FreeMigrItem( p_item );
    return rc;
}

/**
 *  Main routine of migr pthread
 */
static void   *Thr_Migr( void *arg )
{
    int            rc;
    lmgr_t         lmgr;
    void          *p_queue_entry;

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, MIGR_TAG, "Could not connect to database (error %d). Exiting.", rc );
        exit( rc );
    }

    while ( Queue_Get( &migr_queue, &p_queue_entry ) == 0 )
    {
        ManageEntry( &lmgr, ( migr_item_t * ) p_queue_entry, FALSE );
    }

    /* Error occured in migr queue management... */
    DisplayLog( LVL_CRIT, MIGR_TAG, "An error occured in migration queue management. Exiting." );
    exit( -1 );
    return NULL;                /* for avoiding compiler warnings */
}

/**
 *  Migrate a single file
 */
int migrate_one_file( const char * file, int flags )
{
    int            rc;
    lmgr_t         lmgr;
    entry_id_t     id;
    attr_set_t     attr_set;
#ifndef _HAVE_FID
    struct stat    st;
#endif

    migr_flags = flags;
    migr_pol_mod = NULL;

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, MIGR_TAG, "Could not connect to database (error %d). Exiting.", rc );
        exit( rc );
    }

    /* get entry id */
#ifdef _HAVE_FID
    rc = Lustre_GetFidFromPath( file, &id );
    if (rc != 0)
    {
        DisplayLog( LVL_CRIT, MIGR_TAG, "%s: %s", file, strerror(-rc) );
        return rc;
    }
#else
    /* get dev/inode */
    if ( lstat( file, &st ) )
    {
        rc = -errno;
        DisplayLog( LVL_CRIT, MIGR_TAG, "Cannot stat %s: %s",
                    file, strerror(-rc) );
        return rc;
    }

    id.inode = st.st_ino;
    id.device = st.st_dev;
    id.validator = st.st_ctime;
#endif

    /* needed info from DB */
    rc = init_db_attr_mask( &attr_set );
    if (rc)
        return rc;

    rc = ListMgr_Get( &lmgr, &id, &attr_set );
    if (rc)
    {
        if ( rc == DB_NOT_EXISTS )
            DisplayLog( LVL_MAJOR, MIGR_TAG, "%s: this entry is not known in database", file );
        return rc;
    }

#ifndef _HAVE_FID
    /* overrides it with posix info */
    PosixStat2EntryAttr( &st, &attr_set, TRUE );

    /* set update time of the stucture */
    ATTR_MASK_SET( &attr_set, md_update );
    ATTR( &attr_set, md_update ) = time( NULL );
#endif

    rc = ManageEntry( &lmgr, Entry2MigrItem( &id, &attr_set ) , TRUE );
    if ( rc >= 0 && rc < MIGR_ST_COUNT )
        DisplayLog( LVL_MAJOR, MIGR_TAG, "%s: %s", file, migr_status_descr[rc] );
    return rc;
}

/* array of migr threads */
static pthread_t *thread_ids = NULL;

int start_migration_threads( unsigned int nb_threads )
{
    unsigned int   i;

    thread_ids = ( pthread_t * ) MemCalloc( nb_threads, sizeof( pthread_t ) );
    if ( !thread_ids )
    {
        DisplayLog( LVL_CRIT, MIGR_TAG, "Memory error in %s", __FUNCTION__ );
        return ENOMEM;
    }

    for ( i = 0; i < nb_threads; i++ )
    {
        if ( pthread_create( &thread_ids[i], NULL, Thr_Migr, NULL ) != 0 )
        {
            int            rc = errno;
            DisplayLog( LVL_CRIT, MIGR_TAG, "Error %d creating migration thread in %s: %s", rc,
                        __FUNCTION__, strerror( rc ) );
            return rc;
        }
    }

    return 0;
}

/**
 * Update the status of all current migrations.
 * \param lmgr          [IN] connexion to database
 * \param p_nb_reset    [OUT] number of migration reset
 * \param p_nb_total    [OUT] total number of migration status checked
 */
int  check_current_migrations( lmgr_t * lmgr, unsigned int *p_nb_reset,
                               unsigned int * p_nb_total,
                               time_t timeout )
{
    int            rc;
    struct lmgr_iterator_t *it = NULL;

    lmgr_filter_t  filter;
    filter_value_t fval;

    migr_item_t migr_item;

    unsigned int nb_returned = 0;
    unsigned int nb_aborted = 0;
    int          attr_mask_sav = 0;
#ifdef _BACKUP_FS
    unsigned int allow_cached_attrs = 0;
    unsigned int need_fresh_attrs = 0;
#endif

    /* attributes to be retrieved */
    ATTR_MASK_INIT( &migr_item.entry_attr );
    ATTR_MASK_SET( &migr_item.entry_attr, fullpath );
    ATTR_MASK_SET( &migr_item.entry_attr, path_update );
    /* /!\ don't retrieve status, to force getting it from the filesystem */

#ifdef _BACKUP_FS
    ATTR_MASK_SET( &migr_item.entry_attr, type );

    /* what information the backend needs from DB? */
    rc = rbhext_status_needs( TYPE_NONE, &allow_cached_attrs, &need_fresh_attrs );
    if (rc != 0)
    {
        DisplayLog(LVL_MAJOR, MIGR_TAG, "Unexpected error from rbhext_status_needs(), in %s line %u: %d",
                   __FUNCTION__, __LINE__, rc );
        return rc;
    }
    migr_item.entry_attr.attr_mask |= allow_cached_attrs;
#endif

    attr_mask_sav = migr_item.entry_attr.attr_mask;

    rc = lmgr_simple_filter_init( &filter );
    if ( rc )
        return rc;

    /* if timeout is > 0, only select entries whose last update
     * is old enough (last_update <= now - timeout) or NULL*/
    if ( timeout > 0 )
    {
        fval.val_int = time(NULL) - timeout;
        rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_md_update, LESSTHAN,
                fval, FILTER_FLAG_ALLOW_NULL );
        if ( rc )
            return rc;
    }

    /* filter on current status (also check values with NULL status) */
    fval.val_int = STATUS_ARCHIVE_RUNNING;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_status, EQUAL, fval,
                FILTER_FLAG_ALLOW_NULL );
    if ( rc )
        return rc;

#ifdef ATTR_INDEX_invalid
    /* don't retrieve invalid entries (allow entries with invalid == NULL) */
    fval.val_int = TRUE;
    rc = lmgr_simple_filter_add( &filter, ATTR_INDEX_invalid, NOTEQUAL, fval,
            FILTER_FLAG_ALLOW_NULL );
    if ( rc )
        return rc;
#endif

    it = ListMgr_Iterator( lmgr, &filter, NULL, NULL );

    if ( it == NULL )
    {
        lmgr_simple_filter_free( &filter );
        DisplayLog( LVL_CRIT, MIGR_TAG,
                    "Error retrieving the list of current migrations. Recovery cancelled." );
        return -1;
    }

    memset( &migr_item, 0, sizeof(migr_item_t) );
    migr_item.entry_attr.attr_mask = attr_mask_sav;

    while ( (rc = ListMgr_GetNext( it, &migr_item.entry_id, &migr_item.entry_attr ))
                == DB_SUCCESS )
    {
        nb_returned ++;

        if ( ATTR_MASK_TEST(  &migr_item.entry_attr, fullpath ) )
            DisplayLog( LVL_VERB, MIGR_TAG, "Updating status of '%s'...",
                ATTR( &migr_item.entry_attr, fullpath ) );

        /* check entry */
        if ( check_entry( lmgr, &migr_item, &migr_item.entry_attr ) == MIGR_OK )
        {
            /* check new status */
            if ( ATTR_MASK_TEST( &migr_item.entry_attr, status )
                 && ( ATTR(&migr_item.entry_attr, status) != STATUS_ARCHIVE_RUNNING ) )
            {
               DisplayLog( LVL_EVENT, MIGR_TAG, "%s: archive finished",
                           ATTR(&migr_item.entry_attr, fullpath) );
               nb_aborted++;
            }
            else
            {
               DisplayLog( LVL_EVENT, MIGR_TAG, "%s: archive still running",
                           ATTR(&migr_item.entry_attr, fullpath) );
            }
            /* update entry status */
            update_entry( lmgr, &migr_item.entry_id,  &migr_item.entry_attr );
        }
        else
            nb_aborted ++;

        /* reset attr_mask, if it was altered by last ListMgr_GetNext() call */
        memset( &migr_item, 0, sizeof(migr_item_t) );
        migr_item.entry_attr.attr_mask = attr_mask_sav;
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
        DisplayLog( LVL_CRIT, MIGR_TAG, "Error %d getting next entry of iterator", rc );
        return -1;
    }

    return 0; 
}
