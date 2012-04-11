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


/**
 * \file    chglog_reader.c
 * \author  Th. Leibovici
 * \brief   Lustre MDT Changelog processing.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "Memory.h"
#include "RobinhoodLogs.h"
#include "entry_processor.h"
#include "RobinhoodMisc.h"
#include "global_config.h"
#include "RobinhoodConfig.h"

#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include "lustre_extended_types.h"
#include <lustre/liblustreapi.h>
#include <lustre/lustre_user.h>

#ifdef _LLAPI_FORKS
#include <signal.h>
#include <sys/wait.h>
#endif

/* for logs */
#define CHGLOG_TAG  "ChangeLog"

/* reader thread info, one per MDT */
typedef struct reader_thr_info_t
{
    /** reader thread index */
    unsigned int thr_index;

    /** thread id */
    pthread_t thr_id;

    /** nbr of records read by this thread */
    unsigned long long nb_read;

    /** time when the last line was read */
    time_t  last_read_time;

    /** last read record id */
    unsigned long long last_read_record;

    /** last record id committed to database */
    unsigned long long last_committed_record;

    /** thread was asked to stop */
    unsigned int force_stop : 1;

    /** log handler */
    void * chglog_hdlr;

    unsigned long long cl_counters[CL_LAST]; /* from boot time */
    unsigned long long cl_reported[CL_LAST]; /* last reported stat (for incremental diff) */
    time_t last_report;

} reader_thr_info_t;


chglog_reader_config_t chglog_reader_config;
static int behavior_flags = 0;

/* stop reading logs when reaching end of file? */
#define one_shot ( behavior_flags & FLAG_ONCE )

/** array of reader info */
static reader_thr_info_t  * reader_info = NULL;


/** Reload configuration for changelog readers */
int            ChgLogRdr_ReloadConfig( void *module_config )
{
    /** @TODO Reload ChangeLog reader config dynamically */
    return 0;
}

/**
 * Close the changelog for a thread.
 */
int log_close( reader_thr_info_t * p_info )
{
    int rc;

    /* close the log and clear input buffers */
    rc = llapi_changelog_fini(&p_info->chglog_hdlr);

    if ( rc )
        DisplayLog(LVL_CRIT, CHGLOG_TAG, "Error %d closing changelog: %s",
            rc, strerror(abs(rc)) );

    return abs(rc);
}

/**
 * Free allocated structures in op_extra_info_t field.
 */
void free_extra_info( void * ptr )
{
    op_extra_info_t * p_info = (op_extra_info_t*)ptr;

    if ( p_info->is_changelog_record && p_info->log_record.p_log_rec )
        llapi_changelog_free( &p_info->log_record.p_log_rec );
}


/**
 * DB callback function: this is called when a given ChangeLog record
 * has been successfully applied to the database.
 */
int log_record_callback( lmgr_t *lmgr, struct entry_proc_op_t * pop, void * param )
{
    int rc;
    const char * mount_point = get_mount_point();
    reader_thr_info_t * p_info = (reader_thr_info_t *) param;
    struct changelog_rec * logrec = pop->extra_info.log_record.p_log_rec;

    /** Check that a log record is set for this entry
     * (should always be the case).
     */
    if ( !pop->extra_info.is_changelog_record || (logrec == NULL ) )
    {
        DisplayLog( LVL_CRIT, CHGLOG_TAG, "Error: log record callback function"
                    " has been called for a non-changelog entry" );
        return EINVAL;
    }


    /* if batch_ack_count == 0, only acknowledge when cr_index == last_read_record */
    if ((chglog_reader_config.batch_ack_count == 0) && (logrec->cr_index < p_info->last_read_record))
        return 0;

    /* batching llapi_changelog_clear() calls.
     * If we reached the last read record, we acknowledge anyway. */
    if ( (chglog_reader_config.batch_ack_count > 1) && (logrec->cr_index < p_info->last_read_record) )
    {
       if ( (logrec->cr_index - p_info->last_committed_record)
                < chglog_reader_config.batch_ack_count )
       {
            /* do nothing, don't clear log now */
            return 0;
       }
    }

    DisplayLog( LVL_DEBUG, CHGLOG_TAG, "Acknowledging ChangeLog records up to #%llu",
                logrec->cr_index );

    DisplayLog( LVL_FULL, CHGLOG_TAG, "llapi_changelog_clear('%s', '%s', %llu)",
                mount_point,
                chglog_reader_config.mdt_def[p_info->thr_index].reader_id,
                logrec->cr_index );

    /* entry has been commited, acknowledge it in llog */
    rc = llapi_changelog_clear( mount_point,
                    chglog_reader_config.mdt_def[p_info->thr_index].reader_id, 
                    logrec->cr_index );

    if (rc)
    {
            DisplayLog( LVL_CRIT, CHGLOG_TAG,
                        "ERROR: llapi_changelog_clear(\"%s\", \"%s\", %llu) returned %d", 
                        mount_point,
                        chglog_reader_config.mdt_def[p_info->thr_index].reader_id,
                        logrec->cr_index, rc );
    }
    else
    {
        /* update thread stats */
        p_info->last_committed_record = logrec->cr_index;
    }

    return rc;

}

#ifdef _LUSTRE_HSM

static const char * event_name[] = {
    "archive", "restore", "cancel", "release", "remove", "state"
};
#define CL_EVENT_MAX 5
#endif


/**
 * This handles a single log record.
 */
static int process_log_rec( reader_thr_info_t * p_info, struct changelog_rec * p_rec )
{
    entry_proc_op_t op;
    int            st;

    int            opnum;
    const char     *optype;
    char flag_buff[256] = "";

#ifdef _LUSTRE_HSM
    if ( p_rec->cr_type == CL_HSM )
    {
        const char * event = NULL;
        if ( hsm_get_cl_event( p_rec->cr_flags ) > CL_EVENT_MAX )
            event = "unknown";
        else
            event = event_name[hsm_get_cl_event( p_rec->cr_flags )];

        snprintf(flag_buff, 256, "(%s%s,rc=%d)", event,
                 hsm_get_cl_flags( p_rec->cr_flags ) & CLF_HSM_DIRTY? ",dirty":"",
                 hsm_get_cl_error( p_rec->cr_flags ));
    }
#endif

    /* display the log record in debug mode */
    if (p_rec->cr_namelen)
    {
        /* this record has a 'name' field. */
        DisplayLog( LVL_DEBUG, CHGLOG_TAG, "%llu %02d%-5s %u.%09u 0x%x%s t="DFID" p="DFID" %.*s",
                    p_rec->cr_index, p_rec->cr_type,
                    changelog_type2str(p_rec->cr_type),
                    (uint32_t)cltime2sec(p_rec->cr_time),
                    cltime2nsec(p_rec->cr_time),
                    p_rec->cr_flags & CLF_FLAGMASK, flag_buff,
                    PFID(&p_rec->cr_tfid),
                    PFID(&p_rec->cr_pfid),
                    p_rec->cr_namelen, p_rec->cr_name);
    }
    else
    {
        /* no 'name' field. */
        DisplayLog( LVL_DEBUG, CHGLOG_TAG, "%llu %02d%-5s %u.%09u 0x%x%s t="DFID,
                    p_rec->cr_index, p_rec->cr_type,
                    changelog_type2str(p_rec->cr_type), 
                    (uint32_t)cltime2sec(p_rec->cr_time),
                    cltime2nsec(p_rec->cr_time),
                    p_rec->cr_flags & CLF_FLAGMASK, flag_buff,
                    PFID(&p_rec->cr_tfid) );
    }

    opnum = p_rec->cr_type ;
    optype = changelog_type2str(opnum);

    if ( optype == NULL )
    {
        DisplayLog( LVL_CRIT, CHGLOG_TAG,
                    "Unsupported log record type %d. Skipping record.",
                    opnum );
        return EINVAL;
    }

    /* update stats */
    if ((opnum >= 0) && (opnum < CL_LAST))
        p_info->cl_counters[opnum] ++;
    else {
        DisplayLog( LVL_CRIT, CHGLOG_TAG,
                    "Log record type %d out of bounds.",
                    opnum );
        return EINVAL;
    }

    /* is this record interesting? */
    switch ( opnum )
    {
        /* The following events are interesting
         * for HSM purpose or policy application:
         * - CREATE: a file is created
         * - OPEN/CLOSE: a file is accessed
         * - UNLINK: a file may need to be removed from HSM
         * - TIME: file mtime has changed
         * - SETATTR: a file attribute has changed
         * - TRUNC: file content is modified
         * - HSM: HSM flags/status have changed
         * - RENAME and EXT: a file has been renamed
         *   (its fileclass could have changed)
         */
        case CL_CREATE:
        case CL_OPEN:
        case CL_CLOSE:
        case CL_UNLINK:
        case CL_SETATTR:
#ifdef CL_SPLITTED_TIME
        case CL_MTIME:
        case CL_CTIME:
#else
        case CL_TIME:
#endif
        case CL_TRUNC:
        case CL_HSM:
        case CL_EXT:
        case CL_RENAME:
#ifdef HAVE_SHOOK
        case CL_XATTR:
#endif
            /* OK */
            break;

        case CL_SOFTLINK:
        case CL_MKDIR:
        case CL_RMDIR:
            /* handle those events for non-HSM purposes */
            break;

        case CL_MARK:
        case CL_HARDLINK:
        case CL_MKNOD:
        case CL_IOCTL:
#ifndef HAVE_SHOOK
        case CL_XATTR:
#endif
#ifdef CL_SPLITTED_TIME
        case CL_ATIME:
#endif
            DisplayLog( LVL_FULL, CHGLOG_TAG, "Ignoring event %s", optype );
            /* free the record */
            llapi_changelog_free( &p_rec );
            return 0;
    }

    /* build the record to be processed in the pipeline */

    InitEntryProc_op( &op );

    /* first, it will check if it already exists in database */
    op.pipeline_stage = STAGE_GET_INFO_DB;

    /* set log record */
    op.extra_info_is_set = TRUE;
    op.extra_info.is_changelog_record = TRUE;
    op.extra_info.log_record.p_log_rec = p_rec;

    /* set mdt name */
    op.extra_info.log_record.mdt =
        chglog_reader_config.mdt_def[p_info->thr_index].mdt_name;

    op.extra_info_free_func = free_extra_info;

    /* set callback function + args */
    op.callback_func = log_record_callback;
    op.callback_param = p_info;

    /* Set entry ID */
    st = EntryProcessor_SetEntryId( &op, &p_rec->cr_tfid );

    if ( st )
    {
        DisplayLog( LVL_CRIT, CHGLOG_TAG, "Error %d setting entry id", st );
        return st;
    }

    /* Push the entry to the pipeline */
    st = EntryProcessor_Push( &op );

    if ( st )
    {
            DisplayLog( LVL_CRIT, CHGLOG_TAG,
                        "CRITICAL ERROR: EntryProcessor_Push returned %d", st );
            free_extra_info(&op.extra_info);
            return st;
    }

    return 0; 
}

static inline void cl_update_stats(reader_thr_info_t * info, struct changelog_rec * p_rec)
{
        /* update thread info */
        info->last_read_time = time(NULL);
        info->nb_read ++;
        info->last_read_record =  p_rec->cr_index;
}


/* get a changelog line (with retries) */
typedef enum {cl_ok, cl_continue, cl_stop} cl_status_e;

static cl_status_e cl_get_one(reader_thr_info_t * info,  struct changelog_rec ** pp_rec)
{
    int rc;
    /* get next record */
    rc = llapi_changelog_recv( info->chglog_hdlr, pp_rec);

    /* is it EOF ? */
    if ( rc == 1 )
    {
        /* do we exit at EOF? */
        if ( !one_shot )
        {
            /* reopen */
            char mdtdevice[128];
            int flags;

            snprintf( mdtdevice, 128, "%s-%s", get_fsname(),
                      chglog_reader_config.mdt_def[info->thr_index].mdt_name );
            log_close(info);

            if ( chglog_reader_config.force_polling )
            {
                DisplayLog( LVL_FULL, CHGLOG_TAG,
                    "EOF reached on changelog, reopening in %d sec",
                    chglog_reader_config.polling_interval);
                    /* sleep during polling interval */
                    rh_sleep( chglog_reader_config.polling_interval );
                    flags = CHANGELOG_FLAG_BLOCK;
            }
            else
            {
                DisplayLog( LVL_EVENT, CHGLOG_TAG,
                    "WARNING: EOF reached on ChangeLog whereas FOLLOW flag "
                    "was specified. Re-openning in 1 sec..." );
                    rh_sleep( 1 );
                    flags = CHANGELOG_FLAG_BLOCK | CHANGELOG_FLAG_FOLLOW;
            }
            /* opening the log again (from last_read_record + 1) */
            rc = llapi_changelog_start( &info->chglog_hdlr, flags,
                                        mdtdevice, info->last_read_record + 1 );
            if (rc==0)
                return cl_continue;
            else
                return cl_stop;
        }
        else
            return cl_stop;
    }
    else if ( rc == -EINTR )
    {
        DisplayLog( LVL_EVENT, CHGLOG_TAG,
                    "llapi_changelog_recv() interrupted. Retrying." );
        return cl_continue;
    }

    if ( rc )
    {
        DisplayLog( LVL_CRIT, CHGLOG_TAG,
                    "Error in llapi_changelog_recv(): %d: %s",
                    rc, strerror(abs(rc)));
        rh_sleep(1);
        /* try to recover from this error */
        return cl_continue;
    }
    else if ( *pp_rec == NULL )
    {
        DisplayLog( LVL_CRIT, CHGLOG_TAG,
                    "Error in llapi_changelog_recv(): NULL pointer returned" );
        rh_sleep(1);
        /* try to recover from this error */
        return cl_continue;
    }

    /* sucessfully retrieved a record */

    /* update thread info */
    cl_update_stats(info, *pp_rec);

    return cl_ok;
}


/** a thread that reads lines from a given changelog */
void * chglog_reader_thr( void *  arg )
{
    reader_thr_info_t * info = (reader_thr_info_t*) arg;
    struct changelog_rec * p_rec = NULL;
    cl_status_e st;

    do
    {
        st = cl_get_one(info, &p_rec); 
        if (st == cl_continue )
            continue;
        else if (st == cl_stop)
            break;

#ifdef _CL_RNM_OVER
        /* First rename record indicates the renamed entry.
         * Second rename record eventually indicates the overwritten entry, 
         * and the target name.
         * if record is CL_RENAME => read the next one
         *  - only push the target CL_EXT event (rename target)
         *    with old fashion format (t=renamed entry)
         *  - if the rename removes another entry,
         *    push an dummy unlink record.
         */
        if (p_rec->cr_type == CL_RENAME)
        {
            /* read next record */
            struct changelog_rec * p_rec2 = NULL;

            st = cl_get_one(info, &p_rec2); 
            if (st == cl_continue)
                continue;
            else if (st == cl_stop)
                break;

            if (p_rec2->cr_type != CL_EXT)
            {
                DisplayLog( LVL_MAJOR, CHGLOG_TAG,
                           "CL_RENAME record not immediatly followed by CL_EXT" );
                /* process record(s) as usual... */
                process_log_rec( info, p_rec );
                process_log_rec( info, p_rec2 );
            }
            else if (FID_IS_ZERO(&p_rec2->cr_tfid))
            {
                /* rename record without removing a target.
                 * translate this record to a CL_EXT with tfid = the renamed object
                 */
                p_rec2->cr_tfid = p_rec->cr_tfid;

                DisplayLog( LVL_DEBUG, CHGLOG_TAG,
                           "Standard rename: object="DFID", name=%.*s",
                            PFID(&p_rec2->cr_tfid), p_rec2->cr_namelen,
                            p_rec2->cr_name );
                process_log_rec(info, p_rec2);
                /* 1st p_rec can be freed */
                llapi_changelog_free( &p_rec );
            }
            else
            {
                /* rename that removes a target entry.
                 * push CL_UNLINK (as p_rec) + CL_EXT (as p_rec2) */
                lustre_fid rename_fid = p_rec->cr_tfid; /* the renamed object */
                lustre_fid remove_fid = p_rec2->cr_tfid; /* the removed object */
                uint64_t idx1 = p_rec->cr_index;
                uint64_t idx2 = p_rec2->cr_index;

                /* duplicate 2nd changelog rec */
                /* XXX /!\ this line is based on the fact that llapi_changelog_recv()
                 * always allocate CR_MAXSIZE, so it is safe to copy the target name
                 * in rec1 without overflow */
                memcpy(p_rec, p_rec2, CR_MAXSIZE); /* both CL_EXT now */
                p_rec->cr_index = idx1;
                p_rec->cr_type = CL_UNLINK;
                p_rec->cr_tfid = remove_fid;
                p_rec2->cr_index = idx2;
                p_rec2->cr_type = CL_EXT;
                p_rec2->cr_tfid = rename_fid;

                DisplayLog( LVL_DEBUG, CHGLOG_TAG,
                           "Unlink: object="DFID", name=%.*s",
                            PFID(&p_rec->cr_tfid), p_rec->cr_namelen,
                            p_rec->cr_name );
                DisplayLog( LVL_DEBUG, CHGLOG_TAG,
                           "Rename: object="DFID", new name=%.*s",
                            PFID(&p_rec2->cr_tfid), p_rec2->cr_namelen,
                            p_rec2->cr_name );
                /* process unlink before rename, to make sure there is no name conflict */
                process_log_rec( info, p_rec );
                process_log_rec( info, p_rec2 );
            }
        }
        else
        {
            /* single record */
#endif
            /* handle the line and push it to the pipeline */
            process_log_rec( info, p_rec );
#ifdef _CL_RNM_OVER
        }
#endif

    } while ( !info->force_stop );
     /* loop until a TERM signal is caught */

    log_close(info);

//end_of_thr:

    DisplayLog(LVL_CRIT, CHGLOG_TAG, "Changelog reader thread terminating");
    return NULL;

}

#ifdef _LLAPI_FORKS
/* In early Lustre 2.0 releases, llapi_changelog_start() forks a process
 * that keeps in <defunc> state.
 * So we work around this issue by trapping SIGCHILD signals.
 */
static void action_sigchld( int sig )
{
    pid_t child ;
    do
    {
        /* wait for all terminated children
         * and stop on end of list or error.
         */
        child = waitpid( -1, NULL, WNOHANG ) ;
    } while ( child > 0 );

}
#endif


/** start ChangeLog Reader module */
int            ChgLogRdr_Start( chglog_reader_config_t * p_config, int flags )
{
    int i, j, rc;
    char mdtdevice[128];
#ifdef _LLAPI_FORKS
    struct sigaction act_sigchld ;
#endif

    /* check that the FS is mounted and has the good type */
    if ( CheckFSInfo( global_config.fs_path, "lustre", NULL,
                      global_config.check_mounted, TRUE ) )
    {
        DisplayLog(LVL_CRIT, CHGLOG_TAG, "ERROR checking filesystem %s",
            global_config.fs_path );
        return EINVAL;
    }

    /* check parameters */
#ifdef _DEBUG_CHGLOG
    printf( "Parameters: mdt_count = %u, mdt_def = %p\n",
            p_config->mdt_count, p_config->mdt_def );
#endif

    if ( (p_config->mdt_count == 0) || (p_config->mdt_def == NULL) )
    {
        DisplayLog( LVL_CRIT, CHGLOG_TAG,
                    "ERROR: no MDT ChangeLog has been defined in configuration" );
        return EINVAL;
    }
    else if ( p_config->mdt_count > 1 )
    {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,  
                   "ERROR: multi-MDT filesystems are not supported in the current version");
          return ENOTSUP;
    }

    /* saves the current config and parameter flags */
    chglog_reader_config = *p_config;
    behavior_flags = flags;

    /* create thread params */    
    reader_info = (reader_thr_info_t*)MemCalloc(p_config->mdt_count,
                                                sizeof( reader_thr_info_t ));
    if ( reader_info == NULL )
        return ENOMEM;

#ifdef _LLAPI_FORKS
    /* initialize sigchild handler */
    memset( &act_sigchld, 0, sizeof( act_sigchld ) ) ;
    act_sigchld.sa_flags = 0 ;
    act_sigchld.sa_handler = action_sigchld ;
    if( sigaction( SIGCHLD, &act_sigchld, NULL ) == -1 )
    {
        DisplayLog(LVL_CRIT, CHGLOG_TAG,
                   "ERROR: Could not initialize SIGCHLD handler: %s",
                   strerror(errno) );
        return errno;
    }
    DisplayLog(LVL_DEBUG, CHGLOG_TAG,  
               "Ready to trap SIGCHLD from liblustreapi child process" );
#endif

    Alert_StartBatching();

    /* create one reader per MDT */
    for ( i = 0; i < p_config->mdt_count ; i++ )
    {

        /* retrieve from the first unacknowledged record */
        unsigned long long last_rec = 0;

        reader_info[i].thr_index = i;
        reader_info[i].thr_id = (pthread_t)NULL;
        reader_info[i].nb_read = 0;
        reader_info[i].last_read_time = 0;
        reader_info[i].last_read_record = 0;
        reader_info[i].last_committed_record = 0;
        reader_info[i].chglog_hdlr = NULL;
        reader_info[i].force_stop = FALSE;
        for (j = 0; j < CL_LAST; j++)
        {
            reader_info[i].cl_counters[j] = 0;
            reader_info[i].cl_reported[j] = 0;
        }
        reader_info[i].last_report = time(NULL);

        snprintf( mdtdevice, 128, "%s-%s", get_fsname(),
                  p_config->mdt_def[i].mdt_name );

        DisplayLog( LVL_DEBUG, CHGLOG_TAG, "Opening chglog for %s", mdtdevice );

        /* open the changelog (if we are in one_shot mode,
         * don't use the CHANGELOG_FLAG_FOLLOW flag)
         */
        rc = llapi_changelog_start( &reader_info[i].chglog_hdlr,
                                    (one_shot || p_config->force_polling?0:CHANGELOG_FLAG_FOLLOW)
                                    | CHANGELOG_FLAG_BLOCK,
                                    mdtdevice, last_rec );

        if ( rc )
        {
                DisplayLog(LVL_CRIT, CHGLOG_TAG,
                    "ERROR %d opening changelog for MDT '%s': %s",
                    rc, mdtdevice, strerror(abs(rc)) );
                return abs(rc);
        }

        /* then create the thread that manages it */
        if ( pthread_create(&reader_info[i].thr_id, NULL, chglog_reader_thr,
                            (void*)&reader_info[i]) )
        {
            int err = errno;
            DisplayLog(LVL_CRIT, CHGLOG_TAG,
                "ERROR creating ChangeLog reader thread: %s",
                strerror(err) );
            return err;
        }
        
    }

    return 0;
}


/** terminate ChangeLog Readers */
int            ChgLogRdr_Terminate(  )
{
    unsigned int i;

    /* ask threads to stop */
    for ( i = 0; i < chglog_reader_config.mdt_count; i++ )
    {
        reader_info[i].force_stop = TRUE;
    }

    DisplayLog( LVL_EVENT, CHGLOG_TAG,
                "Stop request has been sent to all ChangeLog reader threads" );

    /** wannot wait for thread to stop because they are stuck in llapi_chglog_recv()? */
    /** @TODO check if chglog recv return EINTR? */
    /*  ChgLogRdr_Wait(  ); */

    return 0;
}


/** wait for ChangeLog Readers termination */
int            ChgLogRdr_Wait(  )
{
    int i;
    void * ret;

    for ( i = 0; i < chglog_reader_config.mdt_count; i++ )
    {
        pthread_join( reader_info[i].thr_id, &ret );
    }

    Alert_EndBatching();

    return 0;
}

/** dump changelog processing stats */
int            ChgLogRdr_DumpStats(  )
{
    unsigned int i,j;
    char tmp_buff[256];
    char * ptr;
    struct tm paramtm;

    /* ask threads to stop */

    for ( i = 0; i < chglog_reader_config.mdt_count; i++ )
    {
        DisplayLog( LVL_MAJOR, "STATS", "ChangeLog reader #%u:", i );

        DisplayLog( LVL_MAJOR, "STATS", "   fs_name    =   %s",
                    get_fsname() );
        DisplayLog( LVL_MAJOR, "STATS", "   mdt_name   =   %s",
                    chglog_reader_config.mdt_def[i].mdt_name );
        DisplayLog( LVL_MAJOR, "STATS", "   reader_id  =   %s",
                    chglog_reader_config.mdt_def[i].reader_id );
        DisplayLog( LVL_MAJOR, "STATS", "   lines read =   %llu",
                    reader_info[i].nb_read );

        strftime( tmp_buff, 256, "%Y/%m/%d %T",
                  localtime_r( &reader_info[i].last_read_time, &paramtm ) );
        DisplayLog( LVL_MAJOR, "STATS", "   last line read  =   %s", tmp_buff );

        DisplayLog( LVL_MAJOR, "STATS", "   last read record id      = %llu",
                    reader_info[i].last_read_record );
        DisplayLog( LVL_MAJOR, "STATS", "   last committed record id = %llu",
                    reader_info[i].last_committed_record );

        if ( reader_info[i].force_stop )
            DisplayLog( LVL_MAJOR, "STATS", "   status = terminating");

        DisplayLog( LVL_MAJOR, "STATS", "   ChangeLog stats:");

        tmp_buff[0] = '\0';
        ptr = tmp_buff;
        for (j = 0; j < CL_LAST; j++)
        {
            /* flush full line */
            if (ptr - tmp_buff >= 80)
            {
                DisplayLog( LVL_MAJOR, "STATS", "   %s", tmp_buff );
                tmp_buff[0] = '\0';
                ptr = tmp_buff;
            }
            if (ptr == tmp_buff)
                ptr += sprintf( ptr, "%s: %llu", changelog_type2str(j),
                                reader_info[i].cl_counters[j] );
            else
                ptr += sprintf( ptr, ", %s: %llu", changelog_type2str(j),
                                reader_info[i].cl_counters[j] );
        }
        /* last unflushed line */
        if (ptr != tmp_buff)
            sprintf( ptr, ", %s: %llu", changelog_type2str(j),
                     reader_info[i].cl_counters[j] );
    }

    return 0;
}

/** store changelog stats to the database */
int            ChgLogRdr_StoreStats( lmgr_t * lmgr )
{
    unsigned int i;
    char tmp_buff[256];
    struct tm paramtm;

    /* ask threads to stop */

    if ( chglog_reader_config.mdt_count > 1 )
        DisplayLog( LVL_MAJOR, CHGLOG_TAG, "WARNING: more than 1 MDT changelog reader, only 1st reader stats will be stored in DB" );
    else if ( chglog_reader_config.mdt_count < 1 )
        return ENOENT; /* nothing to be stored */

    sprintf( tmp_buff, "%llu", reader_info[0].last_read_record );
    ListMgr_SetVar( lmgr, CL_LAST_READ_ID, tmp_buff );

    strftime( tmp_buff, 256, "%Y/%m/%d %T",
              localtime_r( &reader_info[0].last_read_time, &paramtm ) );
    ListMgr_SetVar( lmgr, CL_LAST_READ_TIME, tmp_buff );

    sprintf( tmp_buff, "%llu", reader_info[0].last_committed_record );
    ListMgr_SetVar( lmgr, CL_LAST_COMMITTED, tmp_buff );


    for (i = 0; i < CL_LAST; i++)
    {
        /* get and set (increment) */
        char varname[256];
        char last_val[256];
        unsigned long long last, current, diff;
        sprintf( varname, "%s_%s", CL_COUNT_PREFIX, changelog_type2str(i) );
        if ( ListMgr_GetVar( lmgr, varname, last_val ) != DB_SUCCESS )
            last = 0;
        else
            last = str2bigint(last_val);

        /* diff = current - last_reported */
        current = reader_info[0].cl_counters[i];
        diff = current - reader_info[0].cl_reported[i];

        /* new value = last + diff */
        sprintf( tmp_buff, "%llu", last + diff );
        if ( ListMgr_SetVar( lmgr, varname, tmp_buff ) == DB_SUCCESS )
            /* last_reported is now current */
            reader_info[0].cl_reported[i] = current;

        /* save diff */
        sprintf( varname, "%s_%s", CL_DIFF_PREFIX, changelog_type2str(i) );
        sprintf( tmp_buff, "%llu", diff );
        ListMgr_SetVar( lmgr, varname, tmp_buff );
    }

    /* indicate diff interval */
    sprintf( tmp_buff, "%lu", time(NULL) - reader_info[0].last_report );
    ListMgr_SetVar( lmgr, CL_DIFF_INTERVAL, tmp_buff );
    reader_info[0].last_report = time(NULL);

    return 0;
}

