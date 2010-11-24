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
int log_record_callback( struct entry_proc_op_t * pop, void * param )
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


/**
 * This handles a single log record.
 */
static int process_log_rec( reader_thr_info_t * p_info, struct changelog_rec * p_rec )
{
    entry_proc_op_t op;
    int            st;

    int            opnum;
    const char           *optype;

    /* display the log record in debug mode */
    if (p_rec->cr_namelen)
    {
        /* this record has a 'name' field. */
        DisplayLog( LVL_DEBUG, CHGLOG_TAG, "%llu %02d%-5s %u.%09u 0x%x t="DFID" p="DFID" %.*s",
                    p_rec->cr_index, p_rec->cr_type,
                    changelog_type2str(p_rec->cr_type),
                    (uint32_t)cltime2sec(p_rec->cr_time),
                    cltime2nsec(p_rec->cr_time),
                    p_rec->cr_flags & CLF_FLAGMASK, PFID(&p_rec->cr_tfid),
                    PFID(&p_rec->cr_pfid),
                    p_rec->cr_namelen, p_rec->cr_name);
    }
    else
    {
        /* no 'name' field. */
        DisplayLog( LVL_DEBUG, CHGLOG_TAG, "%llu %02d%-5s %u.%09u 0x%x t="DFID,
                    p_rec->cr_index, p_rec->cr_type,
                    changelog_type2str(p_rec->cr_type), 
                    (uint32_t)cltime2sec(p_rec->cr_time),
                    cltime2nsec(p_rec->cr_time),
                    p_rec->cr_flags & CLF_FLAGMASK, PFID(&p_rec->cr_tfid) );
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
            /* OK */
            break;

        case CL_SOFTLINK:
        case CL_MKDIR:
        case CL_RMDIR:
#ifndef _LUSTRE_HSM
            /* handle those events for non-HSM purposes */
            break;
#endif
        case CL_MARK:
        case CL_HARDLINK:
        case CL_MKNOD:
        case CL_IOCTL:
        case CL_XATTR:
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


/** a thread that reads lines from a given changelog */
void * chglog_reader_thr( void *  arg )
{
    reader_thr_info_t * info = (reader_thr_info_t*) arg;
    struct changelog_rec * p_rec = NULL;
    int rc;

    do
    {
        /* get next record */
        rc = llapi_changelog_recv( info->chglog_hdlr, &p_rec);

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
                    continue;
                else
                    break;
            }
            else
                break;
        }
        else if ( rc == -EINTR )
        {
            DisplayLog( LVL_EVENT, CHGLOG_TAG,
                        "llapi_changelog_recv() interrupted. Retrying." );
            continue;
        }

        if ( rc )
        {
            DisplayLog( LVL_CRIT, CHGLOG_TAG,
                        "Error in llapi_changelog_recv(): %d: %s",
                        rc, strerror(abs(rc)));
            goto end_of_thr;
        }
        else if ( p_rec == NULL )
        {
            DisplayLog( LVL_CRIT, CHGLOG_TAG,
                        "Error in llapi_changelog_recv(): NULL pointer returned" );
            goto end_of_thr;
        }

        /* update thread info */
        info->last_read_time = time(NULL);
        info->nb_read ++;
        info->last_read_record =  p_rec->cr_index;

        /* handle the line and push it to the pipeline */
        process_log_rec( info, p_rec );

    } while ( !info->force_stop );
     /* loop until a TERM signal is caught */

    log_close(info);

end_of_thr:

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
    int i, rc;
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
    return 0;
}

/** dump changelog processing stats */
int            ChgLogRdr_DumpStats(  )
{
    unsigned int i;
    char tmp_buff[256];
    struct tm paramtm;

    /* ask threads to stop */

    for ( i = 0; i < chglog_reader_config.mdt_count; i++ )
    {
        DisplayLog( LVL_MAJOR, "STATS", "ChangeLog reader #%u:", i );

        DisplayLog( LVL_MAJOR, "STATS", "  fs_name    =   %s",
                    get_fsname() );
        DisplayLog( LVL_MAJOR, "STATS", "  mdt_name   =   %s",
                    chglog_reader_config.mdt_def[i].mdt_name );
        DisplayLog( LVL_MAJOR, "STATS", "  reader_id  =   %s",
                    chglog_reader_config.mdt_def[i].reader_id );
        DisplayLog( LVL_MAJOR, "STATS", "  lines read =   %llu",
                    reader_info[i].nb_read );

        strftime( tmp_buff, 256, "%Y/%m/%d %T",
                  localtime_r( &reader_info[i].last_read_time, &paramtm ) );
        DisplayLog( LVL_MAJOR, "STATS", "  last line read  =   %s", tmp_buff );

        DisplayLog( LVL_MAJOR, "STATS", "  last read record id      = %llu",
                    reader_info[i].last_read_record );
        DisplayLog( LVL_MAJOR, "STATS", "  last committed record id = %llu",
                    reader_info[i].last_committed_record );

        if ( reader_info[i].force_stop )
            DisplayLog( LVL_MAJOR, "STATS", "  status = terminating");
    }

    return 0;
}

