/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2007, 2008, 2009, 2010 CEA/DAM
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

#include "list_mgr.h"
#include "RobinhoodLogs.h"
#include "xplatform_print.h"
#include "Memory.h"
#include "RobinhoodMisc.h"

#include <errno.h>
#include <libgen.h>             /* for basename */
#include <dirent.h>             /* for DIR */
#include <sys/ioctl.h>
#include <pthread.h>

#include "lustre_extended_types.h"
#include <lustre/liblustreapi.h>
#include <lustre/lustre_user.h>

/* for logs */
#define TAG_STRIPE "GetStripe"
#define TAG_CR_STRIPE "CreateStriped"
#define TAG_OSTDF "OST_df"
#define TAG_POOLDF "pool_df"
#define TAG_MDSSTAT "mds_stat"
#define TAG_FIDPATH "FidPath"

/** initialize access to lustre */
int Lustre_Init(  )
{
#ifdef HAVE_LLAPI_MSG_LEVEL
    llapi_msg_set_level( LLAPI_MSG_OFF ); 
/*    llapi_msg_set_level( LLAPI_MSG_MAX ); */
#endif

    return 0;
}

int File_GetStripeByPath( const char *entry_path, stripe_info_t * p_stripe_info,
                          stripe_items_t * p_stripe_items )
{
    /* buffer used for llapi_get_stripe.
     * oversize it to 4kB because there can be many stripe entries
     * in the case of join'ed files.
     */
    int            rc;
    char           lum_buffer[4096];
    struct lov_user_md *p_lum = ( struct lov_user_md * ) lum_buffer;
#ifdef LOV_USER_MAGIC_V3
    struct lov_user_md_v3 *p_lum3;
#endif

    unsigned int   i;

    if ( !entry_path || !entry_path[0] )
        return -EFAULT;

    memset( lum_buffer, 0, sizeof( lum_buffer ) );
    rc = llapi_file_get_stripe( entry_path, p_lum );

    if ( rc != 0 )
    {
        if ( rc == -ENODATA )
            DisplayLog( LVL_DEBUG, TAG_STRIPE,
                        "File %s has no stripe information",
                        entry_path );
        else if ( ( rc != -ENOENT ) && ( rc != -ESTALE ) )
            DisplayLog( LVL_CRIT, TAG_STRIPE,
                        "Error %d getting stripe info for %s", rc,
                        entry_path );
        return rc;
    }

    /* Check protocol version number */
    if ( p_lum->lmm_magic == LOV_USER_MAGIC_V1 )
    {

        if ( p_stripe_info )
        {
            p_stripe_info->stripe_size = p_lum->lmm_stripe_size;
            p_stripe_info->stripe_count = p_lum->lmm_stripe_count;
            p_stripe_info->pool_name[0] = '\0';
        }

        if ( p_stripe_items )
        {
            p_stripe_items->count = p_lum->lmm_stripe_count;

            if ( p_lum->lmm_stripe_count > 0 )
            {
                p_stripe_items->stripe_units =
                    ( storage_unit_id_t * ) MemCalloc( p_lum->lmm_stripe_count,
                                                       sizeof( storage_unit_id_t ) );

                /* fill OST ids */
                for ( i = 0; i < p_lum->lmm_stripe_count; i++ )
                {
                    p_stripe_items->stripe_units[i] = p_lum->lmm_objects[i].l_ost_idx;
                }
            }
            else
            {
                p_stripe_items->stripe_units = NULL;
            }
        }

        return 0;

    }
#ifdef LOV_USER_MAGIC_V3
    else if ( p_lum->lmm_magic == LOV_USER_MAGIC_V3 )
    {
        p_lum3 = ( struct lov_user_md_v3 * ) p_lum;

        if ( p_stripe_info )
        {
            p_stripe_info->stripe_size = p_lum3->lmm_stripe_size;
            p_stripe_info->stripe_count = p_lum3->lmm_stripe_count;
            strncpy( p_stripe_info->pool_name, p_lum3->lmm_pool_name, LOV_MAXPOOLNAME );
        }

        if ( p_stripe_items )
        {
            p_stripe_items->count = p_lum3->lmm_stripe_count;

            if ( p_lum3->lmm_stripe_count > 0 )
            {
                p_stripe_items->stripe_units =
                    ( storage_unit_id_t * ) MemCalloc( p_lum3->lmm_stripe_count,
                                                       sizeof( storage_unit_id_t ) );

                /* fill OST ids */
                for ( i = 0; i < p_lum3->lmm_stripe_count; i++ )
                {
                    p_stripe_items->stripe_units[i] = p_lum3->lmm_objects[i].l_ost_idx;
                }
            }
            else
            {
                p_stripe_items->stripe_units = NULL;
            }
        }

        return 0;
    }
#endif
    else
    {
        DisplayLog( LVL_CRIT, TAG_STRIPE, "Unsupported Luster magic number for %s: %#X",
                    entry_path, p_lum->lmm_magic );
        return -EINVAL;
    }
}


#ifdef HAVE_LLAPI_GETPOOL_INFO
int File_CreateSetStripe( const char * path, const stripe_info_t * old_stripe )
{
    int rc;

    /* try to restripe using previous pool name */
    if ( !EMPTY_STRING( old_stripe->pool_name ) )
    {
        rc = llapi_file_create_pool( path, old_stripe->stripe_size,
                                     -1, old_stripe->stripe_count, 0,
                                     (char *)old_stripe->pool_name );
        if ( rc == 0 || rc == -EEXIST )
            return rc;
        else
        {
            DisplayLog( LVL_MAJOR, TAG_CR_STRIPE, "Error %d creating '%s' in pool '%s': %s",
                        rc, path, old_stripe->pool_name, strerror(-rc) );
            DisplayLog( LVL_MAJOR, TAG_CR_STRIPE, "Trying to create it without pool information..." );
        }
    }

    rc = llapi_file_create( path, old_stripe->stripe_size,
                            -1, old_stripe->stripe_count, 0 );
    if ( rc != 0 || rc == -EEXIST )
        DisplayLog( LVL_MAJOR, TAG_CR_STRIPE,
                    "Error %d creating '%s' with stripe. Trying to create it without specific stripe...",
                    rc, path );
    return rc;
}
#endif

#ifdef _HAVE_FID

#define FIDDIR      ".lustre/fid"
#define FIDDIRLEN   11 /* to optimize string concatenation */


/**
 * Build .lustre/fid path associated to a handle.
 */
int BuildFidPath( const entry_id_t * p_id,       /* IN */
                  char *path )                   /* OUT */
{
    char          *curr = path;
    unsigned int  mlen = 0;

    if ( !p_id || !path )
        return EFAULT;

    /* filesystem root */
    strcpy( path, get_mount_point(&mlen) );
    curr += mlen;

    /* fid directory */
    strcpy( curr, "/" FIDDIR "/" );
    curr += FIDDIRLEN + 2;

    /* add fid string */
    curr += sprintf( curr, DFID, PFID(p_id) );

#ifdef _DEBUG
    DisplayLog( LVL_FULL, TAG_FIDPATH, "FidPath=%s", path );
#endif

    return 0;
}

/* Get POSIX path from fid (fid2path wrapper) */
int Lustre_GetFullPath( const entry_id_t * p_id, char *fullpath, unsigned int len )
{
    char          *curr = fullpath;
    int            rc;
    long long      recno = -1;
    int            linkno = 0;
    char           fid[256];
    const char    *mpath = NULL;
    unsigned int   mlen = 0;

    mpath = get_mount_point(&mlen);

    /* set mountpoint at the beginning of the path */
    strcpy( fullpath, mpath );
    curr += mlen;

/* add the slash only if fid2path doesn't */
#ifndef _FID2PATH_LEADING_SLASH
    /* add slash */
    *curr = '/';
    curr ++;
#endif
    /* just in case fid2path returns nothing */
    *curr = '\0';

    /* fid string */
    sprintf( fid, DFID, PFID(p_id) );

    /* MDT device */

    /* ask the path to lustre */
    rc = llapi_fid2path( mpath, fid, curr, len - mlen - 2, &recno,
                         &linkno );

    if ( (rc != 0) && (rc != -ENOENT) && (rc != -ESTALE) )
        DisplayLog( LVL_CRIT, "Fid2Path",
                    "Error %d calling llapi_fid2path(%s,%s,%lld,%d), errno=%d."
                    " Cannot retrieve full path for %s",
                    rc, mpath, fid, recno, linkno, errno, fid );

    return rc;
}

/* Get fid from Posix Path (path2fid wrapper) */
int Lustre_GetFidFromPath( const char *fullpath, entry_id_t * p_id )
{
    int            rc;
    rc = llapi_path2fid( fullpath, p_id );

    if ( (rc != 0) && (rc != -ENOENT) && (rc != -ESTALE) )
        DisplayLog( LVL_DEBUG, "Path2Fid", "llapi_path2fid(%s)=%d, seq=%llx,"
                    " oid=%x, ver=%x",
                    fullpath, rc, p_id->f_seq, p_id->f_oid, p_id->f_ver );

    return rc;
}

#endif

#ifdef _LUSTRE_HSM

/** get Lustre status and convert it to internal Robinhood status */
int LustreHSM_GetStatus( const char *path, file_status_t * p_status,
                         int *no_release, int *no_archive )
{
    int            rc;
    struct hsm_user_state file_status;

    /* initialize outputs */
    *p_status = STATUS_NEW;
    *no_release = FALSE;
    *no_archive = FALSE;

    /* get status */
    rc = llapi_hsm_state_get( path, &file_status );

    if ( (rc !=0) && (rc != -ENOENT) && (rc != -ESTALE) )
        DisplayLog( LVL_DEBUG, "HSMFlags", "llapi_hsm_state_get(%s)=%d", path,
                    rc );
    if (rc != 0)
        return rc;

    /* user flags */

    if ( file_status.hus_states & HS_NORELEASE )
        *no_release = TRUE;
    if ( file_status.hus_states & HS_NOARCHIVE )
        *no_archive = TRUE;

    /* clear them */
    file_status.hus_states &= ~ (HS_NORELEASE|HS_NOARCHIVE);

    /* pending actions */

    if ( file_status.hus_in_progress_action == HUA_ARCHIVE )
    {
        *p_status = STATUS_ARCHIVE_RUNNING;
        return 0;
    }
    else if ( file_status.hus_in_progress_action == HUA_RESTORE )
    {
        *p_status = STATUS_RESTORE_RUNNING;
        return 0;
    }
    else if ( file_status.hus_in_progress_action == HUA_RELEASE )
    {
        DisplayLog( LVL_DEBUG, "HSMFlags", "Entry %s is being released", path );
    }
    else if ( file_status.hus_in_progress_action == HUA_REMOVE )
    {
        DisplayLog( LVL_DEBUG, "HSMFlags", "Entry %s is being removed", path );
    }

    /* status flags */
    if ( ( file_status.hus_states & HSM_FLAGS_MASK ) == 0 )
    {
        *p_status = STATUS_NEW;
    }
    else if ( file_status.hus_states & HS_DIRTY )
    {
        *p_status = STATUS_MODIFIED;
    }
    else if ( file_status.hus_states & HS_ARCHIVED )
    {
        /* "and not dirty" is ensured by the previous test */
        if ( file_status.hus_states & HS_RELEASED )
        {
            /* file is archived in HSM, and released from Lustre */
            *p_status = STATUS_RELEASED;
        }
        else
        {
            /* file is up-to-date in HSM, and not released in Lustre */
            *p_status = STATUS_SYNCHRO;
        }
    }
    else if ( file_status.hus_states & HS_EXISTS )
    {
       /* new file, not yet archived successfully */
       *p_status = STATUS_MODIFIED;
    }
    else
    {
        /* In this case: file has non null status and !HS_DIRTY and !HS_ARCHIVED
         * and !HS_EXISTS.
         * Maybe is it HS_RELEASED without being HS_ARCHIVED (empty file?)
         * or maybe is it LOST???
         */
        DisplayLog( LVL_MAJOR, "HSMFlags", "Entry %s has inconsistent or"
                    " unknown HSM flags %#X",
                    path, file_status.hus_states );
        return EINVAL;
    }

    /** @TODO what if special LOST flag is set??? */

    return 0;
}

/* for traces */
const char * HSMAction2str( enum hsm_user_action action )
{
    switch( action )
    {
        case HUA_ARCHIVE: return "ARCHIVE";
        case HUA_RESTORE: return "RESTORE";
        case HUA_RELEASE: return "RELEASE";
        case HUA_REMOVE: return "REMOVE";
        case HUA_CANCEL: return "CANCEL";
        default: return "Unknown";
    }
    
}

/** Trigger a HSM action */
int LustreHSM_Action( enum hsm_user_action action, const entry_id_t * p_id,
                      const char * hints, unsigned int archive_num )
{
    struct hsm_user_request * req;
    int data_len = 0;
    int rc;
    const char * mpath;

    if ( hints != NULL )
        data_len = strlen(hints)+1;

    req = llapi_hsm_user_request_alloc(1, data_len);

    if (!req)
    {
        rc = -errno;
        DisplayLog( LVL_CRIT, "HSMAction", "Cannot create HSM request: %s",
            strerror(-rc) );
        return rc;
    }

    req->hur_request.hr_action = action;
    req->hur_request.hr_archive_num = archive_num;

    req->hur_user_item[0].hui_fid = *p_id;
    req->hur_user_item[0].hui_extent.offset = 0 ;
    /* XXX for now, always transfer entire file */
    req->hur_user_item[0].hui_extent.length = -1LL;

    req->hur_request.hr_itemcount = 1;

    if ( hints != NULL )
    {
        req->hur_request.hr_data_len = data_len;
        memcpy(hur_data(req), hints, data_len);
    }
    else
    {
        req->hur_request.hr_data_len = 0;
    }

    mpath = get_mount_point(NULL);
    rc = llapi_hsm_request( mpath, req );

    free( req );
    if (rc)
        DisplayLog( LVL_CRIT, "HSMAction", "ERROR performing HSM request(%s,"
                    " root=%s, fid="DFID"): %s",
                    HSMAction2str(action), mpath, PFID(p_id),
                    strerror(-rc) );
    return rc;

}

#endif


/** Retrieve OST usage info ('ost df')
 *  @return 0 on success
 *          ENODEV if ost_index > ost index max of this FS
 */
int Get_OST_usage( const char *fs_path, unsigned int ost_index, struct statfs *ost_statfs )
{
    struct obd_statfs stat_buf;
    struct obd_uuid uuid_buf;
    int            rc;

    /* sanity check */
    if ( !ost_statfs )
        return EFAULT;
    memset( &stat_buf, 0, sizeof( struct obd_statfs ) );
    memset( &uuid_buf, 0, sizeof( struct obd_uuid ) );

    /* llapi_obd_statfs does not modify path (checked in code) */
    rc = llapi_obd_statfs( (char*)fs_path, LL_STATFS_LOV, ost_index, &stat_buf,
                           &uuid_buf );

    if ( rc == -ENODEV )
        /* end of list */
        return -rc;
    else if ( rc == -EAGAIN )
    {
       /* gap in OST indexes? */
       DisplayLog( LVL_EVENT, TAG_OSTDF, "OST #%u does not exist in filesystem %s",
                   ost_index, fs_path );
       return -rc;
    }
    else if ( rc != 0 )
    {
       /* other error */
        DisplayLog( LVL_CRIT, TAG_OSTDF,
                    "Error %d in llapi_obd_statfs(). Cannot retrieve info"
                    " about OST #%u", -rc, ost_index );
        return -rc;
    }

    /* convert info to struct statfs */
    ost_statfs->f_bsize = stat_buf.os_bsize;
    ost_statfs->f_blocks = stat_buf.os_blocks;
    ost_statfs->f_bfree = stat_buf.os_bfree;
    ost_statfs->f_bavail = stat_buf.os_bavail;

    return 0;
}

#ifdef HAVE_LLAPI_GETPOOL_INFO
/** Retrieve pool usage info
 *  @return 0 on success
 */
int Get_pool_usage( const char *poolname, struct statfs *pool_statfs )
{
    struct statfs  ost_statfs;
    int            rc, i, count;
    char           pool[LOV_MAXPOOLNAME + 10];
#ifdef FIND_MAX_OSTS
    char          *ostlist[FIND_MAX_OSTS];
    char           buffer[4096];
#else /* no max OST count since Lustre 2.2 */
    unsigned int obdcount = 256;
    char        **ostlist = NULL;
    int          bufsize = sizeof(struct obd_uuid) * obdcount;
    char *buffer = MemAlloc(bufsize + (sizeof(*ostlist) * obdcount));
    ostlist = (char **)(buffer + bufsize);
#endif

    /* sanity check */
    if ( !pool_statfs )
        return EFAULT;

    memset( pool_statfs, 0, sizeof( struct statfs ) );

    /* retrieve list of OSTs in the pool */
    sprintf( pool, "%s.%s", get_fsname(), poolname );
#ifdef FIND_MAX_OSTS
    rc = llapi_get_poolmembers(pool, ostlist, FIND_MAX_OSTS, buffer, 4096);
#else
    do {
        rc = llapi_get_poolmembers(pool, ostlist, obdcount, buffer, bufsize);
        if (rc == -EOVERFLOW)
        {
            /* buffer too small, increase obdcount by 2 */
            obdcount *= 2;
            bufsize = sizeof(struct obd_uuid) * obdcount;
            buffer = MemRealloc(buffer, bufsize + (sizeof(*ostlist) * obdcount));
            if (buffer == NULL)
                return ENOMEM;
            ostlist = (char **)(buffer + bufsize);
        }
    } while (rc == -EOVERFLOW);
#endif

    if ( rc < 0 )
        return -rc;

    count = rc;

    /* get OST info and sum them */
    for ( i = 0; i < count; i++ )
    {
        char          *ost;
        int            index;
        /* get ost index in <fsname>-OST<index>_UUID */
        ost = strrchr( ostlist[i], '-' );
        if ( !ost )
        {
            DisplayLog( LVL_CRIT, TAG_POOLDF, "Invalid OST format: '%s'", ostlist[i] );
            return EINVAL;
        }

        /* skip '-' */
        ost++;
        if ( sscanf( ost, "OST%d", &index ) != 1 )
        {
            DisplayLog( LVL_CRIT, TAG_POOLDF, "Could not find OST index in"
                        " string '%s'", ost );
            return EINVAL;
        }

        rc = Get_OST_usage( get_mount_point(NULL), index, &ost_statfs );
        if ( rc )
            return rc;

        /* sum info to struct statfs */
        pool_statfs->f_blocks += ost_statfs.f_blocks;
        pool_statfs->f_bfree += ost_statfs.f_bfree;
        pool_statfs->f_bavail += ost_statfs.f_bavail;
        pool_statfs->f_bsize = ost_statfs.f_bsize;
    }

    return 0;
}
#endif

#ifdef _MDS_STAT_SUPPORT
/* This code is an adaptation of llapi_mds_getfileinfo() in liblustreapi.
 * It is unused for now, but could be useful when SOM will be implemented.
 */
int lustre_mds_stat( char *fullpath, DIR * parent, struct stat *inode )
{
    /* this buffer must be large enough for handling filename */
    char           buffer[1024];
    struct lov_user_mds_data *lmd = ( struct lov_user_mds_data * ) buffer;
    char          *filename;
    int            rc;

    /* sanity checks */
    if ( ( parent == NULL ) || ( fullpath == NULL ) || ( inode == NULL ) )
        return EINVAL;

    filename = basename( fullpath );

    if ( filename == NULL )
        filename = fullpath;

    memset( lmd, 0, sizeof( buffer ) );
    strncpy( buffer, filename, strlen( filename ) + 1 );
    rc = ioctl( dirfd( parent ), IOC_MDC_GETFILEINFO, ( void * ) lmd );

    if ( rc )
    {
        if ( errno == ENOTTY )
        {
            /* ioctl is not supported, it is not a lustre fs.
             * Do the regular lstat(2) instead. */
            rc = lstat( fullpath, inode );
            if ( rc )
            {
                DisplayLog( LVL_CRIT, TAG_MDSSTAT, "Error: %s: lstat failed for %s",
                            __FUNCTION__, fullpath );
                return rc;
            }
        }
        else if ( ( errno == ENOENT ) || ( errno == ESTALE ) )
        {
            DisplayLog( LVL_MAJOR, TAG_MDSSTAT, "Warning: %s: %s does not exist",
                        __FUNCTION__, fullpath );
            return ENOENT;
        }
        else
        {
            DisplayLog( LVL_CRIT, TAG_MDSSTAT,
                        "Error: %s: IOC_MDC_GETFILEINFO failed for %s",
                        __FUNCTION__, fullpath );
            return rc;
        }
    }
    else
        *inode = lmd->lmd_st;

    return 0;

}


#ifdef _HAVE_FID

static DIR * fid_dir_fd = NULL;
static pthread_mutex_t dir_lock = PTHREAD_MUTEX_INITIALIZER;

int lustre_mds_stat_by_fid( const entry_id_t * p_id, struct stat *inode )
{
    char filename[MAXNAMLEN];
    char           buffer[1024];
    struct lov_user_mds_data *lmd = ( struct lov_user_mds_data * ) buffer;
    int rc;

    /* ensure fid directory is opened */
    if ( fid_dir_fd == NULL )
    {
        P( dir_lock );
        if ( fid_dir_fd == NULL )
        {
            char path[RBH_PATH_MAX];
            char *curr = path;
            unsigned int mlen;

            /* filesystem root */
            strcpy( path, get_mount_point(&mlen) );
            curr += mlen;

            /* fid directory */
            strcpy( curr, "/" FIDDIR );

            /* open fir directory */
            fid_dir_fd = opendir( path );
        }
        V( dir_lock );
        if ( fid_dir_fd == NULL )
            return errno;
    }

    sprintf( filename, DFID, PFID(p_id) );
    memset( lmd, 0, sizeof( buffer ) );
    strncpy( buffer, filename, strlen( filename ) + 1 );

    rc = ioctl( dirfd( fid_dir_fd ), IOC_MDC_GETFILEINFO, ( void * ) lmd );

    if ( rc )
    {
        if ( errno == ENOTTY )
        {
            return ENOTSUP;
        }
        else if ( ( errno == ENOENT ) || ( errno == ESTALE ) )
        {
            DisplayLog( LVL_MAJOR, TAG_MDSSTAT, "Warning: %s: %s does not exist",
                        __FUNCTION__, filename );
            return ENOENT;
        }
        else
        {
            DisplayLog( LVL_CRIT, TAG_MDSSTAT,
                        "Error: %s: IOC_MDC_GETFILEINFO failed for %s",
                        __FUNCTION__, filename );
            return errno;
        }
    }

    *inode = lmd->lmd_st;
    return 0;
}
#endif

#endif
