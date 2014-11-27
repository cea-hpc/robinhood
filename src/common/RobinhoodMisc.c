/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2004-2009 CEA/DAM
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

#include "global_config.h"
#include "RobinhoodMisc.h"
#include "RobinhoodLogs.h"
#include "RobinhoodConfig.h"
#include "xplatform_print.h"
#include "uidgidcache.h"
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <pthread.h>
#include <libgen.h>             /* for dirname */
#include <stdarg.h>
#include <fnmatch.h>
#include <sys/types.h>
#include <utime.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>

#ifndef HAVE_GETMNTENT_R
#include "mntent_compat.h"
#else
#include <mntent.h>             /* for handling mntent */
#endif


/* Exa-scale definitions ;-) */
#define KILO_BYTE  (1024LL)
#define MEGA_BYTE  (1024LL * KILO_BYTE )
#define GIGA_BYTE  (1024LL * MEGA_BYTE )
#define TERA_BYTE  (1024LL * GIGA_BYTE )
#define PETA_BYTE  (1024LL * TERA_BYTE )
#define EXA_BYTE   (1024LL * PETA_BYTE )

/* Durations  */
#define MINUTE   60
#define HOUR  (60*MINUTE)
#define DAY   (24*HOUR)
#define WEEK  (7*DAY)
#define YEAR  (365*DAY)


void Exit( int error_code )
{
    DisplayLog( LVL_MAJOR, "EXIT", "Exiting program with code %d", error_code );

#if 0
    /* Remove pid_file, if any */
    if ( pid_file != NULL )
    {
        if ( unlink( pid_file ) != 0 )
        {
            DisplayLog( LVL_CRIT, "EXIT",
                        "Could not remove pid file %s: %s", pid_file, strerror( errno ) );
        }
    }
#endif

    FlushLogs(  );
    exit( error_code );
}

/* global info about the filesystem to be managed */
static char   *mount_point;
static char    fsname[RBH_PATH_MAX] = "";
static dev_t   dev_id = 0;
static uint64_t fs_key = 0;
static entry_id_t root_id;

/* to optimize string concatenation */
static unsigned int mount_len = 0;

#ifdef _HAVE_FID
#define FIDDIR      "/.lustre/fid/"
static char *fid_dir;
#endif

/* used at initialization time, to avoid several modules
 * that start in parallel to check it several times.
 */
static pthread_mutex_t mount_point_lock = PTHREAD_MUTEX_INITIALIZER;

#define LAST_32PRIME    0xFFFFFFFB
#define LAST_64PRIME    0xFFFFFFFFFFFFFFC5
static uint64_t hash_name(const char * str)
{
    unsigned int i;
    uint64_t val = 1;

    for ( i = 0; i < strlen(str); i++ )
        val = ( val << 5 ) - val + (unsigned int) ( str[i] );

    return val % LAST_32PRIME;
}

static uint64_t fsidto64(fsid_t fsid)
{
    uint64_t out;
    if (sizeof(fsid_t) <= sizeof(uint64_t))
    {
        memset(&out, 0, sizeof(out));
        memcpy((&out)+(sizeof(out)-sizeof(fsid_t)), &fsid, sizeof(fsid));
        DisplayLog(LVL_DEBUG, __func__, "sizeof(fsid)=%lu <= 64bits, fsid as 64=%"PRIX64, sizeof(fsid_t), out);
        return out;
    }
    else
    {
        unsigned int i;
        out = 1;
        char * str = (char *)(&fsid);

        for ( i = 0; i < sizeof(fsid_t); i++ )
            out = ( out << 5 ) - out + (unsigned int) ( str[i] );

        out = out % LAST_64PRIME;
        DisplayLog(LVL_DEBUG, __func__, "sizeof(fsid)=%lu > 64bits, hash64(fsid)=%"PRIX64, sizeof(fsid_t), out);
        return out;
    }
}

/* this set of functions is for retrieving/checking mount point
 * and fs name (once for all threads):
 */
static void _set_mount_point( char *mntpnt )
{
    char path[RBH_PATH_MAX + 100];

    /* cannot change during a run */
    if (mount_len == 0)
    {
        strcpy(path, mntpnt );

        /* remove final slash, if any */
        if ( (mount_len > 1) && (path[mount_len-1] == '/') )
        {
            path[mount_len-1] = '\0';
        }

        mount_point = strdup( path );
        mount_len = strlen( mount_point );

#ifdef _HAVE_FID
        /* Now, the .fid directory */
        strcat(path, FIDDIR);
        fid_dir = strdup(path);
#endif
    }
}

static int path2id(const char *path, entry_id_t *id);

static int set_fs_info(char *name, char * mountp, dev_t dev, fsid_t fsid)
{
    int rc = 0;

    P(mount_point_lock);
    _set_mount_point(mountp);
    strcpy(fsname, name);
    dev_id = dev;

    switch (global_config.fs_key)
    {
        case FSKEY_FSNAME:
            fs_key = hash_name(name);
            DisplayLog(LVL_DEBUG, "FSInfo", "fs_key: hash(fsname)=%"PRIX64, fs_key);
            break;
        case FSKEY_FSID:
            fs_key = fsidto64(fsid);
            DisplayLog(LVL_DEBUG, "FSInfo", "fs_key: fsid as 64=%"PRIX64, fs_key);
            break;
        case FSKEY_DEVID:
            fs_key = dev_id;
            DisplayLog(LVL_DEBUG, "FSInfo", "fs_key: devid=%"PRIX64, fs_key);
            break;
        default:
            DisplayLog(LVL_MAJOR, "FSInfo", "Invalid fs_key type %#x", global_config.fs_key);
            fs_key = 0;
    }

    /* now, path2id can be called */
    rc = path2id(global_config.fs_path, &root_id);
    if (rc)
        DisplayLog(LVL_CRIT, "FSInfo", "Failed to get id for root directory %s: %s",
                   mountp, strerror(-rc));

    V(mount_point_lock);
    return rc;
}

/* retrieve the mount point from any module
 * without final slash.
 */
const char          *get_mount_point( unsigned int * plen )
{
    if (plen) (*plen) = mount_len;
    return mount_point;
}

#if _HAVE_FID
/* Retrieve the .fid directory */
const char          *get_fid_dir( void )
{
    return fid_dir;
}
#endif

/* retrieve fsname from any module */
const char          *get_fsname( void )
{
    return fsname;
}

/* return Filesystem device id  */
dev_t          get_fsdev( void )
{
    return dev_id;
}

uint64_t       get_fskey( void )
{
    return fs_key;
}

const entry_id_t *get_root_id(void)
{
    return &root_id;
}



/**
 * send a mail
 */
int SendMail( const char *recipient, const char *subject, const char *message )
{
    char           buffer[MAIL_TITLE_MAX];
    FILE          *fichier;

    snprintf( buffer, MAIL_TITLE_MAX, MAIL " -s \"%s\" %s", subject, recipient );

    if ( ( fichier = popen( buffer, "w" ) ) == NULL )
    {
        DisplayLog( LVL_CRIT, "SENDMAIL", "Error %d sending mail with the following command=%s",
                    errno, buffer );
        return -1;
    }
    fwrite( message, strlen( message ), 1, fichier );
    pclose( fichier );
    return 0;
}

/**
 * Search for Robinhood config file.
 * search a config file with the given name (+extension)
 * If cfg_in is empty: search any config in config paths
 * /!\ not thread safe
 */
int SearchConfig(const char *cfg_in, char *cfg_out, int *changed, char *unmatched,
                 size_t max_len)
{
    static const char *default_cfg_path = SYSCONFDIR"/robinhood.d/"PURPOSE_EXT;
    DIR *dir;
    struct dirent *ent;
    const char *cfg = cfg_in;

    *changed = 1; /* most of the cases */

    if (cfg == NULL || EMPTY_STRING(cfg))
    {
        /* check if a default config file is specified */
        cfg = getenv(DEFAULT_CFG_VAR);
    }

    /* set unmatched, for better logging */
    if (unmatched) {
        if (cfg)
            rh_strncpy(unmatched, cfg, max_len);
        else
            snprintf(unmatched, max_len, "%s/*.conf", default_cfg_path);
    }

    if (cfg == NULL || EMPTY_STRING(cfg)) {
        int found = 0;

        /* look for files in default config path */
        dir = opendir( default_cfg_path );
        if ( dir ) {
            while ( (ent = readdir(dir)) != NULL )
            {
                /* ignore .xxx files */
                if (ent->d_name[0] == '.')
                    continue;
                if (fnmatch("*.conf", ent->d_name, 0) && fnmatch("*.cfg", ent->d_name, 0))
                    /* not a config file */
                    continue;

                sprintf( cfg_out, "%s/%s", default_cfg_path, ent->d_name );
                if ( access(cfg_out, F_OK) == 0 ) {
                    /* that file matches. */
                    found ++;
                    if (found >= 2)
                        /* No need to continue. */
                        break;
                }
            }

            closedir(dir);
       }

       if (found == 1) {
           /* Only one file found. cfg_out is already set. We're
            * good. */
           return 0;
       }
    }
    else if (access(cfg, F_OK) == 0)
    {
        /* the specified config file exists */
        if (cfg_out != cfg)
            rh_strncpy(cfg_out, cfg, max_len);
        *changed=0;
        return 0;
    }
    else if (strchr(cfg, '/'))
    {
        /* the argument is a path (not a single name
         * and this path was not found) */
        *changed=0;
        goto notfound;
    }
    else /* look for a file in the given paths */
    {
        char cfg_cp[RBH_PATH_MAX] = "";
        int has_ext = (strchr(cfg, '.') != NULL);

        rh_strncpy(cfg_cp, cfg, MIN2(max_len, RBH_PATH_MAX));

        /* if the file already has an extension, try path/name */
        if (has_ext)
        {
            snprintf(cfg_out, max_len, "%s/%s", default_cfg_path, cfg_cp);
            if (access(cfg_out, F_OK) == 0)
                return 0;
        }

        /* try path/name.cfg, path/name.conf */
        snprintf(cfg_out, max_len, "%s/%s.conf", default_cfg_path, cfg_cp);
        if (access(cfg_out, F_OK) == 0)
            return 0;

        snprintf(cfg_out, max_len, "%s/%s.cfg", default_cfg_path, cfg_cp);
        if (access(cfg_out, F_OK) == 0)
            return 0;
    }

notfound:
    /* no file found, cleaning cfg_out */
    cfg_out[0] = '\0';
    return -ENOENT;
}


#define TEST_LOCK_FILE_PERIOD 10

/*
 * This function is blocking as long as the lock file is present.
 * Optionaly updates an action timestamp, at each test.
 */
void TestLockFile( time_t * p_last_action )
{

    while ( access( global_config.lock_file, F_OK ) == 0 )
    {
        if ( p_last_action )
            *p_last_action = time( NULL );

        DisplayLog( LVL_MAJOR, "LOCK", "Lock file %s detected, waiting %ds",
                    global_config.lock_file, TEST_LOCK_FILE_PERIOD );
        rh_sleep( TEST_LOCK_FILE_PERIOD );
    }

}

char          *uid2str( uid_t uid, char *username )
{
    struct passwd *p = GetPwUid( uid );
    if ( p != NULL )
        strcpy( username, p->pw_name );
    else
        sprintf( username, "%d", ( int ) uid );

    return username;
}

char          *gid2str( gid_t gid, char *groupname )
{
    struct group  *g = GetGrGid( gid );
    if ( g != NULL )
        strcpy( groupname, g->gr_name );
    else
        sprintf( groupname, "%d", ( int ) gid );

    return groupname;
}

const char * mode2type(mode_t mode)
{
    if ( S_ISREG( mode ) )
        return STR_TYPE_FILE;
    else if ( S_ISDIR( mode ) )
        return STR_TYPE_DIR;
    else if ( S_ISLNK( mode ) )
        return STR_TYPE_LINK;
    else if ( S_ISCHR( mode ) )
        return STR_TYPE_CHR;
    else if ( S_ISBLK( mode ) )
        return STR_TYPE_BLK;
    else if ( S_ISFIFO( mode ) )
        return STR_TYPE_FIFO;
    else if ( S_ISSOCK( mode ) )
        return STR_TYPE_SOCK;
    else
        return NULL;
}

void PosixStat2EntryAttr( struct stat *p_inode, attr_set_t * p_attr_set, int size_info )
{
    ATTR_MASK_SET( p_attr_set, owner );
    uid2str( p_inode->st_uid, ATTR( p_attr_set, owner ) );

    ATTR_MASK_SET( p_attr_set, gr_name );
    gid2str( p_inode->st_gid, ATTR( p_attr_set, gr_name ) );

    if ( size_info )
    {
        ATTR_MASK_SET( p_attr_set, size );
        ATTR( p_attr_set, size ) = p_inode->st_size;

        ATTR_MASK_SET( p_attr_set, blocks );
        ATTR( p_attr_set, blocks ) = p_inode->st_blocks;

#ifdef ATTR_INDEX_blksize
        ATTR_MASK_SET( p_attr_set, blksize );
        ATTR( p_attr_set, blksize ) = p_inode->st_blksize;
#endif

        /* times are also wrong when they come from the MDT device */
        ATTR_MASK_SET( p_attr_set, last_access );
        ATTR( p_attr_set, last_access ) =
            MAX3( p_inode->st_atime, p_inode->st_mtime, p_inode->st_ctime );

        ATTR_MASK_SET( p_attr_set, last_mod );
        ATTR( p_attr_set, last_mod ) = p_inode->st_mtime;
    }

#ifdef ATTR_INDEX_creation_time
    if (ATTR_MASK_TEST(p_attr_set, creation_time))
    {
        /* creation time is always <= ctime */
        if (p_inode->st_ctime < ATTR(p_attr_set, creation_time))
            ATTR(p_attr_set, creation_time) = p_inode->st_ctime;
    }
    else
    {
        ATTR_MASK_SET(p_attr_set, creation_time);
        ATTR(p_attr_set, creation_time) = p_inode->st_ctime;
    }
#endif

#ifdef ATTR_INDEX_type
    const char * type = mode2type(p_inode->st_mode);
    if (type != NULL)
    {
        ATTR_MASK_SET( p_attr_set, type );
        strcpy(ATTR( p_attr_set, type ), type);
    }
#endif

#ifdef ATTR_INDEX_nlink
    ATTR_MASK_SET( p_attr_set, nlink );
    ATTR( p_attr_set, nlink ) = p_inode->st_nlink;
#endif
#ifdef ATTR_INDEX_mode
    ATTR_MASK_SET( p_attr_set, mode );
    ATTR( p_attr_set, mode ) = p_inode->st_mode & 07777 ; /*  mode + sticky bits */
#endif
}

#ifndef HAVE_GETMNTENT_R

/* if getmntent_r() does not exists, define it as a wrapper of getmntent().
 * use a lock to ensure thread-safety.
 */
static pthread_mutex_t mntent_lock = PTHREAD_MUTEX_INITIALIZER;

/* copy a mntent structure to caller's buffer */
static int copy_mntent( struct mntent *mntout, char *buf, int buflen,
                        const struct mntent * mntin )
{
    char * curr = buf;

    if (!buf || !mntout)
        return EFAULT;

    if (strlen(mntin->mnt_fsname)+1
        +strlen(mntin->mnt_dir)+1
        +strlen(mntin->mnt_type)+1
        +strlen(mntin->mnt_opts)+1 > buflen )
        return ENOMEM;

    strcpy(curr, mntin->mnt_fsname );
    mntout->mnt_fsname = curr;
    curr += strlen(mntin->mnt_fsname)+1;

    strcpy(curr, mntin->mnt_dir );
    mntout->mnt_dir = curr;
    curr += strlen(mntin->mnt_dir)+1;

    strcpy(curr, mntin->mnt_type );
    mntout->mnt_type = curr;
    curr += strlen(mntin->mnt_type)+1;

    strcpy(curr, mntin->mnt_opts );
    mntout->mnt_opts = curr;
    curr += strlen(mntin->mnt_opts)+1;

    mntout->mnt_freq   = mntin->mnt_freq;
    mntout->mnt_passno = mntin->mnt_passno;

    return 0;
}

static struct mntent *getmntent_r(FILE *fp, struct mntent *mntbuf,
                           char *buf, int buflen)
{
    struct mntent * pmntent;
    /* struct mntent *getmntent(FILE *fp); */
    P(mntent_lock);
    pmntent = getmntent(fp);
    /* copy mntent structure to caller buffer */
    if (pmntent)
    {
        if ( copy_mntent(mntbuf, buf, buflen, pmntent) != 0 )
            pmntent = NULL; /* causes an error */
    }
    V(mntent_lock);
    return pmntent;
}
#endif

/* Check mount point and FS type.
 * Also return the associated device number.
 * (for STAY_IN_FS security option).
 */
int CheckFSInfo( char *path, char *expected_type,
                 dev_t * p_fs_dev, char * fsname_out,
                 int check_mounted, int save_fs )
{
    FILE          *fp;
    struct mntent *p_mnt;
    struct mntent  mnt_ent;
    char           mnt_buff[4096];

    char           rpath[RBH_PATH_MAX];
    char           mntdir[RBH_PATH_MAX];
    char           tmp_buff[RBH_PATH_MAX];
    char          *parentmntdir;
    char           fs_spec[RBH_PATH_MAX];

    char           type[256];

    struct stat    pathstat;
    struct stat    parentmntstat;

    size_t         pathlen, outlen;
    char * name = NULL;


    if ( ( expected_type == NULL ) || ( expected_type[0] == '\0' ) )
    {
        DisplayLog( LVL_CRIT, "CheckFS", "/!\\ ERROR /!\\ No filesystem type specified" );
        return EINVAL;
    }

    /* convert to canonic path */
    /* let realpath determine the output length (NULL argument) */
    char * tmp_path = realpath( path, NULL );
    if ( tmp_path == NULL )
    {
        DisplayLog( LVL_CRIT, "CheckFS", "Error %d in realpath(%s): %s",
                    errno, ( path ? path : "<null>" ), strerror( errno ) );
        return errno;
    }
    if (strlen(tmp_path) >= RBH_PATH_MAX)
    {
        free(tmp_path);
        DisplayLog( LVL_CRIT, "CheckFS", "Path length is too long!" );
        return ENAMETOOLONG;
    }
    /* safe because of previous check */
    strcpy(rpath, tmp_path);
    /* now can release tmp path */
    free(tmp_path);

    /* open mount tab and look for the given path */
    outlen = 0;

    fp = setmntent( MOUNTED, "r" );

    if ( fp == NULL )
    {
        DisplayLog( LVL_CRIT, "CheckFS", "Error %d in setmntent(%s): %s",
                    errno, MOUNTED, strerror( errno ) );
        return errno;
    }

    while ( ( p_mnt = getmntent_r( fp, &mnt_ent, mnt_buff, 4096 ) ) != NULL )
    {
        /* get the longest matching path */

        if ( p_mnt->mnt_dir != NULL )
        {

            pathlen = strlen( p_mnt->mnt_dir );

            /* if check_mounted is FALSE, root filesystem is allowed */
            if ( !check_mounted && ( pathlen > outlen )
                 && !strcmp( p_mnt->mnt_dir, "/" ) )
            {
                DisplayLog( LVL_DEBUG, "CheckFS",
                            "Root mountpoint is allowed for matching %s, type=%s, fs=%s",
                            rpath, p_mnt->mnt_type, p_mnt->mnt_fsname );
                outlen = pathlen;
                rh_strncpy(mntdir, p_mnt->mnt_dir, RBH_PATH_MAX);
                rh_strncpy(type, p_mnt->mnt_type, 256);
                rh_strncpy(fs_spec, p_mnt->mnt_fsname, RBH_PATH_MAX);
            }
            /* in other cases, the filesystem must be <mountpoint>/<smthg> or <mountpoint>\0 */
            else if ( ( pathlen > outlen ) &&
                      !strncmp( rpath, p_mnt->mnt_dir, pathlen ) &&
                      ( ( rpath[pathlen] == '/' ) || ( rpath[pathlen] == '\0' ) ) )
            {
                DisplayLog( LVL_FULL, "CheckFS",
                            "%s is under mountpoint %s, type=%s, fs=%s",
                            rpath, p_mnt->mnt_dir, p_mnt->mnt_type, p_mnt->mnt_fsname );

                outlen = pathlen;
                rh_strncpy(mntdir, p_mnt->mnt_dir, RBH_PATH_MAX);
                rh_strncpy(type, p_mnt->mnt_type, 256);
                rh_strncpy(fs_spec, p_mnt->mnt_fsname, RBH_PATH_MAX);
            }
        }
    }

    if ( outlen <= 0 )
    {
        DisplayLog( LVL_CRIT, "CheckFS", "No mount entry matches '%s' in %s", rpath, MOUNTED );
        DisplayLog( LVL_CRIT, "CheckFS",
                    "Set 'check_mounted = FALSE' in configuration to force using root filesystem" );
        endmntent( fp );
        return ENOENT;
    }


    /* display the matching entry */
    DisplayLog( LVL_EVENT, "CheckFS",
                "'%s' matches mount point '%s', type=%s, fs=%s", rpath, mntdir, type, fs_spec );

    /* check filesystem type */
    if ( strcasecmp( type, expected_type ) )
    {
        if (check_mounted)
        {
            DisplayLog( LVL_CRIT, "CheckFS",
                        "/!\\ ERROR /!\\ The specified type for '%s' (%s) does not match actual filesystem type (%s)",
                        rpath, expected_type, type );
            endmntent( fp );
            return EINVAL;
        }
        else
        {
            DisplayLog( LVL_MAJOR, "CheckFS",
                        "/!\\ WARNING /!\\ The specified type for '%s' (%s) "
                        "does not match actual filesystem type (%s).",
                        rpath, expected_type, type );
            DisplayLog( LVL_MAJOR, "CheckFS", "check_mounted is disabled: continuing." );
        }
    }

    /* stat the given fs_path */
    if ( stat( rpath, &pathstat ) != 0 )
    {
        DisplayLog( LVL_CRIT, "CheckFS",
                    "/!\\ ERROR /!\\ Couldn't stat '%s': %s", rpath, strerror( errno ) );
        endmntent( fp );
        return errno;
    }

    /* Stat upper level of mount point, to check if
     * the filesystem is mounted (device  must be different).
     * (dirname modifies string content, so we work on a copy
     * in tmp_buff).
     */
    strcpy( tmp_buff, mntdir );
    parentmntdir = dirname( tmp_buff );

    if ( lstat( parentmntdir, &parentmntstat ) != 0 )
    {
        DisplayLog( LVL_CRIT, "CheckFS",
                    "/!\\ ERROR /!\\ Couldn't stat %s: %s", parentmntdir, strerror( errno ) );
        endmntent( fp );
        return errno;
    }

    /* check that filesystem device is different from root (except if check_mounted is disabled) */
    if ( ( pathstat.st_dev == parentmntstat.st_dev ) && check_mounted )
    {
        DisplayLog( LVL_CRIT, "CheckFS",
                    "/!\\ ERROR /!\\ Filesystem '%s' is not mounted ! dev(%s)=dev(%s)=%#"
                    PRIx64, mntdir, parentmntdir, rpath, (uint64_t)parentmntstat.st_dev );
        endmntent( fp );
        return ENOENT;
    }

#ifdef _LUSTRE
    if (!strcmp(type, "lustre"))
    {
        char *ptr;
        ptr = strstr( fs_spec, ":/" );
        if ( ptr != NULL )
        {
            name = ptr + 2;
        }
        else
            name = fs_spec;
    }
    else
#endif
        name = fs_spec;

    /* all checks are OK */

    if ( save_fs )
    {
        int rc;

        /* getting filesystem fsid (needed for fskey) */
        if (global_config.fs_key == FSKEY_FSID)
        {
            struct statfs stf;
            if (statfs(mntdir, &stf))
            {
                int rc = -errno;
                DisplayLog( LVL_CRIT, "CheckFS", "ERROR calling statfs(%s): %s",
                    mntdir, strerror(-rc) );
                    return rc;
            }
            /* if fsid == 0, it may mean that fsid is not significant on the current system
             * => DISPLAY A WARNING */
            if (fsidto64(stf.f_fsid) == 0)
                DisplayLog(LVL_MAJOR, "CheckFS", "WARNING: fsid(0) doesn't look significant on this system."
                           "It should not be used as fs_key!");

            rc = set_fs_info(name, mntdir, pathstat.st_dev, stf.f_fsid);
        }
        else
        {
            fsid_t dummy_fsid;
            memset(&dummy_fsid, 0, sizeof(fsid_t));
            rc = set_fs_info(name, mntdir, pathstat.st_dev, dummy_fsid);
        }
        if (rc)
            return rc;
    }

    if ( p_fs_dev != NULL )
        *p_fs_dev = pathstat.st_dev;

    if ( fsname_out != NULL )
        strcpy(fsname_out, name);

    endmntent( fp );
    return 0;
}                               /* CheckFSInfo */

/**
 * Initialize filesystem access and retrieve current devid/fs_key
 * - global_config must be set
 * - initialize mount_point, fsname and dev_id
 */
int InitFS( void )
{
    static int initialized = FALSE;
    int rc;

    if (initialized)
        return 0;

    /* Initialize mount point info */
#ifdef _LUSTRE
    if (!strcmp( global_config.fs_type, "lustre" ))
    {
        if ( ( rc = Lustre_Init() ) )
        {
            DisplayLog( LVL_CRIT, "InitFS", "Error %d initializing liblustreapi", rc );
            return rc;
        }
    }
#endif

    rc = CheckFSInfo( global_config.fs_path, global_config.fs_type, NULL, NULL,
                      global_config.check_mounted, TRUE );
    if (rc)
    {
        DisplayLog( LVL_CRIT, "InitFS", "Error %d checking Filesystem", rc );
        return rc;
    }

    /* OK */
    initialized = TRUE;
    return 0;
}

/**
 * This is to be called after a dev_id change was detected
 * return 0 if fskey is unchanged and update mount_point, fsname and dev_id
 * else, return -1
 */
int ResetFS( void )
{
    char   name[RBH_PATH_MAX];
    dev_t  dev;
    struct statfs stf;
    int rc;
    /* check depending on FS key type:
     * - fsname: check mount tab
     * - fsid: check statfs
     * - devid: check dev_id
     */
    switch (global_config.fs_key)
    {
        case FSKEY_FSNAME:
            /* get and compare FS name */
            rc = CheckFSInfo( global_config.fs_path, global_config.fs_type, NULL, name,
                              global_config.check_mounted, FALSE );
            if (rc)
                return rc;
            /* did the name changed ? */
            if (strcmp(name, fsname))
            {
                DisplayLog(LVL_CRIT, "FSInfo", "fsname change detected: %s->%s",
                           fsname, name);
                RaiseAlert( "Filesystem changed",
                             "fsname of '%s' has changed !!! %s->%s => EXITING",
                            global_config.fs_path, fsname, name );
                return -1;
            }
            /* update fsid and devid */
            rc = CheckFSInfo( global_config.fs_path, global_config.fs_type, NULL, NULL,
                              global_config.check_mounted, TRUE );
            return rc;

        case FSKEY_FSID:
            /* get and compare FS ID */
            if (statfs(global_config.fs_path, &stf))
            {
                rc = -errno;
                DisplayLog( LVL_CRIT, "FSInfo", "ERROR calling statfs(%s): %s",
                    global_config.fs_path, strerror(-rc) );
                    return rc;
            }
            if (fsidto64(stf.f_fsid) != fs_key)
            {
                DisplayLog(LVL_CRIT, "FSInfo", "fsid change detected: %"PRIX64"->%"PRIX64,
                           fs_key, fsidto64(stf.f_fsid));
                RaiseAlert( "Filesystem changed",
                             "fsid of '%s' has changed !!! %"PRIX64"->%"PRIX64" => EXITING",
                            global_config.fs_path, fs_key, fsidto64(stf.f_fsid) );
                return -1;
            }
            /* update fsname and devid */
            rc = CheckFSInfo( global_config.fs_path, global_config.fs_type, NULL, NULL,
                              global_config.check_mounted, TRUE );
            return rc;

        case FSKEY_DEVID:
            /* get and compare dev id */
            rc = CheckFSInfo( global_config.fs_path, global_config.fs_type, &dev, NULL,
                              global_config.check_mounted, FALSE );
            if (rc)
                return rc;
            /* did the device change? */
            if (dev != dev_id)
            {
                DisplayLog(LVL_CRIT, "FSInfo", "devid change detected: %"PRI_DT"->%"PRI_DT,
                           dev_id, dev);

                RaiseAlert( "Filesystem changed",
                             "devid of '%s' has changed !!! %"PRI_DT"->%"PRI_DT" => EXITING",
                            global_config.fs_path, dev_id, dev );
                return -1;
            }
            /* update fsname and fsid */
            rc = CheckFSInfo( global_config.fs_path, global_config.fs_type, NULL, NULL,
                              global_config.check_mounted, TRUE );
            return rc;

        default:
            DisplayLog(LVL_MAJOR, "FSInfo", "Invalid fs_key type %#x", global_config.fs_key);
            return -1;
    }

}


/**
 *  Check that FS path is the same as the last time.
 */
int CheckLastFS( void )
{
    int            rc;
    lmgr_t         lmgr;
    char           value[1024];

    rc = ListMgr_InitAccess( &lmgr );
    if ( rc )
    {
        DisplayLog( LVL_CRIT, "CheckFS", "Error %d connecting to database", rc );
        return rc;
    }
    rc = ListMgr_GetVar( &lmgr, FS_PATH_VAR, value );
    if ( rc == DB_SUCCESS )
    {
        if ( strcmp( value, global_config.fs_path ) )
        {
            DisplayLog( LVL_CRIT, "CheckFS",
                        "Filesystem %s does not correspond to database content (%s)",
                        global_config.fs_path, value );
            DisplayLog( LVL_CRIT, "CheckFS", "Drop the database and restart the daemon." );
            rc = -1;
        }
        else
        {
            DisplayLog( LVL_DEBUG, "CheckFS", "%s matches database content.",
                        global_config.fs_path );
            rc = 0;
        }
    }
    else if ( rc == DB_NOT_EXISTS )
    {
        DisplayLog( LVL_FULL, "CheckFS", FS_PATH_VAR "='%s'.", global_config.fs_path );
        rc = ListMgr_SetVar( &lmgr, FS_PATH_VAR, global_config.fs_path );
        if ( rc )
            DisplayLog( LVL_CRIT, "CheckFS", "Error %d setting variable 'FS_path'", rc );
    }
    else
    {
        DisplayLog( LVL_CRIT, "CheckFS", "Error %d retrieving variable 'FS_path'", rc );
    }

    ListMgr_CloseAccess( &lmgr );
    return rc;
}

/* return 0 if thread has been killed,
 * -1 if thread was already terminated.
 */
int TerminateThread( pthread_t thread_id )
{
    if ( pthread_cancel( thread_id ) == ESRCH )
        return -1;

    return 0;
}


/* Format a size in a "human readable" format */

char          *FormatFileSize( char *buff, size_t str_sz, uint64_t file_size )
{
    if ( file_size < KILO_BYTE )
        snprintf( buff, str_sz, "%llu", (unsigned long long)file_size );
    else if ( file_size < MEGA_BYTE )
        snprintf( buff, str_sz, "%.2f KB", ( 0.0 + file_size ) / ( 0.0 + KILO_BYTE ) );
    else if ( file_size < GIGA_BYTE )
        snprintf( buff, str_sz, "%.2f MB", ( 0.0 + file_size ) / ( 0.0 + MEGA_BYTE ) );
    else if ( file_size < TERA_BYTE )
        snprintf( buff, str_sz, "%.2f GB", ( 0.0 + file_size ) / ( 0.0 + GIGA_BYTE ) );
    else if ( file_size < PETA_BYTE )
        snprintf( buff, str_sz, "%.2f TB", ( 0.0 + file_size ) / ( 0.0 + TERA_BYTE ) );
    else if ( file_size < EXA_BYTE )
        snprintf( buff, str_sz, "%.2f PB", ( 0.0 + file_size ) / ( 0.0 + PETA_BYTE ) );
    else
        snprintf( buff, str_sz, "%.2f EB", ( 0.0 + file_size ) / ( 0.0 + EXA_BYTE ) );

    return buff;

}

/* Format a duration (in seconds) to a string with days hours minutes seconds... */

char          *FormatDuration( char *buff, size_t str_sz, time_t duration )
{

    unsigned int   days;
    unsigned int   hours;
    unsigned int   minutes;
    unsigned int   secondes;

    size_t         written = 0;

    if ( duration < 0 )
        duration = -duration;

    days = ( unsigned int ) duration / DAY;
    hours = ( ( unsigned int ) duration % DAY ) / HOUR;
    minutes = ( ( unsigned int ) duration % HOUR ) / MINUTE;
    secondes = ( ( unsigned int ) duration % MINUTE );

    buff[0] = '\0';

    if ( days > 0 )
        written += snprintf( buff, str_sz, "%ud ", days );

    if ( hours > 0 )
        written += snprintf( buff + written, str_sz - written, "%uh ", hours );

    if ( minutes > 0 )
        written += snprintf( buff + written, str_sz - written, "%.2umin ", minutes );

    if ( secondes > 0 || duration == 0 )
        written += snprintf( buff + written, str_sz - written, "%.2us ", secondes );

    if ( ( written > 0 ) && ( buff[written - 1] == ' ' ) )
        buff[written - 1] = '\0';

    return buff;

}

/* Format a duration (in seconds) to a string with the best fitting unit (float value) */

char          *FormatDurationFloat( char *buff, size_t str_sz, time_t duration )
{
    float          days;
    float          hours;
    float          minutes;
    unsigned int   secondes;

    if ( duration < 0 )
        duration = -duration;

    days = ( float ) duration / DAY;
    hours = ( float ) duration / HOUR;
    minutes = ( float ) duration / MINUTE;
    secondes = ( unsigned int ) duration;

    buff[0] = '\0';

    if ( days >= 1.0 )
        snprintf( buff, str_sz, "%.1fd", days );
    else if ( hours >= 1.0 )
        snprintf( buff, str_sz, "%.1fh", hours );
    else if ( minutes >= 1.0 )
        snprintf( buff, str_sz, "%.1fmin", minutes );
    else
        snprintf( buff, str_sz, "%.2us", secondes );

    return buff;

}


/**
 * Convert a string to a long integer
 * @return -1 on error.
 */
long long str2bigint( const char *str )
{
    char           suffix[256];
    int            nb_read;
    long long      value;

    if ( str == NULL )
        return -1;

    nb_read = sscanf( str, "%lld%s", &value, suffix );

    if ( nb_read <= 0 )
        return -1;              /* invalid format */

    if ( ( nb_read == 1 ) || ( suffix[0] == '\0' ) )
        return value;           /* no suffix => 0K */
    else
        return -1;
}


/**
 * Convert a string to a boolean
 * @return -1 on error.
 */
int str2bool( const char *str )
{
    if ( str == NULL )
        return -1;

    if ( !strcmp( str, "1" ) || !strcasecmp( str, "TRUE" ) ||
         !strcasecmp( str, "YES" ) || !strcasecmp( str, "ENABLED" ) || !strcasecmp( str, "ON" ) )
        return 1;

    if ( !strcmp( str, "0" ) || !strcasecmp( str, "FALSE" ) ||
         !strcasecmp( str, "NO" ) || !strcasecmp( str, "DISABLED" ) || !strcasecmp( str, "OFF" ) )
        return 0;

    return -1;
}


/**
 * Convert a string to a duration in seconds
 * @return -1 on error.
 */
int str2duration( const char *str )
{
    int            nb_read, duration;
    char           suffix[256];

    if ( str == NULL )
        return -1;

    nb_read = sscanf( str, "%d%s", &duration, suffix );

    if ( nb_read <= 0 )
        return -1;              /* invalid format */

    if ( ( nb_read == 1 ) || ( suffix[0] == '\0' ) )
        return duration;        /* no suffix: duration in seconds */

    if ( !strcasecmp( suffix, "s" ) || !strcasecmp( suffix, "sec" ) )
        return duration;
    if ( !strcasecmp( suffix, "m" ) || !strcasecmp( suffix, "min" ) )
        return MINUTE * duration;
    if ( !strcasecmp( suffix, "h" ) || !strcasecmp( suffix, "hour" ) )
        return HOUR * duration;
    if ( !strcasecmp( suffix, "d" ) || !strcasecmp( suffix, "day" ) )
        return DAY * duration;
    if ( !strcasecmp( suffix, "w" ) || !strcasecmp( suffix, "week" ) )
        return WEEK * duration;
    if ( !strcasecmp( suffix, "y" ) || !strcasecmp( suffix, "year" ) )
        return YEAR * duration;

    return -1;
}


/**
 * Convert a string to a size (in bytes)
 * @return -1 on error.
 */
uint64_t str2size( const char *str )
{
    int                   nb_read;
    unsigned long long    size;
    char                  suffix[256];

    if ( str == NULL )
        return (uint64_t) -1LL;

    nb_read = sscanf( str, "%llu%s", &size, suffix );

#ifdef _DEBUG_PARSING
    printf( "nb_read = %d, str = %s, size = %llu, suffix = %s\n", nb_read, str, size, suffix );
#endif

    if ( nb_read <= 0 )
        return (uint64_t) -1LL;     /* invalid format */

    if ( ( nb_read == 1 ) || ( suffix[0] == '\0' ) )
        return size;            /* no suffix: size in bytes */

    if ( !strcasecmp( suffix, "B" ) )
        return size;
    if ( !strcasecmp( suffix, "kB" ) || !strcasecmp( suffix, "K" ) )
        return ( KILO_BYTE * size );
    if ( !strcasecmp( suffix, "MB" ) ||  !strcasecmp( suffix, "M" ) )
        return ( MEGA_BYTE * size );
    if ( !strcasecmp( suffix, "GB" ) ||  !strcasecmp( suffix, "G" ) )
        return ( GIGA_BYTE * size );
    if ( !strcasecmp( suffix, "TB" ) || !strcasecmp( suffix, "T" ))
        return ( TERA_BYTE * size );
    if ( !strcasecmp( suffix, "PB" ) || !strcasecmp( suffix, "P" ) )
        return ( PETA_BYTE * size );
    if ( !strcasecmp( suffix, "EB" ) || !strcasecmp( suffix, "E" ) )
        return ( EXA_BYTE * size );

    return (uint64_t) -1LL;
}

/**
 * extracts up to count digits from src string,
 * and copy them to dest string. dest is completed
 * with '\0'.
 * @return the number of digits copied to dest.
 */
static inline int extract_digits( const char * src, char * dest, unsigned int count )
{
    unsigned int i;
    unsigned int cpd = 0;
    for ( i = 0; (i < count) && (src[i] != '\0'); i++ )
    {
        dest[i] = src[i];
        cpd ++;
    }
    dest[cpd] = '\0';
    return cpd;
}

/** parse date/time yyyymmdd[HH[MM[SS]]] */
time_t str2date( const char *str )
{
    struct tm datetime = {
        .tm_sec = 0,
        .tm_min = 0,
        .tm_hour = 0,
        .tm_mday = 0,
        .tm_mon = 0,
        .tm_year = 0,
        .tm_wday = 0,
        .tm_yday = 0,
        .tm_isdst = -1
    };
    char tmpstr[16];
    int  tmpint;
    const char * curr = str;

    /* extract year */
    if (extract_digits(curr, tmpstr, 4) < 4)
        return (time_t)-1;
    curr += 4;
    if ((tmpint = str2int(tmpstr)) == -1)
        return (time_t)-1;
    datetime.tm_year = tmpint - 1900; /* 1900 => 0 */

    /* extract month */
    if (extract_digits(curr, tmpstr, 2) < 2)
        return (time_t)-1;
    curr += 2;
    if ((tmpint = str2int(tmpstr)) <= 0)
        return (time_t)-1;
    else if (tmpint > 12)
        return (time_t)-1;
    datetime.tm_mon = tmpint - 1; /* January => 0 */

    /* extract day */
    if (extract_digits(curr, tmpstr, 2) < 2)
        return (time_t)-1;
    curr += 2;
    if ((tmpint = str2int(tmpstr)) <= 0)
        return (time_t)-1;
    else if (tmpint > 31)
        return (time_t)-1;
    datetime.tm_mday = tmpint; /* 1st => 1 */

    /* extract hours */
    tmpint = extract_digits(curr, tmpstr, 2);
    if (tmpint == 0) /* not specified */
        goto convert;
    else if (tmpint < 2) /* invalid */
        return (time_t)-1;
    curr += 2;
    if ((tmpint = str2int(tmpstr)) == -1)
        return (time_t)-1;
    else if (tmpint > 23)
        return (time_t)-1;
    datetime.tm_hour = tmpint;

    /* extract minutes */
    tmpint = extract_digits(curr, tmpstr, 2);
    if (tmpint == 0) /* not specified */
        goto convert;
    else if (tmpint < 2) /* invalid */
        return (time_t)-1;
    curr += 2;
    if ((tmpint = str2int(tmpstr)) == -1)
        return (time_t)-1;
    else if (tmpint > 59)
        return (time_t)-1;
    datetime.tm_min = tmpint;

    /* extract seconds */
    tmpint = extract_digits(curr, tmpstr, 2);
    if (tmpint == 0) /* not specified */
        goto convert;
    else if (tmpint < 2) /* invalid */
        return (time_t)-1;
    curr += 2;
    if ((tmpint = str2int(tmpstr)) == -1)
        return (time_t)-1;
    else if (tmpint > 59)
        return (time_t)-1;
    datetime.tm_sec = tmpint;

    if (*curr != '\0')
        return (time_t)-1;

convert:
     return mktime(&datetime);
}

#define TYPEINDEX(mode) (((mode) >> 12) & 0x0f)
#define TYPECHAR(mode)  ("0pcCd?bB-?l?s???" [TYPEINDEX(mode)])

/* The special bits. If set, display SMODE0/1 instead of MODE0/1 */
static const mode_t SBIT[] = {
    0, 0, S_ISUID,
    0, 0, S_ISGID,
    0, 0, S_ISVTX
};

/* The 9 mode bits to test */
static const mode_t MBIT[] = {
    S_IRUSR, S_IWUSR, S_IXUSR,
    S_IRGRP, S_IWGRP, S_IXGRP,
    S_IROTH, S_IWOTH, S_IXOTH
};

static const char MODE1[]  = "rwxrwxrwx";
static const char MODE0[]  = "---------";
static const char SMODE1[] = "..s..s..t";
static const char SMODE0[] = "..S..S..T";

/*
 * Return the standard ls-like mode string from a file mode.
 * This is static and so is overwritten on each call.
 */
const char *mode_string(mode_t mode, char *buf)
{
    int i;

    for (i = 0; i < 9; i++) {
        if (mode & SBIT[i])
            buf[i] = (mode & MBIT[i]) ? SMODE1[i] : SMODE0[i];
        else
            buf[i] = (mode & MBIT[i]) ? MODE1[i] : MODE0[i];
    }
    return buf;
}


/**
 *  Print attributes to a string
 */
int PrintAttrs( char *out_str, size_t strsize, const attr_set_t * p_attr_set, int overide_mask, int brief )
{
    int            mask = p_attr_set->attr_mask;
    size_t         written = 0;
    char           tmpbuf[24576];
    const char *   format;

    if ( overide_mask )
        mask = mask & overide_mask;

    if ( mask & ATTR_MASK_fullpath )
    {
        if (brief)
            format = "path='%s',";
        else
            format = "Path:     \"%s\"\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      ATTR( p_attr_set, fullpath ) );
    }
    /* this information is redundant with fullpath,
     * so only display it if path is not known */
    else if ( mask & ATTR_MASK_name )
    {
        if (brief)
            format = "name='%s',";
        else
            format = "Name:     \"%s\"\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      ATTR( p_attr_set, name ) );
    }
    if ( mask & ATTR_MASK_parent_id )
    {
        if (brief)
            format = "parent="DFID",";
        else
            format = "Parent:   "DFID"\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      PFID(&ATTR(p_attr_set, parent_id)) );
    }
#ifdef ATTR_INDEX_type
    if ( mask & ATTR_MASK_type )
    {
        if (brief)
            format = "type=%s,";
        else
            format = "Type:     %s\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      ATTR( p_attr_set, type ) );
    }
#endif
    if (mask & ATTR_MASK_nlink)
    {
        if (brief)
            format = "nlink=%u,";
        else
            format = "Nlinks:   %u\n";
        written +=
            snprintf(out_str + written, strsize - written, format,
                      ATTR(p_attr_set, nlink));
    }

#ifdef ATTR_INDEX_mode
    if ( mask & ATTR_MASK_mode )
    {
        if (brief)
            format = "mode=%#o,";
        else
            format = "Mode:     %#o\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      ATTR( p_attr_set, mode ) );
    }
#endif

    if ( mask & ATTR_MASK_owner )
    {
        if (brief)
            format = "owner=%s,";
        else
            format = "Owner:    \"%s\"\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      ATTR( p_attr_set, owner ) );
    }
    if ( mask & ATTR_MASK_gr_name )
    {
        if (brief)
            format = "group=%s,";
        else
            format = "Group:    \"%s\"\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      ATTR( p_attr_set, gr_name ) );
    }
    if ( mask & ATTR_MASK_size )
    {
        if (brief)
        {
            written += snprintf( out_str + written, strsize - written, "size=%"PRIu64",",
                        ATTR( p_attr_set, size ));
        }
        else
        {
            FormatFileSize( tmpbuf, sizeof(tmpbuf), ATTR( p_attr_set, size ) );
            written += snprintf( out_str + written, strsize - written, "Size:     %s\n", tmpbuf );
        }
    }
    if ( mask & ATTR_MASK_blocks )
    {
        if (brief)
            format = "blocks=%Lu,";
        else
            format = "Blocks:   %Lu\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      ATTR( p_attr_set, blocks ) );
    }
    if ( mask & ATTR_MASK_depth )
    {
        if (brief)
            format = "depth=%u,";
        else
            format = "Depth:    %u\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      ATTR( p_attr_set, depth ) );
    }
#ifdef ATTR_INDEX_dircount
    if ( mask & ATTR_MASK_dircount )
    {
        if (brief)
            format = "dircount=%u,";
        else
            format = "DirCount: %u\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      ATTR( p_attr_set, dircount ) );
    }
#endif
    if ( mask & ATTR_MASK_last_access )
    {
        if (brief)
        {
            written +=
                snprintf( out_str + written, strsize - written, "access=%u,", ATTR( p_attr_set, last_access ));
        }
        else
        {
            FormatDurationFloat( tmpbuf, 256, time( NULL ) - ATTR( p_attr_set, last_access ) );
            written +=
                snprintf( out_str + written, strsize - written, "Last Access: %s ago\n", tmpbuf );
        }
    }
    if ( mask & ATTR_MASK_last_mod )
    {
        if (brief)
        {
            written += snprintf( out_str + written, strsize - written, "modif=%u,",
                    ATTR( p_attr_set, last_mod ));
        }
        else
        {
            FormatDurationFloat( tmpbuf, 256, time( NULL ) - ATTR( p_attr_set, last_mod ) );
            written += snprintf( out_str + written, strsize - written, "Last Mod: %s ago\n", tmpbuf );
        }
    }
#ifdef ATTR_INDEX_creation_time
    if ( mask & ATTR_MASK_creation_time )
    {
        if (brief)
        {
            written += snprintf( out_str + written, strsize - written, "creation=%lu,",
                                 (unsigned long)ATTR( p_attr_set, creation_time ));
        }
        else
        {
            FormatDurationFloat( tmpbuf, 256, time( NULL ) - ATTR( p_attr_set, creation_time ) );
            written += snprintf( out_str + written, strsize - written, "Creation: %s ago\n", tmpbuf );
        }
    }
#endif

#ifdef ATTR_INDEX_last_copy
    if ( mask & ATTR_MASK_last_copy )
    {
        if (brief)
        {
            written += snprintf( out_str + written, strsize - written, "copy=%u,",
                    ATTR( p_attr_set, last_copy ));
        }
        else
        {
            FormatDurationFloat( tmpbuf, 256, time( NULL ) - ATTR( p_attr_set, last_copy ) );
            written += snprintf( out_str + written, strsize - written, "Last Copy: %s ago\n", tmpbuf );
        }
    }
#endif

#ifdef _LUSTRE
    if ( mask & ATTR_MASK_stripe_items)
    {
        if (brief)
            format = "stripes={%s},";
        else
            format = "Stripes: %s\n";
        written +=
            snprintf( out_str + written, strsize - written, format,
                      FormatStripeList( tmpbuf, sizeof(tmpbuf), &ATTR( p_attr_set, stripe_items), brief));
    }

    if (mask & ATTR_MASK_stripe_info)
    {
        if (brief){
            if (!EMPTY_STRING(ATTR( p_attr_set, stripe_info).pool_name)) {
                format = "stripe_count=%u,stripe_size=%"PRIu64",ost_pool=%s,";
                written +=
                    snprintf( out_str + written, strsize - written, format,
                              ATTR( p_attr_set, stripe_info).stripe_count,
                              ATTR( p_attr_set, stripe_info).stripe_size,
                              ATTR( p_attr_set, stripe_info).pool_name );
            }
            else
            {
                format = "stripe_count=%u,stripe_size=%"PRIu64",";
                written +=
                    snprintf( out_str + written, strsize - written, format,
                              ATTR( p_attr_set, stripe_info).stripe_count,
                              ATTR( p_attr_set, stripe_info).stripe_size );
            }
        }
        else
        {
            FormatFileSize( tmpbuf, 256, ATTR( p_attr_set, stripe_info).stripe_size);
            if (!EMPTY_STRING(ATTR( p_attr_set, stripe_info).pool_name)) {
                format = "Stripe count: %u\n"
                         "Stripe size:  %s\n"
                         "OST pool:     %s\n";
                written +=
                    snprintf( out_str + written, strsize - written, format,
                              ATTR( p_attr_set, stripe_info).stripe_count, tmpbuf,
                              ATTR( p_attr_set, stripe_info).pool_name );
            }
            else
            {
                format = "Stripe count: %u\n"
                         "Stripe size:  %s\n";
                written +=
                    snprintf( out_str + written, strsize - written, format,
                              ATTR( p_attr_set, stripe_info).stripe_count,
                              tmpbuf);
            }
        }
    }
#endif

#ifdef ATTR_INDEX_status
    if ( mask & ATTR_MASK_status )
    {
        if (brief)
        {
            written +=
                snprintf(out_str + written, strsize - written, "status=%s,",
                         db_status2str(ATTR(p_attr_set, status), 1));
        }
        else
        {
            written +=
                snprintf( out_str + written, strsize - written, "Status:  %s\n",
                         db_status2str(ATTR(p_attr_set, status), 0));
        }
    }
#endif

#ifdef ATTR_INDEX_backendpath
    if ( mask & ATTR_MASK_backendpath )
    {
        if (brief)
        {
            written +=
                snprintf(out_str + written, strsize - written, "backendpath='%s',",
                         ATTR(p_attr_set, backendpath));
        }
        else
        {
            written +=
                snprintf( out_str + written, strsize - written, "Backend path: \"%s\"\n",
                         ATTR(p_attr_set, backendpath));
        }
    }
#endif

    if ( mask & ATTR_MASK_link )
    {
        if (brief)
        {
            written +=
                snprintf(out_str + written, strsize - written, "lnk='%s',",
                         ATTR(p_attr_set, link));
        }
        else
        {
            written +=
                snprintf( out_str + written, strsize - written, "link: \"%s\"\n",
                         ATTR(p_attr_set, link));
        }
    }


    if (brief && written) {
        /* remove final , */
        out_str[written-1] = '\0';
        written --;
    }

    return written;
}

/* helpers for attr change */
#define APPLYTAG "ChgAttr"
#define LOG_ATTR_CHANGE(_nfunc, _arg_fmt, _dr, _rc, ...) do { \
            if (_rc)                                          \
                DisplayLog(LVL_CRIT, APPLYTAG, "%s("_arg_fmt") failed: %s", _nfunc, __VA_ARGS__, strerror(_rc)); \
            else                                              \
                DisplayReport("%s%s("_arg_fmt")", _dr?"(dry-run) ":"", _nfunc, __VA_ARGS__ ); \
        } while(0)

/**
 *  Apply attribute changes
 *  \param change_mask mask of attributes to be changed
 */
int            ApplyAttrs(const entry_id_t *p_id, const attr_set_t * p_attr_new,
                          const attr_set_t * p_attr_old,
                          int change_mask, int dry_run)
{
    int  mask = p_attr_new->attr_mask & change_mask;
    int rc, err = 0;
    const char *chattr_path = NULL;
#ifdef _HAVE_FID
    char fid_path[RBH_PATH_MAX];
#endif

    if (!mask)
        return 0;

    if (!ATTR_MASK_TEST(p_attr_new, fullpath))
    {
#ifdef _HAVE_FID
        /* build fid path */
        BuildFidPath( p_id, fid_path );
        chattr_path = fid_path;
#else
        DisplayLog(LVL_CRIT, APPLYTAG, "No path: cannot apply changes to entry");
        return -EINVAL;
#endif
    }
    else
        chattr_path = ATTR(p_attr_new, fullpath);

    if ( mask & ATTR_MASK_fullpath )
    {
        if (!ATTR_MASK_TEST(p_attr_old, fullpath))
        {
            DisplayLog(LVL_CRIT, APPLYTAG, "Cannot rename: source path is unknown");
            err++;
        }
        else
        {
            if (!dry_run && rename(ATTR(p_attr_old,fullpath), ATTR(p_attr_new, fullpath)))
               rc = errno;
            else
               rc = 0;

            LOG_ATTR_CHANGE("rename", "%s, %s", dry_run, rc,
                            ATTR(p_attr_old,fullpath), ATTR(p_attr_new, fullpath));
        }
    }
    else if ( mask & ATTR_MASK_parent_id )
    {
        /* can't change parent without changing path!!! */
    }
    else if ( mask & ATTR_MASK_name )
    {
        /* just change name */
    }

#ifdef ATTR_INDEX_type
    if ( mask & ATTR_MASK_type )
    {
        /* can't change entry type without creating/removing it */
    }
#endif
    if ( mask & (ATTR_MASK_owner | ATTR_MASK_gr_name))
    {
        uid_t u = -1;
        gid_t g = -1;

        if (mask & ATTR_MASK_owner)
        {
            struct passwd p;
            char buf[4096];
            struct passwd *res = NULL;

            rc = getpwnam_r(ATTR(p_attr_new, owner), &p, buf, 4096,
                           &res);
            if (rc == 0 && res != NULL)
                u = res->pw_uid;
        }

        if (mask & ATTR_MASK_gr_name)
        {
            struct group gs;
            char buf[4096];
            struct group *res = NULL;

            rc = getgrnam_r(ATTR(p_attr_new, gr_name), &gs, buf, 4096,
                            &res);
            if (rc == 0 && res != NULL)
                g = res->gr_gid;
        }

        if (u != -1 || g != -1)
        {

            if (!dry_run && lchown(chattr_path, u, g))
               rc = errno;
            else
               rc = 0;

            LOG_ATTR_CHANGE("lchown", "%s, u=%d, g=%d", dry_run, rc,
                            chattr_path, u, g);
        }
    }

#ifdef ATTR_INDEX_mode
    /* always set mode after chown, as it can be changed by chown */
    if ( mask & ATTR_MASK_mode )
    {

        if (!dry_run && chmod(chattr_path,  ATTR(p_attr_new, mode)))
           rc = errno;
        else
           rc = 0;

        LOG_ATTR_CHANGE("chmod", "%s, %#o", dry_run, rc,
                        chattr_path,  ATTR(p_attr_new, mode));
    }
#endif


#ifdef _LUSTRE
    if ( mask & ATTR_MASK_stripe_items)
    {
    }

    if (mask & ATTR_MASK_stripe_info)
    {
    }
#endif

#ifdef ATTR_INDEX_status
    if ( mask & ATTR_MASK_status )
    {
    }
#endif


    if ( mask & ATTR_MASK_size )
    {
        /* if new size is zero: truncate.
         * else, we have no idea of what's in the file...
         */
    }
    if (mask & (ATTR_MASK_last_access | ATTR_MASK_last_mod))
    {
        struct utimbuf t = {
            .actime = -1,
            .modtime = -1
        };
        int get_stat = 0;

        if (mask & ATTR_MASK_last_access)
            t.actime = ATTR(p_attr_new, last_access);
        if (mask & ATTR_MASK_last_mod)
            t.modtime = ATTR(p_attr_new, last_mod);

        /* if there is still a value == -1, we must fill it
         * or utime will set a bad value
         */
        if (t.actime == -1)
        {
            if (ATTR_MASK_TEST(p_attr_old, last_access))
                t.actime = ATTR(p_attr_old, last_access);
            else
                /* need to get old value of atime */
                get_stat = 1;
        }
        if (t.modtime == -1)
        {
            if (ATTR_MASK_TEST(p_attr_old, last_mod))
                t.modtime = ATTR(p_attr_old, last_mod);
            else
                /* need to get old value of atime */
                get_stat = 1;
        }
        if (get_stat)
        {
            struct stat st;
            if (lstat(chattr_path, &st) == 0)
            {
                if (t.modtime == -1)
                    t.modtime = st.st_mtime;
                if (t.actime == -1)
                    t.actime = st.st_atime;
            }
        }

        if (!dry_run && utime(chattr_path, &t))
           rc = errno;
        else
           rc = 0;

        LOG_ATTR_CHANGE("utime", "%s, a=%ld, m=%ld", dry_run, rc,
                        chattr_path, t.actime, t.modtime);
    }

    return err;
}



/** Compute greatest common divisor (GCD) of 2 numbers */
unsigned int gcd( unsigned int x, unsigned int y )
{
    unsigned int   a = x;
    unsigned int   b = y;
    while ( ( a * b ) != 0 )
    {
        if ( a > b )
            a = a - b;
        if ( a < b )
            b = b - a;
        if ( a == b )
            b = 0;
    }
    return a;
}

/** Ensure that the thread is suspended for a given amount
 * of time, event if the process gets interrupts.
 */
void rh_sleep( unsigned int seconds )
{
   time_t start = time(NULL);
   int remain = seconds;
   int spent;

   while ( remain > 0 )
   {
       remain = sleep( remain );
       if ( remain <= 0 )
       {
           spent = time(NULL)-start;
           if ( spent < seconds )
               remain = seconds - spent;
       }
   }
}

/** replace a pattern in a string with another sub-string
 * \param str_in_out must be large enough to receive
 *  the resulting string, and cannot exceed 1024.
 */
int str_replace( char * str_in_out, const char * to_be_replaced,
                 const char * replacement )
{
    size_t len_from = strlen(to_be_replaced);
    size_t len_to = strlen(replacement);
    char * curr;

    /* - same size: replace inline
     * - smaller string size: replace inline then shift
     */

    for ( curr = strstr( str_in_out, to_be_replaced );
          curr != NULL;
          curr = strstr( curr, to_be_replaced ) )
    {
        unsigned int i;
        char * curr_src;
        char tmp_buff[1024];

        /* if replacement is longer, save end of line */
        if ( len_to > len_from )
            strcpy(tmp_buff, curr+len_from);

        for ( i=0; i<len_to; i++ )
            curr[i]=replacement[i];
        curr = curr + len_to;

        /* if replacement is smaller, need to shift */
        if ( len_to < len_from )
        {
            if ( ((char*)(curr + len_from - len_to))[0] == '\0' )
                curr[0]='\0';

            /* shift of len_from - len_to*/
            for ( curr_src = curr + len_from - len_to, i=0;
                  *curr_src != '\0';
                  curr_src ++, i++ )
            {
                curr[i] = *curr_src;
                if ( curr_src[1] == '\0' )
                    curr[i+1] = '\0';
            }
        }
        else if ( len_to > len_from )
            /* copy saved data */
            strcpy(curr, tmp_buff);
    }
    return 0;
}



/**
 * extract relative path from full path
 */
int relative_path( const char * fullpath, const char * root, char * rel_path )
{
    size_t len;
    char rootcopy[1024];

    if (!strcmp(root, fullpath))
    {
        /* arg is root */
        rel_path[0]='\0';
        return 0;
    }

    /* copy root path */
    strcpy(rootcopy, root);
    len = strlen(rootcopy);

    /* add '/' if needed */
    if ( (len > 1) && (rootcopy[len-1] != '/') )
    {
        rootcopy[len] = '/';
        rootcopy[len+1] = '\0';
        len++;
    }

    /* test if the full path starts with the same dirs */
    if (strncmp(rootcopy, fullpath,len))
    {
        DisplayLog(LVL_MAJOR, "RelPath", "ERROR: file path '%s' is not under filesystem root '%s'",
                   fullpath, rootcopy);
        return -EINVAL;
    }

    strcpy( rel_path, fullpath+len );
    return 0;
}

/**
 * Put a string into double quotes and escape double quotes
 */
static char * escape_shell_arg( const char * in, char * out )
{
    char * curr_out = out;
    const char * curr_in = in;
    curr_out[0] = '"';
    curr_out++;

    while (*curr_in)
    {
        if (*curr_in != '"')
        {
            *curr_out = *curr_in;
            curr_out++;
        }
        else
        {
            curr_out[0] = '\\';
            curr_out[1] = '"';
            curr_out+=2;
        }
        curr_in++;
    }
    curr_out[0]='"';
    curr_out[1]='\0';

    return out;
}

int execute_shell_command(int quiet, const char * cmd, int argc, ...)
{
#define SHCMD "ShCmd"
    static const char quiet_string[]  = ">/dev/null 2>/dev/null";
    char argbuf[4096];
    va_list arglist;
    char *cmdline;
    char *curr;
    int rc, i;
    int exrc;
    size_t s = 0;

    /* count needed bytes */
    s = strlen(cmd);
    va_start(arglist, argc);
    for (i = 0; i < argc; i++)
        s += 2*strlen(va_arg(arglist, char *)) + 3; /* 2N+2 for escaping, +1 for white space */
    va_end(arglist);
    s += sizeof(quiet_string) + 1; /* +1 for '\0' */

    cmdline = MemAlloc(s);
    if (cmdline == NULL)
        return -ENOMEM;

    curr = cmdline;
    curr += sprintf(cmdline, "%s", cmd);

    va_start(arglist, argc);
    for (i = 0; i < argc; i++)
        curr += sprintf(curr, " %s",
                        escape_shell_arg(va_arg(arglist, char *), argbuf));
    va_end(arglist);
    if (quiet)
        curr += sprintf(curr, " %s", quiet_string);

    DisplayLog(LVL_DEBUG, SHCMD, "Executing command: %s", cmdline);
    rc = system(cmdline);

    if (WIFEXITED(rc))
    {
        const char *str_error;
        exrc = WEXITSTATUS(rc);
        if (exrc == 0)
        {
            DisplayLog(LVL_DEBUG, SHCMD, "Command successful");
            rc = 0;
            goto outfree;
        }

        /* shell special return values */
        if (exrc == 126)
            str_error = "permission problem or command is not an executable";
        else if (exrc == 127)
            str_error = "command not found";
        else if (exrc == 128)
            str_error = "invalid argument to exit";
        else
            str_error = "external command exited";

        DisplayLog( LVL_CRIT, SHCMD,
                    "ERROR: %s, error %d (cmdline=%s)",
                    str_error, (signed char)exrc, cmdline );
        rc = -exrc;
    }
    else if (WIFSIGNALED(rc))
    {
            DisplayLog( LVL_CRIT, SHCMD,
                        "ERROR: command terminated by signal %d. cmdline=%s",
                        WTERMSIG(rc), cmdline );
            rc = -EINTR;
    }

outfree:
    MemFree(cmdline);
    return rc;
}

char *quote_shell_arg(const char *arg)
{
    const char *replace_with = "'\\''";
    char *arg_walk, *quoted, *quoted_walk;
    int count = 0;

    arg_walk = (char *) arg;
    while (*arg_walk) {
        if (*arg_walk == '\'') {
            ++count;
            if (count < 0) {
                /* It's unlikely given our input, but avoid integer overflow. */
                return NULL;
            }
        }
        ++arg_walk;
    }

    quoted = (char *)calloc(1, strlen(arg) +
                            (count * strlen(replace_with)) + 2 + 1);
    if (!quoted)
        return NULL;

    quoted_walk = quoted;
    *quoted_walk = '\'';
    ++quoted_walk;

    arg_walk = (char *) arg;
    while (*arg_walk) {
        if (*arg_walk == '\'') {
            strcat(quoted_walk, replace_with);
            quoted_walk += strlen(replace_with);
        } else {
            *quoted_walk = *arg_walk;
            ++quoted_walk;
        }
        ++arg_walk;
    }

    *quoted_walk = '\'';
    ++quoted_walk;
    *quoted_walk = '\0';

    return quoted;
}

/**
 * Replace special parameters {cfgfile}, {fspath}, ...
 * in the given cmd line.
 * Result string is allocated using malloc()
 * and must be released using free().
 * \param replace_array char** of param1, value1, param2, value2, ..., NULL, NULL
 */
char *replace_cmd_parameters(const char *cmd_in, const char **replace_array)
{
#define CMDPARAMS "CmdParams"
    int i;
    char *pass_begin = NULL;
    char *begin_var;
    char *end_var;
    char *quoted_arg;
    const char *var_value;

    pass_begin = strdup(cmd_in);
    if (!pass_begin)
        return NULL;

    do
    {
        char * new_str = NULL;

        /* look for a variable */
        begin_var = strchr( pass_begin, '{' );

        /* no more variables */
        if ( !begin_var )
            break;

        *begin_var = '\0';
        begin_var++;

        /* get matching '}' */
        end_var = strchr( begin_var, '}' );
        if (!end_var)
        {
            DisplayLog(LVL_CRIT,CMDPARAMS, "ERROR: unmatched '{' in command parameters '%s'", cmd_in);
            free(pass_begin);
            errno = EINVAL;
            return NULL;
        }

        *end_var = '\0';
        end_var++;

        var_value = NULL;

        /* compute final length, depending on variable name */
        for (i = 0; replace_array[i] != NULL; i += 2)
        {
            const char *param = replace_array[i];
            const char *value = replace_array[i+1];

            if (!strcasecmp(begin_var, param))
            {
                var_value = value;
                break;
            }
        }

        if (var_value == NULL)
        {
            DisplayLog(LVL_CRIT,CMDPARAMS, "ERROR: unknown parameter '%s' in command parameters '%s'", begin_var, cmd_in);
            errno = EINVAL;
            goto err_free;
        }

        quoted_arg = quote_shell_arg(var_value);
        if (!quoted_arg)
            goto err_free;

        /* allocate a new string */
        new_str = malloc(strlen(pass_begin) + strlen(quoted_arg) +
                         strlen(end_var) + 1);
        if (!new_str)
            goto err_free_quoted;

        sprintf(new_str, "%s%s%s", pass_begin, quoted_arg, end_var);

        free(pass_begin);
        free(quoted_arg);
        pass_begin = new_str;

    } while(1);

    return pass_begin;

err_free_quoted:
    free(quoted_arg);
err_free:
    free(pass_begin);
    return NULL;
}

void upperstr(char *str)
{
    int i = 0;

    for(i = 0; str[i]; i++)
       str[i] = toupper(str[i]);
}


#ifdef ATTR_INDEX_status
/* ===  status display and conversion routines === */

/* status conversion array */
struct status_descr
{
    file_status_t db_status;
    char * short_descr;
}
status_array[] =
{
#ifdef _LUSTRE_HSM
    { STATUS_UNKNOWN, "n/a" },
    { STATUS_NEW, "new" },
    { STATUS_MODIFIED, "modified" },
    { STATUS_RESTORE_RUNNING, "retrieving" },
    { STATUS_ARCHIVE_RUNNING, "archiving" },
    { STATUS_SYNCHRO, "synchro" },
    { STATUS_RELEASED, "released" },
    { STATUS_RELEASE_PENDING, "release_pending" },

    /* alternative names */
    { STATUS_UNKNOWN, "unknown" },
    { STATUS_MODIFIED, "dirty" },
    { STATUS_RESTORE_RUNNING, "restoring" },

#define ALLOWED_STATUS "unknown, new, modified|dirty, retrieving|restoring, archiving, synchro, released, release_pending"

#elif defined(_HSM_LITE)
    { STATUS_UNKNOWN, "n/a" },
    { STATUS_NEW, "new" },
    { STATUS_MODIFIED, "modified" },
    { STATUS_RESTORE_RUNNING, "retrieving" },
    { STATUS_ARCHIVE_RUNNING, "archiving" },
    { STATUS_SYNCHRO, "synchro" },
    { STATUS_RELEASED, "released" },
    { STATUS_RELEASE_PENDING, "release_pending" },
    { STATUS_REMOVED, "removed" },

    /* alternative names */
    { STATUS_UNKNOWN, "unknown" },
    { STATUS_MODIFIED, "dirty" },
    { STATUS_RESTORE_RUNNING, "restoring" },

#define ALLOWED_STATUS "unknown, new, modified|dirty, retrieving|restoring, archiving, synchro, removed, released, release_pending"

#endif
    { (file_status_t)-1, NULL }
};

const char * db_status2str( file_status_t status, int csv )
{
    struct status_descr * curr;

    for ( curr = status_array; curr->short_descr != NULL; curr ++ )
    {
       if ( status == curr->db_status )
       {
           return curr->short_descr;
       }
    }
    /* not found */
    return "?";
}

file_status_t status2dbval( char * status_str )
{
    struct status_descr * curr;
    int len;

    if (  (status_str == NULL) || (status_str[0] == '\0') )
        return (file_status_t)-1;

    len = strlen( status_str );

    for ( curr = status_array; curr->short_descr != NULL; curr ++ )
    {
       if ( !strncmp( status_str, curr->short_descr, len ) )
            return curr->db_status;
    }
    /* not found */
    return (file_status_t)-1;
}

const char * allowed_status()
{
    return ALLOWED_STATUS;
}

#endif /* status attr exists */

static int path2id(const char *path, entry_id_t *id)
{
    int rc;
#ifdef _HAVE_FID
    rc = Lustre_GetFidFromPath( path, id );
    if (rc)
        return rc;
#else
    struct stat st;
    if (lstat(path, &st))
    {
        rc = -errno;
        DisplayLog( LVL_CRIT,"path2id", "ERROR: cannot stat '%s': %s",
                    path, strerror(-rc) );
        return rc;
    }
    /* build id from dev/inode*/
    id->inode = st.st_ino;
    id->fs_key = get_fskey();
    id->validator = st.st_ctime;
#endif
    return 0;
}

#define MKDIR_TAG "MkDir"
int mkdir_recurse(const char * full_path, mode_t mode, entry_id_t *dir_id)
{
    char path_copy[MAXPATHLEN];
    const char * curr;
    int rc;
    int exists = 0;

    if ( strncmp(global_config.fs_path, full_path, strlen(global_config.fs_path)) != 0 )
    {
        DisplayLog( LVL_MAJOR, MKDIR_TAG, "Error: '%s' in not under filesystem root '%s'",
                    full_path, global_config.fs_path );
        return -EINVAL;
    }
    /* skip fs root */
    curr = full_path + strlen(global_config.fs_path);

    if ( *curr == '\0' ) /* full_path is root dir */
    {
        exists = 1;
        goto get_id;
    }
    else if ( *curr != '/' ) /* slash expected */
    {
        DisplayLog( LVL_MAJOR, MKDIR_TAG, "Error: '%s' in not under filesystem root '%s'",
                    full_path, global_config.fs_path );
        return -EINVAL;
    }

    /* skip first slash */
    curr ++;

    while( (curr = strchr( curr, '/' )) != NULL )
    {
         /* if fullpath = '/a/b',
         * curr = &(fullpath[2]);
         * so, copy 2 chars to get '/a'.
         * and set fullpath[2] = '\0'
         */
        int path_len = curr - full_path;

        /* extract directory name */
        rh_strncpy(path_copy, full_path, path_len);
        path_copy[path_len]='\0';

        DisplayLog(LVL_FULL, MKDIR_TAG, "mkdir(%s)", path_copy );
        if ( (mkdir( path_copy, mode ) != 0) && (errno != EEXIST) )
        {
            rc = -errno;
            DisplayLog( LVL_CRIT, MKDIR_TAG, "mkdir(%s) failed: %s",
                        path_copy, strerror(-rc) );
            return rc;
        }

        curr++;
    }

    /* finaly create last level of dir */
    DisplayLog(LVL_FULL, MKDIR_TAG, "mkdir(%s)", full_path );
    if ( (mkdir( full_path, mode ) != 0) && (errno != EEXIST) )
    {
        rc = -errno;
        DisplayLog( LVL_CRIT, MKDIR_TAG, "mkdir(%s) failed: %s", full_path, strerror(-rc) );
        return rc;
    }
    else if (errno == EEXIST)
        exists = 1;

get_id:
    /* must return directory id */
    if (dir_id)
    {
        rc = path2id(full_path, dir_id);
        if (rc)
            return rc;
    }

    if (exists)
        return -EEXIST;
    else
        return 0;
}

/* create parent directory, and return its id (even if it already exists) */
static inline int create_parent_of(const char * child_path, entry_id_t * p_parent_id)
{
    char tmp[RBH_PATH_MAX];
    char * destdir;

    /* copy to tmp buffer as dirname modifies its argument */
    strcpy( tmp, child_path );
    /* extract parent dir path */
    destdir = dirname( tmp );
    if (destdir == NULL)
    {
        DisplayLog( LVL_CRIT, MKDIR_TAG, "Error extracting directory path of '%s'",
                    child_path );
        return -EINVAL;
    }

    /* create the directory */
    return mkdir_recurse(destdir, 0750, p_parent_id);
}


#define CREAT_TAG "Create"
/* create an object with the given attributes */
int create_from_attrs(const attr_set_t * attrs_in,
                      attr_set_t * attrs_out,
                      entry_id_t *new_id, int overwrite, int setstripe)
{
    char link[RBH_PATH_MAX] = "";
    const char * fspath;
    int rc;
    struct stat st_dest;
    int fd;
    mode_t mode_create = 0;
    int set_mode = FALSE;

    if (!ATTR_MASK_TEST( attrs_in, fullpath ) || !ATTR_MASK_TEST(attrs_in, type))
    {
        DisplayLog( LVL_MAJOR, CREAT_TAG, "Missing mandatory attribute to create entry");
        return -EINVAL;
    }
    fspath = ATTR(attrs_in, fullpath);

    /* initialize out attrs */
    ATTR_MASK_INIT( attrs_out );

    /* first create parent and retrieve parent id */
    rc = create_parent_of(fspath, &ATTR(attrs_out, parent_id));
    if (rc != 0 && rc != -EEXIST)
        return rc;
    else
        ATTR_MASK_SET(attrs_out, parent_id);

    if (!strcasecmp(ATTR(attrs_in, type), STR_TYPE_DIR))
    {
        /* entry is a directory */
        if (ATTR_MASK_TEST(attrs_in,mode))
            mode_create = ATTR(attrs_in, mode);
        else
            mode_create = 750;

        /* then create the directory itself */
        rc = mkdir(fspath, mode_create)?-errno:0;
        if (rc != 0 && rc != -EEXIST)
            return rc;
        else if (rc == -EEXIST)
            set_mode = TRUE;
    }
    else if (!strcasecmp(ATTR(attrs_in, type), STR_TYPE_LINK))
    {
        /* entry is a symlink */

        if (!ATTR_MASK_TEST(attrs_in, link))
        {
            DisplayLog( LVL_MAJOR, CREAT_TAG, "Missing mandatory attribute 'link' to create link");
            return -EINVAL;
        }

        if ( symlink(ATTR(attrs_in, link), fspath) != 0 )
        {
            rc = -errno;
            DisplayLog( LVL_MAJOR,  CREAT_TAG, "Error creating symlink %s->\"%s\" in filesystem: %s",
                        fspath, link, strerror(-rc) );
            return rc;
        }
        /* can't set mode on a symlink */
    }
    else if (!strcasecmp(ATTR(attrs_in, type), STR_TYPE_FILE))
    {
        int created = FALSE;

        if (ATTR_MASK_TEST(attrs_in, mode))
            mode_create = ATTR(attrs_in, mode);
        else
            mode_create = 0640; /* default */

#ifdef _LUSTRE
        if (setstripe)
        {
            /* create the file with the appropriate stripe in Lustre */
            if (ATTR_MASK_TEST(attrs_in, stripe_info))
            {
                rc = CreateStriped( fspath, &ATTR(attrs_in, stripe_info), overwrite );
                if (rc == 0 || rc == -EEXIST)
                {
                    created = TRUE;
                    set_mode= TRUE;
                }
                else
                    DisplayLog(LVL_MAJOR, CREAT_TAG, "setstripe failed: trying to create file with default striping");
            }
        }
        else
        {
            /* create with no stripe */
            rc = CreateWithoutStripe( fspath, mode_create & 07777, overwrite );
            if (rc == 0)
            {
                created = TRUE;
                set_mode = FALSE;
            }
            else if (rc == -EEXIST)
            {
                created = TRUE;
                set_mode = TRUE;
            }
            else
                DisplayLog(LVL_MAJOR, CREAT_TAG, "create(O_LOV_DELAY_CREATE) failed: trying to create file with default striping");
        }
#endif
        if (!created)
        {
            fd = creat(fspath, mode_create & 07777);
            if (fd < 0)
            {
                rc = -errno;
                DisplayLog( LVL_CRIT, CREAT_TAG, "ERROR: couldn't create '%s': %s",
                            fspath, strerror(-rc) );
                return rc;
            }
            else
                close(fd);
        }

        /* set times */
        if (ATTR_MASK_TEST(attrs_in, last_mod))
        {
            struct utimbuf utb;
            utb.modtime = ATTR(attrs_in, last_mod);

            if (ATTR_MASK_TEST(attrs_in, last_access))
                utb.actime = ATTR(attrs_in, last_access);
            else
                utb.actime = utb.modtime;

            /* set the same mtime as in the DB */
            DisplayLog( LVL_FULL, CREAT_TAG, "Restoring times for '%s': atime=%lu, mtime=%lu",
                        fspath, utb.actime, utb.modtime );
            if ( utime( fspath, &utb ) )
                DisplayLog( LVL_MAJOR, CREAT_TAG, "Warning: couldn't restore times for '%s': %s",
                            fspath, strerror(errno) );
        }
    }
    else
    {
        /* type not supported */
        DisplayLog( LVL_CRIT, CREAT_TAG, "Error: cannot restore entries of type '%s' (%s)",
                    ATTR(attrs_in,type), fspath );
        return -ENOTSUP;
    }

    /* set owner, group */
    if ( ATTR_MASK_TEST( attrs_in, owner ) || ATTR_MASK_TEST( attrs_in, gr_name ) )
    {
        uid_t uid = -1;
        gid_t gid = -1;
        char buff[4096];

        if ( ATTR_MASK_TEST( attrs_in, owner ) )
        {
            struct passwd pw;
            struct passwd * p_pw;

            if ((getpwnam_r( ATTR(attrs_in, owner ), &pw, buff, 4096, &p_pw ) != 0)
                 || (p_pw == NULL))
            {
                DisplayLog( LVL_MAJOR, CREAT_TAG, "Warning: couldn't resolve uid for user '%s'",
                            ATTR(attrs_in, owner ));
                uid = -1;
            }
            else
                uid = p_pw->pw_uid;
        }

        if ( ATTR_MASK_TEST( attrs_in, gr_name ) )
        {
            struct group gr;
            struct group * p_gr;
            if ((getgrnam_r( ATTR(attrs_in, gr_name ), &gr, buff, 4096, &p_gr ) != 0)
                 || (p_gr == NULL))
            {
                DisplayLog( LVL_MAJOR, CREAT_TAG, "Warning: couldn't resolve gid for group '%s'",
                            ATTR(attrs_in, gr_name ) );
                gid = -1;
            }
            else
                gid = p_gr->gr_gid;
        }

        DisplayLog( LVL_FULL, CREAT_TAG, "Restoring owner/group for '%s': uid=%u, gid=%u",
                    fspath, uid, gid );

        if ( lchown( fspath, uid, gid ) )
        {
            rc = -errno;
            DisplayLog( LVL_MAJOR, CREAT_TAG, "Warning: cannot set owner/group for '%s': %s",
                        fspath, strerror(-rc) );
        }
        else
        {
            /* According to chown(2) manual: chown may clear sticky bits even if root does it,
             * so, we must set the mode again if it contains special bits */
            if (!set_mode && (mode_create & 07000))
                set_mode = TRUE;
        }
    }

    if (set_mode)
    {
        /* set the same mode as in the backend */
        DisplayLog( LVL_FULL, CREAT_TAG, "Restoring mode for '%s': mode=%#o",
                    fspath, mode_create & 07777 );
        if ( chmod( fspath, mode_create & 07777 ) )
            DisplayLog( LVL_MAJOR, CREAT_TAG, "Warning: couldn't restore mode for '%s': %s",
                        fspath, strerror(errno) );
    }

    if ( lstat( fspath, &st_dest ) )
    {
        rc = -errno;
        DisplayLog( LVL_CRIT, CREAT_TAG, "ERROR: lstat() failed on restored entry '%s': %s",
                    fspath, strerror(-rc) );
        return rc;
    }

#ifdef _HAVE_FID
    /* get the new fid */
    rc = Lustre_GetFidFromPath( fspath, new_id );
    if (rc)
        return rc;
#else
    /* build id from dev/inode*/
    new_id->inode =  st_dest.st_ino;
    new_id->fs_key = get_fskey();
    new_id->validator =  st_dest.st_ctime;
#endif

    /* update with the new attributes */
    PosixStat2EntryAttr(&st_dest, attrs_out, TRUE);

    /* copy missing info: path, name, link, ...*/
    strcpy( ATTR( attrs_out, fullpath ), fspath );
    ATTR_MASK_SET( attrs_out, fullpath );

    char *name = strrchr(fspath, '/');
    if (name)
    {
        name++;
        strcpy(ATTR(attrs_out, name), name);
        ATTR_MASK_SET(attrs_out, name);
    }
    ATTR(attrs_out, path_update) = time(NULL);
    ATTR_MASK_SET(attrs_out, path_update);
    ATTR(attrs_out, md_update) = time(NULL);
    ATTR_MASK_SET(attrs_out, md_update);

    if (S_ISLNK(st_dest.st_mode))
    {
        strcpy(ATTR(attrs_out,link), link);
        ATTR_MASK_SET(attrs_out, link);
    }

#ifdef _LUSTRE
    /* get new stripe */
    if (S_ISREG(st_dest.st_mode))
    {
        /* get the new stripe info */
        if ( File_GetStripeByPath( fspath,
                                   &ATTR( attrs_out, stripe_info ),
                                   &ATTR( attrs_out, stripe_items ) ) == 0 )
        {
            ATTR_MASK_SET( attrs_out, stripe_info );
            ATTR_MASK_SET( attrs_out, stripe_items );
        }
    }
#endif
    return 0;
}

#ifdef _HAVE_FID
void path_check_update(const entry_id_t *p_id,
                       const char *fid_path, attr_set_t *p_attrs,
                       int attr_mask)
{
    int rc;
    if (attr_mask & (ATTR_MASK_name | ATTR_MASK_parent_id))
    {
        rc = Lustre_GetNameParent(fid_path, 0, &ATTR(p_attrs, parent_id),
                                  ATTR(p_attrs, name), RBH_NAME_MAX);
        if (rc == 0)
        {
            ATTR_MASK_SET(p_attrs, name);
            ATTR_MASK_SET(p_attrs, parent_id);
            /* update path refresh time */
            ATTR_MASK_SET(p_attrs, path_update);
            ATTR(p_attrs, path_update) = time(NULL);
        }
        else if (rc != -ENOENT)
        {
            DisplayLog(LVL_MAJOR, "PathCheck", "Failed to get parent+name for "DFID": %s",
                       PFID(p_id), strerror(-rc));
        }
    }

    /* if fullpath is in the policy, get the fullpath */
    if (attr_mask & ATTR_MASK_fullpath)
    {
        rc = Lustre_GetFullPath(p_id, ATTR(p_attrs, fullpath), RBH_PATH_MAX);
        if (rc == 0)
            ATTR_MASK_SET(p_attrs, fullpath);
        else if (rc != -ENOENT)
            DisplayLog(LVL_MAJOR, "PathCheck", "Failed to retrieve fullpath for "DFID": %s",
                       PFID(p_id), strerror(-rc));
    }
}
#endif


