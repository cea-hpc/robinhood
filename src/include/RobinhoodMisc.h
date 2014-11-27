/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2004-2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * \file  RobinhoodMisc.h
 * \brief Common tools for parsing, converting, checking FS status...
 */
#ifndef _ROBINHOOD_MISC_H
#define _ROBINHOOD_MISC_H

#include "list_mgr.h"

#include <sys/stat.h>
#ifndef __FreeBSD__
#include <sys/vfs.h>
#endif
#include <dirent.h>
#include <stdint.h>
#include "xplatform_print.h"

/**
 * Common info
 */
#define DEFAULT_CFG_VAR "RBH_CFG_DEFAULT"

/**
 *  Miscelaneous parsing macros
 */
#define EMPTY_STRING( s ) ( (s)[0] == '\0' )
#define FINAL_SLASH( s ) ( (strlen(s) > 1) && (s[strlen(s)-1] == '/') )
#define SLASH_IN( s ) ( strchr( s, '/') != NULL )
#define REMOVE_FINAL_SLASH( s ) ( (s)[strlen(s)-1] = '\0' )
#define IS_ABSOLUTE_PATH( s ) ( ((s)[0]) && ((s)[0] == '/'))

#define ANY_LEVEL_MATCH( _s_ ) (strstr(_s_,"**") != NULL)

#define WILDCARDS_IN( s ) ( strchr(s,'*') || strchr(s,'?') || strchr(s,'[') \
                            || strchr(s,']') || strchr(s,'{') || strchr(s,'}') )
#define STAR_SLASH_BEGIN( s ) (((s)[0]=='*') && ((s)[1]=='/'))

#define bool2str( _b_ )   ((_b_)?"TRUE":"FALSE")

#define rh_strncpy(_s1, _s2, _sz) do { strncpy(_s1, _s2, _sz-1); if (_sz > 0) (_s1)[_sz-1] = '\0'; } while(0)

/**
 *  Other useful definitions
 */

#define MIN2( _a_, _b_ ) ( (_a_)<(_b_) ? (_a_) : (_b_) )
#define MIN3( _a_, _b_, _c_ ) ( MIN2( MIN2( (_a_) , (_b_) ) , (_c_) ) )

#define MAX2( _a_, _b_ ) ( (_a_)>(_b_) ? (_a_) : (_b_) )
#define MAX3( _a_, _b_, _c_ ) ( MAX2( MAX2( (_a_) , (_b_) ) , (_c_) ) )

#ifndef P
#define P(_mutex_) pthread_mutex_lock(&(_mutex_))
#endif
#ifndef V
#define V(_mutex_) pthread_mutex_unlock(&(_mutex_))
#endif

/* displaying FID */
#ifndef _HAVE_FID
#undef DFID
#undef PFID
#undef SFID
#undef RFID

#define DFID "%"PRIX64"/%"PRI_STI
#define PFID(_pid) (_pid)->fs_key, (_pid)->inode
#define SFID "0X%"PRIX64"/%"PRI_STI
#define RFID(_pid) &((_pid)->fs_key), &((_pid)->inode)
#define FID_SCAN_CNT 2
#else
#define FID_SCAN_CNT 3
#endif

/**
 * Send a mail
 */
#define MAIL_TITLE_MAX  1024
int            SendMail( const char *recipient, const char *subject, const char *message );

/**
 * Search for Robinhood config file
 */
int SearchConfig(const char * cfg_in, char * cfg_out, int * changed, char * unmatched,
                 size_t max_len);

/**
 * This function is blocking as long as the lock file is present.
 * Optionaly updates an action timestamp, at each test.
 */
void           TestLockFile( time_t * p_last_action );

/**
 * Convert a Posix attributes structure (returned by lstat)
 * to an attribute set.
 * @param size_info indicates if size info is set in the stat structure.
 */
void           PosixStat2EntryAttr( struct stat *p_inode, attr_set_t * p_attr_set, int size_info );

/* convert file mode to DB type string */
const char * mode2type(mode_t mode);

/** Retrieve the name associated to a user (or the text representation of its uid if the user doesn't exist) */
char          *uid2str( uid_t uid, char *username );
/** Retrieve the name associated to a user (or the text representation of its uid if the user doesn't exist) */
char          *gid2str( gid_t gid, char *groupname );


/**
 * Check mount point and FS type.
 * Also return the associated device number.
 * (for STAY_IN_FS security option).
 */
int            CheckFSInfo( char *path, char *expected_type,
                            dev_t * p_fs_dev, char * fsname,
                            int check_mounted, int save_fs );


/**
 * Initialize filesystem access and retrieve current devid/fs_key
 * - global_config must be set
 * - initialize mount_point, fsname and dev_id
 */
int InitFS( void );

/**
 * This is to be called after a dev_id change was detected
 * return 0 if fskey is unchanged and update mount_point, fsname and dev_id
 * else, return -1
 */
int ResetFS( void );


/**
 *  Check that FS path is the same as the last time.
 */
int            CheckLastFS( void );

/* retrieve FS info */
const char    *get_mount_point( unsigned int * plen );
const char    *get_fid_dir( void );
const char    *get_fsname( void );
dev_t          get_fsdev( void );
uint64_t       get_fskey( void );
const entry_id_t *get_root_id(void);

/**
 * extract relative path from full path
 */
int relative_path( const char * fullpath, const char * root, char * rel_path );

/* create an object with the given attributes */
int create_from_attrs(const attr_set_t * attrs_in,
                      attr_set_t * attrs_out,
                      entry_id_t *new_id, int overwrite, int setstripe);

#ifdef _LUSTRE

/** initialize access to lustre */
int            Lustre_Init( void );

/** Retrieve stripe info for a file */
int            File_GetStripeByPath( const char *entry_path, stripe_info_t * p_stripe_info,
                                     stripe_items_t * p_stripe_items );

int File_GetStripeByDirFd(int dirfd, const char *fname,
                          stripe_info_t * p_stripe_info,
                          stripe_items_t * p_stripe_items);
/**
 * check if a file has data on the given OST.
 */
int DataOnOST(size_t fsize, unsigned int ost_index, const stripe_info_t * sinfo, const stripe_items_t * sitems);


#ifdef HAVE_LLAPI_GETPOOL_INFO
/** Create a file with the given stripe information */
int CreateStriped( const char * path, const stripe_info_t * old_stripe, int overwrite );
int CreateWithoutStripe( const char * path, mode_t mode, int overwrite );
#endif

#ifdef _HAVE_FID
int            BuildFidPath( const entry_id_t * p_id /* IN */ , char *path /* OUT */  );
int            Lustre_GetFullPath( const entry_id_t * p_id, char *fullpath, unsigned int len );
int            Lustre_GetFidFromPath( const char *fullpath, entry_id_t * p_id );
int            Lustre_GetFidByFd(int fd, entry_id_t * p_id);
int            Lustre_GetNameParent(const char *path, int linkno,
                                    lustre_fid *pfid, char *name, int namelen);

void path_check_update(const entry_id_t *p_id,
                       const char *fid_path, attr_set_t *p_attrs,
                       int attr_mask);

#define FID_IS_ZERO(_pf) (((_pf)->f_seq == 0) && ((_pf)->f_oid == 0))

#endif

#ifdef HAVE_CHANGELOGS
/* if the FS has changelogs, define function for converting changelog time */
static inline time_t cltime2sec( uint64_t cltime )
{
   /* extract secs from time field */
   return cltime >> 30;
}

static inline unsigned int cltime2nsec( uint64_t cltime )
{
   /* extract nanosecs: */
   return cltime & ((1<<30) - 1);
}
#endif

#ifdef _LUSTRE_HSM
int            LustreHSM_GetStatus( const char *path, file_status_t * p_status,
                                    int *no_release, int *no_archive );

/** Trigger a HSM action
 * \param action HUA_ARCHIVE, HUA_RESTORE, HUA_RELEASE, HUA_REMOVE, HUA_CANCEL
 */
int LustreHSM_Action(enum hsm_user_action action, const entry_id_t * p_id,
                     const char * hints, unsigned int archive_id);
#endif

/** Retrieve OST usage info ('ost df') */
int            Get_OST_usage( const char *fs_path, unsigned int ost_index, struct statfs *ost_statfs );

#ifdef HAVE_LLAPI_GETPOOL_INFO
/** Retrieve pool usage info */
int            Get_pool_usage( const char *poolname, struct statfs *pool_statfs );
#endif

/** Retrieve file information from MDS */
int lustre_mds_stat(char *fullpath, int parentfd, struct stat *inode);
#ifdef _HAVE_FID
int lustre_mds_stat_by_fid( const entry_id_t * p_id, struct stat *inode );
#endif

#ifndef _MDT_SPECIFIC_LOVEA
/**
 * build LOVEA buffer from stripe information
 * @return size of significant information in buffer.
 */
ssize_t BuildLovEA(const entry_id_t * p_id, const attr_set_t * p_attrs, void * buff, size_t buf_sz);
#endif

#endif /* lustre */

#ifdef HAVE_SHOOK
int ShookGetStatus(const char * path, file_status_t * p_status);
int ShookRecoverById(const entry_id_t * p_id, file_status_t * p_status);
#endif

/**
 * Shoot a thread.
 */
int            TerminateThread( pthread_t thread_id );

/**
 * Clean termination of the daemon + display message in log
 */
void           Exit( int error_code );

/**
 * Format functions
 */
char          *FormatFileSize( char *buff, size_t str_sz, uint64_t file_size );
char          *FormatDuration( char *buff, size_t str_sz, time_t duration );
char          *FormatDurationFloat( char *buff, size_t str_sz, time_t duration );

#ifdef _LUSTRE
char          *FormatStripeList( char *buff, size_t sz, const stripe_items_t * p_stripe_items,
                                 int brief );

#endif

/*
 * Parsing functions
 */

/**
 * Convert a string to a boolean
 * @return -1 on error.
 */
int            str2bool( const char *str );

/**
 * Convert a string to an integer
 * @return -1 on error.
 */
static inline int str2int(const char *str)
{
    char           suffix[256];
    int            nb_read, value;

    if (str == NULL)
        return -1;

    nb_read = sscanf(str, "%d%s", &value, suffix);

    if (nb_read <= 0)
        return -1;              /* invalid format */

    if ((nb_read == 1) || (suffix[0] == '\0'))
        return value;           /* no suffix => 0K */
    else
        return -1;
}

/**
 * Convert a string to a long integer
 * @return -1 on error.
 */
long long str2bigint( const char *str );

/**
 * Convert a string to a duration in seconds
 * @return -1 on error.
 */
int            str2duration( const char *str );

/**
 * Convert a string to a size (in bytes)
 * @return -1 on error.
 */
uint64_t str2size( const char *str );


/** parse date/time yyyymmdd[HH[MM[SS]]] */
time_t str2date( const char *str );

/** convert mode to rwxrwxrwx string */
const char *mode_string(mode_t mode, char *buf);

/**
 *  Print attributes to a string
 *  \param overide_mask if != 0, overide attrmask with this one
 *  \param brief brief notation for diff
 */
int            PrintAttrs( char *out_str, size_t strsize, const attr_set_t * p_attr_set,
                           int overide_mask, int brief );

/**
 *  Apply attribute changes
 *  \param change_mask mask of attributes to be changed
 */
int            ApplyAttrs(const entry_id_t * p_id,
                          const attr_set_t * p_attr_new, const attr_set_t * p_attr_old,
                          int change_mask, int dry_run);


/** Compute greatest common divisor (GCD) of 2 numbers */
unsigned int   gcd( unsigned int x, unsigned int y );

/** Ensure that the thread is suspended for a given amount
 * of time, event if the process gets interrupts.
 */
void rh_sleep( unsigned int seconds );

/* signal safe semaphore ops with error logging */
/* man (3) sem_wait/sem_post: on error, the value of the semaphore is left unchanged */
#define sem_wait_safe(_s) while(sem_wait(_s)) if (errno != EINTR && errno != EAGAIN) \
                              DisplayLog(LVL_CRIT,"sem","ERROR: sem_wait operation failed: %s", strerror(errno))
#define sem_post_safe(_s) while(sem_post(_s)) if (errno != EINTR && errno != EAGAIN) \
                              DisplayLog(LVL_CRIT,"sem","ERROR: sem_post operation failed: %s", strerror(errno))

/**
 * Interuptible sleep.
 * returns when _v != 0.
 */
#define rh_intr_sleep( _s, _v ) do { unsigned int _i; for (_i=0; (_i<_s) && !(_v); _i++) rh_sleep(1); } while(0)


#define rh_usleep(_usec) usleep(_usec)

/** replace a pattern in a string with another sub-string
 * \param str_in_out must be large enough to receive
 *  the resulting string, and cannot exceed 1024.
 */
int str_replace( char * str_in_out, const char * to_be_replaced,
                 const char * replacement );

/**
 * Execute a shell command and analyze the return code
 */
int execute_shell_command(int quiet, const char * cmd, int argc, ...);

char *quote_shell_arg(const char *arg);

/**
 * Replace special parameters {cfgfile}, {fspath}, ...
 * in the given cmd line.
 * Result string is allocated using malloc()
 * and must be released using free().
 * \param replace_array char** of param1, value1, param2, value2, ..., NULL, NULL
 */
char * replace_cmd_parameters(const char * cmd_in, const char **replace_array);

/** convert to upper case */
void upperstr(char *str);

/** recursively create a directoy and return its id */
int mkdir_recurse(const char * full_path, mode_t mode, entry_id_t *dir_id);

#ifdef ATTR_INDEX_status
/** status conversion functions */
const char * db_status2str( file_status_t status, int csv );
file_status_t status2dbval( char * status_str );
const char * allowed_status( void );
#endif

#endif
