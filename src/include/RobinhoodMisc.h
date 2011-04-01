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

#ifdef _SHERPA
#include "DiversCache.h"
#endif

#include <sys/stat.h>
#ifndef __FreeBSD__
#include <sys/vfs.h>
#endif
#include <dirent.h>
#include <stdint.h>

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

/**
 *  Other usefull definitions
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

/**
 * Send a mail
 */
#define MAIL_TITLE_MAX  1024
int            SendMail( const char *recipient, const char *subject, const char *message );

/**
 * Search for Robinhood config file
 */
int SearchConfig( char * cfg_out );

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

/** Retrieve the name associated to a user (or the text representation of its uid if the user doesn't exist) */
char          *uid2str( uid_t uid, char *username );
/** Retrieve the name associated to a user (or the text representation of its uid if the user doesn't exist) */
char          *gid2str( gid_t gid, char *groupname );


/**
 * Check mount point and FS type.
 * Also return the associated device number.
 * (for STAY_IN_FS security option).
 */
int            CheckFSInfo( char *path, char *expected_type, dev_t * p_fs_dev,
                            int check_mounted, int save_fs );

/**
 *  Check that FS path is the same as the last time.
 */
int            CheckLastFS(  );

/**
 * extract relative path from full path
 */
int relative_path( const char * fullpath, const char * root, char * rel_path );

#ifdef _LUSTRE

/** initialize access to lustre */
int            Lustre_Init(  );

/** Retrieve stripe info for a file */
int            File_GetStripeByPath( const char *entry_path, stripe_info_t * p_stripe_info,
                                     stripe_items_t * p_stripe_items );

/** Create a file with the given stripe information */
int File_CreateSetStripe( const char * path, const stripe_info_t * stripe );

void           set_mount_point( char *mntpnt );
char          *get_mount_point(  );
void           set_fsname( char *name );
char          *get_fsname(  );
#ifdef _HAVE_FID
int            BuildFidPath( const entry_id_t * p_id /* IN */ , char *path /* OUT */  );
int            Lustre_GetFullPath( const entry_id_t * p_id, char *fullpath, unsigned int len );
int            Lustre_GetFidFromPath( const char *fullpath, entry_id_t * p_id );
#endif

#ifdef HAVE_CHANGELOGS
/* if the FS has changelogs, define fonction for converting changelog time */
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
int LustreHSM_Action( enum hsm_user_action action, const entry_id_t * p_id,
                      const char * hints, unsigned int archive_num );
#endif

/** Retrieve OST usage info ('ost df') */
int            Get_OST_usage( char *fs_path, unsigned int ost_index, struct statfs *ost_statfs );

#ifdef HAVE_LLAPI_GETPOOL_INFO
/** Retrieve pool usage info */
int            Get_pool_usage( const char *poolname, struct statfs *pool_statfs );
#endif

#ifdef _MDS_STAT_SUPPORT
/** Retrieve file information from MDS */
int lustre_mds_stat( char *fullpath, DIR * parent, struct stat *inode );
#ifdef _HAVE_FID
int lustre_mds_stat_by_fid( const entry_id_t * p_id, struct stat *inode );
#endif
#endif

#endif

#ifdef _SHERPA

/* Init libSherpaCacheP with config file path */
int InitSherpa(char * sherpa_config, char * logfile, char *reportfile);

/* get the reference and cache definition for an entry */
int Sherpa_GetEntryDescription( char * cache_path, /* in */
                                char * reference_path,  /* out */
                                char * relative_path, /* out */
                                struct EntreeCache * cache_info ); /* out */

enum what_to_do { do_update, do_skip, do_rm };
enum what_to_do SherpaManageEntry( const entry_id_t * p_id, attr_set_t * p_attrs,
                                   int match_classes);
#endif

#ifdef HAVE_SHOOK
int ShookGetStatus(const char * path, file_status_t * p_status);
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

char          *FormatStripeList( char *buff, size_t sz, const stripe_items_t * p_stripe_items );

/*
 * Parsing functions
 */

/**
 * Convert a string to a boolean
 * @return -1 on error.
 */
int            str2bool( char *str );

/**
 * Convert a string to an integer
 * @return -1 on error.
 */
int            str2int( char *str );


/**
 * Convert a string to a duration in seconds
 * @return -1 on error.
 */
int            str2duration( const char *str );

/**
 * Convert a string to a size (in bytes)
 * @return -1 on error.
 */
uint64_t str2size( char *str );


/** parse date/time yyyymmdd[HH[MM[SS]]] */
time_t str2date( char *str ); 

/**
 *  Print attributes to a string
 */
int            PrintAttrs( char *out_str, size_t strsize, const attr_set_t * p_attr_set,
                           int overide_mask );

/** Compute greatest common divisor (GCD) of 2 numbers */
unsigned int   gcd( unsigned int x, unsigned int y );

/** Ensure that the thread is suspended for a given amount
 * of time, event if the process gets interrupts.
 */
void rh_sleep( unsigned int seconds );

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
int execute_shell_command( const char * cmd, int argc, ... );

#endif
