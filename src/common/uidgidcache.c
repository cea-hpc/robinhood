/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2007, 2008, 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 *
 * \file    $RCSfile: uidgidcache.c,v $
 * \author  $Author: leibovic $
 * \date    $Date: 2008/02/15 10:37:38 $
 * \brief   Cache user and groups relatives information.
 *
 * Cache user and groups relative information.
 *
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "uidgidcache.h"
#include "RW_Lock.h"
#include "RobinhoodLogs.h"
#include "Memory.h"

#if HAVE_STRING_H
#   include <string.h>
#endif

#include <stdio.h>

/* -------------- parameters ------------ */

/* prime number for hashing passwd entries.
 * it must be high enough compared to the number
 * of users.
 */
#define PRIME_PW  113

/* prime number for hashing group entries.
 * It must be high enough compared to the
 * number of groups */
#define PRIME_GR  23

#define ALPHABET_LEN  10
#define SHIFT         229

/* init buffer size for storing alt groups for a user */
#define ALT_GROUPS_SZ   1024
/* init buffer size for group members for a group */
#define GROUP_MEMB_SZ   4096

#define LOGTAG  "UidGidCache"

/* -------------- cache and hashtables management ------------ */

typedef struct pw_cacheent__
{
    struct passwd  pw;

    /* this buffer is used by getpwnam_r for storing strings.
     * it contains all group names the user owns to.
     */
    char           *buffer;
    int            buf_size;

    /* for chaining entries */
    struct pw_cacheent__ *p_next;

} pw_cacheent_t;


typedef struct gr_cacheent__
{
    struct group   gr;

    /* this buffer is used by getgr_gid_r for storing group members.
     */
    char           *buffer;
    int            buf_size;

    /* for chaining entries */
    struct gr_cacheent__ *p_next;

} gr_cacheent_t;


/* cache of PW entries */
static struct pw_hash_head__
{
    rw_lock_t      list_lock;
    pw_cacheent_t *pw_list;

} pw_hash[PRIME_PW];


/* cache of group entries */

static struct gr_hash_head__
{
    rw_lock_t      list_lock;
    gr_cacheent_t *gr_list;

} gr_hash[PRIME_GR];


/* stats about the cache */
unsigned int   pw_nb_set = 0;
unsigned int   pw_nb_get = 0;
unsigned int   gr_nb_set = 0;
unsigned int   gr_nb_get = 0;


/* HashBuff : hash a buffer of any size */

static unsigned int HashBuff( caddr_t p_val, size_t val_len, unsigned int modulus )
{
    unsigned int   i;
    unsigned int   hash_val = 0;
    char           c = 0;
    char          *str = ( char * ) ( p_val );

    /* compute for each chunk of 8 bits */
    for ( i = 0; i < val_len; i++ )
    {
        c = str[i];
        hash_val = ( hash_val * ALPHABET_LEN + ( unsigned int ) c + SHIFT ) % modulus;
    }

    return hash_val;
}


/* get an entry in the pwent cache */
static struct passwd *HashGetPwent( uid_t pw_uid )
{
    pw_cacheent_t *p_curr;
    struct passwd *result = NULL;

    /* compute hash key */
    unsigned int   hash_val = HashBuff( ( caddr_t ) & pw_uid, sizeof( pw_uid ), PRIME_PW );

    /* lock the list (read lock), for searching the entry */

    P_r( &pw_hash[hash_val].list_lock );

    for ( p_curr = pw_hash[hash_val].pw_list; p_curr != NULL; p_curr = p_curr->p_next )
    {
        if ( p_curr->pw.pw_uid == pw_uid )
        {
            result = &p_curr->pw;
            pw_nb_get++;
            break;
        }
    }

    V_r( &pw_hash[hash_val].list_lock );

    return result;
}



static void HashInsertPwent( pw_cacheent_t * pwentry )
{
    /* compute hash key */
    unsigned int   hash_val = HashBuff( ( caddr_t ) & ( pwentry->pw.pw_uid ),
                                        sizeof( pwentry->pw.pw_uid ),
                                        PRIME_PW );

    /* lock the list (write lock), for inserting entry (at first position) */

    P_w( &pw_hash[hash_val].list_lock );

    pwentry->p_next = pw_hash[hash_val].pw_list;
    pw_hash[hash_val].pw_list = pwentry;

    pw_nb_set++;

    V_w( &pw_hash[hash_val].list_lock );

}


/* get an entry in the grent cache */
static struct group *HashGetGrent( gid_t gr_gid )
{
    gr_cacheent_t *p_curr;
    struct group  *result = NULL;

    /* compute hash key */
    unsigned int   hash_val = HashBuff( ( caddr_t ) & gr_gid, sizeof( gr_gid ), PRIME_GR );

    /* lock the list (read lock), for searching the entry */

    P_r( &gr_hash[hash_val].list_lock );

    for ( p_curr = gr_hash[hash_val].gr_list; p_curr != NULL; p_curr = p_curr->p_next )
    {
        if ( p_curr->gr.gr_gid == gr_gid )
        {
            result = &p_curr->gr;
            gr_nb_get++;
            break;
        }
    }

    V_r( &gr_hash[hash_val].list_lock );

    return result;
}



static void HashInsertGrent( gr_cacheent_t * grentry )
{
    /* compute hash key */
    unsigned int   hash_val = HashBuff( ( caddr_t ) & ( grentry->gr.gr_gid ),
                                        sizeof( grentry->gr.gr_gid ),
                                        PRIME_GR );

    /* lock the list (write lock), for inserting entry (at first position) */

    P_w( &gr_hash[hash_val].list_lock );

    grentry->p_next = gr_hash[hash_val].gr_list;
    gr_hash[hash_val].gr_list = grentry;

    gr_nb_set++;

    V_w( &gr_hash[hash_val].list_lock );

}



/* ------------ exported functions ------------ */


/* Initialization of pwent and grent caches */
int InitUidGid_Cache( void )
{
    unsigned int   i;

    /* initialize locks on hash table slots */
    for ( i = 0; i < PRIME_PW; i++ )
    {
        rw_lock_init( &pw_hash[i].list_lock );
        pw_hash[i].pw_list = NULL;
    }

    for ( i = 0; i < PRIME_GR; i++ )
    {
        rw_lock_init( &gr_hash[i].list_lock );
        gr_hash[i].gr_list = NULL;
    }
    return 0;
}

/* get pw entry for the given uid */
struct passwd *GetPwUid( uid_t owner )
{
    struct passwd *result;
    pw_cacheent_t *p_pwcacheent = NULL;
    int            rc;

    /* is the entry in the cache? */
    result = HashGetPwent( owner );

    /* if no, allocate a pw cache entry
     * and ask the system to fill it */
    if ( result == NULL )
    {
        /* entry allocation */
        p_pwcacheent = malloc(sizeof(pw_cacheent_t));
        if ( p_pwcacheent == NULL )
            return NULL;
        memset( p_pwcacheent, 0, sizeof( pw_cacheent_t ) );

        p_pwcacheent->buf_size = ALT_GROUPS_SZ;
        p_pwcacheent->buffer = malloc(p_pwcacheent->buf_size);
        if ( p_pwcacheent->buffer == NULL )
            goto out_free;

retry:
        if ( ( ( rc = getpwuid_r( owner, &p_pwcacheent->pw,
                                  p_pwcacheent->buffer,
                                  p_pwcacheent->buf_size,
                                  &result ) ) != 0 ) || ( result == NULL ) )
        {
            /* try with larger buff */
            if (rc == ERANGE)
            {
                p_pwcacheent->buf_size *= 2;
                DisplayLog( LVL_FULL, LOGTAG, "got ERANGE error from getpwuid_r: trying with buf_size=%u",
                            p_pwcacheent->buf_size );
                p_pwcacheent->buffer = realloc(p_pwcacheent->buffer,
                                               p_pwcacheent->buf_size);
                if ( p_pwcacheent->buffer == NULL )
                    goto out_free;
                else
                    goto retry;
            }
            if ( ( rc != 0 ) && ( rc != ENOENT ) &&
                 ( rc != ESRCH ) && ( rc != EBADF ) && ( rc != EPERM ) )
                DisplayLog( LVL_CRIT, LOGTAG, "ERROR %d in getpwuid_r: %s",
                            rc, strerror( rc ) );
            goto out_free;
        }

        /* insert it to hash table */
        HashInsertPwent( p_pwcacheent );

        result = &p_pwcacheent->pw;
    }

    return result;

out_free:
    if ( p_pwcacheent != NULL )
    {
        if ( p_pwcacheent->buffer != NULL )
            free(p_pwcacheent->buffer);

        free( p_pwcacheent );
    }
    return NULL;
}



struct group  *GetGrGid( gid_t grid )
{

    struct group  *result;
    gr_cacheent_t *p_grcacheent;
    int            rc;

    /* is the entry in the cache? */
    result = HashGetGrent( grid );

    /* if no, allocate a gr cache entry
     * and ask the system to fill it */
    if ( result == NULL )
    {
        /* entry  allocation */
        p_grcacheent = malloc(sizeof(gr_cacheent_t));
        if ( p_grcacheent == NULL )
            return NULL;
        memset( p_grcacheent, 0, sizeof( gr_cacheent_t ) );

        p_grcacheent->buf_size = GROUP_MEMB_SZ;
        p_grcacheent->buffer = malloc(p_grcacheent->buf_size);
        if ( p_grcacheent->buffer == NULL )
            goto out_free;

retry:
        if ( ( ( rc = getgrgid_r( grid, &p_grcacheent->gr,
                                  p_grcacheent->buffer,
                                  p_grcacheent->buf_size,
                                  &result ) ) != 0 ) || ( result == NULL ) )
        {
            /* try with larger buff */
            if (rc == ERANGE)
            {
                p_grcacheent->buf_size *= 2;
                DisplayLog( LVL_FULL, LOGTAG, "got ERANGE error from getgrgid_r: trying with buf_size=%u",
                            p_grcacheent->buf_size );
                p_grcacheent->buffer = realloc(p_grcacheent->buffer,
                                               p_grcacheent->buf_size);
                if ( p_grcacheent->buffer == NULL )
                    goto out_free;
                else
                    goto retry;
            }

            if ( ( rc != 0 ) && ( rc != ENOENT ) &&
                 ( rc != ESRCH ) && ( rc != EBADF ) && ( rc != EPERM ) )
                DisplayLog( LVL_CRIT, LOGTAG, "ERROR %d in getgrgid_r : %s",
                            rc, strerror( rc ) );

            /* gid not found */
            goto out_free;
        }

        /* insert it to hash table */
        HashInsertGrent( p_grcacheent );

        result = &p_grcacheent->gr;

    }

    return result;

out_free:
    if ( p_grcacheent != NULL )
    {
        if ( p_grcacheent->buffer != NULL )
            free(p_grcacheent->buffer);

        free( p_grcacheent );
    }
    return NULL;
}
