/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2007, 2008, 2009 CEA/DAM
 * Copyright 2016 Cray Inc. All rights reserved.
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
#include "rbh_logs.h"
#include "Memory.h"

#if HAVE_STRING_H
#   include <string.h>
#endif

#include <stdio.h>
#include <unistd.h>

#include <glib.h>

/* -------------- parameters ------------ */

/* init buffer size for storing alt groups for a user */
size_t alt_groups_sz;

/* init buffer size for group members for a group */
size_t group_memb_sz;

#define LOGTAG  "UidGidCache"

/* -------------- cache and hashtables management ------------ */

typedef struct pw_cacheent__ {
    struct passwd pw;
} pw_cacheent_t;

typedef struct gr_cacheent__ {
    struct group gr;
} gr_cacheent_t;

/* cache of PW entries */
static struct {
    rw_lock_t lock;
    GHashTable *cache;
} pw_hash;

/* cache of group entries */
static struct {
    rw_lock_t lock;
    GHashTable *cache;
} gr_hash;

/* stats about the cache */
unsigned int pw_nb_set = 0;
unsigned int pw_nb_get = 0;
unsigned int gr_nb_set = 0;
unsigned int gr_nb_get = 0;

/* ------------ exported functions ------------ */

/* Initialization of pwent and grent caches */
int InitUidGid_Cache(void)
{
    long res;

    /* initialize locks on hash table slots */
    rw_lock_init(&pw_hash.lock);
    pw_hash.cache = g_hash_table_new(NULL, NULL);

    rw_lock_init(&gr_hash.lock);
    gr_hash.cache = g_hash_table_new(NULL, NULL);

    /* Try to size the memory needed to get the strings for getpwuid_r
     * and getgrgid_r. */
    res = sysconf(_SC_GETPW_R_SIZE_MAX);
    if (res == -1 || res > 4096)
        alt_groups_sz = 4096;
    else
        alt_groups_sz = res;

    res = sysconf(_SC_GETGR_R_SIZE_MAX);
    if (res == -1 || res > 4096)
        group_memb_sz = 4096;
    else
        group_memb_sz = res;

    return 0;
}

/* get user name for the given uid */
const struct passwd *GetPwUid(uid_t owner)
{
    struct passwd *result;
    pw_cacheent_t *p_pwcacheent;
    pw_cacheent_t *entry2;
    int            rc;
    char          *buffer;
    int            buf_size;

    /* is the entry in the cache? */
    P_r(&pw_hash.lock);
    p_pwcacheent =
        g_hash_table_lookup(pw_hash.cache, (void *)(uintptr_t) owner);
    V_r(&pw_hash.lock);

    if (p_pwcacheent)
        pw_nb_get++;

    /* if no, allocate a pw cache entry
     * and ask the system to fill it */
    if (p_pwcacheent == NULL) {
        /* entry allocation */
        p_pwcacheent = calloc(1, sizeof(pw_cacheent_t));
        if (p_pwcacheent == NULL)
            return NULL;

        buf_size = alt_groups_sz;
        buffer = malloc(buf_size);
        if (buffer == NULL)
            goto out_free;

 retry:
        rc = getpwuid_r(owner, &p_pwcacheent->pw, buffer, buf_size, &result);
        if (rc != 0 || result == NULL) {
            /* try with larger buff */
            if (rc == ERANGE) {
                buf_size *= 2;
                DisplayLog(LVL_FULL, LOGTAG,
                           "got ERANGE error from getpwuid_r: trying with buf_size=%u",
                           buf_size);
                buffer = realloc(buffer, buf_size);
                if (buffer == NULL)
                    goto out_free;
                else
                    goto retry;
            }
            if (rc != 0 && rc != ENOENT && rc != ESRCH &&
                rc != EBADF && rc != EPERM)
                DisplayLog(LVL_CRIT, LOGTAG, "ERROR %d in getpwuid_r: %s",
                           rc, strerror(rc));
            goto out_free;
        }

        /* We only care about the name */
        p_pwcacheent->pw.pw_name = strdup(p_pwcacheent->pw.pw_name);
        if (p_pwcacheent->pw.pw_name == NULL)
            goto out_free;

        p_pwcacheent->pw.pw_passwd = NULL;
        p_pwcacheent->pw.pw_gecos = NULL;
        p_pwcacheent->pw.pw_dir = NULL;
        p_pwcacheent->pw.pw_shell = NULL;

        free(buffer);

        /* insert it to hash table */
        P_w(&pw_hash.lock);

        /* Another thread may have inserted it in the meantime. Check
         * again. */
        entry2 = g_hash_table_lookup(pw_hash.cache, (void *)(uintptr_t) owner);
        if (entry2) {
            free(p_pwcacheent->pw.pw_name);
            free(p_pwcacheent);
            p_pwcacheent = entry2;
            pw_nb_get++;
        } else {
            g_hash_table_insert(pw_hash.cache,
                                (void *)(uintptr_t) p_pwcacheent->pw.pw_uid,
                                p_pwcacheent);
            pw_nb_set++;
        }
        V_w(&pw_hash.lock);
    }

    return &p_pwcacheent->pw;

 out_free:
    if (p_pwcacheent != NULL) {
        free(buffer);
        free(p_pwcacheent);
    }
    return NULL;
}

const struct group *GetGrGid(gid_t grid)
{
    struct group  *result;
    gr_cacheent_t *p_grcacheent;
    gr_cacheent_t *entry2;
    int            rc;
    char          *buffer;
    int            buf_size;

    /* is the entry in the cache? */
    P_r(&gr_hash.lock);
    p_grcacheent = g_hash_table_lookup(gr_hash.cache, (void *)(uintptr_t) grid);
    V_r(&gr_hash.lock);

    if (p_grcacheent)
        gr_nb_get++;

    /* if no, allocate a gr cache entry
     * and ask the system to fill it */
    if (p_grcacheent == NULL) {
        /* entry allocation */
        p_grcacheent = calloc(1, sizeof(gr_cacheent_t));
        if (p_grcacheent == NULL)
            return NULL;

        buf_size = group_memb_sz;
        buffer = malloc(buf_size);
        if (buffer == NULL)
            goto out_free;

 retry:
        rc = getgrgid_r(grid, &p_grcacheent->gr, buffer, buf_size, &result);
        if (rc != 0 || result == NULL) {
            /* try with larger buff */
            if (rc == ERANGE) {
                buf_size *= 2;
                DisplayLog(LVL_FULL, LOGTAG,
                           "got ERANGE error from getgrgid_r: trying with buf_size=%u",
                           buf_size);
                buffer = realloc(buffer, buf_size);
                if (buffer == NULL)
                    goto out_free;
                else
                    goto retry;
            }

            if (rc != 0 && rc != ENOENT && rc != ESRCH &&
                rc != EBADF && rc != EPERM)
                DisplayLog(LVL_CRIT, LOGTAG, "ERROR %d in getgrgid_r : %s",
                           rc, strerror(rc));

            /* gid not found */
            goto out_free;
        }

        /* We only care about the name */
        p_grcacheent->gr.gr_name = strdup(p_grcacheent->gr.gr_name);
        if (p_grcacheent->gr.gr_name == NULL)
            goto out_free;

        p_grcacheent->gr.gr_passwd = NULL;
        p_grcacheent->gr.gr_mem = NULL;

        free(buffer);

        /* insert it to hash table */
        P_w(&gr_hash.lock);

        /* Another thread may have inserted it in the meantime. Check
         * again. */
        entry2 = g_hash_table_lookup(gr_hash.cache, (void *)(uintptr_t) grid);
        if (entry2) {
            free(p_grcacheent->gr.gr_name);
            free(p_grcacheent);
            p_grcacheent = entry2;
            gr_nb_get++;
        } else {
            g_hash_table_insert(gr_hash.cache,
                                (void *)(uintptr_t) p_grcacheent->gr.gr_gid,
                                p_grcacheent);
            gr_nb_set++;
        }
        V_w(&gr_hash.lock);
    }

    return &p_grcacheent->gr;

 out_free:
    if (p_grcacheent != NULL) {
        free(buffer);
        free(p_grcacheent);
    }
    return NULL;
}
