/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009 CEA/DAM
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
 * \file    $RCSfile: testuidgidcache.c,v $
 * \author  $Author: leibovic $
 * \date    $Date: 2008/02/15 10:37:38 $
 * \brief   Cache user and groups relatives information.
 *
 * Cache user and groups relative information.
 *
 *
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "uidgidcache.h"
#include "rbh_logs.h"

#include <stdio.h>
#include <sys/time.h>
#include <assert.h>
#include <string.h>

/* Overwrite getpwuid_r and getgrgid_r as used by the UID/GID cache,
 * so we can feed many more ids than present in the system. This test
 * and the cache only care about the names and the UID/GID, so don't
 * fill the rest of the structures. */
#define MAX_UID 100000
int getpwuid_r(uid_t uid, struct passwd *pwd,
               char *buf, size_t buflen, struct passwd **result)
{
    pwd->pw_uid = uid;
    sprintf(buf, "%ld", (long)uid);
    pwd->pw_name = buf;

    if (uid >= MAX_UID)
        *result = NULL;
    else
        *result = pwd;

    return 0;
}

#define MAX_GID 100000
int getgrgid_r(gid_t gid, struct group *grp,
               char *buf, size_t buflen, struct group **result)
{
    grp->gr_gid = gid;
    sprintf(buf, "%ld", (long)gid);
    grp->gr_name = buf;

    if (gid >= MAX_GID)
        *result = NULL;
    else
        *result = grp;

    return 0;
}

/* avoid linking with all robinhood libs */
log_config_t log_config = { .debug_level = LVL_DEBUG };

void DisplayLogFn(log_level debug_level, const char *tag, const char *format, ...)
{
    if (LVL_DEBUG >= debug_level)
    {
        va_list args;

        va_start(args, format);
        vprintf(format, args);
        va_end(args);
        printf("\n");
    }
}

int main(int argc, char **argv)
{
    unsigned int   i;
    uid_t          u;
    gid_t          g;
    unsigned int   c;
    struct timeval t0, tc, tr, tlast = {0};

    InitUidGid_Cache();

    printf("Reference test of getpwuid (%u items)\n", MAX_UID);
    gettimeofday(&t0, NULL);
    for (i = 0; i <= MAX_UID/10; i++)
        for (u = 0; u < 10; u++)
            getpwuid(u);
    gettimeofday(&tc, NULL);
    timersub(&tc, &t0, &tr);
    printf("Elapsed time: %ld.%06ld (%.10f/s)\n", tr.tv_sec, tr.tv_usec,
           (tr.tv_sec + tr.tv_usec/1000000.0) / MAX_UID);

    printf("\nReference test of getgrgid (%u items)\n", MAX_GID);
    gettimeofday(&t0, NULL);
    for (i = 0; i <= MAX_GID/10; i++)
        for (u = 0; u < 10; u++)
            getgrgid(u);
    gettimeofday(&tc, NULL);
    timersub(&tc, &t0, &tr);
    printf("Elapsed time: %ld.%06ld (%.10f/s)\n", tr.tv_sec, tr.tv_usec,
           (tr.tv_sec + tr.tv_usec/1000000.0) / MAX_GID);

    printf("\nTest of password cache\n");
    for (i = 0; i <= 10; i++)
    {
        c = 0;
        for (u = 0; u < MAX_UID; u++)
        {
            struct passwd *ppw;

            ppw = GetPwUid(u);
            if (ppw)
                c++;
        }
        gettimeofday(&tc, NULL);
        timersub(&tc, &t0, &tr);
        if (i == 0)
            tlast = tr;
        printf("loop %u, %u items: %lu.%06lu\n", i, c, tr.tv_sec, tr.tv_usec);
        if (i == 1) {
            /* compute speedup */
            float ratio = (tlast.tv_sec*1000000.0 + tlast.tv_usec)/(tr.tv_sec*1000000.0 + tr.tv_usec);
            printf("SPEED-UP: %.2f%%\n", 100.0*(ratio - 1));

            printf("Elapsed time: %ld.%06ld (%.10f/s)\n", tr.tv_sec, tr.tv_usec,
                   (tr.tv_sec + tr.tv_usec/1000000.0) / MAX_UID);
        }
        t0 = tc;
    }

    /* Now, check that the values returned are correct */
    for (u = 0; u < MAX_UID; u++)
    {
        struct passwd *ppw;
        char buf[50];

        ppw = GetPwUid(u);
        assert(ppw != NULL);
        assert(ppw->pw_uid == u);

        sprintf(buf, "%ld", (long)u);
        assert(strcmp(ppw->pw_name, buf) == 0);
    }

    assert(GetPwUid(MAX_UID) == NULL);

    printf("\nTest of group cache\n");

    gettimeofday(&t0, NULL);

    for (i = 0; i <= 10; i++)
    {
        c = 0;
        for (g = 0; g < MAX_GID; g++)
        {
            struct group  *pgr;

            pgr = GetGrGid(g);
            if (pgr)
                c++;
        }
        gettimeofday(&tc, NULL);
        timersub(&tc, &t0, &tr);
        if (i == 0)
            tlast = tr;
        printf("loop %u, %u items: %lu.%06lu\n", i, c, tr.tv_sec, tr.tv_usec);
        if (i == 1) {
            /* compute speedup */
            float ratio = (tlast.tv_sec*1000000.0 + tlast.tv_usec)/(tr.tv_sec*1000000.0 + tr.tv_usec);
            printf("SPEED-UP: %.2f%%\n", 100.0*(ratio - 1));

            printf("Elapsed time: %ld.%06ld (%.10f/s)\n", tr.tv_sec, tr.tv_usec,
                   (tr.tv_sec + tr.tv_usec/1000000.0) / MAX_GID);
        }
        t0 = tc;
    }

    /* Now, check that the values returned are correct */
    for (g = 0; g < MAX_GID; g++)
    {
        struct group *pgr;
        char buf[50];

        pgr = GetGrGid(g);
        assert(pgr != NULL);
        assert(pgr->gr_gid == g);

        sprintf(buf, "%ld", (long)g);
        assert(strcmp(pgr->gr_name, buf) == 0);
    }

    assert(GetGrGid(MAX_GID) == NULL);

    return 0;
}
