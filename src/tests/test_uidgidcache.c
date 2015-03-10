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

int main( int argc, char **argv )
{
    unsigned int   i;
    uid_t          u;
    gid_t          g;
    unsigned int   c;
    struct timeval t0, tc, tr, tlast = {0};

    InitUidGid_Cache();

    gettimeofday(&t0, NULL);

    for ( i = 0; i <= 10; i++ )
    {
        c = 0;
        for ( u = 0; u <= 100; u++ )
        {
            struct passwd *ppw;

            ppw = GetPwUid( u );
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
        }
        t0 = tc;
    }

    for ( i = 0; i <= 10; i++ )
    {
        c = 0;
        for ( g = 0; g <= 100; g++ )
        {

            struct group  *pgr;

            pgr = GetGrGid( g );
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
        }
        t0 = tc;
    }

    return 0;
}
