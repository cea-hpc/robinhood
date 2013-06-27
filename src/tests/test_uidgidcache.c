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
#include <stdio.h>

typedef struct pw_cacheent__
{
    struct passwd  pw;

    /* le buffer utilise par getpwnam_r pour stocker les chaines
       512 => ~100/chaine contenue dans la structure */
    char           buffer[512];

    /* pour le chainage */
    struct pw_cacheent__ *p_next;

} pw_cacheent_t;


typedef struct gr_cacheent__
{
    struct group   gr;

    /* le buffer utilise par getpwnam_r pour stocker les chaines
       on compte 16 caracteres par membre * 256 membres */
    char           buffer[16 * 256];

    /* pour le chainage */
    struct gr_cacheent__ *p_next;

} gr_cacheent_t;

int main( int argc, char **argv )
{
    unsigned int   i;

    InitUidGid_Cache(  );

    for ( i = 0; i <= 10000; i++ )
    {
        uid_t          u;
        struct passwd *ppw;

        u = i % 2000 + 2000;
        ppw = GetPwUid( u );

    }

    for ( i = 0; i <= 5000; i++ )
    {
        gid_t          g;
        struct group  *pgr;

        g = i % 1000 + 5000;
        pgr = GetGrGid( g );

    }

    return 0;
}
