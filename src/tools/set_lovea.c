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

/* read rbh-diff as input, and set lov for MDT objects */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <stdlib.h>
#include <sys/param.h>

#include "lustre_extended_types.h"

static ssize_t hex2bin(const char * hex, void * buff)
{
    const char *currh = hex;
    unsigned char *currb = buff;
    while(*currh)
    {
        if (sscanf(currh,"%2hhx", currb) != 1)
        {
            fprintf(stderr,"ERROR: could not read a byte from hex value at '%s'\n", currh);
            return -1;
        }
        currh += 2;
        currb ++;
    }
    return (currb-(unsigned char*)buff);
}

static int set_lov_ea(struct lov_user_md * p_lum, size_t sz_in, const char * path)
{
    if (p_lum->lmm_magic == LOV_USER_MAGIC_V1)
    {
        size_t len = sizeof(struct lov_user_md_v1) + p_lum->lmm_stripe_count*sizeof(struct lov_user_ost_data_v1);
        if (sz_in != len)
        {
            fprintf(stderr, "ERROR: wrong input size for lov_user_md_v1: %lu (%lu expected, stripe count=%u)\n",
                    sz_in, len, p_lum->lmm_stripe_count);
            return -1;
        }
        if (lsetxattr (path, XATTR_NAME_LOV, p_lum, len, 0 /* create or replace*/ ))
        {
            fprintf(stderr, "ERROR setting "XATTR_NAME_LOV" xattr on %s: %s\n",
                    path, strerror(errno));
            return -1;
        }
    }
#ifdef LOV_USER_MAGIC_V3
    else if (p_lum->lmm_magic == LOV_USER_MAGIC_V3)
    {
        struct lov_user_md_v3 *p_lum3 = (struct lov_user_md_v3 *)p_lum;

        size_t len = sizeof(struct lov_user_md_v3) + p_lum3->lmm_stripe_count*sizeof(struct lov_user_ost_data_v1);
        if (sz_in != len)
        {
            fprintf(stderr, "ERROR: wrong input size for lov_user_md_v3: %lu (%lu expected)\n",
                    sz_in, len);
            return -1;
        }
        if (lsetxattr (path, XATTR_NAME_LOV, p_lum3, len, 0 /* create or replace*/ ))
        {
            fprintf(stderr, "ERROR setting "XATTR_NAME_LOV" xattr on %s: %s\n",
                    path, strerror(errno));
            return -1;
        }
    }
#endif
    else
    {
        fprintf(stderr, "Invalid magic number %#x in lov_user_md\n", p_lum->lmm_magic);
        return -1;
    }
    printf("%s: "XATTR_NAME_LOV" set successfully\n", path);
    return 0;
}

int main(int argc, char ** argv)
{
    char buff[4096];
    char path[MAXPATHLEN];
    char lum_buff[4096];
    int len;
    ssize_t s;
    char *lovea;
    int nl = 0;
    int errors = 0;
    int ignored_lines = 0;
    int ok = 0;
    char * mdt_root;
    FILE * lovea_stream = stdin;

    if (argc != 2 && argc != 3)
    {
        fprintf(stderr, "Usage: %s <mdt_mount_point> [lovea_file]\n", argv[0]);
        exit(1);
    }
    if (argv[1][0] != '/')
    {
        fprintf(stderr, "ERROR: absolute path expected for <mdt_mount_point>\n");
        fprintf(stderr, "Usage: %s <mdt_mount_point> [lovea_file]\n", argv[0]);
        exit(1);
    }
    mdt_root = argv[1];
    if (argc == 3)
    {
        lovea_stream = fopen(argv[2],"r");
        if (!lovea_stream)
        {
            fprintf(stderr,"Failed to open %s for reading: %s\n",
                argv[2], strerror(errno));
            exit(1);
        }
    }

    while(fgets(buff, 4096, lovea_stream))
    {
        nl++;

        len = strlen(buff);
        /* remove final '\n' */
        if (len > 0 && buff[len-1] == '\n')
            buff[len-1] = '\0';

        /* line format: <relative path of file> <lov_ea(hex)>*/
        lovea = strrchr(buff, ' ');
        if (!lovea)
        {
            fprintf(stderr, "ERROR: Invalid line format or empty line at line %u\n", nl);
            ignored_lines++;
            continue;
        }
        /* split path and lovea */
        *lovea = '\0';
        lovea++;

        /* convert hex buffer to binary */
        s = hex2bin(lovea, lum_buff);
        if (s < 0)
        {
            errors ++;
            continue;
        }

        if (s < sizeof(struct lov_user_md_v1))
        {
            fprintf(stderr, "ERROR: lov_ea is too small: %Lu/%Lu bytes\n",
                    (unsigned long long)s, (unsigned long long)sizeof(struct lov_user_md_v1));
            errors ++;
            continue;
        }

        sprintf(path, "%s/ROOT/%s", mdt_root, buff);
        if (set_lov_ea((struct lov_user_md *)lum_buff, s, path))
        {
            errors ++;
            continue;
        }
        ok ++;
    }
    printf("\nSummary: %u input lines, %u ok, %u ignored, %u errors\n", nl, ok, ignored_lines, errors);
    if (errors)
        exit(1);
    else
        exit(0);
}
