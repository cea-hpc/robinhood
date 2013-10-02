/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2013 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/* read rbh-diff fid_remap as input, and update trusted.fid xattr for OST objects */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <stdlib.h>
#include <sys/param.h>
#include <search.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>

#include "lustre_extended_types.h"
#include "RobinhoodMisc.h"

static void usage(char *argv0)
{
        fprintf(stderr, "Usage: %s <ost_index> <ost_mount_point> <fid_remap_file>\n", basename(argv0));
        exit(1);
}


int main(int argc, char ** argv)
{
    char buff[4096];
    char path[RBH_PATH_MAX];
    char xattr[4096];
    ssize_t s;
    int len, rc;
    int nl = 0;
    int errors = 0;
    int ignored_lines = 0;
    int ok = 0;
    int match_ost = 0;
    const char *ost_root, *file;
    int idx = 0;
    FILE *f;

    if (argc != 4)
        usage(argv[0]);

    if (argv[2][0] != '/')
    {
        fprintf(stderr, "ERROR: absolute path expected for <ost_mount_point>\n");
        usage(argv[0]);
    }
    idx = str2int(argv[1]);
    if (idx == -1)
    {
        fprintf(stderr, "ERROR: positive integer expected for <ost_index>\n");
        usage(argv[0]);
    }
    ost_root = argv[2];
    file = argv[3];

    f = fopen(file,"r");
    if (f == NULL)
    {
        rc = errno;
        fprintf(stderr,"Failed to open %s for reading: %s\n",
                file, strerror(errno));
        exit(rc);
    }

    while(fgets(buff, 4096, f))
    {
        unsigned int ost_idx = 0, snum = 0;
        uint64_t     obj_id = 0;
        lustre_fid   oldfid = {0},
                     newfid = {0};

        nl++;

        len = strlen(buff);
        /* remove final '\n' */
        if (len > 0 && buff[len-1] == '\n')
            buff[len-1] = '\0';

        /* line format: ost_idx obj_id oldfid newfid */
        if (sscanf(buff, "%u %u %"PRIu64" ["SFID"] ["SFID"]",
                   &ost_idx, &snum, &obj_id, RFID(&oldfid), RFID(&newfid)) != 9)
        {
            fprintf(stderr, "ERROR: Invalid line format or empty line at line %u\n", nl);
            ignored_lines++;
            continue;
        }

        if (idx != ost_idx)
        {
            ignored_lines++;
            continue;
        }
        match_ost++;

        /* build path related to object index */
        sprintf(path,"%s/O/0/d%u/%"PRIu64, ost_root, (unsigned int)(obj_id % 32), obj_id);

        /* get previous fid for the object */
        s = lgetxattr(path, "trusted.fid", xattr, 4096);
        if (s < 0)
        {
            fprintf(stderr, "Can't check previous FID for object %"PRIu64" (%s): %s.\n",
                    obj_id, path, strerror(errno));
            errors ++;
            continue;
        }
        if (s != sizeof(struct filter_fid))
        {
            fprintf(stderr, "ERROR: unexpected size for fid xattr: %zu != %zu\n",
                    s, sizeof(lustre_fid));
            errors ++;
            continue;
        }
        struct filter_fid *ffid = (struct filter_fid *)xattr;

        // ff_parent.f_ver == file stripe number
        oldfid.f_ver = snum;
        newfid.f_ver = snum;

        if (memcmp(&ffid->ff_parent, &oldfid, sizeof(lustre_fid)))
        {
            if (memcmp(&ffid->ff_parent, &newfid, sizeof(lustre_fid)) == 0)
                fprintf(stderr, "ERROR: new FID is already set for object %"PRIu64" (%s): "
                        "current="DFID", old="DFID", new="DFID"\n", obj_id, path,
                        PFID(&ffid->ff_parent), PFID(&oldfid), PFID(&newfid));
            else
                fprintf(stderr, "ERROR: unexpected FID for object %"PRIu64" (%s): "
                        "current="DFID", expected="DFID"\n", obj_id, path,
                        PFID(&ffid->ff_parent), PFID(&oldfid));
            errors ++;
            continue;
        }
        if (ffid->ff_objid != obj_id)
        {
            fprintf(stderr, "ERROR: object id doesn't match! got: %"PRIu64", expected: %"PRIu64" (%s)\n",
                    (uint64_t)ffid->ff_objid, obj_id, path);
            errors ++;
            continue;
        }

        /* set the filter with the right fid */
        memcpy(&ffid->ff_parent, &newfid, sizeof(lustre_fid));
        printf("objid %"PRIu64": "DFID"->"DFID"\n", obj_id, PFID(&oldfid), PFID(&newfid));
        if (lsetxattr(path, "trusted.fid", ffid, sizeof(struct filter_fid), XATTR_REPLACE))
        {
            fprintf(stderr, "ERROR: failed to update object's FID for object %"PRIu64" (%s): %s",
                    obj_id, path, strerror(errno));
            errors ++;
            continue;
        }
        ok ++;
    }
    printf("\nSummary: %u input lines, %u matching ost#%u, %u success, %u ignored, %u errors\n",
           nl, match_ost, idx, ok, ignored_lines, errors);
    if (errors)
        exit(1);
    else
        exit(0);
}
