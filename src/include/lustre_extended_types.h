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
 * \file   lustre_extended_types.h 
 * \brief  Specific types for handling lustre data.
 */
#ifndef _LUSTRE_EXTRA_TYPES_H
#define _LUSTRE_EXTRA_TYPES_H

#ifdef _LUSTRE
#define LPX64 "%#llx"
#define LPX64i "%llx"
#include <sys/types.h>
#include <asm/types.h>
#endif

#include <assert.h>
#define LASSERT assert

#ifndef _LUSTRE_API_HEADER
#include <lustre/liblustreapi.h>
#else
#include <lustre/lustreapi.h>
#endif

#ifndef DFID_NOBRACE
#define DFID_NOBRACE SFID
#endif

/* missing prototypes in lustre1.8 */
#if defined(HAVE_LLAPI_GETPOOL_INFO) && !defined(_HAVE_FID)
extern int llapi_get_poollist(const char *name, char **poollist, int list_size,
                              char *buffer, int buffer_size);
extern int llapi_get_poolmembers(const char *poolname, char **members,
                                 int list_size, char *buffer, int buffer_size);
#endif

#ifndef HAVE_OBD_STATFS
struct obd_statfs {
        __u64           os_type;
        __u64           os_blocks;
        __u64           os_bfree;
        __u64           os_bavail;
        __u64           os_files;
        __u64           os_ffree;
        __u8            os_fsid[40];
        __u32           os_bsize;
        __u32           os_namelen;
        __u64           os_maxbytes;
        __u32           os_state;       /* positive error code on server */
        __u32           os_spare1;
        __u32           os_spare2;
        __u32           os_spare3;
        __u32           os_spare4;
        __u32           os_spare5;
        __u32           os_spare6;
        __u32           os_spare7;
        __u32           os_spare8;
        __u32           os_spare9;
};
#endif

#ifndef HAVE_LUSTRE_JOIN_TYPE 

/* @todo  clean management of these types: lov_user_ost_data_join et obd_statfs */

struct lov_user_ost_data_join
{                                                /* per-stripe data structure */
    __u64          l_extent_start;               /* extent start */
    __u64          l_extent_end;                 /* extent end */
    __u64          l_object_id;                  /* OST object ID */
    __u64          l_object_gr;                  /* OST object group (creating MDS number) */
    __u32          l_ost_gen;                    /* generation of this OST index */
    __u32          l_ost_idx;                    /* OST index in LOV */
} __attribute__ ( ( packed ) );

/* Identifier for a single log object */
struct llog_logid
{
    __u64          lgl_oid;
    __u64          lgl_ogr;
    __u32          lgl_ogen;
} __attribute__ ( ( packed ) );

struct lov_user_md_join
{                                                /* LOV EA user data (host-endian) */
    __u32          lmm_magic;                    /* magic number = LOV_MAGIC_JOIN */
    __u32          lmm_pattern;                  /* LOV_PATTERN_RAID0, LOV_PATTERN_RAID1 */
    __u64          lmm_object_id;                /* LOV object ID */
    __u64          lmm_object_gr;                /* LOV object group */
    __u32          lmm_stripe_size;              /* size of stripe in bytes */
    __u32          lmm_stripe_count;             /* num stripes in use for this object */
    __u32          lmm_extent_count;             /* extent count of lmm */
    __u64          lmm_tree_id;                  /* mds tree object id */
    __u64          lmm_tree_gen;                 /* mds tree object gen */
    struct llog_logid lmm_array_id;              /* mds extent desc llog object id */
    struct lov_user_ost_data_join lmm_objects[0];       /* per-stripe data */
} __attribute__ ( ( packed ) );

#endif


#ifdef HAVE_CHANGELOG_EXTEND_REC
    #define CL_REC_TYPE struct changelog_ext_rec
#else
    #define CL_REC_TYPE struct changelog_rec
#endif

#endif
