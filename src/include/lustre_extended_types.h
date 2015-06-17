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
#ifndef LPX64
#define LPX64 "%#llx"
#endif

#ifndef LPX64i
#define LPX64i "%llx"
#endif

#ifndef LPU64
#define LPU64 "%llu"
#endif

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

#ifndef LOV_MAX_STRIPE_COUNT
/* in old lustre versions, this is not defined in lustre_user.h */
#define LOV_MAX_STRIPE_COUNT 160
#endif

#ifndef DFID_NOBRACE
#define DFID_NOBRACE LPX64":0x%x:0x%x"
#endif
#ifndef XATTR_NAME_LOV
#define XATTR_NAME_LOV "trusted.lov"
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

#ifdef HAVE_CHANGELOGS

#include <stdbool.h>

/*
 * Untangle the various changes of Lustre userspace changelog
 * API. Originally, there was the "struct changelog_rec". Then the
 * "struct changelog_ext_rec" was added in Lustre 2.5, and all records
 * given to the applications were converted to that format by
 * liblustreapi. Then in Lustre 2.7, the commit 0f22e4 removed "struct
 * changelog_ext_rec" and introduced the flexible format.
 *
 * Define HAVE_FLEX_CL for 2.7 Lustre, use HAVE_CHANGELOG_EXTEND_REC
 * for Lustre 2.4->2.6, and nothing for the older versions.
 *
 * Add accessors to make sense of all that:
 *
 * rh_rename_one_record: if the changelog is a CL_RENAME,
 * rh_rename_one_record() will return false if it is followed by a
 * CL_EXT record. Since the LU-1331 fix, rename operations use only
 * one changelog record.
 *
 * rh_get_cl_cr_name(): return a pointer to cr_name
 */

#if HAVE_DECL_CLF_RENAME
/* Lustre 2.7 */
#define CL_REC_TYPE struct changelog_rec
#define HAVE_FLEX_CL            /* Flexible changelogs */

static inline bool rh_is_rename_one_record(const struct changelog_rec *rec)
{
    return rec->cr_flags & CLF_RENAME;
}

static inline char *rh_get_cl_cr_name(const struct changelog_rec *rec)
{
    return changelog_rec_name((struct changelog_rec *)rec);
}

/* This doesn't make sense anymore but it is still defined by Lustre
 * 2.7. */
#undef HAVE_CHANGELOG_EXTEND_REC

#elif HAVE_CHANGELOG_EXTEND_REC
/* Lustre 2.3 to 2.6. */
#define CL_REC_TYPE struct changelog_ext_rec

static inline bool rh_is_rename_one_record(const struct changelog_ext_rec *rec)
{
    return rec->cr_flags & CLF_EXT_VERSION;
}

static inline char *rh_get_cl_cr_name(const struct changelog_ext_rec *rec)
{
    /* Don't use changelog_rec_name() because the cr_name has been
     * moved by changelog_extend_rec(). So cr_name is always at the
     * same spot, rename or not. */
    return (char *)rec->cr_name;
}

#else
/* Lustre 2.1 to 2.2 */
#define CL_REC_TYPE struct changelog_rec

static inline bool rh_is_rename_one_record(const struct changelog_rec *rec)
{
    return false;
}

static inline char *rh_get_cl_cr_name(const struct changelog_rec *rec)
{
    return (char *)rec->cr_name;
}

#endif

#endif  /* HAVE_CHANGELOGS */

#ifdef _HAVE_FID

/* The following stuff is to decode link EA from userspace */

#include <byteswap.h>
#include <assert.h>

/* undefined types in lustre_idl */
#define be32_to_cpu(x) bswap_32(x)
#define be64_to_cpu(x) (__u64)bswap_64(x)
#define CLASSERT assert
#define LASSERTF(a,b,c) assert(a)
typedef void * lnet_nid_t;
typedef time_t cfs_time_t;

#ifdef _LUSTRE_IDL_HEADER
/* lustre_idl.h references many undefined symbols
 * in functions or structures we don't need.
 * So ignore the warnings. */
#pragma GCC push_options
#pragma GCC diagnostic ignored "-Wimplicit-function-declaration"
#include <lustre/lustre_idl.h>
#pragma GCC pop_options
#else

/* Workaround for lustre 2.6.0 (waiting for LU-3613):
 * if XATTR_NAME_LINK is not defined in Lustre headers,
 * we have to define it by ourselves until patch for LU-3613 lands.
 */
#ifndef XATTR_NAME_LINK
#define XATTR_NAME_LINK "trusted.link"
#endif

struct link_ea_header {
        __u32 leh_magic;
        __u32 leh_reccount;
        __u64 leh_len;      /* total size */
        /* future use */
        __u32 padding1;
        __u32 padding2;
};

/** Hardlink data is name and parent fid.
 * Stored in this crazy struct for maximum packing and endian-neutrality
 */
struct link_ea_entry {
        /** __u16 stored big-endian, unaligned */
        unsigned char      lee_reclen[2];
        unsigned char      lee_parent_fid[sizeof(struct lu_fid)];
        char               lee_name[0];
}__attribute__((packed));

static inline void fid_be_to_cpu(struct lu_fid *dst, const struct lu_fid *src)
{
    dst->f_seq = be64_to_cpu(src->f_seq);
    dst->f_oid = be32_to_cpu(src->f_oid);
    dst->f_ver = be32_to_cpu(src->f_ver);
}
enum fid_seq {
    FID_SEQ_OST_MDT0    = 0,
    FID_SEQ_LLOG        = 1, /* unnamed llogs */
    FID_SEQ_ECHO        = 2,
    FID_SEQ_OST_MDT1    = 3,
    FID_SEQ_OST_MAX     = 9, /* Max MDT count before OST_on_FID */
    FID_SEQ_LLOG_NAME   = 10, /* named llogs */
    FID_SEQ_RSVD        = 11,
    FID_SEQ_IGIF        = 12,
    FID_SEQ_IGIF_MAX    = 0x0ffffffffULL,
    FID_SEQ_IDIF        = 0x100000000ULL,
    FID_SEQ_IDIF_MAX    = 0x1ffffffffULL,
    FID_SEQ_START       = 0x200000000ULL,
    FID_SEQ_LOCAL_FILE  = 0x200000001ULL,
    FID_SEQ_DOT_LUSTRE  = 0x200000002ULL,
    FID_SEQ_LOCAL_NAME  = 0x200000003ULL,
    FID_SEQ_SPECIAL     = 0x200000004ULL,
    FID_SEQ_QUOTA       = 0x200000005ULL,
    FID_SEQ_QUOTA_GLB   = 0x200000006ULL,
    FID_SEQ_ROOT        = 0x200000007ULL,  /* Located on MDT0 */
    FID_SEQ_NORMAL      = 0x200000400ULL,
    FID_SEQ_LOV_DEFAULT = 0xffffffffffffffffULL
};
static inline int fid_seq_is_rsvd(const __u64 seq)
{
        return (seq > FID_SEQ_OST_MDT0 && seq <= FID_SEQ_RSVD);
};
static inline int fid_seq_is_idif(const __u64 seq)
{
        return seq >= FID_SEQ_IDIF && seq <= FID_SEQ_IDIF_MAX;
}
static inline int fid_is_idif(const struct lu_fid *fid)
{
        return fid_seq_is_idif(fid->f_seq);
}
static inline int fid_seq_is_igif(const __u64 seq)
{
        return seq >= FID_SEQ_IGIF && seq <= FID_SEQ_IGIF_MAX;
}
static inline int fid_is_igif(const struct lu_fid *fid)
{
        return fid_seq_is_igif(fid->f_seq);
}
static inline int fid_is_sane(const struct lu_fid *fid)
{
    return fid != NULL &&
           ((fid->f_seq >= FID_SEQ_START && fid->f_ver == 0) ||
        fid_is_igif(fid) || fid_is_idif(fid) ||
        fid_seq_is_rsvd(fid->f_seq));
}
#endif


struct lu_buf {
        void   *lb_buf;
        ssize_t lb_len;
};

struct linkea_data {
        /**
         ** Buffer to keep link EA body.
         **/
        struct lu_buf           *ld_buf;
        /**
         ** The matched header, entry and its lenght in the EA
         **/
        struct link_ea_header   *ld_leh;
        struct link_ea_entry    *ld_lee;
        int                     ld_reclen;
};

#define LINKEA_NEXT_ENTRY(ldata)        \
        (struct link_ea_entry *)((char *)ldata.ld_lee + ldata.ld_reclen)

#define LINKEA_FIRST_ENTRY(ldata)       \
        (struct link_ea_entry *)(ldata.ld_leh + 1)

#endif /* _HAVE_FID */


#endif
