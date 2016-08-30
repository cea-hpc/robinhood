/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2007, 2008, 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "rbh_logs.h"
#include "xplatform_print.h"
#include "Memory.h"
#include "rbh_misc.h"
#include "rbh_basename.h"

#include <errno.h>
#include <dirent.h> /* for DIR */
#include <sys/ioctl.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef _HAVE_FID
#include <attr/xattr.h> /* needed by Lustre_GetNameParent() */
#endif

#include "lustre_extended_types.h"

/* for logs */
#define TAG_STRIPE      "GetStripe"
#define TAG_CR_STRIPE   "CreateStriped"
#define TAG_OSTDF       "OST_df"
#define TAG_POOLDF      "pool_df"
#define TAG_MDSSTAT     "mds_stat"
#define TAG_FIDPATH     "FidPath"
#define TAG_LLAPI       "llapi"

#if HAVE_LLAPI_LOG_CALLBACKS
/**
 * Map LLAPI log levels to robinhood's ones.
 * The reverse mapping is performed by rbh_msg_level_convert() in rbh_logs.c
 */
static inline int lustre_msg_level_convert(enum llapi_message_level level)
{
    switch (level & LLAPI_MSG_MASK) {
    case LLAPI_MSG_FATAL:
    case LLAPI_MSG_ERROR:
        return LVL_CRIT;

    case LLAPI_MSG_WARN:
        return LVL_MAJOR;

    case LLAPI_MSG_NORMAL:
    case LLAPI_MSG_INFO:
        return LVL_VERB;

    case LLAPI_MSG_DEBUG:
    default:
        return LVL_DEBUG;
    }
}

static void display_llapi_msg(enum llapi_message_level lvl, int err,
                              const char *fmt, va_list ap)
{
    vDisplayLog(lustre_msg_level_convert(lvl), TAG_LLAPI, fmt, ap);
}
#endif

/** initialize access to lustre */
int Lustre_Init(void)
{
#if HAVE_LLAPI_MSG_LEVEL
# if HAVE_LLAPI_LOG_CALLBACKS
    rbh_adjust_log_level_external();
    llapi_error_callback_set(display_llapi_msg);
    llapi_info_callback_set(display_llapi_msg);
# else
    llapi_msg_set_level(LLAPI_MSG_OFF);
# endif
#endif
    return 0;
}

static int fill_stripe_info(struct lov_user_md *p_lum,
                            stripe_info_t *p_stripe_info,
                            stripe_items_t *p_stripe_items)
{
#ifdef LOV_USER_MAGIC_V3
    struct lov_user_md_v3 *p_lum3;
#endif
    unsigned int i;

    if (!p_lum)
        return -EFAULT;

    /* Check protocol version number */
    if (p_lum->lmm_magic == LOV_USER_MAGIC_V1) {
        if (p_stripe_info) {
            p_stripe_info->stripe_size = p_lum->lmm_stripe_size;
            p_stripe_info->stripe_count = p_lum->lmm_stripe_count;
            p_stripe_info->pool_name[0] = '\0';
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
            p_stripe_info->validator = p_lum->lmm_layout_gen;
#endif
        }

        if (p_stripe_items) {

            if (p_lum->lmm_stripe_count > 0 &&
                (p_lum->lmm_pattern & LOV_PATTERN_F_RELEASED) == 0) {
                p_stripe_items->count = p_lum->lmm_stripe_count;
                p_stripe_items->stripe =
                    (stripe_item_t *) MemCalloc(p_lum->lmm_stripe_count,
                                                sizeof(stripe_item_t));

                if (p_stripe_items->stripe == NULL)
                    return -ENOMEM;

                /* fill OST ids */
                for (i = 0; i < p_lum->lmm_stripe_count; i++) {
                    p_stripe_items->stripe[i].ost_idx =
                        p_lum->lmm_objects[i].l_ost_idx;
                    p_stripe_items->stripe[i].ost_gen =
                        p_lum->lmm_objects[i].l_ost_gen;
#ifdef HAVE_OBJ_ID
                    p_stripe_items->stripe[i].obj_id =
                        p_lum->lmm_objects[i].l_object_id;
#ifdef HAVE_OBJ_SEQ
                    p_stripe_items->stripe[i].obj_seq =
                        p_lum->lmm_objects[i].l_object_seq;
#else
                    p_stripe_items->stripe[i].obj_seq =
                        p_lum->lmm_objects[i].l_object_gr;
#endif
#else /* new structure (union of fid and id/seq) */
                    p_stripe_items->stripe[i].obj_id =
                        p_lum->lmm_objects[i].l_ost_oi.oi.oi_id;
                    p_stripe_items->stripe[i].obj_seq =
                        p_lum->lmm_objects[i].l_ost_oi.oi.oi_seq;
#endif
                }
            } else {
                p_stripe_items->count = 0;
                p_stripe_items->stripe = NULL;
            }
        }

        return 0;

    }
#ifdef LOV_USER_MAGIC_V3
    else if (p_lum->lmm_magic == LOV_USER_MAGIC_V3) {
        p_lum3 = (struct lov_user_md_v3 *)p_lum;

        if (p_stripe_info) {
            p_stripe_info->stripe_size = p_lum3->lmm_stripe_size;
            p_stripe_info->stripe_count = p_lum3->lmm_stripe_count;
            strncpy(p_stripe_info->pool_name, p_lum3->lmm_pool_name,
                    LOV_MAXPOOLNAME);
            p_stripe_info->pool_name[MAX_POOL_LEN - 1] = '\0';
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
            p_stripe_info->validator = p_lum3->lmm_layout_gen;
#endif
        }

        if (p_stripe_items) {
            if (p_lum3->lmm_stripe_count > 0 &&
                (p_lum3->lmm_pattern & LOV_PATTERN_F_RELEASED) == 0) {
                p_stripe_items->count = p_lum3->lmm_stripe_count;
                p_stripe_items->stripe =
                    (stripe_item_t *) MemCalloc(p_lum3->lmm_stripe_count,
                                                sizeof(stripe_item_t));

                if (p_stripe_items->stripe == NULL)
                    return -ENOMEM;

                /* fill OST ids */
                for (i = 0; i < p_lum3->lmm_stripe_count; i++) {
                    p_stripe_items->stripe[i].ost_idx =
                        p_lum3->lmm_objects[i].l_ost_idx;
                    p_stripe_items->stripe[i].ost_gen =
                        p_lum3->lmm_objects[i].l_ost_gen;
#ifdef HAVE_OBJ_ID
                    p_stripe_items->stripe[i].obj_id =
                        p_lum3->lmm_objects[i].l_object_id;
#ifdef HAVE_OBJ_SEQ
                    p_stripe_items->stripe[i].obj_seq =
                        p_lum3->lmm_objects[i].l_object_seq;
#else
                    p_stripe_items->stripe[i].obj_seq =
                        p_lum3->lmm_objects[i].l_object_gr;
#endif
#else /* new structure (union of fid and id/seq) */
                    p_stripe_items->stripe[i].obj_id =
                        p_lum3->lmm_objects[i].l_ost_oi.oi.oi_id;
                    p_stripe_items->stripe[i].obj_seq =
                        p_lum3->lmm_objects[i].l_ost_oi.oi.oi_seq;
#endif
                }
            } else {
                p_stripe_items->count = 0;
                p_stripe_items->stripe = NULL;
            }
        }

        return 0;
    }
#endif
    else {
        DisplayLog(LVL_CRIT, TAG_STRIPE,
                   "Unsupported Luster magic number from getstripe: %#X",
                   p_lum->lmm_magic);
        return -EINVAL;
    }
}

static void set_empty_stripe(stripe_info_t *p_stripe_info,
                             stripe_items_t *p_stripe_items)
{
    if (p_stripe_info) {
        memset(p_stripe_info, 0, sizeof(stripe_info_t));
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
        p_stripe_info->validator = VALID_NOSTRIPE;
#endif
    }

    if (p_stripe_items)
        memset(p_stripe_items, 0, sizeof(stripe_items_t));
}

#define LUM_SIZE_MAX (sizeof(struct lov_user_md_v3) + \
                  (LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1)))

int File_GetStripeByPath(const char *entry_path, stripe_info_t *p_stripe_info,
                         stripe_items_t *p_stripe_items)
{
    int rc;
    struct lov_user_md *p_lum;

    if (!entry_path || !entry_path[0])
        return -EFAULT;

    p_lum = (struct lov_user_md *)MemAlloc(LUM_SIZE_MAX);
    if (!p_lum)
        return -ENOMEM;

    memset(p_lum, 0, LUM_SIZE_MAX);
    rc = llapi_file_get_stripe(entry_path, p_lum);

    if (rc != 0) {
        if (rc == -ENODATA) {
            DisplayLog(LVL_DEBUG, TAG_STRIPE,
                       "File %s has no stripe information", entry_path);
            set_empty_stripe(p_stripe_info, p_stripe_items);
            rc = 0;
        } else if ((rc != -ENOENT) && (rc != -ESTALE))
            DisplayLog(LVL_CRIT, TAG_STRIPE,
                       "Error %d getting stripe info for %s", rc, entry_path);
        goto out_free;
    }

    rc = fill_stripe_info(p_lum, p_stripe_info, p_stripe_items);

 out_free:
    MemFree(p_lum);
    return rc;
}

int File_GetStripeByDirFd(int dirfd, const char *fname,
                          stripe_info_t *p_stripe_info,
                          stripe_items_t *p_stripe_items)
{
    int rc = 0;
    struct lov_user_md *p_lum;

    if (!fname || !fname[0])
        return -EFAULT;

    p_lum = MemAlloc(LUM_SIZE_MAX);
    if (!p_lum)
        return -ENOMEM;

    strcpy((char *)p_lum, fname);
    rc = ioctl(dirfd, IOC_MDC_GETFILESTRIPE, p_lum);
    if (rc == 0) {
        rc = fill_stripe_info(p_lum, p_stripe_info, p_stripe_items);
    } else {
        rc = -errno;

        if (rc == -ENODATA) {
            DisplayLog(LVL_DEBUG, TAG_STRIPE,
                       "File %s has no stripe information", fname);
            set_empty_stripe(p_stripe_info, p_stripe_items);
            rc = 0;
        } else if ((rc != -ENOENT) && (rc != -ESTALE)) {
            DisplayLog(LVL_CRIT, TAG_STRIPE,
                       "Error %d getting stripe info for %s", rc, fname);
        }
    }

    MemFree(p_lum);

    return rc;
}

/**
 * check if a file has data on the given OST.
 */
bool DataOnOST(size_t fsize, unsigned int ost_index,
               const stripe_info_t *sinfo, const stripe_items_t *sitems)
{
    unsigned int stripe_blocks, i;

    /* if file is empty, the answer is obviously NO */
    if (fsize == 0)
        return false;
    /* If file size is > (stripe_count-1)*stripe_size, it has at least
     * one byte on the last stripe, and a full stripe block on all previous
     * OSTs the answer is yes.
     * Note: this test works if stripe count is 1, and file is > 0.
     */
    else if (fsize > (sinfo->stripe_count - 1) * sinfo->stripe_size)
        return true;

    /* insane value, file may not be striped */
    if (sinfo->stripe_size == 0)
        return false;

    /* In the remaining cases, we must check stripe_items.
     * First compute the number of full stripe blocks,
     * and the remaining piece of data.
     */
    stripe_blocks = fsize / sinfo->stripe_size;
    /* one more block? */
    if (fsize % sinfo->stripe_size)
        stripe_blocks++;

    /* check if ost_index is in the first stripe_blocks */
    for (i = 0; i < sinfo->stripe_count && i < stripe_blocks; i++) {
        if (sitems->stripe[i].ost_idx == ost_index)
            return true;
    }
    /* no matched OST */
    return false;
}

#define div_upper_round(_n, _d) (((_n)/(_d)) + ((_n) % (_d) ? 1 : 0))

/** computes blocks on the given OST */
blkcnt_t BlocksOnOST(blkcnt_t blocks, unsigned int ost_index,
                     const stripe_info_t *sinfo, const stripe_items_t *sitems)
{
    unsigned long full_stripes, last_stripe_size, match_full, extra_blocks = 0;
    int i;
    int stripe_index = -1;

    /* if block=0 the answer is obviously 0 */
    if (blocks == 0)
        return 0;
    /* unsane value, file may not be stripped */
    if (sinfo->stripe_size == 0)
        return 0;

    if ((sinfo->stripe_size % DEV_BSIZE) != 0) {
        DisplayLog(LVL_CRIT, __func__,
                   "Unexpected stripe_size value %lu: not a multiple of DEV_BSIZE (%u)",
                   sinfo->stripe_size, DEV_BSIZE);
        return 0;
    }

    /* what is the stripe index for this OST? */
    for (i = 0; i < sinfo->stripe_count; i++) {
        if (sitems->stripe[i].ost_idx == ost_index) {
            stripe_index = i;
            break;
        }
    }
    if (stripe_index == -1)
        /* no data on the given OST */
        return 0;

    full_stripes = (blocks * DEV_BSIZE) / sinfo->stripe_size;
    last_stripe_size = (blocks * DEV_BSIZE) % sinfo->stripe_size;

    /* how many full stripes for the given index? */
    match_full = (full_stripes - stripe_index) / sinfo->stripe_count;
    /* + an extra stripe? */
    if (((full_stripes - stripe_index) % sinfo->stripe_count) > 0)
        match_full++;
    else
        /* last full stripe is just before this OST: extra blocks are on it */
        extra_blocks = div_upper_round(last_stripe_size, DEV_BSIZE);

    /* return value (in blocks):
     * match_full * stripe_size / DEV_BSIZE + extra_blocks
     */
    return match_full * (sinfo->stripe_size / DEV_BSIZE) + extra_blocks;
}

#ifdef HAVE_LLAPI_GETPOOL_INFO
int CreateStriped(const char *path, const stripe_info_t *old_stripe,
                  int overwrite)
{
    int rc;

    /* try to restripe using previous pool name */
    if (!EMPTY_STRING(old_stripe->pool_name))
        rc = llapi_file_create_pool(path, old_stripe->stripe_size,
                                    -1, old_stripe->stripe_count, 0,
                                    (char *)old_stripe->pool_name);
    else
        rc = llapi_file_create(path, old_stripe->stripe_size,
                               -1, old_stripe->stripe_count, 0);
    if ((rc == -EEXIST) && overwrite) {
        if (unlink(path)) {
            rc = -errno;
            DisplayLog(LVL_MAJOR, TAG_CR_STRIPE,
                       "Can't remove previous entry %s: %s", path,
                       strerror(-rc));
            return rc;
        }
        return CreateStriped(path, old_stripe,
                             false /*target not expected to exist */);
    } else if (rc != 0 && rc != -EEXIST) {
        DisplayLog(LVL_MAJOR, TAG_CR_STRIPE,
                   "Error %d creating '%s' with stripe.", rc, path);
    }
    return rc;
}

/* create a file with no stripe information */
int CreateWithoutStripe(const char *path, mode_t mode, int overwrite)
{
    int rc;
    int fd = open(path, O_CREAT | O_LOV_DELAY_CREATE, mode);
    if (fd < 0) {
        rc = -errno;
        if (rc == -EEXIST && overwrite) {
            if (unlink(path)) {
                rc = -errno;
                DisplayLog(LVL_MAJOR, TAG_CR_STRIPE,
                           "Can't remove previous entry %s: %s", path,
                           strerror(-rc));
                return rc;
            }
            return CreateWithoutStripe(path, mode,
                                       false /*target not expected to exist */
                                       );
        }
        DisplayLog(LVL_MAJOR, TAG_CR_STRIPE,
                   "Failed to create %s without striping information: %s", path,
                   strerror(-rc));
        return rc;
    }
    close(fd);
    return 0;
}
#endif

#ifdef _HAVE_FID

#define FIDDIR      ".lustre/fid"

/**
 * Build .lustre/fid path associated to a handle.
 */
int BuildFidPath(const entry_id_t *p_id,   /* IN */
                 char *path)
{   /* OUT */
    if (!p_id || !path)
        return EFAULT;

    sprintf(path, "%s" DFID, get_fid_dir(), PFID(p_id));

#ifdef _DEBUG
    DisplayLog(LVL_FULL, TAG_FIDPATH, "FidPath=%s", path);
#endif

    return 0;
}

/* Get POSIX path from fid (fid2path wrapper) */
int Lustre_GetFullPath(const entry_id_t *p_id, char *fullpath,
                       unsigned int len)
{
    char *curr = fullpath;
    int rc;
    long long recno = -1;
    int linkno = 0;
    char fid[256];
    const char *mpath = NULL;
    unsigned int mlen = 0;

    mpath = get_mount_point(&mlen);

    /* set mountpoint at the beginning of the path */
    strcpy(fullpath, mpath);
    curr += mlen;

/* add the slash only if fid2path doesn't */
#ifndef _FID2PATH_LEADING_SLASH
    /* add slash */
    *curr = '/';
    curr++;
#endif
    /* just in case fid2path returns nothing */
    *curr = '\0';

    /* fid string */
    sprintf(fid, DFID, PFID(p_id));

    /* MDT device */

    /* ask the path to lustre */
    rc = llapi_fid2path(mpath, fid, curr, len - mlen - 2, &recno, &linkno);

    if ((rc != 0) && (rc != -ENOENT) && (rc != -ESTALE))
        DisplayLog(LVL_CRIT, "Fid2Path",
                   "Error %d calling llapi_fid2path(%s,%s,%lld,%d), errno=%d."
                   " Cannot retrieve full path for %s",
                   rc, mpath, fid, recno, linkno, errno, fid);
    /* curr == fullpath => fullpath is root: '/'
     * so don't remove final slash */
    else if (curr != fullpath) {
        while (FINAL_SLASH(fullpath))
            REMOVE_FINAL_SLASH(fullpath);
    }

    return rc;
}

/* Get fid from Posix Path (path2fid wrapper) */
int Lustre_GetFidFromPath(const char *fullpath, entry_id_t *p_id)
{
    int rc;
    rc = llapi_path2fid(fullpath, p_id);

    if ((rc != 0) && (rc != -ENOENT) && (rc != -ESTALE))
        DisplayLog(LVL_DEBUG, "Path2Fid", "llapi_path2fid(%s)=%d, seq=%llx,"
                   " oid=%x, ver=%x",
                   fullpath, rc, p_id->f_seq, p_id->f_oid, p_id->f_ver);

    return rc;
}

/* Get fid from a file descriptor (fd2fid wrapper) */
int Lustre_GetFidByFd(int fd, entry_id_t *p_id)
{
    int rc = 0;
#ifdef HAVE_FD2FID
#define FD2FID "llapi_fd2fid()"
    rc = llapi_fd2fid(fd, p_id);
#else
#define FD2FID "ioctl(LL_IOC_PATH2FID)"
    if (ioctl(fd, LL_IOC_PATH2FID, p_id) != 0)
        rc = -errno;
#endif

    if ((rc != 0) && (rc != -ENOENT) && (rc != -ESTALE))
        DisplayLog(LVL_DEBUG, "Fd2Fid", FD2FID "=%d", rc);

    return rc;
}

/** get (name+parent_id) for an entry
 * \param linkno hardlink index
 * \retval -ENODATA after last link
 * \retval -ERANGE if namelen is too small
 */
int Lustre_GetNameParent(const char *path, int linkno,
                         lustre_fid *pfid, char *name, int namelen)
{
    int rc, i, len;
    char buf[4096];
    struct linkea_data ldata = { 0 };
    struct lu_buf lb = { 0 };

    rc = lgetxattr(path, XATTR_NAME_LINK, buf, sizeof(buf));
    if (rc < 0)
        return -errno;

    lb.lb_buf = buf;
    lb.lb_len = sizeof(buf);
    ldata.ld_buf = &lb;
    ldata.ld_leh = (struct link_ea_header *)buf;

    ldata.ld_lee = LINKEA_FIRST_ENTRY(ldata);
    ldata.ld_reclen = (ldata.ld_lee->lee_reclen[0] << 8)
        | ldata.ld_lee->lee_reclen[1];

    if (linkno >= ldata.ld_leh->leh_reccount)
        /* beyond last link */
        return -ENODATA;

    for (i = 0; i < linkno; i++) {
        ldata.ld_lee = LINKEA_NEXT_ENTRY(ldata);
        ldata.ld_reclen = (ldata.ld_lee->lee_reclen[0] << 8)
            | ldata.ld_lee->lee_reclen[1];
    }

    memcpy(pfid, &ldata.ld_lee->lee_parent_fid, sizeof(*pfid));
    fid_be_to_cpu(pfid, pfid);

    if (!fid_is_sane(pfid)) {
        DisplayLog(LVL_MAJOR, __func__, "insane fid: " DFID, PFID(pfid));
        return -EPROTO;
    }

    len = ldata.ld_reclen - sizeof(struct link_ea_entry);
    if (len >= namelen)
        return -ERANGE;

    strncpy(name, ldata.ld_lee->lee_name, len);
    name[len] = '\0';
    return 0;
}
#endif

/** Retrieve OST usage info ('ost df')
 *  @return 0 on success
 *          ENODEV if ost_index > ost index max of this FS
 */
int Get_OST_usage(const char *fs_path, unsigned int ost_index,
                  struct statfs *ost_statfs)
{
    struct obd_statfs stat_buf;
    struct obd_uuid uuid_buf;
    int rc;

    /* sanity check */
    if (!ost_statfs)
        return EFAULT;
    memset(&stat_buf, 0, sizeof(struct obd_statfs));
    memset(&uuid_buf, 0, sizeof(struct obd_uuid));

    /* zero the output */
    memset(ost_statfs, 0, sizeof(struct statfs));

    /* llapi_obd_statfs does not modify path (checked in code) */
    rc = llapi_obd_statfs((char *)fs_path, LL_STATFS_LOV, ost_index, &stat_buf,
                          &uuid_buf);

    if (rc == -ENODEV)
        /* end of list */
        return -rc;
    else if (rc == -EAGAIN) {
        /* gap in OST indexes? */
        DisplayLog(LVL_EVENT, TAG_OSTDF,
                   "OST #%u does not exist in filesystem %s", ost_index,
                   fs_path);
        return -rc;
    } else if (rc != 0) {
        /* other error */
        DisplayLog(LVL_CRIT, TAG_OSTDF,
                   "Error %d in llapi_obd_statfs(). Cannot retrieve info"
                   " about OST #%u", -rc, ost_index);
        return -rc;
    }

    /* convert info to struct statfs */
    ost_statfs->f_bsize = stat_buf.os_bsize;
    ost_statfs->f_blocks = stat_buf.os_blocks;
    ost_statfs->f_bfree = stat_buf.os_bfree;
    ost_statfs->f_bavail = stat_buf.os_bavail;

    return 0;
}

#ifdef HAVE_LLAPI_GETPOOL_INFO
/** Retrieve pool usage info
 *  @return 0 on success
 */
int Get_pool_usage(const char *poolname, struct statfs *pool_statfs)
{
    struct statfs ost_statfs;
    int rc, i, count;
    char pool[LOV_MAXPOOLNAME + 10];
#ifdef FIND_MAX_OSTS
    char *ostlist[FIND_MAX_OSTS];
    char buffer[4096];
#else /* no max OST count since Lustre 2.2 */
    unsigned int obdcount = 256;
    char **ostlist = NULL;
    int bufsize = sizeof(struct obd_uuid) * obdcount;
    char *buffer = MemAlloc(bufsize + (sizeof(*ostlist) * obdcount));
    ostlist = (char **)(buffer + bufsize);

    /* sanity check */
    if (!pool_statfs) {
        MemFree(buffer);
        return EFAULT;
    }
#endif

    memset(pool_statfs, 0, sizeof(struct statfs));

    /* retrieve list of OSTs in the pool */
    sprintf(pool, "%s.%s", get_fsname(), poolname);
#ifdef FIND_MAX_OSTS
    rc = llapi_get_poolmembers(pool, ostlist, FIND_MAX_OSTS, buffer, 4096);
#else
    do {
        rc = llapi_get_poolmembers(pool, ostlist, obdcount, buffer, bufsize);
        if (rc == -EOVERFLOW) {
            /* buffer too small, increase obdcount by 2 */
            obdcount *= 2;
            bufsize = sizeof(struct obd_uuid) * obdcount;
            buffer =
                MemRealloc(buffer, bufsize + (sizeof(*ostlist) * obdcount));
            if (buffer == NULL)
                return ENOMEM;
            ostlist = (char **)(buffer + bufsize);
        }
    } while (rc == -EOVERFLOW);
#endif

    if (rc < 0)
        return -rc;

    count = rc;

    /* get OST info and sum them */
    for (i = 0; i < count; i++) {
        char *ost;
        int index;
        /* get ost index in <fsname>-OST<index>_UUID */
        ost = strrchr(ostlist[i], '-');
        if (!ost) {
            DisplayLog(LVL_CRIT, TAG_POOLDF, "Invalid OST format: '%s'",
                       ostlist[i]);
            return EINVAL;
        }

        /* skip '-' */
        ost++;
        if (sscanf(ost, "OST%d", &index) != 1) {
            DisplayLog(LVL_CRIT, TAG_POOLDF, "Could not find OST index in"
                       " string '%s'", ost);
            return EINVAL;
        }

        rc = Get_OST_usage(get_mount_point(NULL), index, &ost_statfs);
        if (rc)
            return rc;

        /* sum info to struct statfs */
        pool_statfs->f_blocks += ost_statfs.f_blocks;
        pool_statfs->f_bfree += ost_statfs.f_bfree;
        pool_statfs->f_bavail += ost_statfs.f_bavail;
        pool_statfs->f_bsize = ost_statfs.f_bsize;
    }

    return 0;
}
#endif

/* This code is an adaptation of llapi_mds_getfileinfo() in liblustreapi.
 * It is unused for now, but could be useful when SOM will be implemented.
 */
int lustre_mds_stat(const char *fullpath, int parentfd, struct stat *inode)
{
    /* this buffer must be large enough for handling filename */
    char buffer[1024];
    struct lov_user_mds_data *lmd = (struct lov_user_mds_data *)buffer;
    const char *filename;
    int rc;

    /* sanity checks */
    if ((fullpath == NULL) || (inode == NULL))
        return EINVAL;

    filename = rh_basename(fullpath);

    memset(lmd, 0, sizeof(buffer));
    rh_strncpy(buffer, filename, strlen(filename) + 1);
    rc = ioctl(parentfd, IOC_MDC_GETFILEINFO, (void *)lmd);

    if (rc) {
        if (errno == ENOTTY) {
            /* ioctl is not supported, it is not a lustre fs.
             * Do the regular lstat(2) instead. */
            rc = lstat(fullpath, inode);
            if (rc) {
                DisplayLog(LVL_CRIT, TAG_MDSSTAT,
                           "Error: %s: lstat failed for %s", __func__,
                           fullpath);
                return rc;
            }
        } else if ((errno == ENOENT) || (errno == ESTALE)) {
            DisplayLog(LVL_MAJOR, TAG_MDSSTAT, "Warning: %s: %s does not exist",
                       __func__, fullpath);
            return ENOENT;
        } else {
            DisplayLog(LVL_CRIT, TAG_MDSSTAT,
                       "Error: %s: IOC_MDC_GETFILEINFO failed for %s: rc=%d, errno=%d",
                       __func__, fullpath, rc, errno);
            return rc;
        }
    } else
        *inode = lmd->lmd_st;

    return 0;

}

#ifdef _HAVE_FID
static DIR *fid_dir_fd = NULL;
static pthread_mutex_t dir_lock = PTHREAD_MUTEX_INITIALIZER;

int lustre_mds_stat_by_fid(const entry_id_t *p_id, struct stat *inode)
{
    char filename[MAXNAMLEN];
    char buffer[1024];
    struct lov_user_mds_data *lmd = (struct lov_user_mds_data *)buffer;
    int rc;

    /* ensure fid directory is opened */
    if (fid_dir_fd == NULL) {
        P(dir_lock);
        if (fid_dir_fd == NULL) {
            char path[RBH_PATH_MAX];
            char *curr = path;
            unsigned int mlen;

            /* filesystem root */
            strcpy(path, get_mount_point(&mlen));
            curr += mlen;

            /* fid directory */
            strcpy(curr, "/" FIDDIR);

            /* open fir directory */
            fid_dir_fd = opendir(path);
        }
        V(dir_lock);
        if (fid_dir_fd == NULL)
            return errno;
    }

    sprintf(filename, DFID, PFID(p_id));
    memset(lmd, 0, sizeof(buffer));
    rh_strncpy(buffer, filename, strlen(filename) + 1);

    rc = ioctl(dirfd(fid_dir_fd), IOC_MDC_GETFILEINFO, (void *)lmd);

    if (rc) {
        if (errno == ENOTTY) {
            return ENOTSUP;
        } else if ((errno == ENOENT) || (errno == ESTALE)) {
            DisplayLog(LVL_MAJOR, TAG_MDSSTAT, "Warning: %s: %s does not exist",
                       __func__, filename);
            return ENOENT;
        } else {
            DisplayLog(LVL_CRIT, TAG_MDSSTAT,
                       "Error: %s: IOC_MDC_GETFILEINFO failed for %s",
                       __func__, filename);
            return errno;
        }
    }

    *inode = lmd->lmd_st;
    return 0;
}
#endif

#define BRIEF_OST_FORMAT "ost#%u:%u"
#define HUMAN_OST_FORMAT "ost#%u: %u"

void append_stripe_list(GString *str, const stripe_items_t *p_stripe_items,
                        bool brief)
{
    const char *format;
    int i;

    if (!p_stripe_items || (p_stripe_items->count == 0)) {
        g_string_append(str, "(none)");
        return;
    }

    format = brief ? BRIEF_OST_FORMAT ", " : HUMAN_OST_FORMAT ", ";

    for (i = 0; i < p_stripe_items->count; i++) {
        /* no comma after last item */
        if (i == p_stripe_items->count - 1)
            format = brief ? BRIEF_OST_FORMAT : HUMAN_OST_FORMAT;

        g_string_append_printf(str, format, p_stripe_items->stripe[i].ost_idx,
                               p_stripe_items->stripe[i].obj_id);
    }
}

#ifndef _MDT_SPECIFIC_LOVEA
/**
 * build LOVEA buffer from stripe information
 * @return size of significant information in buffer.
 */
ssize_t BuildLovEA(const entry_id_t *p_id, const attr_set_t *p_attrs,
                   void *buff, size_t buf_sz)
{
    int i;
    size_t len = 0;

    if (!ATTR_MASK_TEST(p_attrs, stripe_info))  /* no stripe info */
        return 0;

    /* check inconsistent values */
    if (!ATTR_MASK_TEST(p_attrs, stripe_items) ||
        (ATTR(p_attrs, stripe_items).count !=
         ATTR(p_attrs, stripe_info).stripe_count)) {
        DisplayLog(LVL_MAJOR, "BuildLovEA",
                   "ERROR: inconsistent stripe info for " DFID, PFID(p_id));
        return -1;
    }

    /* is there a pool? */
    if (EMPTY_STRING(ATTR(p_attrs, stripe_info).pool_name)) {
        /* no => build lov_user_md_v1 */
        struct lov_user_md_v1 *p_lum = (struct lov_user_md_v1 *)buff;
        len = sizeof(struct lov_user_md_v1) +
            ATTR(p_attrs,
                 stripe_info).stripe_count *
            sizeof(struct lov_user_ost_data_v1);

        /* check buffer size */
        if (buf_sz < len)
            return -1;

        p_lum->lmm_magic = LOV_USER_MAGIC_V1;
        p_lum->lmm_pattern = LOV_PATTERN_RAID0; /* the only supported for now */
#ifdef _HAVE_FID
        p_lum->lmm_object_id = p_id->f_oid;
        p_lum->lmm_object_seq = p_id->f_seq;
#else /* lmm_object_gr for Lustre 1.x */
        p_lum->lmm_object_id = p_id->inode;
        p_lum->lmm_object_gr = 0;
#endif
        p_lum->lmm_stripe_size = ATTR(p_attrs, stripe_info).stripe_size;
        p_lum->lmm_stripe_count = ATTR(p_attrs, stripe_info).stripe_count;
        p_lum->lmm_stripe_offset = 0;

        /* set stripe items */
        for (i = 0; i < ATTR(p_attrs, stripe_items).count; i++) {
            p_lum->lmm_objects[i].l_ost_idx =
                ATTR(p_attrs, stripe_items).stripe[i].ost_idx;
            p_lum->lmm_objects[i].l_ost_gen =
                ATTR(p_attrs, stripe_items).stripe[i].ost_gen;
#ifdef HAVE_OBJ_ID
            p_lum->lmm_objects[i].l_object_id =
                ATTR(p_attrs, stripe_items).stripe[i].obj_id;
#ifdef HAVE_OBJ_SEQ
            p_lum->lmm_objects[i].l_object_seq =
                ATTR(p_attrs, stripe_items).stripe[i].obj_seq;
#else
            p_lum->lmm_objects[i].l_object_gr =
                ATTR(p_attrs, stripe_items).stripe[i].obj_seq;
#endif
#else /* new structure (union of fid and id/seq) */
            p_lum->lmm_objects[i].l_ost_oi.oi.oi_id =
                ATTR(p_attrs, stripe_items).obj_id;
            p_lum->lmm_objects[i].l_ost_oi.oi.oi_seq =
                ATTR(p_attrs, stripe_items).stripe[i].obj_seq;
#endif
        }
        return len;
    } else {
        /* yes => build lov_user_md_v3 */
        struct lov_user_md_v3 *p_lum = (struct lov_user_md_v3 *)buff;
        len = sizeof(struct lov_user_md_v3) +
            ATTR(p_attrs,
                 stripe_info).stripe_count *
            sizeof(struct lov_user_ost_data_v1);

        /* check buffer size */
        if (buf_sz < len)
            return (size_t)-1;

        p_lum->lmm_magic = LOV_USER_MAGIC_V3;
        p_lum->lmm_pattern = LOV_PATTERN_RAID0; /* the only supported for now */
#ifdef _HAVE_FID
        p_lum->lmm_object_id = p_id->f_oid;
        p_lum->lmm_object_seq = p_id->f_seq;
#else /* lmm_object_gr for Lustre 1.x */
        p_lum->lmm_object_id = p_id->inode;
        p_lum->lmm_object_gr = 0;
#endif
        p_lum->lmm_stripe_size = ATTR(p_attrs, stripe_info).stripe_size;
        p_lum->lmm_stripe_count = ATTR(p_attrs, stripe_info).stripe_count;
        p_lum->lmm_stripe_offset = 0;
        /* pool name */
        rh_strncpy(p_lum->lmm_pool_name, ATTR(p_attrs, stripe_info).pool_name,
                   LOV_MAXPOOLNAME);

        /* set stripe items */
        for (i = 0; i < ATTR(p_attrs, stripe_items).count; i++) {
            p_lum->lmm_objects[i].l_ost_idx =
                ATTR(p_attrs, stripe_items).stripe[i].ost_idx;
            p_lum->lmm_objects[i].l_ost_gen =
                ATTR(p_attrs, stripe_items).stripe[i].ost_gen;
#ifdef HAVE_OBJ_ID
            p_lum->lmm_objects[i].l_object_id =
                ATTR(p_attrs, stripe_items).stripe[i].obj_id;
#ifdef HAVE_OBJ_SEQ
            p_lum->lmm_objects[i].l_object_seq =
                ATTR(p_attrs, stripe_items).stripe[i].obj_seq;
#else
            p_lum->lmm_objects[i].l_object_gr =
                ATTR(p_attrs, stripe_items).stripe[i].obj_seq;
#endif
#else /* new structure (union of fid and id/seq) */
            p_lum->lmm_objects[i].l_ost_oi.oi.oi_id =
                ATTR(p_attrs, stripe_items).obj_id;
            p_lum->lmm_objects[i].l_ost_oi.oi.oi_seq =
                ATTR(p_attrs, stripe_items).stripe[i].obj_seq;
#endif
        }
        return len;
    }
}
#endif
