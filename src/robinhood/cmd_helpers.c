/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 * Copyright 2013 Cray Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * Command for recovering filesystem content after a disaster (backup flavor)
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <libgen.h>             /* Posix versions of dirname/basename */

#include "cmd_helpers.h"
#include "rbh_cfg.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "Memory.h"
#include "xplatform_print.h"

#define SCRUB_TAG "Scrubber"
#define P2ID_TAG "Path2Id"

/* Initially empty array. This is a LIFO array; oldest elements are
 * stacked from the last entry to the first. When element 0 is
 * occupied, it is time to increase the size of the array. */
static wagon_t * dir_array = NULL;
static unsigned int array_len; /* number of elements in array. */
static unsigned int array_first; /* index of first valid element in array. */
#define array_used (array_len-array_first)

#define LS_CHUNK    1 /* TODO - ListMgr_GetChild will get confused if not 1 */

static size_t what_2_power(size_t s)
{
    size_t c = 1;
    while (c < s)
        c <<= 1;
    return c;
}

/* Copy the ids and names array. */
static void copy_arrays(const wagon_t *src,
                        wagon_t *dest, int dst_first,
                        int count)
{
    int src_first = 0;

    while(count) {
        dest[dst_first].id = src[src_first].id;
        dest[dst_first].fullname = strdup(src[src_first].fullname);

        src_first ++;
        dst_first ++;
        count --;
    }
}

/** add a list of ids to the scrubbing array */
static int add_id_list(const wagon_t * list,
                       unsigned int count)
{
    /* always add at the beginning to have LIFO behavior */

    /* is there enough room before the first item ? */
    if (count <= array_first)
    {
        /* copy it just before the head (entries must be consecutive) */
        copy_arrays(list, dir_array, array_first-count, count);

        array_first -= count;

#ifdef _DEBUG_ID_LIST
        printf("1)...<new_ids:%u-%u><ids:%u-%u>...(len=%u)\n",
               array_first, array_first+count-1,
               array_first+count, array_len-1, array_len);
#endif
    }
    /* is the array empty ?*/
    else if ((array_used == 0) && (count <= array_len))
    {
        /* copy from the beginning */
        copy_arrays(list, dir_array, array_len - count, count);
        array_first = array_len - count;

#ifdef _DEBUG_ID_LIST
        printf("2) <new_ids:%u-%u>...(len=%u)\n",
               array_first, array_len-1, array_len);
#endif
    }
    else /* increase array size */
    {
        wagon_t * dir_array_new;
        size_t new_len = what_2_power(array_len + count);

        dir_array_new = MemAlloc(new_len * sizeof(wagon_t));
        if (!dir_array_new)
            return -ENOMEM;

        /* First, transfer current ids and names */
        if (dir_array) {
            if (array_used)
                memcpy(&dir_array_new[new_len-array_used], &dir_array[array_first],
                       array_used * sizeof(wagon_t));
            MemFree(dir_array);
        }

        /* update array info */
        dir_array = dir_array_new;
        array_first = new_len-array_used;
        array_len = new_len;

        /* Then copy new ids */
        copy_arrays(list, dir_array, array_first-count, count);
        array_first -= count;

#ifdef _DEBUG_ID_LIST
        printf("3)...<ids:%u-%u>...(len=%u)\n",
                array_first, array_len-1, array_len);
#endif
    }
    return 0;
}

/** release a list of ids from the array */
static inline void rbh_scrub_release_list(unsigned int first, unsigned int count)
{
    free_wagon(dir_array, first, first+count);

    if (first != array_first)
        DisplayLog(LVL_CRIT, SCRUB_TAG, "IMPLEMENTATION ISSUE: array_first was %u, is now %u\n",
                   first, array_first);
    array_first += count;

#ifdef _DEBUG_ID_LIST
    printf("released %u-%u\n", array_first - count, array_first - 1);
#endif
}


/** scan sets of directories
 * \param cb_func, callback function for each set of directory
 */
int rbh_scrub(lmgr_t   * p_mgr, const wagon_t * id_list,
              unsigned int id_count, uint64_t dir_attr_mask,
              scrub_callback_t cb_func,
              void * arg)
{
    wagon_t * curr_array;
    unsigned int count;
    lmgr_filter_t  filter;
    filter_value_t fv;
    int i, rc;
    int last_err = 0;

    rc = add_id_list(id_list, id_count);
    if (rc)
        return rc;

    /* only get subdirs (for scanning) */
    fv.value.val_str = STR_TYPE_DIR;
    lmgr_simple_filter_init( &filter );
    lmgr_simple_filter_add( &filter, ATTR_INDEX_type, EQUAL, fv, 0 );

    /* while the array is not empty */
    while (array_used > 0)
    {
        unsigned int res_count = 0;
        wagon_t * child_ids;
        attr_set_t * child_attrs;

        /* get a set of entry_ids */
        curr_array = &dir_array[array_first];
        if (array_used < LS_CHUNK)
        {
            /* get all available dirs */
            count = array_used;
        }
        else
        {
            /* get a constant chunk */
            count = LS_CHUNK;
        }

#ifdef _DEBUG_ID_LIST
        printf("processing %u-%u\n", array_first, array_first+count-1);
#endif

        /* read childs */
        res_count = 0;
        child_ids = NULL;
        child_attrs = NULL;

        rc = ListMgr_GetChild(p_mgr, &filter, curr_array, count, dir_attr_mask,
                              &child_ids, &child_attrs, &res_count);

        if (rc)
        {
            DisplayLog(LVL_CRIT, SCRUB_TAG, "ListMgr_GetChild() terminated with error %d", rc);
            /* @TODO free allocated resources */
            break;
        }

        /* Call the callback func for each listed dir */
        rc = cb_func(child_ids, child_attrs, res_count, arg);
        if (rc)
            /* XXX break the scan? */
            last_err = rc;

        /* can release the list of input ids */
        rbh_scrub_release_list(array_first, count);

        /* copy entry ids before freeing them */
        /* TODO: we could transfer the pathname instead of strdup() them. */
        add_id_list(child_ids, res_count);

        /* attributes no longer needed */
        /* release attrs */
        if (child_attrs)
        {
            for (i = 0; i < res_count; i++)
                ListMgr_FreeAttrs(&child_attrs[i]);
            MemFree(child_attrs);
            child_attrs = NULL;
        }

        /* free the returned id array */
        if (child_ids)
        {
            free_wagon(child_ids, 0, res_count);
            MemFree(child_ids);
            child_ids = NULL;
        }
    }
    lmgr_simple_filter_free(&filter);

    return last_err;
}

int Path2Id(const char *path, entry_id_t * id)
{
    int rc;
    unsigned int len;
    const char * mnt = get_mount_point(&len);
    char rpath[RBH_PATH_MAX];

    char * tmp_path = realpath( path, NULL );
    if ( tmp_path == NULL )
    {
        rc = -errno;
        DisplayLog(LVL_CRIT, P2ID_TAG, "Error in realpath(%s): %s",
                   path, strerror(-rc));
        return rc;
    }
    if (strlen(tmp_path) >= RBH_PATH_MAX)
    {
        free(tmp_path);
        DisplayLog( LVL_CRIT, P2ID_TAG, "Path length is too long!" );
        return -ENAMETOOLONG;
    }
    /* safe because of previous check */
    strcpy(rpath, tmp_path);
    /* now can release tmp path */
    free(tmp_path);

    /* check that path is under FS root */
    if (strncmp(mnt, rpath, len))
    {
        /* if path differs from realpath, display both */
        if (strcmp(path, rpath))
            DisplayLog(LVL_CRIT, P2ID_TAG, "Error: %s (%s) is not under filesystem root %s",
                       path, rpath, mnt);
        else
            DisplayLog(LVL_CRIT, P2ID_TAG, "Error: %s is not under filesystem root %s",
                       path, mnt);
        return -EINVAL;
    }
#ifndef _HAVE_FID
    struct stat inode;
    if (lstat(path, &inode))
        return -errno;

    id->inode = inode.st_ino;
    id->fs_key = get_fskey();
    id->validator = inode.st_ctime;
    return 0;
#else
    /* perform path2fid */
    rc = Lustre_GetFidFromPath(path, id);
    return rc;
#endif
}


struct __diffattr {
    uint64_t mask;       /* 0 for last */
    char * name;    /* NULL for last */
    int negate;     /* negate the given mask */
} diffattrs[] = {
    { ATTR_MASK_fullpath | ATTR_MASK_parent_id | ATTR_MASK_name, "path", 0 },
    { POSIX_ATTR_MASK | ATTR_MASK_link, "posix", 0 },
#ifdef _LUSTRE
    { ATTR_MASK_stripe_info | ATTR_MASK_stripe_items, "stripe", 0 },
#endif
    { ATTR_MASK_fullpath | ATTR_MASK_name | ATTR_MASK_parent_id
     | POSIX_ATTR_MASK | ATTR_MASK_link
#ifdef ATTR_INDEX_creation_time
        | ATTR_MASK_creation_time
#endif
#ifdef ATTR_INDEX_status
        | ATTR_MASK_status
#endif
#ifdef _LUSTRE
        | ATTR_MASK_stripe_info | ATTR_MASK_stripe_items
#endif
    , "all", 0},
    { ATTR_MASK_last_mod | ATTR_MASK_last_access
#ifdef ATTR_INDEX_creation_time
        | ATTR_MASK_creation_time
#endif
, "notimes", 1},
    { ATTR_MASK_last_access, "noatime", 1},

    { 0, NULL, 0 }
};

/* parse attrset for --diff option */
int parse_diff_mask(const char * arg, uint64_t *diff_mask, char * msg)
{
    uint64_t mask_pos = 0;
    uint64_t mask_neg = 0;
    struct __diffattr *attr;
    char buff[4096];
    char *curr, *init;

    /* tmp copy of argument */
    rh_strncpy(buff, arg, 4096);
    init = buff;

    while ((curr = strtok(init, ",")) != NULL)
    {
        init = NULL;
        int found = 0;
        for (attr = diffattrs; attr->name != NULL; attr++)
        {
            if (!strcasecmp(attr->name, curr))
            {
                found = 1;
                if (attr->negate)
                    mask_neg |= attr->mask;
                else
                    mask_pos |= attr->mask;
            }
        }
        if (!found) {
            sprintf(msg, "invalid diff attr '%s'", curr);
            return -EINVAL;
        }
    }

    *diff_mask = (mask_pos & ~mask_neg);
    return 0;
}

/** print an attribute from attrs structure */
const char *attr2str(attr_set_t *attrs, const entry_id_t *id,
                     int attr_index, int csv, name_func name_resolver,
                     char *out, size_t out_sz)
{
    time_t tt;
    struct tm stm;

    if  (attr_index != ATTR_INDEX_fullpath /* specific case */
         && (attrs->attr_mask & (1 << attr_index)) == 0)
        return "";
    if (attr_index >= ATTR_COUNT)
        return STATUS_ATTR(attrs, attr_index - ATTR_COUNT);

    switch(attr_index)
    {
        case ATTR_INDEX_fullpath:
            if (ATTR_MASK_TEST(attrs, fullpath))
                return ATTR(attrs, fullpath);
            else if (name_resolver != NULL)
                return name_resolver(id, attrs, out);
            else
                return "n/a"; /* TODO fid2path if possible? */
        case ATTR_INDEX_avgsize:
            if (csv)
                snprintf(out, out_sz, "%"PRIu64, ATTR(attrs, avgsize));
            else
                FormatFileSize(out, out_sz, ATTR(attrs, avgsize));
            return out;
        case ATTR_INDEX_dircount:
            snprintf(out, out_sz, "%u", ATTR(attrs, dircount));
            return out;
        case ATTR_INDEX_parent_id:
            snprintf(out, out_sz, DFID, PFID(&ATTR(attrs, parent_id)));
            return out;

        case ATTR_INDEX_link:
             return ATTR(attrs, link);

        case ATTR_INDEX_type:
             return ATTR(attrs, type);
        case ATTR_INDEX_nlink:
             snprintf(out, out_sz, "%u", ATTR(attrs, nlink));
             return out;

        case ATTR_INDEX_depth:
             snprintf(out, out_sz, "%u", ATTR(attrs, depth));
             return out;

        case ATTR_INDEX_name:
             return ATTR(attrs, name);

        case ATTR_INDEX_mode:
            if (csv)
                snprintf(out, out_sz, "%#03o", ATTR(attrs, mode));
            else
            {
                memset(out, 0, out_sz);
                mode_string(ATTR(attrs, mode), out);
            }
            return out;

        case ATTR_INDEX_owner: return ATTR(attrs, owner);
        case ATTR_INDEX_gr_name: return ATTR(attrs, gr_name);
        case ATTR_INDEX_blocks:
            if (csv)
                snprintf(out, out_sz, "%"PRIu64, ATTR(attrs, blocks) * DEV_BSIZE);
            else
                FormatFileSize(out, out_sz, ATTR(attrs, blocks) * DEV_BSIZE);
            return out;

        case ATTR_INDEX_size:
            if (csv)
                snprintf(out, out_sz, "%"PRIu64, ATTR(attrs, size));
            else
                FormatFileSize(out, out_sz, ATTR(attrs, size));
            return out;

        case ATTR_INDEX_last_access:
            tt = ATTR(attrs, last_access);
            strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;

        case ATTR_INDEX_last_mod:
            tt = ATTR(attrs, last_mod);
            strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;

        case ATTR_INDEX_creation_time:
            tt = ATTR(attrs, creation_time);
            strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;

        case ATTR_INDEX_md_update:
            tt = ATTR(attrs, md_update);
            strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;

        case ATTR_INDEX_path_update:
            tt = ATTR(attrs, path_update);
            strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;

        case ATTR_INDEX_fileclass:
            return ATTR(attrs, fileclass);

        case ATTR_INDEX_class_update:
            tt = ATTR(attrs, class_update);
            strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;

#ifdef ATTR_INDEX_invalid
        case ATTR_INDEX_invalid: return (ATTR(attrs, invalid) ? "yes" : "no");
#endif
#ifdef ATTR_INDEX_last_archive
        case ATTR_INDEX_last_archive:
            tt = ATTR(attrs, last_archive);
            strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;
#endif
#ifdef ATTR_INDEX_last_restore
        case ATTR_INDEX_last_restore:
            tt = ATTR(attrs, last_restore);
            strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;
#endif
#ifdef ATTR_INDEX_backendpath
        case ATTR_INDEX_backendpath:
            return ATTR(attrs, backendpath);
#endif

#ifdef _LUSTRE
        case ATTR_INDEX_stripe_info:
            if (csv)
                snprintf(out, out_sz, "%10u, %11"PRIu64", %9s",
                    ATTR(attrs, stripe_info).stripe_count,
                    ATTR(attrs, stripe_info).stripe_size,
                    ATTR(attrs, stripe_info).pool_name);
            else {
                char tmp[128];
                FormatFileSize(tmp, sizeof(tmp), ATTR(attrs, stripe_info).stripe_size);
                sprintf(out, "%10u, %11s, %9s",
                    ATTR(attrs, stripe_info).stripe_count, tmp,
                    ATTR(attrs, stripe_info).pool_name);
            }
            return out;

        case ATTR_INDEX_stripe_items:
            FormatStripeList(out, out_sz, &ATTR(attrs, stripe_items), csv);
            return out;
#endif
    }
    return "?";
}

/** display helper type */
typedef const char *(*result2str_func)(const db_value_t *val, bool csv,
                                       char *out, size_t out_sz);


/** display helper functions */
static const char *print_res_status(const db_value_t *val, bool csv,
                                    char *out, size_t out_sz)
{
    return status_format(val->value_u.val_str);
}

static const char *print_res_class(const db_value_t *val, bool csv,
                                   char *out, size_t out_sz)
{
    return class_format(val->value_u.val_str);
}

static const char *print_res_count(const db_value_t *val, bool csv,
                                   char *out, size_t out_sz)
{
    snprintf(out, out_sz, "%llu", val->value_u.val_biguint);
    return out;
}

static const char *print_res_string(const db_value_t *val, bool csv,
                                    char *out, size_t out_sz)
{
    rh_strncpy(out, val->value_u.val_str, out_sz);
    return out;
}

static const char *print_res_size(const db_value_t *val, bool csv,
                                  char *out, size_t out_sz)
{
    if (csv)
        snprintf(out, out_sz, "%llu", val->value_u.val_biguint);
    else
        FormatFileSize(out, out_sz, val->value_u.val_biguint);

    return out;
}

static const char *print_res_space(const db_value_t *val, bool csv,
                                   char *out, size_t out_sz)
{
    if (csv)
        snprintf(out, out_sz, "%llu", val->value_u.val_biguint * DEV_BSIZE);
    else
        FormatFileSize(out, out_sz, val->value_u.val_biguint * DEV_BSIZE);

    return out;
}

static const char *print_res_empty(const db_value_t *val, bool csv,
                                   char *out, size_t out_sz)
{
    out[0] = '\0';
    return out;
}

/** attribute display specification for reports */
struct attr_display_spec {
    int attr_index;
    const char  *name;
    unsigned int length_csv;
    unsigned int length_full;
    result2str_func result2str;
} attr[] = {
        {ATTR_INDEX_fullpath,  "path", 40, 40},
        {ATTR_INDEX_name,      "name", 10, 10},
        {ATTR_INDEX_depth,     "depth", 3, 3},
        {ATTR_INDEX_dircount,  "dircount", 8, 8},
        {ATTR_INDEX_type,      "type", 8, 8, print_res_string},
        {ATTR_INDEX_mode,      "mode", 4, 6},
        {ATTR_INDEX_nlink,     "nlink", 5, 5},
        {ATTR_INDEX_parent_id, "parent_id", 20, 20},
        {ATTR_INDEX_owner,     "user", 10, 10, print_res_string},
        {ATTR_INDEX_gr_name,   "group", 10, 10, print_res_string},
        {ATTR_INDEX_link,      "link", 20, 20},
        {ATTR_INDEX_fileclass, "fileclass", 30, 30, print_res_class},
        /* times */
        {ATTR_INDEX_last_access,   "last_access", 20, 20},
        {ATTR_INDEX_last_mod,      "last_mod", 20, 20},
        {ATTR_INDEX_creation_time, "creation", 20, 20},
        {ATTR_INDEX_md_update,     "md updt", 20, 20},
        {ATTR_INDEX_path_update,   "path updt", 20, 20},
        {ATTR_INDEX_class_update,  "class updt", 20, 20},
#ifdef ATTR_INDEX_last_archive
        {ATTR_INDEX_last_archive, "last_archive", 20, 20},
#endif
#ifdef ATTR_INDEX_last_restore
        {ATTR_INDEX_last_restore, "last_restore", 20, 20},
#endif
        /* sizes */
        /* 15 digits for 999To, 10 chars for 1024.21 GB */
        {ATTR_INDEX_blocks,    "spc_used", 15, 10, print_res_space},
        {ATTR_INDEX_avgsize,   "avgsize", 15, 10},
        {ATTR_INDEX_size,      "size", 15, 10, print_res_size},

#ifdef ATTR_INDEX_invalid
        {ATTR_INDEX_invalid, "invalid", 3, 3}, /* yes/no */
#endif
#ifdef ATTR_INDEX_backendpath
        {ATTR_INDEX_backendpath, "backend_path", 40, 40},
#endif
#define STRIPE_TITLE "stripe_cnt, stripe_size,      pool"
        {ATTR_INDEX_stripe_info,  STRIPE_TITLE, sizeof(STRIPE_TITLE), sizeof(STRIPE_TITLE)},
        {ATTR_INDEX_stripe_items, "stripes", 30, 30},
        {-1, "count", 10, 10, print_res_count}, /* count */

        {0, NULL, 0, 0}, /* final element */
};

static inline struct attr_display_spec *attr_info(int index)
{
    int i;
    static struct attr_display_spec tmp_rec = {-1, "?", 1, 1, NULL};


    if (index >= ATTR_COUNT) /* status */
    {
        /* build a special decriptor (/!\ not reentrant) */
        tmp_rec.attr_index = index;
        tmp_rec.name = get_sm_instance(index - ATTR_COUNT)->db_field;
        tmp_rec.length_csv = tmp_rec.length_full = 15;
        tmp_rec.result2str = print_res_status;
        return &tmp_rec;
    }

    for (i = 0; attr[i].name != NULL; i++)
        if (attr[i].attr_index == index)
            return &attr[i];

    tmp_rec.attr_index = index;
    tmp_rec.result2str = print_res_empty;
    return &tmp_rec;
}

static inline int rec_len(struct attr_display_spec *rec, bool csv)
{
    return csv? rec->length_csv : rec->length_full;
}

const char *attrindex2name(unsigned int index)
{
    int i;

    if (index >= ATTR_COUNT) /* status */
        return get_sm_instance(index - ATTR_COUNT)->db_field;

    for (i = 0; attr[i].name != NULL; i++)
        if (attr[i].attr_index == index)
            return attr[i].name;

    return "?";
}

unsigned int attrindex2len(unsigned int index, int csv)
{
    int i;

    if (index >= ATTR_COUNT) /* status */
        return 15;

    for (i = 0; attr[i].name != NULL; i++)
        if (attr[i].attr_index == index)
            return (csv? attr[i].length_csv : attr[i].length_full);

    return 1; /* for '?' */
}

#define PROF_CNT_LEN     8
#define PROF_RATIO_LEN   7

/** standard attribute display for reports */
void print_attr_list_custom(int rank_field, int *attr_list, int attr_count,
                            profile_field_descr_t *p_profile, bool csv,
                            const char * custom_title, int custom_len)
{
    int i;
    int coma = 0;
    struct attr_display_spec *rec;

    if (rank_field)
    {
        printf("rank");
        coma = 1;
    }
    for (i=0; i < attr_count; i++)
    {
        rec = attr_info(attr_list[i]);
        if (coma)
            printf(", %*s", rec_len(rec, csv), rec->name);
        else
        {
            printf("%*s", rec_len(rec, csv), rec->name);
            coma = 1;
        }
    }
    if (p_profile)
    {
        if (p_profile->attr_index == ATTR_INDEX_size)
        {
            for (i=0; i < SZ_PROFIL_COUNT; i++)
            {
                if (coma)
                    printf(", %*s", PROF_CNT_LEN, size_range[i].title);
                else
                {
                    printf("%*s", PROF_CNT_LEN, size_range[i].title);
                    coma = 1;
                }
            }
            if (p_profile->range_ratio_len > 0)
            {
                char tmp[128];
                char tmp1[128];
                char tmp2[128];
                if (p_profile->range_ratio_start + p_profile->range_ratio_len == SZ_PROFIL_COUNT)
                    sprintf(tmp, "ratio(%s..inf)", print_brief_sz( SZ_MIN_BY_INDEX(p_profile->range_ratio_start), tmp1));
                else
                    sprintf(tmp, "ratio(%s..%s-)",
                            print_brief_sz( SZ_MIN_BY_INDEX(p_profile->range_ratio_start), tmp1),
                            print_brief_sz( SZ_MIN_BY_INDEX(p_profile->range_ratio_start + p_profile->range_ratio_len), tmp2));

                printf(", %*s", PROF_RATIO_LEN, tmp);
            }
        }
    }
    if (custom_title)
    {
        if (coma)
            printf(", %*s", custom_len, custom_title);
        else
            printf("%*s", custom_len, custom_title);
    }
    printf("\n");
}

void print_attr_values_custom(int rank, int *attr_list, int attr_count,
                              attr_set_t * attrs, const entry_id_t *id,
                              bool csv, name_func name_resolver,
                              const char *custom, int custom_len)
{
    int i, coma = 0;
    char str[24576];
    struct attr_display_spec *rec;

    if (rank)
    {
        printf("%4d", rank);
        coma = 1;
    }

    for (i=0; i < attr_count; i++)
    {
        rec = attr_info(attr_list[i]);
        if (coma)
            printf(", %*s", rec_len(rec, csv),
                   attr2str(attrs, id, attr_list[i], csv, name_resolver, str, sizeof(str)));
        else
        {
            printf("%*s", rec_len(rec, csv),
                   attr2str(attrs, id, attr_list[i], csv, name_resolver, str, sizeof(str)));
            coma = 1;
        }
    }
    if (custom)
    {
        if (coma)
            printf(", %*s", custom_len, custom);
        else
            printf("%*s", custom_len, custom);
    }
    printf("\n");
}

/* return attr name to be displayed */
static inline const char *attrdesc2name(const report_field_descr_t *desc,
                                        struct attr_display_spec *rec)
{
    switch(desc->attr_index)
    {
        case -1:
            if (desc->report_type == REPORT_COUNT)
                return "count";
            break;
        case ATTR_INDEX_size:
            if (desc->report_type == REPORT_MIN)
                return "min_size";
            else if (desc->report_type == REPORT_MAX)
                return "max_size";
            else if (desc->report_type == REPORT_AVG)
                return "avg_size";
            else if (desc->report_type == REPORT_SUM)
                return "volume";
            else
                return "size";
        /*default: */
    }
    return rec->name;
}

/**
 * Generic function to display a report
 */
void display_report(const report_field_descr_t *descr, unsigned int field_count,
                    const db_value_t *result, unsigned int result_count,
                    const profile_field_descr_t *prof_descr, profile_u *p_prof,
                    bool csv, bool header, int rank)
{
    unsigned int i;
    struct attr_display_spec *rec;

    if (header)
    {
        if (rank)
            printf("rank, ");

        rec = attr_info(descr[0].attr_index);
        printf("%-*s", rec_len(rec, csv), attrdesc2name(&descr[0], rec));

        for (i = 1; i < field_count && i < result_count; i++)
        {
            if (!result || !DB_IS_NULL(&result[i]))
            {
                rec = attr_info(descr[i].attr_index);
                printf( ", %*s", rec_len(rec, csv), attrdesc2name(&descr[i], rec));
            }
        }
        if (prof_descr)
        {
            if (prof_descr->attr_index == ATTR_INDEX_size)
            {
                for (i=0; i < SZ_PROFIL_COUNT; i++)
                    printf(", %*s", PROF_CNT_LEN, size_range[i].title);

                if (prof_descr->range_ratio_len > 0)
                {
                    char tmp[128];
                    char tmp1[128];
                    char tmp2[128];
                    if (prof_descr->range_ratio_start + prof_descr->range_ratio_len == SZ_PROFIL_COUNT)
                        sprintf(tmp, "ratio(%s..inf)", print_brief_sz( SZ_MIN_BY_INDEX(prof_descr->range_ratio_start), tmp1));
                    else
                        sprintf(tmp, "ratio(%s..%s)",
                                print_brief_sz( SZ_MIN_BY_INDEX(prof_descr->range_ratio_start), tmp1),
                                print_brief_sz( SZ_MIN_BY_INDEX(prof_descr->range_ratio_start + prof_descr->range_ratio_len) -1, tmp2));

                    printf(", %*s", PROF_RATIO_LEN, tmp);
                }
            }
        }

        printf("\n");
    }

    if (result)
    {
        if (rank)
            printf("%4d, ", rank);

        char tmpstr[1024];
        for (i = 0; i < field_count && i < result_count; i++)
        {
            rec = attr_info(descr[i].attr_index);

            if (!DB_IS_NULL(&result[i]) || i == 0) /* tag first column */
                printf("%s%*s", i == 0 ? "":", ", rec_len(rec, csv),
                       rec->result2str(&result[i], csv, tmpstr, sizeof(tmpstr)));
        }

        if (prof_descr && p_prof)
        {
            if (prof_descr->attr_index == ATTR_INDEX_size)
            {
                uint64_t tot=0;
                uint64_t range=0;

                for (i=0; i < SZ_PROFIL_COUNT; i++)
                {
                    printf(", %*"PRIu64, PROF_CNT_LEN, p_prof->size.file_count[i]);
                    tot += p_prof->size.file_count[i];
                    if ((prof_descr->range_ratio_len > 0) &&
                        (i >= prof_descr->range_ratio_start) &&
                        (i < prof_descr->range_ratio_start + prof_descr->range_ratio_len))
                        range += p_prof->size.file_count[i];
                }

                if (prof_descr->range_ratio_len > 0)
                    printf(", %.2f%%", 100.0*range/tot);
            }
        }

        printf("\n");
    }
}


