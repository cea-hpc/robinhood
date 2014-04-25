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
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
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
              unsigned int id_count, int dir_attr_mask,
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
    int mask;       /* 0 for last */
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
int parse_diff_mask(const char * arg, int * diff_mask, char * msg)
{
    int mask_pos = 0;
    int mask_neg = 0;
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


