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

#include <glib.h>
#include <unistd.h>

#include "uidgidcache.h"
#include "cmd_helpers.h"
#include "rbh_cfg.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "Memory.h"
#include "xplatform_print.h"
#include "status_manager.h"

#define SCRUB_TAG "Scrubber"
#define P2ID_TAG "Path2Id"

/* Initially empty array. This is a LIFO array; oldest elements are
 * stacked from the last entry to the first. When element 0 is
 * occupied, it is time to increase the size of the array. */
static wagon_t *dir_array = NULL;
static unsigned int array_len;   /* number of elements in array. */
static unsigned int array_first; /* index of first valid element in array. */
#define array_used (array_len-array_first)

#define LS_CHUNK    1  /* TODO - ListMgr_GetChild will get confused if not 1 */

static size_t what_2_power(size_t s)
{
    size_t c = 1;
    while (c < s)
        c <<= 1;
    return c;
}

/* Copy the ids and names array. */
static void copy_arrays(const wagon_t *src,
                        wagon_t *dest, int dst_first, int count)
{
    int src_first = 0;

    while (count) {
        dest[dst_first].id = src[src_first].id;
        dest[dst_first].fullname = strdup(src[src_first].fullname);

        src_first++;
        dst_first++;
        count--;
    }
}

/** add a list of ids to the scrubbing array */
static int add_id_list(const wagon_t *list, unsigned int count)
{
    /* always add at the beginning to have LIFO behavior */

    /* is there enough room before the first item ? */
    if (count <= array_first) {
        /* copy it just before the head (entries must be consecutive) */
        copy_arrays(list, dir_array, array_first - count, count);

        array_first -= count;

#ifdef _DEBUG_ID_LIST
        printf("1)...<new_ids:%u-%u><ids:%u-%u>...(len=%u)\n",
               array_first, array_first + count - 1,
               array_first + count, array_len - 1, array_len);
#endif
    }
    /* is the array empty ? */
    else if ((array_used == 0) && (count <= array_len)) {
        /* copy from the beginning */
        copy_arrays(list, dir_array, array_len - count, count);
        array_first = array_len - count;

#ifdef _DEBUG_ID_LIST
        printf("2) <new_ids:%u-%u>...(len=%u)\n",
               array_first, array_len - 1, array_len);
#endif
    } else {    /* increase array size */

        wagon_t *dir_array_new;
        size_t new_len = what_2_power(array_len + count);

        dir_array_new = MemAlloc(new_len * sizeof(wagon_t));
        if (!dir_array_new)
            return -ENOMEM;

        /* First, transfer current ids and names */
        if (dir_array) {
            if (array_used)
                memcpy(&dir_array_new[new_len - array_used],
                       &dir_array[array_first], array_used * sizeof(wagon_t));
            MemFree(dir_array);
        }

        /* update array info */
        dir_array = dir_array_new;
        array_first = new_len - array_used;
        array_len = new_len;

        /* Then copy new ids */
        copy_arrays(list, dir_array, array_first - count, count);
        array_first -= count;

#ifdef _DEBUG_ID_LIST
        printf("3)...<ids:%u-%u>...(len=%u)\n",
               array_first, array_len - 1, array_len);
#endif
    }
    return 0;
}

/** release a list of ids from the array */
static inline void rbh_scrub_release_list(unsigned int first,
                                          unsigned int count)
{
    free_wagon(dir_array, first, first + count);

    if (first != array_first)
        DisplayLog(LVL_CRIT, SCRUB_TAG,
                   "IMPLEMENTATION ISSUE: array_first was %u, is now %u\n",
                   first, array_first);
    array_first += count;

#ifdef _DEBUG_ID_LIST
    printf("released %u-%u\n", array_first - count, array_first - 1);
#endif
}

/** scan sets of directories
 * \param cb_func, callback function for each set of directory
 */
int rbh_scrub(lmgr_t *p_mgr, const wagon_t *id_list,
              unsigned int id_count, attr_mask_t dir_attr_mask,
              scrub_callback_t cb_func, void *arg)
{
    wagon_t *curr_array;
    unsigned int count;
    lmgr_filter_t filter;
    filter_value_t fv;
    int i, rc;
    int last_err = 0;

    rc = add_id_list(id_list, id_count);
    if (rc)
        return rc;

    /* only get subdirs (for scanning) */
    fv.value.val_str = STR_TYPE_DIR;
    lmgr_simple_filter_init(&filter);
    lmgr_simple_filter_add(&filter, ATTR_INDEX_type, EQUAL, fv, 0);

    /* while the array is not empty */
    while (array_used > 0) {
        unsigned int res_count = 0;
        wagon_t *child_ids;
        attr_set_t *child_attrs;

        /* get a set of entry_ids */
        curr_array = &dir_array[array_first];
        if (array_used < LS_CHUNK) {
            /* get all available dirs */
            count = array_used;
        } else {
            /* get a constant chunk */
            count = LS_CHUNK;
        }

#ifdef _DEBUG_ID_LIST
        printf("processing %u-%u\n", array_first, array_first + count - 1);
#endif

        /* read childs */
        res_count = 0;
        child_ids = NULL;
        child_attrs = NULL;

        rc = ListMgr_GetChild(p_mgr, &filter, curr_array, count, dir_attr_mask,
                              &child_ids, &child_attrs, &res_count);

        if (rc) {
            DisplayLog(LVL_CRIT, SCRUB_TAG,
                       "ListMgr_GetChild() terminated with error %d", rc);
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
        if (child_attrs) {
            for (i = 0; i < res_count; i++)
                ListMgr_FreeAttrs(&child_attrs[i]);
            MemFree(child_attrs);
            child_attrs = NULL;
        }

        /* free the returned id array */
        if (child_ids) {
            free_wagon(child_ids, 0, res_count);
            MemFree(child_ids);
            child_ids = NULL;
        }
    }
    lmgr_simple_filter_free(&filter);

    return last_err;
}

int Path2Id(const char *path, entry_id_t *id)
{
    int rc;
    unsigned int len;
    char rpath[RBH_PATH_MAX];
    const char *mnt;
    char *tmp_path;

    mnt = get_mount_point(&len);
    tmp_path = realpath(path, NULL);

    if (tmp_path == NULL) {
        rc = -errno;
        DisplayLog(LVL_CRIT, P2ID_TAG, "Error in realpath(%s): %s",
                   path, strerror(-rc));
        return rc;
    }
    if (strlen(tmp_path) >= RBH_PATH_MAX) {
        free(tmp_path);
        DisplayLog(LVL_CRIT, P2ID_TAG, "Path length is too long!");
        return -ENAMETOOLONG;
    }
    /* safe because of previous check */
    strcpy(rpath, tmp_path);
    /* now can release tmp path */
    free(tmp_path);

    /* check that path is under FS root */
    if (strncmp(mnt, rpath, len)) {
        /* if path differs from realpath, display both */
        if (strcmp(path, rpath))
            DisplayLog(LVL_CRIT, P2ID_TAG,
                       "Error: %s (%s) is not under filesystem root %s", path,
                       rpath, mnt);
        else
            DisplayLog(LVL_CRIT, P2ID_TAG,
                       "Error: %s is not under filesystem root %s", path, mnt);
        return -EINVAL;
    }

    rc = path2id(path, id, NULL);
    return rc;
}

struct __diffattr {
    attr_mask_t mask;   /* 0 for last */
    char *name; /* NULL for last */
    int negate; /* negate the given mask */
} diffattrs[] = {
    {{.std = ATTR_MASK_fullpath | ATTR_MASK_parent_id | ATTR_MASK_name},
      "path", 0},
    {{.std = POSIX_ATTR_MASK | ATTR_MASK_link}, "posix", 0},
#ifdef _LUSTRE
    {{.std = ATTR_MASK_stripe_info | ATTR_MASK_stripe_items}, "stripe", 0},
#endif
    {{.std = ATTR_MASK_fullpath | ATTR_MASK_name | ATTR_MASK_parent_id
#ifdef _LUSTRE
                | ATTR_MASK_stripe_info | ATTR_MASK_stripe_items
#endif
                | POSIX_ATTR_MASK | ATTR_MASK_link | ATTR_MASK_creation_time,
            .status = SMI_MASK(0), /* stands for all status */
            .sm_info = GENERIC_INFO_BIT(0), /* stands for all policy specific
                                                info */
        },"all", 0},
    {{.status = SMI_MASK(0)} /* stands for all status */ , "status", 0},
    {{.std = ATTR_MASK_last_mod | ATTR_MASK_last_access
             | ATTR_MASK_creation_time}, "notimes", 1},
    {{.std = ATTR_MASK_last_access}, "noatime", 1},
    {{0}, NULL, 0}
};

/* parse attrset for --diff option */
int parse_diff_mask(const char *arg, attr_mask_t *diff_mask, char *msg)
{
    attr_mask_t mask_pos = null_mask;
    attr_mask_t mask_neg = null_mask;
    struct __diffattr *attr;
    char buff[4096];
    char *curr, *init;

    /* tmp copy of argument */
    rh_strncpy(buff, arg, 4096);
    init = buff;

    while ((curr = strtok(init, ",")) != NULL) {
        init = NULL;
        int found = 0;
        for (attr = diffattrs; attr->name != NULL; attr++) {
            if (!strcasecmp(attr->name, curr)) {
                found = 1;
                if (attr->negate)
                    mask_neg = attr_mask_or(&mask_neg, &attr->mask);
                else
                    mask_pos = attr_mask_or(&mask_pos, &attr->mask);
            }
        }
        if (!found) {
            sprintf(msg, "invalid diff attr '%s'", curr);
            return -EINVAL;
        }
    }

    *diff_mask = attr_mask_and_not(&mask_pos, &mask_neg);
    return 0;
}

int parse_status_arg(const char *option, char *arg, char **p_st_name,
                     char **p_st_val, bool mandatory_value)
{
    int   sn_len;
    char *delim;

    if (!arg) {
        fprintf(stderr, "Missing mandatory argument <status_name>%s for %s\n",
                mandatory_value ? ":<status_value>" : "[:<status_value>]",
                option);
        return EINVAL;
    }

    /* the expected argument is <status_name>:<status_value> */
    delim = strchr(arg, ':');
    if (delim == NULL && mandatory_value) {
        fprintf(stderr, "Invalid argument for %s: "
                "<status_name>:<status_value> expected\n", option);
        return EINVAL;
    }
    *p_st_name = arg;

    if (delim != NULL) {
        *delim = '\0';
        *p_st_val = delim + 1;
    } else
        *p_st_val = NULL;

    if (EMPTY_STRING(*p_st_name)) {
        fprintf(stderr, "Invalid argument for %s: <status_name>%s expected\n",
                option,
                mandatory_value ? ":<status_value>" : "[:<status_value>]");
        return EINVAL;
    }

    /* if status_name ends with "_status", remove it */
    sn_len = strlen(*p_st_name);
    if ((sn_len > 7) && !strcmp((*p_st_name) + sn_len - 7, "_status"))
        *((char *)((*p_st_name) + sn_len - 7)) = '\0';

    return 0;
}

int check_status_args(const char *status_name, const char *status_value,
                      const char **str_val_new, sm_instance_t **p_smi)
{
    /* resolve the status name now, as config file has been parsed */
    *str_val_new = status_value;
    char buff[1024];

    /* get status index by name */
    *p_smi = smi_by_name(status_name);
    if (*p_smi == NULL) {
        int idx;
        /* try with a policy name */
        if (policy_exists(status_name, &idx)) {
            (*p_smi) = policies.policy_list[idx].status_mgr;
            if (*p_smi == NULL) {
                fprintf(stderr, "ERROR: policy '%s' doesn't manage status\n",
                        status_name);
                return ENOENT;
            }
        } else {
            fprintf(stderr,
                    "ERROR: status manager or policy '%s' is not defined\n",
                    status_name);
            return EINVAL;
        }
    }

    /* check status value */
    if (status_value && !EMPTY_STRING(status_value)) {
        *str_val_new = get_status_str((*p_smi)->sm, status_value);
        if (*str_val_new == NULL) {
            fprintf(stderr, "ERROR: unexpected value for %s_status: '%s'."
                    " Expected values are: %s\n", status_name, status_value,
                    allowed_status_str((*p_smi)->sm, buff, sizeof(buff)));
            return EINVAL;
        }
    }

    return 0;
}

static const char *print_sm_attr(char *out, size_t out_sz, const void *pvalue,
                                 cfg_param_type type, bool csv)
{
    switch (type) {
    case PT_STRING:
        return (char *)pvalue;

    case PT_BOOL:
        {
            const bool *b = pvalue;
            return bool2str(*b);
        }
    case PT_DURATION:
        {
            struct tm stm;
            /* dates managed as 32bits */
            time_t tt = *((unsigned int *)pvalue);
            if (tt == 0)
                return "0";
            strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
            return out;
        }
    case PT_SIZE:
        {
            const uint64_t *s = pvalue;
            if (csv)
                snprintf(out, out_sz, "%" PRIu64, *s);
            else
                FormatFileSize(out, out_sz, *s);
            return out;
        }
    case PT_INT:
        {
            const int32_t *i = pvalue;
            snprintf(out, out_sz, "%d", *i);
            return out;
        }
    case PT_INT64:
        {
            const int64_t *l = pvalue;
            snprintf(out, out_sz, "%" PRId64, *l);
            return out;
        }
    case PT_FLOAT:
        {
            /* no such type in DB */
            const float *f = pvalue;
            snprintf(out, out_sz, "%.2f", *f);
            return out;
        }
    default:
        return "unknown/unhandled type";
    }
}

/** print an attribute from attrs structure */
const char *attr2str(attr_set_t *attrs, const entry_id_t *id,
                     unsigned int attr_index, int csv, name_func name_resolver,
                     char *out, size_t out_sz)
{
    time_t tt;
    struct tm stm;

    /* if attr is not set in mask, print nothing */
    if (attr_index != ATTR_INDEX_fullpath   /* specific case */
        && attr_index != ATTR_INDEX_ID
        && !attr_mask_test_index(&attrs->attr_mask, attr_index))
        return "";

    if (is_status(attr_index))
        return STATUS_ATTR(attrs, attr2status_index(attr_index));
    else if (is_sm_info(attr_index)) {
        unsigned int idx = attr2sminfo_index(attr_index);

        return print_sm_attr(out, out_sz, attrs->attr_values.sm_info[idx],
                             sm_attr_info[idx].def->crit_type, csv);
    }

    switch (attr_index) {
    case ATTR_INDEX_ID:
        snprintf(out, out_sz, DFID, PFID(id));
        return out;

    case ATTR_INDEX_fullpath:
        if (ATTR_MASK_TEST(attrs, fullpath))
            return ATTR(attrs, fullpath);
        else if (name_resolver != NULL)
            return name_resolver(id, attrs, out);
        else
            return "n/a";   /* TODO fid2path if possible? */
    case ATTR_INDEX_avgsize:
        if (csv)
            snprintf(out, out_sz, "%" PRIu64, ATTR(attrs, avgsize));
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
        else {
            memset(out, 0, out_sz);
            mode_string(ATTR(attrs, mode), out);
        }
        return out;

    case ATTR_INDEX_uid:
        if (global_config.uid_gid_as_numbers) {
            snprintf(out, out_sz, "%d", ATTR(attrs, uid).num);
            return out;
        } else {
            return ATTR(attrs, uid).txt;
        }

    case ATTR_INDEX_gid:
        if (global_config.uid_gid_as_numbers) {
            snprintf(out, out_sz, "%d", ATTR(attrs, gid).num);
            return out;
        } else {
            return ATTR(attrs, gid).txt;
        }

    case ATTR_INDEX_blocks:
        if (csv)
            snprintf(out, out_sz, "%" PRIu64, ATTR(attrs, blocks) * DEV_BSIZE);
        else
            FormatFileSize(out, out_sz, ATTR(attrs, blocks) * DEV_BSIZE);
        return out;

    case ATTR_INDEX_size:
        if (csv)
            snprintf(out, out_sz, "%" PRIu64, ATTR(attrs, size));
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

    case ATTR_INDEX_last_mdchange:
        tt = ATTR(attrs, last_mdchange);
        strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
        return out;

    case ATTR_INDEX_creation_time:
        tt = ATTR(attrs, creation_time);
        strftime(out, out_sz, "%Y/%m/%d %T", localtime_r(&tt, &stm));
        return out;

    case ATTR_INDEX_rm_time:
        tt = ATTR(attrs, rm_time);
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
    case ATTR_INDEX_invalid:
        return ATTR(attrs, invalid) ? "yes" : "no";
#endif

#ifdef _LUSTRE
    case ATTR_INDEX_stripe_info:
        if (csv)
            snprintf(out, out_sz, "%10u, %11" PRIu64 ", %9s",
                     ATTR(attrs, stripe_info).stripe_count,
                     ATTR(attrs, stripe_info).stripe_size,
                     ATTR(attrs, stripe_info).pool_name);
        else {
            char tmp[128];
            FormatFileSize(tmp, sizeof(tmp),
                           ATTR(attrs, stripe_info).stripe_size);
            sprintf(out, "%10u, %11s, %9s",
                    ATTR(attrs, stripe_info).stripe_count, tmp,
                    ATTR(attrs, stripe_info).pool_name);
        }
        return out;

    case ATTR_INDEX_stripe_items:
        {
            GString *osts = g_string_new("");

            append_stripe_list(osts, &ATTR(attrs, stripe_items), csv);
            rh_strncpy(out, osts->str, out_sz);
            g_string_free(osts, TRUE);

            return out;
        }
#endif
    }
    return "?";
}

/** display helper type */
typedef const char *(*result2str_func) (const db_value_t *val, bool csv,
                                        char *out, size_t out_sz);

/** display helper functions */
static const char *print_res_status(const db_value_t *val, bool csv,
                                    char *out, size_t out_sz)
{
    return status_format(val->value_u.val_str);
}

static const char *print_res_sm_info(const db_value_t *val, bool csv,
                                     char *out, size_t out_sz)
{
    GString *gs = g_string_new("");

    ListMgr_PrintAttr(gs, val->type, &val->value_u, "");
    rh_strncpy(out, gs->str, out_sz);
    g_string_free(gs, TRUE);

    return out;
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

static const char *print_res_int(const db_value_t *val, bool csv,
                                 char *out, size_t out_sz)
{
    snprintf(out, out_sz, "%d", val->value_u.val_int);
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
static struct attr_display_spec {
    int attr_index;
    const char *name;
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
        {ATTR_INDEX_uid,       "user", 10, 10,  print_res_string},
        {ATTR_INDEX_gid,       "group", 10, 10, print_res_string},
        {ATTR_INDEX_link,      "link", 20, 20},
        {ATTR_INDEX_fileclass, "fileclass", 30, 30, print_res_class},
        /* times */
        {ATTR_INDEX_last_access,   "last_access", 20, 20},
        {ATTR_INDEX_last_mod,      "last_mod", 20, 20},
        {ATTR_INDEX_last_mdchange, "last_mdchange", 20, 20},
        {ATTR_INDEX_creation_time, "creation", 20, 20},
        {ATTR_INDEX_rm_time,       "rm_time", 20, 20},
        {ATTR_INDEX_md_update,     "md updt", 20, 20},
        {ATTR_INDEX_path_update,   "path updt", 20, 20},
        {ATTR_INDEX_class_update,  "class updt", 20, 20},
        /* sizes */
        /* 15 digits for 999To, 10 chars for 1024.21 GB */
        {ATTR_INDEX_blocks,    "spc_used", 15, 10, print_res_space},
        {ATTR_INDEX_avgsize,   "avgsize", 15, 10},
        {ATTR_INDEX_size,      "size", 15, 10, print_res_size},

#ifdef ATTR_INDEX_invalid
        {ATTR_INDEX_invalid, "invalid", 3, 3}, /* yes/no */
#endif
#define STRIPE_TITLE "stripe_cnt, stripe_size,      pool"
        {ATTR_INDEX_stripe_info,  STRIPE_TITLE, sizeof(STRIPE_TITLE),
                                                sizeof(STRIPE_TITLE)},
        {ATTR_INDEX_stripe_items, "stripes", 30, 30},
        {ATTR_INDEX_COUNT,        "count", 10, 10, print_res_count},
        {ATTR_INDEX_ID,           "id", 25, 25},

        {0, NULL, 0, 0}, /* final element */
};

static inline struct attr_display_spec *attr_info(int index)
{
    int i;
    static struct attr_display_spec tmp_rec = { -3, "?", 1, 1, NULL };
    static bool init = false;

    if (!init) {
        init = true;

        if (global_config.uid_gid_as_numbers) {
            /* Change the function to print the UID/GID, as the
             * argument is a number, not a string. */
            for (i = 0; attr[i].name != NULL; i++)
                if (attr[i].attr_index == ATTR_INDEX_uid ||
                    attr[i].attr_index == ATTR_INDEX_gid)
                    attr[i].result2str = print_res_int;
        }
    }

    if (is_status(index)) {
        /* build a special descriptor (/!\ not reentrant) */
        tmp_rec.attr_index = index;
        tmp_rec.name = get_sm_instance(attr2status_index(index))->user_name;
        tmp_rec.length_csv = tmp_rec.length_full = 15;
        tmp_rec.result2str = print_res_status;
        return &tmp_rec;
    } else if (is_sm_info(index)) {
        /* build a special descriptor (/!\ not reentrant) */
        tmp_rec.attr_index = index;
        tmp_rec.name = sm_attr_info[attr2sminfo_index(index)].user_attr_name;
        tmp_rec.length_csv = tmp_rec.length_full = 15;
        tmp_rec.result2str = print_res_sm_info;
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
    return csv ? rec->length_csv : rec->length_full;
}

const char *attrindex2name(unsigned int index)
{
    int i;

    if (is_status(index))
        return get_sm_instance(attr2status_index(index))->user_name;
    else if (is_sm_info(index))
        return sm_attr_info[attr2sminfo_index(index)].user_attr_name;

    for (i = 0; attr[i].name != NULL; i++)
        if (attr[i].attr_index == index)
            return attr[i].name;

    return "?";
}

unsigned int attrindex2len(unsigned int index, int csv)
{
    int i;

    if (is_status(index))
        return 15;
    else if (is_sm_info(index))
        return 30;

    for (i = 0; attr[i].name != NULL; i++)
        if (attr[i].attr_index == index)
            return csv ? attr[i].length_csv : attr[i].length_full;

    return 1;   /* for '?' */
}

#define PROF_CNT_LEN     8
#define PROF_RATIO_LEN   7

/** standard attribute display for reports */
void print_attr_list_custom(int rank_field, unsigned int *attr_list,
                            int attr_count, profile_field_descr_t *p_profile,
                            bool csv, const char *custom_title, int custom_len)
{
    int i;
    int coma = 0;
    struct attr_display_spec *rec;

    if (rank_field) {
        printf("rank");
        coma = 1;
    }
    for (i = 0; i < attr_count; i++) {
        rec = attr_info(attr_list[i]);
        if (coma)
            printf(", %*s", rec_len(rec, csv), rec->name);
        else {
            printf("%*s", rec_len(rec, csv), rec->name);
            coma = 1;
        }
    }
    if (p_profile) {
        if (p_profile->attr_index == ATTR_INDEX_size) {
            for (i = 0; i < SZ_PROFIL_COUNT; i++) {
                if (coma)
                    printf(", %*s", PROF_CNT_LEN, size_range[i].title);
                else {
                    printf("%*s", PROF_CNT_LEN, size_range[i].title);
                    coma = 1;
                }
            }
            if (p_profile->range_ratio_len > 0) {
                char tmp[128];
                char tmp1[128];
                char tmp2[128];
                if (p_profile->range_ratio_start + p_profile->range_ratio_len ==
                    SZ_PROFIL_COUNT)
                    sprintf(tmp, "ratio(%s..inf)",
                            print_brief_sz(SZ_MIN_BY_INDEX
                                           (p_profile->range_ratio_start),
                                           tmp1));
                else
                    sprintf(tmp, "ratio(%s..%s-)",
                            print_brief_sz(SZ_MIN_BY_INDEX
                                           (p_profile->range_ratio_start),
                                           tmp1),
                            print_brief_sz(SZ_MIN_BY_INDEX
                                           (p_profile->range_ratio_start +
                                            p_profile->range_ratio_len), tmp2));

                printf(", %*s", PROF_RATIO_LEN, tmp);
            }
        }
    }
    if (custom_title) {
        if (coma)
            printf(", %*s", custom_len, custom_title);
        else
            printf("%*s", custom_len, custom_title);
    }
    printf("\n");
}

void print_attr_values_custom(int rank, unsigned int *attr_list, int attr_count,
                              attr_set_t *attrs, const entry_id_t *id,
                              bool csv, name_func name_resolver,
                              const char *custom, int custom_len)
{
    int i, coma = 0;
    char str[24576];
    struct attr_display_spec *rec;

    if (rank) {
        printf("%4d", rank);
        coma = 1;
    }

    for (i = 0; i < attr_count; i++) {
        rec = attr_info(attr_list[i]);
        if (coma)
            printf(", %*s", rec_len(rec, csv),
                   attr2str(attrs, id, attr_list[i], csv, name_resolver, str,
                            sizeof(str)));
        else {
            printf("%*s", rec_len(rec, csv),
                   attr2str(attrs, id, attr_list[i], csv, name_resolver, str,
                            sizeof(str)));
            coma = 1;
        }
    }
    if (custom) {
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
    switch (desc->attr_index) {
    case ATTR_INDEX_COUNT:
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
void display_report(const report_field_descr_t *descr,
                    unsigned int field_count, const db_value_t *result,
                    unsigned int result_count,
                    const profile_field_descr_t *prof_descr,
                    profile_u *p_prof, bool csv, bool header, int rank)
{
    unsigned int i;
    struct attr_display_spec *rec;

    if (header) {
        if (rank)
            printf("rank, ");

        rec = attr_info(descr[0].attr_index);
        printf("%*s", rec_len(rec, csv), attrdesc2name(&descr[0], rec));

        for (i = 1; i < field_count && i < result_count; i++) {
            if (!result || !DB_IS_NULL(&result[i])) {
                rec = attr_info(descr[i].attr_index);
                printf(", %*s", rec_len(rec, csv),
                       attrdesc2name(&descr[i], rec));
            }
        }
        if (prof_descr) {
            if (prof_descr->attr_index == ATTR_INDEX_size) {
                for (i = 0; i < SZ_PROFIL_COUNT; i++)
                    printf(", %*s", PROF_CNT_LEN, size_range[i].title);

                if (prof_descr->range_ratio_len > 0) {
                    char tmp[128];
                    char tmp1[128];
                    char tmp2[128];
                    if (prof_descr->range_ratio_start +
                        prof_descr->range_ratio_len == SZ_PROFIL_COUNT)
                        sprintf(tmp, "ratio(%s..inf)",
                                print_brief_sz(SZ_MIN_BY_INDEX
                                               (prof_descr->range_ratio_start),
                                               tmp1));
                    else
                        sprintf(tmp, "ratio(%s..%s)",
                                print_brief_sz(SZ_MIN_BY_INDEX
                                               (prof_descr->range_ratio_start),
                                               tmp1),
                                print_brief_sz(SZ_MIN_BY_INDEX
                                               (prof_descr->range_ratio_start +
                                                prof_descr->range_ratio_len) -
                                               1, tmp2));

                    printf(", %*s", PROF_RATIO_LEN, tmp);
                }
            }
        }

        printf("\n");
    }

    if (result) {
        if (rank)
            printf("%4d, ", rank);

        char tmpstr[1024];
        for (i = 0; i < field_count && i < result_count; i++) {
            rec = attr_info(descr[i].attr_index);

            if (!DB_IS_NULL(&result[i]) || i == 0)  /* tag first column */
                printf("%s%*s", i == 0 ? "" : ", ", rec_len(rec, csv),
                       rec->result2str(&result[i], csv, tmpstr,
                                       sizeof(tmpstr)));
        }

        if (prof_descr && p_prof) {
            if (prof_descr->attr_index == ATTR_INDEX_size) {
                uint64_t tot = 0;
                uint64_t range = 0;

                for (i = 0; i < SZ_PROFIL_COUNT; i++) {
                    printf(", %*" PRIu64, PROF_CNT_LEN,
                           p_prof->size.file_count[i]);
                    tot += p_prof->size.file_count[i];
                    if ((prof_descr->range_ratio_len > 0) &&
                        (i >= prof_descr->range_ratio_start) &&
                        (i <
                         prof_descr->range_ratio_start +
                         prof_descr->range_ratio_len))
                        range += p_prof->size.file_count[i];
                }

                if (prof_descr->range_ratio_len > 0)
                    printf(", %.2f%%", 100.0 * range / tot);
            }
        }

        printf("\n");
    }
}

/** initialize internal resources (glib, llapi, internal resources...) */
int rbh_init_internals(void)
{
    int rc = 0;

#if !(GLIB_CHECK_VERSION(2, 32, 0))
    g_thread_init(NULL);
#endif

    /* Initialize global tools */
#ifdef _LUSTRE
    if ((rc = Lustre_Init()) != 0) {
        fprintf(stderr, "Error %d initializing liblustreapi\n", rc);
        return rc;
    }
#endif

    /* Initilize uidgid cache */
    if (InitUidGid_Cache()) {
        fprintf(stderr, "Error initializing uid/gid cache\n");
        return 1;
    }

    return rc;
}

/** convert a list of attribute indexes into a attribute mask. */
attr_mask_t list2mask(unsigned int *attr_list, int attr_count)
{
    int i;
    attr_mask_t mask = { 0 };

    for (i = 0; i < attr_count; i++) {
        /* skip special values (ID, ...) */
        if (attr_list[i] & ATTR_INDEX_FLG_UNSPEC)
            continue;
        attr_mask_set_index(&mask, attr_list[i]);
    }

    return mask;
}

/** template callback to display stdout and stderr */
int cb_redirect_all(void *arg, char *line, size_t size, int stream)
{
    int len;

    if (line == NULL)
        return -EINVAL;

    len = strnlen(line, size);
    /* terminate the string */
    if (len >= size)
        line[len - 1] = '\0';

    /* remove '\n' */
    if ((len > 0) && (line[len - 1] == '\n'))
        line[len - 1] = '\0';

    switch (stream) {
    case STDOUT_FILENO:
        printf("%s\n", line);
        break;
    case STDERR_FILENO:
        fprintf(stderr, "%s\n", line);
        break;
    }

    return 0;
}
