/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#ifndef _RBH_FIND_H
#define _RBH_FIND_H

#define FIND_TAG "find"

extern attr_mask_t disp_mask;

/* program options */
struct find_opt {
    const char     *user;
    const char     *group;
    const char     *type;
    // size cond: gt/eq/lt <val>
    compare_direction_t sz_compar;
    uint64_t        sz_val;
    const char     *name;
    const char     *class;
    unsigned int    ost_idx;
    const char     *pool;
    /* status manager for -lsstatus */
    const char     *lsstatus_name;
    sm_instance_t  *smi;

    /* status name and value for -status */
    sm_instance_t  *filter_smi;
    char           *filter_status_name;
    char           *filter_status_value;

    // crtime cond: gt/eq/lt <time>
    compare_direction_t crt_compar;
    time_t              crt_val;

    // ctime cond: gt/eq/lt <time>
    compare_direction_t chg_compar;
    time_t              chg_val;

    // mtime cond: gt/eq/lt <time>
    compare_direction_t mod_compar;
    time_t              mod_val;

    // atime cond: gt/eq/lt <time>
    compare_direction_t acc_compar;
    time_t              acc_val;

    char              **exec_cmd;

    /* query option */
    enum {
        bulk_unspec = 0,
        force_bulk,
        force_nobulk
    } bulk;

    /* output flags */
    unsigned int ls:1;
    unsigned int lsost:1;
    unsigned int lsclass:1;
    unsigned int lsstatus:1;
    unsigned int print:1;
    unsigned int printf:1;
    unsigned int escaped:1;

    /* condition flags */
    unsigned int match_user:1;
    unsigned int match_group:1;
    unsigned int match_type:1;
    unsigned int match_size:1;
    unsigned int match_name:1;
    unsigned int match_class:1;
    unsigned int match_crtime:1;
    unsigned int match_mtime:1;
    unsigned int match_atime:1;
    unsigned int match_ctime:1;
#ifdef _LUSTRE
    unsigned int match_ost:1;
    unsigned int match_pool:1;
#endif
    unsigned int match_status:1;
    unsigned int statusneg:1;

    /* -not flags */
    unsigned int userneg:1;
    unsigned int groupneg:1;
    unsigned int nameneg:1;
    unsigned int classneg:1;

    /* case insensitive name */
    unsigned int iname:1;

    /* behavior flags */
    unsigned int no_dir:1;   /* if -t != dir => no dir to be displayed */
    unsigned int dir_only:1; /* if -t dir => only display dir */

    /* actions */
    unsigned int exec:1;

};
extern struct find_opt prog_options;

const char *type2char(const char *type);
const char type2onechar(const char *type);

GArray *prepare_printf_format(const char *format);
void printf_entry(GArray *chunks, const wagon_t *id,
                  const attr_set_t *attrs);
void free_printf_formats(GArray *chunks);

#endif
