/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2004-2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * \file  RobinhoodMisc.h
 * \brief Common tools for parsing, converting, checking FS status...
 */
#ifndef _RBH_CMD_HELPERS_H
#define _RBH_CMD_HELPERS_H

#include "list_mgr.h"
#include "xplatform_print.h"
#include "rbh_misc.h"
#include "status_manager.h"

/* special character sequences for displaying help */

/* Bold start character sequence */
#define _B "[1m"
/* Bold end character sequence */
#define B_ "[m"

/* Underline start character sequence */
#define _U "[4m"
/* Underline end character sequence */
#define U_ "[0m"

/** initialize internal resources (glib, llapi, internal resources...) */
int rbh_init_internals(void);

/** The caller's function to be called for scanned entries */
typedef int (*scrub_callback_t) (wagon_t *id_list,
                                 attr_set_t *attr_list,
                                 unsigned int entry_count, void *arg);

/** scan sets of directories
 * \param cb_func, callback function for each set of directory
 */
int rbh_scrub(lmgr_t *p_mgr, const wagon_t *id_list,
              unsigned int id_count, attr_mask_t dir_attr_mask,
              scrub_callback_t cb_func, void *arg);

int Path2Id(const char *path, entry_id_t *id);

/** Free the content of a wagon list. */
static inline void free_wagon(wagon_t *ids, int first, int last)
{
    int i;

    if (ids) {
        for (i = first; i < last; i++) {
            free(ids[i].fullname);
        }
    }
}

/** parse attrset for --diff option */
int parse_diff_mask(const char *arg, attr_mask_t *diff_mask, char *msg);

/** parse a status argument <status_name|policy_name>[:<status_value>] */
int parse_status_arg(const char *option, char *arg, char **p_st_name,
                     char **p_st_val, bool mandatory_value);

/** check a status argument <status_name|policy_name>[:<status_value>],
 *  after the configuration has been loaded.
 */
int check_status_args(const char *status_name, const char *status_value,
                      const char **str_val_new, sm_instance_t **p_smi);

#define KB  1024LL
#define MB  (KB*KB)
#define GB  (KB*MB)
#define TB  (KB*GB)
#define PB  (KB*TB)
#define EB  (KB*PB)

static inline char *print_brief_sz(uint64_t sz, char *buf)
{
    if (sz < KB)
        sprintf(buf, "%" PRIu64, sz);
    else if (sz < MB)
        sprintf(buf, "%lluK", sz / KB);
    else if (sz < GB)
        sprintf(buf, "%lluM", sz / MB);
    else if (sz < TB)
        sprintf(buf, "%lluG", sz / GB);
    else if (sz < PB)
        sprintf(buf, "%lluT", sz / TB);
    else if (sz < EB)
        sprintf(buf, "%lluP", sz / PB);
    else
        sprintf(buf, "%lluE", sz / EB);
    return buf;
}

/* special attr indexes for display functions */
#define ATTR_INDEX_COUNT ATTR_INDEX_FLG_COUNT
#define ATTR_INDEX_ID    (ATTR_INDEX_FLG_UNSPEC | 0x1)

const char *attrindex2name(unsigned int index);
unsigned int attrindex2len(unsigned int index, int csv);

/** function to try resolving the name from attributes and id */
typedef const char * (*name_func)(const entry_id_t *p_id, attr_set_t *attrs,
                                  char *buff);

const char *attr2str(attr_set_t *attrs, const entry_id_t *id,
                     unsigned int attr_index, int csv, name_func name_resolver,
                     char *out, size_t out_sz);

void print_attr_list_custom(int rank_field, unsigned int *attr_list,
                            int attr_count, profile_field_descr_t *p_profile,
                            bool csv, const char *custom_title, int custom_len);

void print_attr_values_custom(int rank, unsigned int *attr_list, int attr_count,
                              attr_set_t *attrs, const entry_id_t *id,
                              bool csv, name_func name_resolver,
                              const char *custom, int custom_len);

static inline void print_attr_list(int rank_field, unsigned int *attr_list,
                                   int attr_count,
                                   profile_field_descr_t *p_profile, bool csv)
{
    print_attr_list_custom(rank_field, attr_list, attr_count, p_profile, csv,
                           NULL, 0);
}

static inline void print_attr_values(int rank, unsigned int *attr_list,
                                     int attr_count, attr_set_t *attrs,
                                     const entry_id_t *id, bool csv,
                                     name_func name_resolver)
{
    print_attr_values_custom(rank, attr_list, attr_count, attrs, id, csv,
                             name_resolver, NULL, 0);
}

void display_report(const report_field_descr_t *descr,
                    unsigned int field_count, const db_value_t *result,
                    unsigned int result_count,
                    const profile_field_descr_t *prof_descr,
                    profile_u * p_prof, bool csv, bool header, int rank);

/** convert a list of attribute indexes into a attribute mask. */
attr_mask_t list2mask(unsigned int *attr_list, int attr_count);

static inline const char *class_format(const char *class_name)
{
    if (class_name == NULL)
        return "[n/a]";
    else if (EMPTY_STRING(class_name))
        return "[none]";

    return class_name;
}

static inline const char *status_format(const char *name)
{
    if (name == NULL)
        return "[none]";

    return name;
}

/** callback to display stdout and stderr */
int cb_redirect_all(void *arg, char *line, size_t size, int stream);

#endif
