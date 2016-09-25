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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#define TAG "Triggers"
#define tag(_p)             ((_p)->descr->name)

#include "rbh_cfg.h"
#include "rbh_misc.h"
#include "policy_run.h"
#include "run_policies.h"
#include "queue.h"
#include "Memory.h"
#include "xplatform_print.h"
#include <errno.h>
#include <pthread.h>
#include <unistd.h>

#ifdef __FreeBSD__
#include <sys/param.h>
#include <sys/mount.h>
#else /* Linux */
#include <sys/vfs.h>
#endif

/* ------------ Types and global variables ------------ */

#define is_count_trigger(_t_) ((_t_)->hw_type == COUNT_THRESHOLD)
#define check_only(_p) ((_p)->flags & RUNFLG_CHECK_ONLY)
#define one_shot(_p) ((_p)->flags & RUNFLG_ONCE)

static void update_trigger_status(policy_info_t *pol, int i,
                                  trigger_status_t state)
{
    if (i < 0)
        return;

    pol->trigger_info[i].status = state;

    if (state == TRIG_BEING_CHECKED)
        pol->trigger_info[i].last_check = time(NULL);
}

/**
 * Function for checking that filesystem hasn't been unmounted
 */
static bool CheckFSDevice(policy_info_t *pol)
{
    struct stat root_md;

    /* retrieve device of filesystem, to compare it to initial device id */

    if (stat(global_config.fs_path, &root_md) == -1) {
        DisplayLog(LVL_CRIT, tag(pol), "Stat on '%s' failed! Error %d: %s",
                   global_config.fs_path, errno, strerror(errno));
        return false;
    }
    if (root_md.st_dev != pol->fs_dev) {
        /* manage dev id change after umount/mount */
        DisplayLog(LVL_MAJOR, tag(pol),
                   "WARNING: Filesystem device id changed (old=%" PRI_DT
                   ", new=%" PRI_DT "): " "checking if it has been remounted",
                   pol->fs_dev, root_md.st_dev);
        if (ResetFS()) {
            DisplayLog(LVL_CRIT, tag(pol),
                       "Filesystem was unmounted!!! EXITING!");
            Exit(1);
        }
        /* update current fsdev */
        pol->fs_dev = get_fsdev();
    }
    return true;
}

static unsigned long long FSInfo2Blocs512(unsigned long long nb_blocks,
                                          unsigned long long sz_blocks)
{
    uint64_t total_sz;
    unsigned long long nb_blocks_512;
    unsigned long long rest;

    /* avoid useless computations */
    if (sz_blocks == DEV_BSIZE)
        return nb_blocks;

    /* if block size is different from 512 */
    total_sz = nb_blocks * sz_blocks;
    nb_blocks_512 = total_sz / DEV_BSIZE;
    rest = total_sz % DEV_BSIZE;

    if (rest == 0)
        return nb_blocks_512;
    else
        return nb_blocks_512 + 1;
}

/* ------------ Functions for checking each type of trigger ------------ */

static inline int statfs2usage(const struct statfs *p_statfs,
                               unsigned long long *used_vol,
                               double *used_pct,
                               unsigned long long *total_blocks,
                               const char *storage_descr)
{
    /* check df consistency:
     * used = total - free = f_blocks - f_bfree
     * if used + available <= 0, there's something wrong
     */
    if (p_statfs->f_blocks + p_statfs->f_bavail - p_statfs->f_bfree <= 0) {
        DisplayLog(LVL_CRIT, TAG,
                   "ERROR: statfs on %s returned inconsistent values!!!",
                   storage_descr);
        DisplayLog(LVL_CRIT, TAG,
                   "Detail: blks=%" PRIu64 " avail=%" PRIu64 " free=%" PRIu64,
                   p_statfs->f_blocks, p_statfs->f_bavail, p_statfs->f_bfree);
        return -EIO;
    }

    /* number of blocks available to users */
    *total_blocks = (p_statfs->f_blocks + p_statfs->f_bavail
                     - p_statfs->f_bfree);
    *used_pct = 100.0 * ((double)p_statfs->f_blocks - (double)p_statfs->f_bfree)
        / ((double)(*total_blocks));
    *used_vol = (p_statfs->f_blocks - p_statfs->f_bfree) * p_statfs->f_bsize;

    return 0;
}

/** function for checking thresholds (for global FS, single OST,...)
 * @return negative value on error
 * @retval 0 on success (in this case, to_be_purged gives the number
 *                       of blocks to be purged)
 */
static int check_blocks_thresholds(trigger_item_t *p_trigger,
                                   const char *storage_descr,
                                   const struct statfs *p_statfs,
                                   unsigned long long *to_be_purged_512,
                                   double *p_used_pct)
{
    unsigned long long total_user_blocks = 0, block_target = 0, used_vol = 0;
    double used_pct = 0;
    char tmp1[128];
    char tmp2[128];
    char buff[1024];
    int rc;

    *to_be_purged_512 = 0;  /* FIXME 'purged' is policy specific */

    if ((rc = statfs2usage(p_statfs, &used_vol, &used_pct, &total_user_blocks,
                           storage_descr)))
        return rc;

    /* return last usage */
    if (p_used_pct)
        *p_used_pct = used_pct;

    /* is this a condition on volume or percentage ? */
    if (p_trigger->hw_type == VOL_THRESHOLD) {
        FormatFileSize(tmp1, sizeof(tmp1), used_vol);
        FormatFileSize(tmp2, sizeof(tmp2), p_trigger->hw_volume);

        /* compare used volume to threshold */
        DisplayLog(LVL_VERB, TAG, "%s usage: %s / high threshold: %s",
                   storage_descr, tmp1, tmp2);

        if (used_vol < p_trigger->hw_volume) {
            DisplayLog(LVL_DEBUG, TAG,
                       "%s usage is under high threshold: nothing to do.",
                       storage_descr);
            return 0;
        } else if (p_trigger->alert_hw) {
            snprintf(buff, sizeof(buff), "High threshold reached on %s",
                     storage_descr);
            RaiseAlert(buff, "%s\nspaced used: %s (%.2f%%), high threshold: %s",
                       buff, tmp1, used_pct, tmp2);
        } else {
            DisplayLog(LVL_MAJOR, TAG, "High threshold reached on %s (%s): "
                       "spaced used: %s (%.2f%%), high threshold: %s",
                       storage_descr, global_config.fs_path, tmp1, used_pct,
                       tmp2);
        }
    } else if (p_trigger->hw_type == PCT_THRESHOLD) {
        unsigned long long used_hw =
            (unsigned long long)((p_trigger->hw_percent * total_user_blocks) /
                                 100.0);

        DisplayLog(LVL_VERB, TAG,
                   "%s usage: %.2f%% (%" PRIu64
                   " blocks) / high threshold: %.2f%% (%llu blocks)",
                   storage_descr, used_pct,
                   p_statfs->f_blocks - p_statfs->f_bfree,
                   p_trigger->hw_percent, used_hw);

        if (used_pct < p_trigger->hw_percent) {
            DisplayLog(LVL_DEBUG, TAG, "%s usage is under high threshold: "
                       "nothing to do.", storage_descr);
            return 0;
        } else if (p_trigger->alert_hw) {
            FormatFileSize(tmp1, sizeof(tmp1), used_vol);
            snprintf(buff, sizeof(buff), "High threshold reached on %s",
                     storage_descr);
            RaiseAlert(buff,
                       "%s\nspaced used: %s (%.2f%%), high threshold: %.2f%%",
                       buff, tmp1, used_pct, p_trigger->hw_percent);
        } else {
            FormatFileSize(tmp1, sizeof(tmp1), used_vol);
            DisplayLog(LVL_MAJOR, TAG, "High threshold reached on %s (%s): "
                       "spaced used: %s (%.2f%%), high threshold: %.2f%%",
                       storage_descr, global_config.fs_path, tmp1, used_pct,
                       p_trigger->hw_percent);
        }
    }

    /* if we reach this point, high threshold is exceeded.
     * compute the amount of data for reaching low threshold */

    if (p_trigger->lw_type == VOL_THRESHOLD) {
        block_target = (p_trigger->lw_volume / p_statfs->f_bsize);
        if (p_trigger->lw_volume % p_statfs->f_bsize)
            block_target++;
        DisplayLog(LVL_VERB, TAG, "Target usage volume: %s (%llu blocks)",
                   FormatFileSize(tmp1, sizeof(tmp1), p_trigger->lw_volume),
                   block_target);
    } else if (p_trigger->lw_type == PCT_THRESHOLD) {
        block_target =
            (unsigned long
             long)((p_trigger->lw_percent * (double)total_user_blocks) / 100.0);
        DisplayLog(LVL_VERB, TAG,
                   "Target usage percentage: %.2f%% (%llu blocks)",
                   p_trigger->lw_percent, block_target);
    } else {
        DisplayLog(LVL_CRIT, TAG,
                   "Unexpected Low Threshold type %d. Trigger skipped.",
                   p_trigger->lw_type);
        return -EINVAL;
    }

    if (p_statfs->f_blocks - p_statfs->f_bfree <= block_target) {
        DisplayLog(LVL_EVENT, TAG,
                   "Usage is already under low threshold. Do nothing.");
        return 0;
    }

    /* to be purged= blocks used - block_target */
    *to_be_purged_512 =
        FSInfo2Blocs512((p_statfs->f_blocks - p_statfs->f_bfree) - block_target,
                        p_statfs->f_bsize);

    DisplayLog(LVL_EVENT, TAG,
               "%llu blocks (x%u) must be processed on %s (used=%" PRIu64
               ", target=%llu, block size=%zu)", *to_be_purged_512, DEV_BSIZE,
               storage_descr, p_statfs->f_blocks - p_statfs->f_bfree,
               block_target, p_statfs->f_bsize);

    return 0;
}

/** function for checking inode count thresholds
 * @return negative value on error
 * @retval 0 on success (in this case, to_be_purged gives the number of
 *                       entries to be purged)
 */
static int check_count_thresholds(trigger_item_t *p_trigger,
                                  const char *storage_descr,
                                  const struct statfs *p_statfs,
                                  unsigned long long *to_be_purged,
                                  unsigned long long *count_used)
{
    unsigned long long inode_used;

    *to_be_purged = 0;

    /* check df consistency: free < total */
    if (p_statfs->f_ffree > p_statfs->f_files) {
        DisplayLog(LVL_CRIT, TAG,
                   "ERROR: statfs on %s returned inconsistent values!!!",
                   storage_descr);
        DisplayLog(LVL_CRIT, TAG, "Detail: total=%" PRIu64 ", free=%" PRIu64,
                   p_statfs->f_files, p_statfs->f_ffree);
        return -EIO;
    }

    /* number of inodes used */
    inode_used = p_statfs->f_files - p_statfs->f_ffree;

    /* return last usage */
    if (count_used)
        *count_used = inode_used;

    /* check it is a condition on inode count */
    if ((p_trigger->hw_type != COUNT_THRESHOLD)
        || (p_trigger->lw_type != COUNT_THRESHOLD)) {
        DisplayLog(LVL_CRIT, TAG,
                   "Unexpected threshold types %d, %d. Trigger skipped.",
                   p_trigger->hw_type, p_trigger->lw_type);
        return -EINVAL;
    }

    DisplayLog(LVL_EVENT, TAG, "%s entry count: %llu / high threshold: %llu",
               storage_descr, inode_used, p_trigger->hw_count);

    if (inode_used < p_trigger->hw_count) {
        DisplayLog(LVL_VERB, TAG,
                   "%s inode count is under high threshold: nothing to do.",
                   storage_descr);
        return 0;
    } else if (p_trigger->alert_hw) {
        char buff[1024];
        snprintf(buff, sizeof(buff), "High threshold reached on %s",
                 storage_descr);
        RaiseAlert(buff, "%s\nentry count: %llu, high threshold: %llu", buff,
                   inode_used, p_trigger->hw_count);
    }

    /* if we reach this point, high threshold is exceeded compute the amount
     * of data for reaching low threshold */
    DisplayLog(LVL_VERB, TAG, "Target entry count: %llu", p_trigger->lw_count);

    if (inode_used <= p_trigger->lw_count) {
        DisplayLog(LVL_EVENT, TAG,
                   "Inode count is already under low threshold. Do nothing.");
        return 0;
    }

    *to_be_purged = inode_used - p_trigger->lw_count;

    DisplayLog(LVL_EVENT, TAG,
               "%llu entries must be processed in %s (used=%llu, target=%llu)",
               *to_be_purged, storage_descr, inode_used, p_trigger->lw_count);
    return 0;
}

/* -------- NEW CODE -------- */

/** get the total number of usable blocks in the filesystem */
static int total_blocks(unsigned long long *total_user_blocks,
                        unsigned long long *bsize)
{
    struct statfs stfs;
    char traverse_path[RBH_PATH_MAX];

    snprintf(traverse_path, RBH_PATH_MAX, "%s/.", global_config.fs_path);

    if (statfs(traverse_path, &stfs) != 0) {
        int err = errno;

        DisplayLog(LVL_CRIT, TAG, "Could not make a 'df' on %s: error %d: %s",
                   global_config.fs_path, err, strerror(err));
        return err;
    }
    /* number of blocks available to users */
    *total_user_blocks = (stfs.f_blocks + stfs.f_bavail - stfs.f_bfree);
    *bsize = stfs.f_bsize;
    return 0;
}

static int get_fs_usage(policy_info_t *pol, struct statfs *stfs)
{
    char traverse_path[RBH_PATH_MAX];

    snprintf(traverse_path, RBH_PATH_MAX, "%s/.", global_config.fs_path);

    if (!CheckFSDevice(pol))
        return ENODEV;

    /* retrieve filesystem usage info */
    if (statfs(traverse_path, stfs) != 0) {
        int err = errno;

        DisplayLog(LVL_CRIT, tag(pol),
                   "Could not make a 'df' on %s: error %d: %s",
                   global_config.fs_path, err, strerror(err));
        return err;
    }
    return 0;
}

/**
 * @return boolean to indicate if we are in a maintenance window.
 */
static bool check_maintenance_mode(policy_info_t *pol, time_modifier_t *p_mod)
{
    struct tm dt;
    time_t next_maint, now;
    char varstr[128];
    char datestr[128];
    char leftstr[128];

    if (pol->config->pre_maintenance_window == 0)
        return false;

    /* check maintenance mode */
    if ((ListMgr_GetVar(&pol->lmgr, NEXT_MAINT_VAR, varstr, sizeof(varstr)) !=
         DB_SUCCESS)
        || EMPTY_STRING(varstr))
        return false;

    next_maint = str2int(varstr);
    if (next_maint <= 0)    /* invalid value, or disabled */
        return false;

    /* build maintenance date */
    strftime(datestr, sizeof(datestr), "%Y/%m/%d %T",
             localtime_r(&next_maint, &dt));

    now = time(NULL);
    if (next_maint < now) {
        DisplayLog(LVL_DEBUG, TAG, "Maintenance time is in the past (%s): "
                   "no time modifier", datestr);
        return false;
    } else if (now < next_maint - pol->config->pre_maintenance_window) {
        FormatDuration(leftstr, sizeof(leftstr), next_maint -
                       pol->config->pre_maintenance_window - now);

        DisplayLog(LVL_VERB, TAG, "Maintenance time is set (%s): "
                   "maintenance window will start in %s", datestr, leftstr);
        return false;
    } else {    /* this is the pre maintenance window! */

        /* linear function to compute time modifier */
        p_mod->time_factor = ((double)(next_maint - now)) /
            (double)pol->config->pre_maintenance_window;
        p_mod->time_min = pol->config->maint_min_apply_delay;

        FormatDuration(leftstr, sizeof(leftstr), next_maint - now);
        DisplayLog(LVL_MAJOR, TAG, "Currently in maintenance mode "
                   "(maintenance is in %s): time modifier = %.2f%%",
                   leftstr, 100.0 * p_mod->time_factor);
        return true;
    }
}

static int check_trigger_type(trigger_item_t *t)
{
    if ((t->target_type == TGT_NONE || t->target_type == TGT_FS
#ifdef _LUSTRE
        /* XXX does not suport lists of target osts */
        || t->target_type == TGT_OST
#endif
        ) && (t->list_size != 0))
        RBH_BUG("Unexpected target for trigger");
    return 0;
}

/* not a function as it may be int or int64 */
#define min_param(_param, _config) (_param == 0 ? _config : MIN(_param, _config))

/* Set limits for the policy run, according to global policy config
 * and trigger config.
 * Only override the current info when it is too high or not set.
 */
static void set_limits(const policy_info_t *pol, const trigger_item_t *trig,
                       counters_t *limit)
{
    /* set policy run limits */
    if ((trig != NULL) && (trig->max_action_nbr != 0))
        limit->count = min_param(limit->count, trig->max_action_nbr);

    if (pol->config->max_action_nbr != 0)
        limit->count = min_param(limit->count, pol->config->max_action_nbr);

    if ((trig != NULL) && (trig->max_action_vol != 0))
        limit->vol = min_param(limit->vol, trig->max_action_vol);

    if (pol->config->max_action_vol != 0)
        limit->vol = min_param(limit->vol, pol->config->max_action_vol);
}

/**
 * Print policy target description to a string.
 * @return str
 */
static const char *param2targetstr(const policy_param_t *param, char *str,
                                   size_t len)
{
    switch (param->target) {
    case TGT_FS:
        /* snprintf is safer than strncpy as it null terminates string */
        snprintf(str, len, "all");
        return str;
#ifdef _LUSTRE
    case TGT_OST:
        snprintf(str, len, "OST#%u", param->optarg_u.index);
        return str;
    case TGT_POOL:
        snprintf(str, len, "pool %s", param->optarg_u.name);
        return str;
#endif
    case TGT_USER:
        snprintf(str, len, "user %s", param->optarg_u.name);
        return str;
    case TGT_GROUP:
        snprintf(str, len, "group %s", param->optarg_u.name);
        return str;
    case TGT_FILE: /* only for manual actions */
        snprintf(str, len, "entry '%s'", param->optarg_u.name);
        return str;
    case TGT_CLASS:    /* only for manual actions */
        snprintf(str, len, "fileclass %s", param->optarg_u.name);
        return str;
    default:
        RBH_BUG("unexpected trigger target");
    }
}

#ifdef _LUSTRE
struct ost_list {
    unsigned int *list;
    unsigned int count;
};
static inline void ost_list_init(struct ost_list *l)
{
    l->list = NULL;
    l->count = 0;
}

static inline int ost_list_add(struct ost_list *l, unsigned int ost_idx)
{
    l->list = MemRealloc(l->list, (l->count + 1) * sizeof(*l->list));
    if (!l->list)
        return ENOMEM;

    l->list[l->count] = ost_idx;
    l->count++;
    return 0;
}

static inline void ost_list_free(struct ost_list *l)
{
    if (l->list)
        MemFree(l->list);
    l->list = NULL;
    l->count = 0;
}

static inline bool ost_list_is_member(struct ost_list *l,
                                      unsigned int test_member)
{
    int i;
    for (i = 0; i < l->count; i++) {
        if (l->list[i] == test_member)
            return true;
    }
    return false;
}

static int get_ost_max(struct statfs *df, trigger_value_type_t tr_type,
                       struct ost_list *excluded)
{
    int ost_index, rc = 0;
    int ost_max = -1;
    unsigned long long ost_blocks;
    struct statfs stat_max, stat_tmp;
    double max_pct = 0.0, curr_pct = 0.0;
    unsigned long long max_vol = 0LL, curr_vol = 0;
    char ostname[128];

    for (ost_index = 0;; ost_index++) {
        if (ost_list_is_member(excluded, ost_index))
            continue;

        rc = Get_OST_usage(global_config.fs_path, ost_index, &stat_tmp);
        if (rc == ENODEV)   /* end of OST list */
            break;
        else if (rc != 0)
            /* continue with next OSTs */
            continue;

        snprintf(ostname, sizeof(ostname), "OST #%u", ost_index);
        if (statfs2usage(&stat_tmp, &curr_vol, &curr_pct, &ost_blocks, ostname))
            /* continue with next OSTs */
            continue;

        switch (tr_type) {
        case VOL_THRESHOLD:
            if (curr_vol > max_vol) {
                ost_max = ost_index;
                max_vol = curr_vol;
                stat_max = stat_tmp;
            }
            break;
        case PCT_THRESHOLD:
            if (curr_pct > max_pct) {
                ost_max = ost_index;
                max_pct = curr_pct;
                stat_max = stat_tmp;
            }
            break;
        default:
            RBH_BUG("Unexpected OST trigger type");
        }
    }

    if (ost_max == -1)
        /* none found */
        return -ENOENT;

    *df = stat_max;
    return ost_max;
}
#endif

/** build report argument for a user or group */
static void build_user_report_descr(report_field_descr_t info[],
                                    trigger_item_t *trig,
                                    unsigned long long high_blk)
{
    info[0].attr_index = (trig->target_type == TGT_USER ? ATTR_INDEX_uid :
                          ATTR_INDEX_gid);
    info[0].report_type = REPORT_GROUP_BY;
    info[0].sort_flag = SORT_NONE;
    info[0].filter = false;

    if (is_count_trigger(trig)) {
        info[1].attr_index = 0;
        info[1].report_type = REPORT_COUNT;
        info[1].sort_flag = SORT_DESC;  /* start with top consumer */
        info[1].filter = true;
        info[1].filter_compar = MORETHAN_STRICT;
        info[1].filter_value.value.val_biguint = trig->hw_count;
    } else {    /* volume based trigger */

        /* select users/groups having sum(blocks) > high_threshold (blocks) */
        info[1].attr_index = ATTR_INDEX_blocks;
        info[1].report_type = REPORT_SUM;
        info[1].sort_flag = SORT_DESC;  /* start with top consumer */
        info[1].filter = true;
        info[1].filter_compar = MORETHAN_STRICT;
        info[1].filter_value.value.val_biguint = high_blk;
    }
}

/** build request filter for user or group triggers */
static int build_user_report_filter(lmgr_filter_t *filter,
                                    trigger_item_t *trig)
{
    int i;
    filter_value_t fv;
    int attr_index = (trig->target_type == TGT_USER ? ATTR_INDEX_uid :
                      ATTR_INDEX_gid);

    /* FIXME consider released entries in quota? */

    /* if a specific set of users/groups is specified, make a filter for this */

    /* 2 cases: if there is a single user/group, add a simple filter for it:
     * AND owner LIKE ...
     * If there are several users/groups, add a OR sequence:
     * AND (owner LIKE ... OR owner LIKE ...)
     */
    for (i = 0; i < trig->list_size; i++) {
        int flag = 0;

        if (attr_index == ATTR_INDEX_uid) {
            if (set_uid_val(trig->list[i], &fv.value))
                return -EINVAL;
        } else {
            if (set_gid_val(trig->list[i], &fv.value))
                return -EINVAL;
        }

        /* add parenthesis and 'OR' for lists of items */
        if (trig->list_size > 1) {
            if (i == 0) /* first item */
                flag |= FILTER_FLAG_BEGIN;
            else if (i == trig->list_size - 1)  /* last item */
                flag |= FILTER_FLAG_END;

            /* add OR (except for the first item) */
            if (i > 0)
                flag |= FILTER_FLAG_OR;
        }
        /* else: single value in list => flag = 0 */

        lmgr_simple_filter_add(filter, attr_index,
                               global_config.uid_gid_as_numbers ? EQUAL : LIKE,
                               fv, flag);
    }

    return 0;
}

/** check thresholds for a given trigger target */
static int check_statfs_thresholds(trigger_item_t *trig, const char *tgt_name,
                                   struct statfs *stfs, counters_t *limit,
                                   trigger_info_t *tinfo)
{
    int rc;
    double tmp_usage = 0.0;
    unsigned long long tmp_count = 0;

    if (is_count_trigger(trig)) {
        /* inode count */
        rc = check_count_thresholds(trig, tgt_name, stfs, &limit->count,
                                    &tmp_count);
        if (tmp_count > tinfo->last_count)
            tinfo->last_count = tmp_count;
    } else if (trig->target_type == TGT_FS) {
        /* block threshold */
        rc = check_blocks_thresholds(trig, tgt_name, stfs, &limit->blocks,
                                     &tmp_usage);
        if (rc == 0 && tmp_usage > tinfo->last_usage)
            tinfo->last_usage = tmp_usage;
    } else {
        /* blocks on OST or pool */
        rc = check_blocks_thresholds(trig, tgt_name, stfs, &limit->targeted,
                                     &tinfo->last_usage);
        if (tmp_usage > tinfo->last_usage)
            tinfo->last_usage = tmp_usage;
    }
    return rc;
}

/* check threshold on DB report values */
static int check_report_thresholds(trigger_item_t *p_trigger,
                                   db_value_t *result, unsigned int res_count,
                                   counters_t *limit, trigger_info_t *tinfo,
                                   unsigned long long low_blk512,
                                   unsigned long long high_blk512)
{
    const char *what = (p_trigger->target_type == TGT_USER ? "user" : "group");
    char buff[1024];

    if (res_count != 2) {
        DisplayLog(LVL_MAJOR, TAG,
                   "Invalid DB result size %u (2 values expected)", res_count);
        return EINVAL;
    }

    if (is_count_trigger(p_trigger)) {
        DisplayLog(LVL_EVENT, TAG, "%s '%s' exceeds high threshold: "
                   "used: %llu inodes / high threshold: %llu inodes.",
                   what, id_as_str(&result[0].value_u),
                   result[1].value_u.val_biguint, p_trigger->hw_count);

        limit->count = result[1].value_u.val_biguint - p_trigger->lw_count;

        DisplayLog(LVL_EVENT, TAG, "%llu files to be processed for %s '%s' "
                   "(used=%llu, target=%llu)",
                   limit->count, what, id_as_str(&result[0].value_u),
                   result[1].value_u.val_biguint, p_trigger->lw_count);

        if (p_trigger->alert_hw) {
            snprintf(buff, sizeof(buff),
                     "Inode quota exceeded for %s '%s' (in %s)", what,
                     id_as_str(&result[0].value_u), global_config.fs_path);
            RaiseAlert(buff,
                       "%s\n" "%s:       %s\n" "quota:      %llu inodes\n"
                       "usage:      %llu inodes", buff, what,
                       id_as_str(&result[0].value_u), p_trigger->hw_count,
                       result[1].value_u.val_biguint);
        }
    } else {
        char hw_str[128];

        if (p_trigger->hw_type == VOL_THRESHOLD)
            FormatFileSize(hw_str, sizeof(hw_str), p_trigger->hw_volume);
        else if (p_trigger->hw_type == PCT_THRESHOLD)
            snprintf(hw_str, sizeof(hw_str), "%.2f%%", p_trigger->hw_percent);

        DisplayLog(LVL_EVENT, TAG,
                   "%s '%s' exceeds high threshold: used: %llu blocks "
                   "/ high threshold: %llu blocks (x%u).",
                   what, id_as_str(&result[0].value_u),
                   result[1].value_u.val_biguint, high_blk512, DEV_BSIZE);

        limit->blocks = result[1].value_u.val_biguint - low_blk512;

        DisplayLog(LVL_EVENT, TAG, "%llu blocks (x%u) must be processed "
                   "for %s '%s' (used=%llu, target=%llu)",
                   limit->blocks, DEV_BSIZE, what,
                   id_as_str(&result[0].value_u), result[1].value_u.val_biguint,
                   low_blk512);

        if (p_trigger->alert_hw) {
            char usage_str[128];

            FormatFileSize(usage_str, sizeof(usage_str),
                           result[1].value_u.val_biguint * 512);
            snprintf(buff, sizeof(buff),
                     "Volume quota exceeded for %s '%s' (in %s)", what,
                     id_as_str(&result[0].value_u), global_config.fs_path);
            RaiseAlert(buff, "%s\n%s:       %s\nquota:      %s\nspace used: %s",
                       buff, what, id_as_str(&result[0].value_u), hw_str,
                       usage_str);
        }
    }
    return 0;
}

typedef struct target_iterator_t {
    trigger_item_t trig;
    policy_info_t *pol;
    union {
        /* for FS usage */
        unsigned int is_checked;
        /* for DB report iterator */
        struct lmgr_report_t *db_report;
#ifdef _LUSTRE
        /* for OST iterator */
        struct ost_list ost_excl;
        /* for pool iterator */
        unsigned int next_pool_index;
#endif
    } info_u;
    /* for user and groups vol/pct thresholds: save high and low values
     * (in blocks) */
    unsigned long long high_blk512;
    unsigned long long low_blk512;
} target_iterator_t;

/** compute user blocks and save them into it structure */
static int compute_user_blocks(trigger_item_t *trig, target_iterator_t *it)
{
    int rc;
    unsigned long long tb = 0, bs = 0;

    /* check users or groups (possible filter on specified users) */
    /* build the DB report iterator */
    if ((trig->hw_type == PCT_THRESHOLD) || (trig->lw_type == PCT_THRESHOLD)) {
        rc = total_blocks(&tb, &bs);
        if (rc)
            return rc;
    }
    if (trig->hw_type == VOL_THRESHOLD)
        it->high_blk512 = trig->hw_volume / DEV_BSIZE;
    else if (trig->hw_type == PCT_THRESHOLD)
        it->high_blk512 =
            FSInfo2Blocs512((unsigned long)((trig->hw_percent * tb) / 100.0),
                            bs);

    if (trig->lw_type == VOL_THRESHOLD)
        it->low_blk512 = trig->lw_volume / DEV_BSIZE;
    else if (trig->lw_type == PCT_THRESHOLD)
        it->low_blk512 =
            FSInfo2Blocs512((unsigned long)((trig->lw_percent * tb) / 100.0),
                            bs);
    return 0;
}

/** Create an iterator on trigger targets */
static int trig_target_it(target_iterator_t *it, policy_info_t *pol,
                          trigger_item_t *trig)
{
    /* FIXME "scheduled" triggers may accept a target too. */

    int rc;
    /* Iterate on:
     * each specified pool
     * each specified file
     * each OST,user or group, fileclass over the specified limit.
     */
    it->trig = *trig;
    it->pol = pol;

    if (trig->trigger_type == TRIG_ALWAYS) {
        it->info_u.is_checked = 0;
        return 0;
    }

    switch (trig->target_type) {
    case TGT_FS:
        /* no iteration, just check the FS usage */
        it->info_u.is_checked = 0;
        break;
#ifdef _LUSTRE
    case TGT_OST:
        /* get and check the max OST */
        ost_list_init(&it->info_u.ost_excl);
        break;
    case TGT_POOL:
        /* check listed pools */
        it->info_u.next_pool_index = 0;
        break;
#endif
    case TGT_USER:
    case TGT_GROUP:
        {
            /* check users or groups (possible filter on a set of users) */
            report_field_descr_t info[2];   /* [0]user/group: [1]nb_blocks */
            lmgr_filter_t filter;

            /* get the nbr of FS blocks and convert them to high/low block
             * thresholds */
            rc = compute_user_blocks(trig, it);
            if (rc)
                return rc;
            build_user_report_descr(info, trig, it->high_blk512);

            lmgr_simple_filter_init(&filter);
            rc = build_user_report_filter(&filter, trig);
            if (rc)
                return rc;

            it->info_u.db_report =
                ListMgr_Report(&pol->lmgr, info, 2, NULL, &filter, NULL);
            lmgr_simple_filter_free(&filter);

            if (it->info_u.db_report == NULL)
                return -1;
            break;
        }
    case TGT_FILE:
        RBH_BUG("No trigger expected on files: only for manual actions");
    case TGT_CLASS:
        RBH_BUG("No trigger expected on fileclass: only for manual actions");
    case TGT_NONE:
        RBH_BUG("Unexpected trigger type TGT_NONE");
    }

    return 0;
}

/** Get the next target from an iterator */
static int trig_target_next(target_iterator_t *it, target_u *tgt,
                            counters_t *limit, trigger_info_t *tinfo)
{
    struct statfs stfs;
    int rc;
#ifdef _LUSTRE
    char tgtname[128];
#endif

    memset(limit, 0, sizeof(*limit));

    if (it->trig.trigger_type == TRIG_ALWAYS) {
        if (it->info_u.is_checked)
            return ENOENT;  /* end of list */

        /* no limit due to usage level */
        it->info_u.is_checked = 1;
        return 0;
    }

    switch (it->trig.target_type) {
    case TGT_FS:
        if (it->info_u.is_checked)
            return ENOENT;  /* end of list */

        /* check FS usage */
        rc = get_fs_usage(it->pol, &stfs);
        if (rc)
            return rc;
        rc = check_statfs_thresholds(&it->trig, "Filesystem", &stfs, limit,
                                     tinfo);
        if (rc)
            return rc;

        it->info_u.is_checked = 1;

        if (!counter_is_set(limit))
            return ENOENT;
        else
            return 0;
        break;

#ifdef _LUSTRE
    case TGT_OST:
        {
            int ost_index;
            /* get and check the max OST */
            while ((ost_index =
                    get_ost_max(&stfs, it->trig.hw_type, &it->info_u.ost_excl))
                   != -ENOENT) {
                if (ost_index < 0)
                    return -ost_index;
                snprintf(tgtname, sizeof(tgtname), "OST #%u", ost_index);
                /* check thresholds */
                rc = check_statfs_thresholds(&it->trig, tgtname, &stfs, limit,
                                             tinfo);
                if (rc)
                    return rc;
                if (!counter_is_set(limit)) {
                    DisplayLog(LVL_DEBUG, TAG,
                               "Top OSTs are all under high threshold: "
                               "skipping check of other OSTs");
                    return ENOENT;
                } else {
                    tgt->index = ost_index;
                    /* exclude this OST for next loops */
                    if ((rc = ost_list_add(&it->info_u.ost_excl, ost_index)))
                        return rc;
                    return 0;   /* something is to be done */
                }
            }
            return ENOENT;
        }
        break;

    case TGT_POOL:
        for (; it->info_u.next_pool_index < it->trig.list_size;
             it->info_u.next_pool_index++) {
            /* check listed pools */
            const char *pool = it->trig.list[it->info_u.next_pool_index];

            rc = Get_pool_usage(pool, &stfs);
            if (rc) {
                DisplayLog(LVL_CRIT, TAG,
                           "Could not retrieve usage info for pool '%s': %s",
                           pool, strerror(rc));
                continue;
            }
            snprintf(tgtname, sizeof(tgtname), "pool '%s'", pool);
            rc = check_statfs_thresholds(&it->trig, tgtname, &stfs, limit,
                                         tinfo);
            if (rc)
                return rc;
            if (!counter_is_set(limit))
                continue;
            else {
                it->info_u.next_pool_index++;
                tgt->name = pool;
                return 0;   /* something is to be done */
            }
        }
        return ENOENT;
        break;
#endif
    case TGT_USER:
    case TGT_GROUP:
        {
            db_value_t result[2];
            unsigned int result_count = 2;

            while ((rc = ListMgr_GetNextReportItem(it->info_u.db_report,
                                                   result, &result_count,
                                                   NULL)) == DB_SUCCESS) {
                rc = check_report_thresholds(&it->trig, result, result_count,
                                             limit, tinfo, it->low_blk512,
                                             it->high_blk512);
                if (rc)
                    return rc;

                /* reset result count before continuing */
                result_count = 2;

                if (!counter_is_set(limit))
                    continue;
                else {
                    tgt->name = result[0].value_u.val_str;
                    return 0;   /* something is to be done */
                }
            }
            return ENOENT;
            break;
        }
    case TGT_FILE:
        RBH_BUG("No trigger expected on files: only for manual actions");
    case TGT_CLASS:
        RBH_BUG("No trigger expected on fileclass: only for manual actions");
    case TGT_NONE:
        RBH_BUG("Unexpected trigger type TGT_NONE");
    }
    return -1;
}

/** Close the iterator */
static void trig_target_end(target_iterator_t *it)
{
    if (it->trig.trigger_type == TRIG_ALWAYS)
        return;

    switch (it->trig.target_type) {
#ifdef _LUSTRE
    case TGT_OST:
        ost_list_free(&it->info_u.ost_excl);
        break;
#endif
    case TGT_USER:
    case TGT_GROUP:
        ListMgr_CloseReport(it->info_u.db_report);
        break;
    default:
        /* nothing to do */
        return;
    }
    return;
}

static void sprint_ctr(char *str, int size,
                       const counters_t *ctr, policy_target_t tgt_type)
{
    char buff[256];

    if (!counter_is_set(ctr)) {
        strncpy(str, "none", size);
        return;
    }

    FormatFileSize(buff, sizeof(buff), ctr->vol);

#ifdef _LUSTRE
    if (tgt_type == TGT_OST || tgt_type == TGT_POOL) {
        snprintf(str, size, "%llu entries, total volume %s "
                 "(%llu blocks, %llu in target devices)",
                 ctr->count, buff, ctr->blocks, ctr->targeted);
    } else
#endif
    {
        snprintf(str, size, "%llu entries, total volume %s "
                 "(%llu blocks)", ctr->count, buff, ctr->blocks);
    }
}

static void print_ctr(int level, const char *tag, const char *header,
                      const counters_t *ctr, policy_target_t tgt_type)
{
    char buff[256];

    if (!counter_is_set(ctr)) {
        DisplayLog(level, tag, "%s: none", header);
        return;
    }

    FormatFileSize(buff, sizeof(buff), ctr->vol);

#ifdef _LUSTRE
    if (tgt_type == TGT_OST || tgt_type == TGT_POOL) {
        DisplayLog(level, tag, "%s: %llu entries, total volume %s "
                   "(%llu blocks, %llu in target devices)", header,
                   ctr->count, buff, ctr->blocks, ctr->targeted);
    } else
#endif
    {
        DisplayLog(level, tag, "%s: %llu entries, total volume %s "
                   "(%llu blocks)", header, ctr->count, buff, ctr->blocks);
    }
}

static void print_done_vs_target(char *str, int size, const counters_t *done,
                                 const counters_t *target)
{
    int rc = 0;
    char *curr = str;

    str[0] = '\0';

    if (target->count != 0 && done->count < target->count) {
        rc = snprintf(curr, size, "%Lu entries/%Lu targeted", done->count,
                      target->count);
        size -= rc;
        curr += rc;
    }

    if (target->vol != 0 && done->vol < target->vol) {
        rc = snprintf(curr, size, "%s%Lu bytes/%Lu targeted",
                      (curr == str) ? "" : ", ", done->vol, target->vol);
        size -= rc;
        curr += rc;
    }

    if (target->blocks != 0 && done->blocks < target->blocks) {
        rc = snprintf(curr, size, "%s%Lu blocks/%Lu targeted",
                      (curr == str) ? "" : ", ", done->blocks, target->blocks);
        size -= rc;
        curr += rc;
    }

    if (target->targeted != 0 && done->targeted < target->targeted) {
        snprintf(curr, size, "%s%Lu blocks in target device/%Lu targeted",
                 (curr == str) ? "" : ", ", done->blocks, target->blocks);
    }
}

/** store policy run stats to DB */
static void store_policy_run_stats(policy_info_t *pol, time_t start,
                                   time_t end, const char *trigger_info,
                                   const char *status_info)
{
    char var_name[POLICY_NAME_LEN + 128]; /* policy name + suffix (oversized) */
    char val_buff[RBH_PATH_MAX];

    /* clear values for current run */
    snprintf(var_name, sizeof(var_name), "%s" CURR_POLICY_START_SUFFIX,
             tag(pol));
    ListMgr_SetVar(&pol->lmgr, var_name, NULL);
    snprintf(var_name, sizeof(var_name), "%s" CURR_POLICY_TRIGGER_SUFFIX,
             tag(pol));
    ListMgr_SetVar(&pol->lmgr, var_name, NULL);

    /* store last run times */
    snprintf(var_name, sizeof(var_name), "%s" LAST_POLICY_START_SUFFIX,
             tag(pol));
    snprintf(val_buff, sizeof(val_buff), "%lu", (unsigned long)start);
    ListMgr_SetVar(&pol->lmgr, var_name, val_buff);

    snprintf(var_name, sizeof(var_name), "%s" LAST_POLICY_END_SUFFIX, tag(pol));
    snprintf(val_buff, sizeof(val_buff), "%lu", (unsigned long)end);
    ListMgr_SetVar(&pol->lmgr, var_name, val_buff);

    /* store trigger info */
    snprintf(var_name, sizeof(var_name), "%s" LAST_POLICY_TRIGGER_SUFFIX,
             tag(pol));
    ListMgr_SetVar(&pol->lmgr, var_name, trigger_info);

    /* store status info */
    snprintf(var_name, sizeof(var_name), "%s" LAST_POLICY_STATUS_SUFFIX,
             tag(pol));
    ListMgr_SetVar(&pol->lmgr, var_name, status_info);
}

static void store_policy_start_stats(policy_info_t *pol, time_t start,
                                     const char *trigger_info)
{
    char var_name[POLICY_NAME_LEN + 16];
    char val_buff[RBH_PATH_MAX];

    /* store current run times */
    snprintf(var_name, sizeof(var_name), "%s" CURR_POLICY_START_SUFFIX,
             tag(pol));
    snprintf(val_buff, sizeof(val_buff), "%lu", (unsigned long)start);
    ListMgr_SetVar(&pol->lmgr, var_name, val_buff);

    /* store trigger info */
    snprintf(var_name, sizeof(var_name), "%s" CURR_POLICY_TRIGGER_SUFFIX,
             tag(pol));
    ListMgr_SetVar(&pol->lmgr, var_name, trigger_info);
}

/** \param trigger_index -1 if this is a manual run */
static void report_policy_run(policy_info_t *pol, policy_param_t *param,
                              action_summary_t *summary, lmgr_t *lmgr,
                              int trigger_index, int policy_rc)
{
    char buff[1024];
    char *trigger_buff = NULL;
    char *status_buff = NULL;
    char time_buff[128];
    char vol_buff[128];
    char bw_buff[128];
    unsigned int spent;
    time_t time_end = time(NULL);

    print_ctr(LVL_DEBUG, tag(pol), "target", &param->target_ctr, param->target);
    print_ctr(LVL_DEBUG, tag(pol), "done", &summary->action_ctr, param->target);

    if (trigger_index != -1) {
        /* save the summary to trigger_info */
        pol->trigger_info[trigger_index].last_ctr = summary->action_ctr;
        counters_add(&pol->trigger_info[trigger_index].total_ctr,
                     &summary->action_ctr);
        asprintf(&trigger_buff, "trigger: %s (%s), target: %s",
                 trigger2str(&pol->config->trigger_list[trigger_index]),
                 one_shot(pol) ? "one-shot command" : "daemon",
                 param2targetstr(param, buff, sizeof(buff)));
    } else {
        asprintf(&trigger_buff, "manual run, target: %s",
                 param2targetstr(param, buff, sizeof(buff)));
    }

    spent = time_end - summary->policy_start;
    if (spent == 0)
        spent = 1;

    FormatDuration(time_buff, sizeof(time_buff), spent);
    FormatFileSize(vol_buff, sizeof(vol_buff), summary->action_ctr.vol);
    FormatFileSize(bw_buff, sizeof(bw_buff), summary->action_ctr.vol / spent);

    if (policy_rc == 0) {
        DisplayLog(LVL_MAJOR, tag(pol),
                   "Policy run summary: time=%s; target=%s; %llu successful actions (%.2f/sec); "
                   "volume: %s (%s/sec); %u entries skipped; %u errors.",
                   time_buff, param2targetstr(param, buff, sizeof(buff)),
                   summary->action_ctr.count,
                   (float)summary->action_ctr.count / (float)spent,
                   vol_buff, bw_buff, summary->skipped, summary->errors);

        asprintf(&status_buff,
                 "%llu successful actions, volume: %s; %u entries skipped; %u errors",
                 summary->action_ctr.count, vol_buff, summary->skipped,
                 summary->errors);

        if (counter_not_reached(&summary->action_ctr, &param->target_ctr)) {
            trigger_item_t *trig = NULL;

            print_done_vs_target(buff, sizeof(buff), &summary->action_ctr,
                                 &param->target_ctr);

            if (trigger_index != -1) {
                trig = &pol->config->trigger_list[trigger_index];

                DisplayLog(LVL_CRIT, tag(pol),
                           "Warning: could not reach the specified policy target for trigger #%u (%s): %s",
                           trigger_index, trigger2str(trig), buff);

                if (trig->alert_lw) {
                    char title[1024];
                    char ctr1[1024];
                    char ctr2[1024];

                    snprintf(title, sizeof(title),
                             "%s on %s: could not reach policy target",
                             tag(pol), global_config.fs_path);

                    sprint_ctr(ctr1, sizeof(ctr1), &summary->action_ctr,
                               param->target);
                    sprint_ctr(ctr2, sizeof(ctr2), &param->target_ctr,
                               param->target);

                    RaiseAlert(title, "Could not reach the specified target "
                               "for policy '%s', trigger #%u (%s)\n"
                               "%s\nTargeted: %s\nDone: %s", tag(pol),
                               trigger_index, trigger2str(trig),
                               buff, ctr2, ctr1);
                }
            } else {
                DisplayLog(LVL_CRIT, tag(pol),
                           "Warning: could not reach the specified policy target: %s",
                           buff);
            }

            update_trigger_status(pol, trigger_index, TRIG_NOT_ENOUGH);
        } else
            update_trigger_status(pol, trigger_index, TRIG_OK);

    } else if (policy_rc == ENOENT) {
        update_trigger_status(pol, trigger_index, TRIG_NO_LIST);
        DisplayLog(LVL_EVENT, tag(pol),
                   "Could not run policy on %s: no list is available.",
                   param2targetstr(param, buff, sizeof(buff)));

        status_buff = strdup("Could not run policy: no list is available");
    } else if (policy_rc == ECANCELED) {
        update_trigger_status(pol, trigger_index, TRIG_ABORTED);
        DisplayLog(LVL_CRIT, tag(pol),
                   "Policy run aborted after %s; target=%s; %llu successful actions (%.2f/sec); "
                   "volume: %s (%s/sec); %u entries skipped; %u errors.",
                   time_buff, param2targetstr(param, buff, sizeof(buff)),
                   summary->action_ctr.count,
                   (float)summary->action_ctr.count / (float)spent,
                   vol_buff, bw_buff, summary->skipped, summary->errors);

        asprintf(&status_buff,
                 "Policy run aborted after %llu successful actions, volume: %s; %u entries skipped; %u errors",
                 summary->action_ctr.count, vol_buff, summary->skipped,
                 summary->errors);
    } else {
        update_trigger_status(pol, trigger_index, TRIG_CHECK_ERROR);
        DisplayLog(LVL_CRIT, tag(pol), "Error running policy on %s. "
                   "%llu successful actions; volume: %s; %u entries skipped; %u errors.",
                   param2targetstr(param, buff, sizeof(buff)),
                   summary->action_ctr.count, vol_buff,
                   summary->skipped, summary->errors);

        asprintf(&status_buff,
                 "Fatal error running policy after %llu successful actions, volume: %s; %u entries skipped; %u errors",
                 summary->action_ctr.count, vol_buff, summary->skipped,
                 summary->errors);
    }

    store_policy_run_stats(pol, summary->policy_start, time_end,
                           trigger_buff, status_buff);
    free(trigger_buff);
    free(status_buff);

    FlushLogs();
}

/** generic function to check a trigger (TODO to be completed) */
static int check_trigger(policy_info_t *pol, unsigned trigger_index)
{
    policy_param_t param;
    int rc;
    action_summary_t summary;
    trigger_item_t *trig = &pol->config->trigger_list[trigger_index];
    time_modifier_t tmod;
    target_iterator_t it;
    char buff[1024];

    if (!CheckFSDevice(pol))
        return ENODEV;

    memset(&param, 0, sizeof(param));

    rc = check_trigger_type(trig);
    if (rc)
        return rc;
    param.target = trig->target_type;

    update_trigger_status(pol, trigger_index, TRIG_BEING_CHECKED);

    // FIXME, for now, does not check start condition */

    /* iteration on targets over the limit */
    rc = trig_target_it(&it, pol, trig);
    if (rc) {
        update_trigger_status(pol, trigger_index, TRIG_CHECK_ERROR);
        return rc;
    }

    while (!pol->aborted
           && (rc = trig_target_next(&it, &param.optarg_u, &param.target_ctr,
                                     &pol->trigger_info[trigger_index])) == 0
           /* recheck condition as trig_target_next() can be long */
           && !pol->aborted) {

        /* check is done and logged in trig_target_next() */
        if (check_only(pol))
            continue;

        /* complete computed limits with policy and trigger limits */
        set_limits(pol, trig, &param.target_ctr);

        if (check_maintenance_mode(pol, &tmod))
            param.time_mod = &tmod;

        param.action_params = &trig->action_params;

        /* run actions! */
        param2targetstr(&param, buff, sizeof(buff));

        DisplayLog(LVL_EVENT, tag(pol), "Checking policy rules for %s", buff);
        update_trigger_status(pol, trigger_index, TRIG_RUNNING);

        /* insert info to DB about current trigger
         * (for rbh-report --activity) */
        char *trigger_buff;
        asprintf(&trigger_buff, "trigger: %s (%s), target: %s",
                 trigger2str(trig), one_shot(pol) ?
                 "one-shot command" : "daemon", buff);
        store_policy_start_stats(pol, time(NULL), trigger_buff);
        free(trigger_buff);

        memset(&summary, 0, sizeof(summary));
        /* run the policy */
        rc = run_policy(pol, &param, &summary, &pol->lmgr);

        report_policy_run(pol, &param, &summary, &pol->lmgr, trigger_index, rc);

        /* post apply sleep? */
        if (!pol->aborted && counter_is_set(&summary.action_ctr) &&
            trig->post_trigger_wait > 0) {
            DisplayLog(LVL_EVENT, tag(pol),
                       "Waiting %lus before checking other trigger targets.",
                       trig->post_trigger_wait);
            rh_sleep(trig->post_trigger_wait);
        }
    }
    trig_target_end(&it);

    if (pol->aborted)
        update_trigger_status(pol, trigger_index, TRIG_ABORTED);
    else if (rc != ENOENT && rc != 0)
        update_trigger_status(pol, trigger_index, TRIG_CHECK_ERROR);
    else {
        update_trigger_status(pol, trigger_index, TRIG_OK);
        rc = 0;
    }
    return rc;
}

static inline void mk_fake_trigger(trigger_item_t *trig, policy_target_t tgt,
                                   double usage_val)
{
    trig->trigger_type = TRIG_CONDITION;
    trig->target_type = tgt;
    trig->list = NULL;
    trig->list_size = 0;
    trig->check_interval = 0;
    /* HW=LW=target */
    trig->hw_type = PCT_THRESHOLD;
    trig->lw_type = PCT_THRESHOLD;
    trig->hw_percent = usage_val;
    trig->lw_percent = usage_val;
}

/* force running policies on a specific target */
static int targeted_run(policy_info_t *pol, const policy_opt_t *opt)
{
    policy_param_t param;
    int rc;
    action_summary_t summary;
    char buff[1024];

    rc = ListMgr_InitAccess(&pol->lmgr);
    if (rc) {
        DisplayLog(LVL_CRIT, tag(pol),
                   "Could not connect to database (error %d). Trigger checking cannot be started.",
                   rc);
        return rc;
    }

    if (!CheckFSDevice(pol)) {
        rc = ENODEV;
        goto out;
    }

    memset(&param, 0, sizeof(param));

    param.target = opt->target;
    param.optarg_u = opt->optarg_u;
    param.target_ctr.count = opt->max_action_nbr;
    param.target_ctr.vol = opt->max_action_vol;

    if ((param.target == TGT_FS
#ifdef _LUSTRE
         || param.target == TGT_OST || param.target == TGT_POOL
#endif
        ) && opt->usage_pct != -1.0) {
        trigger_item_t trig;
        trigger_info_t info;
        struct statfs stfs;
        char tgtname[256];

        memset(&trig, 0, sizeof(trig));
        memset(&info, 0, sizeof(info));

        /* convert target level to target counter */

        /* build fake trigger */
        mk_fake_trigger(&trig, param.target, opt->usage_pct);

        if (param.target == TGT_FS) {
            strcpy(tgtname, "filesystem");
            rc = get_fs_usage(pol, &stfs);
        }
#ifdef _LUSTRE
        else if (param.target == TGT_OST) {
            snprintf(tgtname, sizeof(tgtname), "OST #%u", param.optarg_u.index);
            rc = Get_OST_usage(global_config.fs_path, param.optarg_u.index,
                               &stfs);
        } else if (param.target == TGT_POOL) {
            snprintf(tgtname, sizeof(tgtname), "pool '%s'",
                     param.optarg_u.name);
            rc = Get_pool_usage(param.optarg_u.name, &stfs);
        }
#endif
        else
            RBH_BUG("Unexpected target type in targeted_run()");

        if (rc) {
            DisplayLog(LVL_CRIT, tag(pol), "Cannot retrieve usage for %s: %s",
                       tgtname, strerror(abs(rc)));
            goto out;
        }

        rc = check_statfs_thresholds(&trig, tgtname, &stfs, &param.target_ctr,
                                     &info);
        if (rc)
            goto out;

        if (check_only(pol)) {
            rc = 0;
            goto out;
        }

        if (!counter_is_set(&param.target_ctr)) {
            DisplayLog(LVL_CRIT, tag(pol),
                       "%s is already under the given threshold", tgtname);
            rc = 0;
            goto out;
        }
    }

    if (!pol->aborted) {
        time_modifier_t tmod;

        /* complete computed limits with policy global limits */
        set_limits(pol, NULL, &param.target_ctr);

        if (check_maintenance_mode(pol, &tmod))
            param.time_mod = &tmod;

        /* run actions! */
        param2targetstr(&param, buff, sizeof(buff));
        DisplayLog(LVL_EVENT, tag(pol), "Checking policy rules for %s", buff);

        /* insert info to DB about current trigger
         * (for rbh-report --activity) */
        char *trigger_buff;
        asprintf(&trigger_buff, "manual run, target: %s", buff);
        store_policy_start_stats(pol, time(NULL), trigger_buff);
        free(trigger_buff);

        memset(&summary, 0, sizeof(summary));
        /* run the policy */
        rc = run_policy(pol, &param, &summary, &pol->lmgr);

        report_policy_run(pol, &param, &summary, &pol->lmgr, -1, rc);

        /* Manual run: no post action delay */
    }

 out:
    ListMgr_CloseAccess(&pol->lmgr);
    return rc;
}

struct targeted_run_arg {
    policy_info_t *policy;
    const policy_opt_t *options;
};
static void *targeted_run_thr(void *arg)
{
    struct targeted_run_arg *targ = (struct targeted_run_arg *)arg;
    int rc;

    rc = targeted_run(targ->policy, targ->options);
    /* this was allocated by the thread starter */
    DisplayLog(LVL_DEBUG, tag(targ->policy),
               "Policy run terminated with status %d", rc);
    MemFree(arg);
    pthread_exit(NULL);
    return NULL;
}

/**
 * Main loop for checking triggers periodically (1 per policy).
 */
static void *trigger_check_thr(void *thr_arg)
{
    unsigned int i;
    int rc;
    unsigned int nb_reset = 0;
    unsigned int nb_total = 0;
    double max_usage;
    char tmpstr[128];
    policy_info_t *pol = (policy_info_t *) thr_arg;
    time_t last_action_check = time(NULL);

    rc = ListMgr_InitAccess(&pol->lmgr);
    if (rc) {
        DisplayLog(LVL_CRIT, tag(pol),
                   "Could not connect to database (error %d). Trigger checking cannot be started.",
                   rc);
        return NULL;
    }

    if (pol->config->check_action_status_on_startup) {
        if (pol->descr->status_current == NULL) {
            DisplayLog(LVL_MAJOR, tag(pol),
                       "'check_action_on_startup' is enabled, but no 'status_current'"
                       " is defined for this policy: skipping action check (check_action_on_startup=no).");
            pol->config->check_action_status_on_startup = false;
        } else {
            DisplayLog(LVL_EVENT, tag(pol),
                       "Checking status of outstanding actions...");
            rc = check_current_actions(pol, &pol->lmgr, &nb_reset, &nb_total);

            if (rc != 0)
                DisplayLog(LVL_CRIT, tag(pol),
                           "Error checking outstanding action status");
            else
                DisplayLog(LVL_EVENT, tag(pol),
                           "%u actions finished / %u total", nb_reset,
                           nb_total);
        }
    }

    do {
        max_usage = 0.0;

        /* check every trigger */
        for (i = 0; i < pol->config->trigger_count; i++) {
            const char *tname = trigger2str(&pol->config->trigger_list[i]);

            if (pol->aborted) {
                DisplayLog(LVL_MAJOR, tag(pol),
                           "Stop requested: aborting trigger check");
                break;
            }

            if (time(NULL) - pol->trigger_info[i].last_check >=
                pol->config->trigger_list[i].check_interval) {
                if (pol->trigger_info[i].last_check != 0)
                    DisplayLog(LVL_DEBUG, tag(pol),
                               "Checking trigger #%u (%s), last check %lus ago",
                               i, tname,
                               time(NULL) - pol->trigger_info[i].last_check);
                else
                    DisplayLog(LVL_DEBUG, tag(pol),
                               "Checking trigger #%u (%s), never checked", i,
                               tname);

                rc = check_trigger(pol, i);

                /* don't update last_check if trigger check failed */
                if (rc != 0) {
                    if (rc == ECANCELED)
                        DisplayLog(LVL_CRIT, tag(pol),
                                   "Trigger #%u (%s): aborted.", i, tname);
                    else
                        DisplayLog(LVL_CRIT, tag(pol), "Trigger #%u (%s): "
                                   "returned error %d... Will retry later",
                                   i, tname, rc);
                } else
                    pol->trigger_info[i].last_check = time(NULL);
            }

            /* in any case compute max usage */
            if (pol->trigger_info[i].last_usage > max_usage)
                max_usage = pol->trigger_info[i].last_usage;
        }

        /* Finally update max_usage in persistent stats */
        if (max_usage > 0.0) {
            snprintf(tmpstr, sizeof(tmpstr), "%.2f", max_usage);
            if (ListMgr_SetVar(&pol->lmgr, USAGE_MAX_VAR, tmpstr) != DB_SUCCESS)
                DisplayLog(LVL_CRIT, tag(pol),
                           "Error updating value of " USAGE_MAX_VAR
                           " variable (value = %s)", tmpstr);
            DisplayLog(LVL_EVENT, tag(pol), "Current usage max is %.2f%%",
                       max_usage);
        }

        if (!one_shot(pol) && !pol->aborted) {
            rh_intr_sleep(pol->gcd_interval, pol->aborted);
            if (pol->aborted)
                goto out;
        } else
            goto out;

        /* cancel old actions */
        if ((pol->config->check_action_status_delay != 0)
            && (time(NULL) - last_action_check >=
                pol->config->check_action_status_delay)) {
            if (pol->descr->status_current == NULL) {
                DisplayLog(LVL_MAJOR, tag(pol),
                           "'check_actions_interval' is enabled but no 'status_current'"
                           " is defined for this policy: disabling action check (check_actions_interval=0).");
                pol->config->check_action_status_delay = 0;
            } else {
                DisplayLog(LVL_EVENT, tag(pol),
                           "Checking status of outstanding actions...");
                rc = check_current_actions(pol, &pol->lmgr, &nb_reset,
                                           &nb_total);

                if (rc != 0)
                    DisplayLog(LVL_CRIT, tag(pol),
                               "Error checking outstanding action status");
                else
                    DisplayLog(LVL_EVENT, tag(pol),
                               "%u actions finished / %u total", nb_reset,
                               nb_total);
            }
        }
    } while (1);

 out:
    ListMgr_CloseAccess(&pol->lmgr);
    pthread_exit(NULL);
    return NULL;
}

/* ------------ Exported functions ------------ */

/** Recompute trigger check interval as the GCD of all triggers
 *  (required after reloading config)
 */
void policy_module_update_check_interval(policy_info_t *policy)
{
    unsigned int i;

    policy->gcd_interval = 1;

    if (policy->config->trigger_count == 0)
        return;

    /* compute GCD of trigger check intervals */
    if (policy->config->trigger_count == 1)
        policy->gcd_interval = policy->config->trigger_list[0].check_interval;
    else if (policy->config->trigger_count > 1) {
        policy->gcd_interval =
            gcd(policy->config->trigger_list[0].check_interval,
                policy->config->trigger_list[1].check_interval);
        for (i = 2; i < policy->config->trigger_count; i++)
            policy->gcd_interval =
                gcd(policy->gcd_interval,
                    policy->config->trigger_list[i].check_interval);
    }

    DisplayLog(LVL_DEBUG, tag(policy), "GCD of trigger check intervals is %us",
               (unsigned int)policy->gcd_interval);
}

/**
 * Initialize module and start checker threads
 */
int policy_module_start(policy_info_t *policy, /* out */
                        policy_descr_t *policy_descr,  /* in */
                        policy_run_config_t *p_config, /* in */
                        const policy_opt_t *options)
{   /* in */
    unsigned int i;
    int rc;

    if (!policy || !policy_descr || !p_config || !options)
        RBH_BUG("Unexpected NULL argument");

    memset(policy, 0, sizeof(*policy));

    policy->descr = policy_descr;
    policy->config = p_config;

    policy->fs_dev = get_fsdev();
    policy->flags = options->flags;

    /* policy-> progress, first_eligible, time_modifier, threads
     * are initialized in policy_run (for internal use in policy_run).
     */

    /* check there is at least 1 trigger */
    if ((options->target == TGT_NONE) && (p_config->trigger_count == 0)) {
        DisplayLog(LVL_CRIT, tag(policy),
                   "No trigger defined in configuration file, and no target "
                   "specified on command line. Disabling action scheduling.");
        return ENOENT;
    } else if (NO_POLICY(&policy_descr->rules)
               && !(options->flags & RUNFLG_IGNORE_POL)) {
        DisplayLog(LVL_CRIT, tag(policy),
                   "No policy rules defined in configuration file... "
                   "Disabling action scheduling.");
        return ENOENT;
    }

    /* Display an info message if no default policy is specified */
    if (!has_default_policy(&policy_descr->rules))
        DisplayLog(LVL_EVENT, tag(policy),
                   "Notice: no 'default' policy rule is defined. "
                   "Unmatched entries will be ignored.");

    /* intervals must only be computed for daemon mode */
    if (!one_shot(policy))
        policy_module_update_check_interval(policy);
    else
        policy->gcd_interval = 1;

    /* initialize worker queue */
    rc = CreateQueue(&policy->queue, p_config->queue_size, AS_ENUM_COUNT - 1,
                     AF_ENUM_COUNT);
    if (rc) {
        DisplayLog(LVL_CRIT, tag(policy),
                   "Error %d initializing action queue", rc);
        return rc;
    }

    /* start worker threads */
    rc = start_worker_threads(policy);
    if (rc)
        /* don't care about leaks here, as the program is going to exit */
        return rc;

    /**  @TODO take max-count and max-vol parameters into account */

    /* Allocate and initialize trigger_info array
     * (only if there is no a specific target)
     */
    if (options->target == TGT_NONE) {
        policy->trigger_info =
            (trigger_info_t *) MemCalloc(p_config->trigger_count,
                                         sizeof(trigger_info_t));
        if (policy->trigger_info == NULL) {
            DisplayLog(LVL_CRIT, tag(policy), "Memory Error in %s", __func__);
            return ENOMEM;
        }

        for (i = 0; i < p_config->trigger_count; i++)
            policy->trigger_info[i].status = TRIG_NOT_CHECKED;

        /* start trigger check thread */
        rc = pthread_create(&policy->trigger_thr, NULL, trigger_check_thr,
                            (void *)policy);
    } else {    /* targeted run */

        /* This structure is to be released by the thread, once it used its
         * contents */
        struct targeted_run_arg *thr_arg =
            MemAlloc(sizeof(struct targeted_run_arg));

        if (thr_arg == NULL) {
            DisplayLog(LVL_CRIT, tag(policy), "Memory Error in %s", __func__);
            return ENOMEM;
        }
        thr_arg->policy = policy;
        thr_arg->options = options;

        rc = pthread_create(&policy->trigger_thr, NULL, targeted_run_thr,
                            (void *)thr_arg);
    }

    if (rc != 0) {
        rc = errno;
        DisplayLog(LVL_CRIT, tag(policy),
                   "Error %d starting trigger thread: %s", rc, strerror(rc));
    }
    return rc;
}

int policy_module_stop(policy_info_t *policy)
{
    policy->aborted = 1;    /* seen by all components, from triggers to worker
                             * threads in policy_run */
    return 0;
}

int policy_module_wait(policy_info_t *policy)
{
    void *returned;
    int rc = 0;

    /* /!\ pb: 2 threads cannot join the same other thread.
     * In one_shot mode, the main thread is already waiting
     * for main thread to end. Thus, the signal manager thread
     * would get an error when trying to join it after abort.
     */
    if (!policy->waiting) {
        /* Ensure SIGTERM is not simultaneous with module start */
        if (policy->trigger_thr != 0) {
            policy->waiting = 1;
            rc = pthread_join(policy->trigger_thr, &returned);
            if (rc != 0)
                DisplayLog(LVL_MAJOR, tag(policy),
                           "pthread_join() returned error %d: %s", rc,
                           strerror(rc));
            else
                policy->waiting = 0;
        }
    } else {
        /* the second thread that needs to join polls the 'waiting' variable */
        while (policy->waiting)
            rh_sleep(1);
    }
    return rc;
}

void policy_module_dump_stats(policy_info_t *policy)
{
    unsigned int status_tab[AS_ENUM_COUNT];
    unsigned long long feedback_tab[AF_ENUM_COUNT];

    unsigned int nb_waiting, nb_items;
    time_t last_submitted, last_started, last_ack;

    char tmp_buff[256];
    char trigstr[256];
    time_t now = time(NULL);
    int i;
    struct tm paramtm;

    /* Stats about triggers */
    DisplayLog(LVL_MAJOR, "STATS", "======= %s policy: trigger stats ======",
               tag(policy));

    /* sanity check */
    if ((policy->config->trigger_list != NULL)
        && (policy->trigger_info != NULL)) {
        for (i = 0; i < policy->config->trigger_count; i++) {
            snprintf(trigstr, sizeof(trigstr), "Trigger #%u (%s)", i,
                     trigger2str(&policy->config->trigger_list[i]));

            switch (policy->trigger_info[i].status) {
            case TRIG_NOT_CHECKED: /* not checked yet */
                DisplayLog(LVL_MAJOR, "STATS", "%-30s: not checked yet.",
                           trigstr);
                break;
            case TRIG_BEING_CHECKED:   /* currently beeing checked */
                DisplayLog(LVL_MAJOR, "STATS", "%-30s: being checked.",
                           trigstr);
                break;
            case TRIG_RUNNING: /* purge running for this trigger */
                DisplayLog(LVL_MAJOR, "STATS", "%-30s: running.", trigstr);
                break;
            case TRIG_OK:  /* no purge is needed */
                strftime(tmp_buff, sizeof(tmp_buff), "%Y/%m/%d %T",
                         localtime_r(&policy->trigger_info[i].last_check,
                                     &paramtm));
                DisplayLog(LVL_MAJOR, "STATS", "%-30s: OK (last check: %s).",
                           trigstr, tmp_buff);
                break;
            case TRIG_NO_LIST: /* no list available */
                strftime(tmp_buff, sizeof(tmp_buff), "%Y/%m/%d %T",
                         localtime_r(&policy->trigger_info[i].last_check,
                                     &paramtm));
                DisplayLog(LVL_MAJOR, "STATS",
                           "%-30s: no list available (last check: %s).",
                           trigstr, tmp_buff);
                break;
            case TRIG_NOT_ENOUGH:  /* not enough candidates */
                strftime(tmp_buff, sizeof(tmp_buff), "%Y/%m/%d %T",
                         localtime_r(&policy->trigger_info[i].last_check,
                                     &paramtm));
                DisplayLog(LVL_MAJOR, "STATS",
                           "%-30s: last run (%s) was incomplete: not enough candidate entries.",
                           trigstr, tmp_buff);
                break;

            case TRIG_CHECK_ERROR: /* Misc Error */
                strftime(tmp_buff, sizeof(tmp_buff), "%Y/%m/%d %T",
                         localtime_r(&policy->trigger_info[i].last_check,
                                     &paramtm));
                DisplayLog(LVL_MAJOR, "STATS", "%-30s: last check failed (%s).",
                           trigstr, tmp_buff);
                break;

            case TRIG_ABORTED: /*  */
                strftime(tmp_buff, sizeof(tmp_buff), "%Y/%m/%d %T",
                         localtime_r(&policy->trigger_info[i].last_check,
                                     &paramtm));
                DisplayLog(LVL_MAJOR, "STATS", "%-30s: last run aborted (%s)",
                           trigstr, tmp_buff);
                break;

            case TRIG_UNSUPPORTED: /* Trigger not supported in this mode */
                DisplayLog(LVL_MAJOR, "STATS", "%-30s: not supported.",
                           trigstr);
                break;
            }

            print_ctr(LVL_MAJOR, "STATS", "    last run",
                      &policy->trigger_info[i].last_ctr,
                      policy->config->trigger_list[i].target_type);
            if (!one_shot(policy))
                print_ctr(LVL_MAJOR, "STATS", "    total   ",
                          &policy->trigger_info[i].total_ctr,
                          policy->config->trigger_list[i].target_type);
        }
    }

    /* Policy stats */
    RetrieveQueueStats(&policy->queue, &nb_waiting, &nb_items, &last_submitted,
                       &last_started, &last_ack, status_tab, feedback_tab);

    DisplayLog(LVL_MAJOR, "STATS", "======= %s policy: action stats ======",
               tag(policy));
    DisplayLog(LVL_MAJOR, "STATS", "idle threads       = %u", nb_waiting);
    DisplayLog(LVL_MAJOR, "STATS", "queued actions     = %u", nb_items);
    DisplayLog(LVL_MAJOR, "STATS", "action status:");

    for (i = 0; i < AS_ENUM_COUNT; i++) {
        /* always display AS_OK and display error only if they have occurred */
        if ((status_tab[i] > 0) || (i == AS_OK))
            DisplayLog(LVL_MAJOR, "STATS", "    %-30s = %u",
                       action_status_descr[i], status_tab[i]);
    }

    if (feedback_tab[AF_TARGETED_OK] > 0)
        DisplayLog(LVL_MAJOR, "STATS",
                   "%llu actions successful/%llu, %s (%llu blocks, %llu in target devices)",
                   feedback_tab[AF_NBR_OK],
                   feedback_tab[AF_NBR_OK] + feedback_tab[AF_NBR_NOK],
                   FormatFileSize(tmp_buff, sizeof(tmp_buff),
                                  feedback_tab[AF_VOL_OK]),
                   feedback_tab[AF_BLOCKS_OK], feedback_tab[AF_TARGETED_OK]);
    else
        DisplayLog(LVL_MAJOR, "STATS",
                   "%llu actions successful/%llu, %s (%llu blocks)",
                   feedback_tab[AF_NBR_OK],
                   feedback_tab[AF_NBR_OK] + feedback_tab[AF_NBR_NOK],
                   FormatFileSize(tmp_buff, sizeof(tmp_buff),
                                  feedback_tab[AF_VOL_OK]),
                   feedback_tab[AF_BLOCKS_OK]);

    if (last_submitted)
        DisplayLog(LVL_MAJOR, "STATS", "last action queued    %2d s ago",
                   (int)(now - last_submitted));

    if (last_started)
        DisplayLog(LVL_MAJOR, "STATS", "last action started   %2d s ago",
                   (int)(now - last_started));

    if (last_ack)
        DisplayLog(LVL_MAJOR, "STATS", "last action completed %2d s ago",
                   (int)(now - last_ack));
}
