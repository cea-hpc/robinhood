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

/**
 * \file lhsm.c
 * \brief implements Lustre/HSM status manager and functions.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "mod_internal.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "list_mgr.h" /* for common robinhood types: entry_id_t, stripe_info_t... */
#include "status_manager.h"

#include <stdbool.h>
#include <glib.h>

/* tag for logs */
#define LHSM_TAG "lhsm"

#define DEFAULT_ARCHIVE_ID  0
#define ARCHIVE_PARAM "archive_id"

/** global static list of excluded variables for action parameters serialization. */
static struct rbh_params *exclude_params = NULL;

/**
 * Get archive_id from action parameters.
 * @return archive_id on success, a negative value or error.
 */
static int get_archive_id(const action_params_t *params)
{
    int         arch_id;
    const char *val = rbh_param_get(params, ARCHIVE_PARAM);

    if (val == NULL)
        return DEFAULT_ARCHIVE_ID;

    arch_id = str2int(val);
    if (arch_id == -1)
    {
        DisplayLog(LVL_MAJOR, LHSM_TAG,
                   "Invalid archive_id '%s': index expected", val);
        return -EINVAL;
    }

    return arch_id;
}


/** Initialize action related global information.
 * Prepare exclude set once to avoid reinitializing it for each action.
 */
static int init_action_global_info(void)
{
    struct rbh_params *new_params;
    int rc;

    if (exclude_params != NULL)
        return 0;

    new_params = (struct rbh_params *)calloc(1, sizeof(*new_params));
    if (new_params == NULL)
        return -ENOMEM;

    /* initialize exclude list, as it is constant */
    rc = rbh_param_set(new_params, ARCHIVE_PARAM, "", true);
    if (rc)
    {
        free(new_params);
        return rc;
    }

    exclude_params = new_params;
    return 0;
}

/** lhsm module initialization function */
static int lhsm_init(struct sm_instance *smi, run_flags_t flags)
{
    return init_action_global_info();
}

/** Trigger an HSM action */
static int lhsm_action(enum hsm_user_action action, const entry_id_t *p_id,
                       const action_params_t *params)
{
    struct hsm_user_request * req;
    int rc;
    char *mpath;
    unsigned int    archive_id = DEFAULT_ARCHIVE_ID;
    GString        *args = NULL;
    const char     *data = NULL;
    int             data_len = 0;

    /* Get archive_id from parameters. */
    rc = get_archive_id(params);
    if (rc < 0)
        return rc;
    archive_id = rc;

    /* Serialize the parameters to pass them to the copytool.
     * exclude archive_id, which is for internal use. */
    args = g_string_new("");
    rc = rbh_params_serialize(params, args, exclude_params,
                              RBH_PARAM_CSV | RBH_PARAM_COMPACT);
    if (rc)
        goto free_args;

    if (!GSTRING_EMPTY(args))
    {
        data = args->str;
        data_len = args->len + 1;
    }

    req = llapi_hsm_user_request_alloc(1, data_len);
    if (!req)
    {
        rc = -errno;
        DisplayLog(LVL_CRIT, LHSM_TAG, "Cannot create HSM request: %s",
                   strerror(-rc));
        goto free_args;
    }

    req->hur_request.hr_action = action;
    req->hur_request.hr_archive_id = archive_id;

    req->hur_user_item[0].hui_fid = *p_id;
    req->hur_user_item[0].hui_extent.offset = 0 ;
    /* XXX for now, always transfer entire file */
    req->hur_user_item[0].hui_extent.length = -1LL;

    req->hur_request.hr_itemcount = 1;
    req->hur_request.hr_data_len = data_len;

    if (data)
        memcpy(hur_data(req), data, data_len);

    /* make tmp copy as llapi_hsm_request arg is not const */
    mpath = strdup(get_mount_point(NULL));
    rc = llapi_hsm_request(mpath, req);
    free(mpath);
    free(req);

    if (rc)
        DisplayLog(LVL_CRIT, LHSM_TAG,
                   "ERROR performing HSM request(%s, root=%s, fid="DFID"): %s",
                   hsm_user_action2name(action),
                   get_mount_point(NULL), PFID(p_id), strerror(-rc));
free_args:
    g_string_free(args, TRUE);
    return rc;
}

/** perform hsm_release action */
static int lhsm_release(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                        const action_params_t *params, post_action_e *after,
                        db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    /* 'after' is set in action callback */
    return lhsm_action(HUA_RELEASE, p_entry_id, params);
}

/** perform hsm_archive action */
static int lhsm_archive(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                        const action_params_t *params, post_action_e *after,
                        db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    /* 'after' is set in action callback */
    return lhsm_action(HUA_ARCHIVE, p_entry_id, params);
}

/** perform hsm_remove action */
static int lhsm_remove(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                       const action_params_t *params, post_action_e *after,
                       db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    /* 'after' is set in action callback */
    int rc = lhsm_action(HUA_REMOVE, p_entry_id, params);
    return rc;
}

/** set of managed status */
typedef enum {
  STATUS_NEW,                   /* file has no HSM flags (just created) */
  STATUS_MODIFIED,              /* file must be archived */
  STATUS_RESTORE_RUNNING,       /* file is being retrieved */
  STATUS_ARCHIVE_RUNNING,       /* file is being archived */
  STATUS_SYNCHRO,               /* file has been synchronized in HSM, file can be purged */
  STATUS_RELEASED,              /* file is released (nothing to do). XXX should not be in DB? */

  STATUS_COUNT,                 /* number of possible file status */
} hsm_status_t;

/* XXX /!\ Keep in sync with hsm_status_t */
static const char *lhsm_status_list[] = {
    [STATUS_NEW]= "new",
    [STATUS_MODIFIED] = "modified",
    [STATUS_RESTORE_RUNNING] = "retrieving",
    [STATUS_ARCHIVE_RUNNING] = "archiving",
    [STATUS_SYNCHRO] = "synchro",
    [STATUS_RELEASED] = "released",
};

static const char *hsm_status2str(hsm_status_t st)
{
    if ((unsigned int)st >= STATUS_COUNT)
        return NULL;
    else
        return lhsm_status_list[st];
}

/** get Lustre status and convert it to an internal scalar status */
static int lhsm_get_status(const char *path, hsm_status_t *p_status,
                           bool *no_release, bool *no_archive)
{
    struct hsm_user_state file_status;
    int                   rc;

    /* initialize outputs */
    *p_status = STATUS_NEW;
    *no_release = false;
    *no_archive = false;

    /* get status */
    rc = llapi_hsm_state_get(path, &file_status);

    if ((rc !=0) && (rc != -ENOENT) && (rc != -ESTALE))
        DisplayLog(LVL_DEBUG, LHSM_TAG, "llapi_hsm_state_get(%s)=%d", path,
                    rc);
    if (rc != 0)
        return rc;

    /* user flags */

    if (file_status.hus_states & HS_NORELEASE)
        *no_release = true;
    if (file_status.hus_states & HS_NOARCHIVE)
        *no_archive = true;

    /* clear them */
    file_status.hus_states &= ~ (HS_NORELEASE|HS_NOARCHIVE);

    /* pending actions */

    if (file_status.hus_in_progress_action == HUA_ARCHIVE)
    {
        *p_status = STATUS_ARCHIVE_RUNNING;
        return 0;
    }
    else if (file_status.hus_in_progress_action == HUA_RESTORE)
    {
        *p_status = STATUS_RESTORE_RUNNING;
        return 0;
    }
    else if (file_status.hus_in_progress_action == HUA_RELEASE)
    {
        DisplayLog(LVL_DEBUG, LHSM_TAG, "Entry %s is being released", path);
    }
    else if (file_status.hus_in_progress_action == HUA_REMOVE)
    {
        DisplayLog(LVL_DEBUG, LHSM_TAG, "Entry %s is being removed", path);
    }

    /* status flags */
    if ((file_status.hus_states & HSM_FLAGS_MASK) == 0)
    {
        *p_status = STATUS_NEW;
    }
    else if (file_status.hus_states & HS_DIRTY)
    {
        *p_status = STATUS_MODIFIED;
    }
    else if (file_status.hus_states & HS_ARCHIVED)
    {
        /* "and not dirty" is ensured by the previous test */
        if (file_status.hus_states & HS_RELEASED)
        {
            /* file is archived in HSM, and released from Lustre */
            *p_status = STATUS_RELEASED;
        }
        else
        {
            /* file is up-to-date in HSM, and not released in Lustre */
            *p_status = STATUS_SYNCHRO;
        }
    }
    else if (file_status.hus_states & HS_EXISTS)
    {
       /* new file, not yet archived successfully */
       *p_status = STATUS_MODIFIED;
    }
    else
    {
        /* In this case: file has non null status and !HS_DIRTY and !HS_ARCHIVED
         * and !HS_EXISTS.
         * Maybe is it HS_RELEASED without being HS_ARCHIVED (empty file?)
         * or maybe is it LOST???
         */
        DisplayLog(LVL_MAJOR, LHSM_TAG,
                   "Entry %s has inconsistent or unknown HSM flags %#X",
                   path, file_status.hus_states);
        return EINVAL;
    }

    /** @TODO what if special LOST flag is set??? */

    return 0;
}

/** get the HSM status of an entry */
static int lhsm_status(struct sm_instance *smi,
                       const entry_id_t *id, const attr_set_t *attrs,
                       attr_set_t *refreshed_attrs)
{
    int rc;
    char fid_path[RBH_PATH_MAX];
    hsm_status_t st;
    bool no_release = false,
         no_archive = false;
    const char *str_st;

    if (ATTR_MASK_TEST(attrs, type) &&
        strcmp(ATTR(attrs, type), STR_TYPE_FILE) != 0)
    {
        /* not a file: no status */
        rc = 0;
        goto clean_status;
    }

    rc = BuildFidPath(id, fid_path);
    if (rc)
        goto clean_status;

    /** @TODO store no_release and no_archive as SM specific attributes */
    rc = lhsm_get_status(fid_path, &st, &no_release, &no_archive);
    if (rc)
        goto clean_status;

    /* set status in refreshed attrs */
    str_st = hsm_status2str(st);
    if (str_st == NULL)
        goto clean_status;

    /* check allocation of sm_status array */
    sm_status_ensure_alloc(&refreshed_attrs->attr_values.sm_status);
    if (refreshed_attrs->attr_values.sm_status == NULL)
    {
        rc = -ENOMEM;
        goto clean_status;
    }

    STATUS_ATTR(refreshed_attrs, smi->smi_index) = str_st;
    ATTR_MASK_STATUS_SET(refreshed_attrs, smi->smi_index);

    return 0;

clean_status:
    if (refreshed_attrs->attr_values.sm_status != NULL)
        /* don't free it as it contains a const char* */
        STATUS_ATTR(refreshed_attrs, smi->smi_index) = NULL;

    /* Clean the status from the mask */
    ATTR_MASK_STATUS_UNSET(refreshed_attrs, smi->smi_index);

    return rc;
}

/** helper to set the LHSM status in attribute structure */
static inline void set_lhsm_status(struct sm_instance *smi, attr_set_t *attrs, hsm_status_t status)
{
    /* new file, status is known */
    sm_status_ensure_alloc(&attrs->attr_values.sm_status);
    STATUS_ATTR(attrs, smi->smi_index) = hsm_status2str(status);
    ATTR_MASK_STATUS_SET(attrs, smi->smi_index);
}

/** helper to compare a LHSM status */
static bool status_equal(struct sm_instance *smi, const attr_set_t *attrs, hsm_status_t status)
{
    return !strcmp(STATUS_ATTR(attrs, smi->smi_index), hsm_status2str(status));
}

/** check this is a supported action */
static bool lhsm_check_action_name(const char *name)
{
    if (strcasecmp(name, "archive") && strcasecmp(name, "release") &&
        /* special values for deleted entries (for lhsm_remove) */
        strcasecmp(name, "removed") && strcasecmp(name, "deleted"))
        return false;

    return true;
}

static int lhsm_action_callback(struct sm_instance *smi,
                                const char *implements, int action_status,
                                const entry_id_t *id, attr_set_t *attrs,
                                post_action_e *what_after)
{
    if (smi == NULL || implements == NULL)
        return -EINVAL;

    if (!strcasecmp(implements, "archive"))
    {
        /* successful archive (asynchronous): now archive_running, else (failed): unchanged. */
        if (action_status == 0)
            set_lhsm_status(smi, attrs, STATUS_ARCHIVE_RUNNING);
        else
            /* (try to) update hsm_status on failure */
            lhsm_status(smi, id, attrs, attrs);

        *what_after = PA_UPDATE;
        return 0;
    }
    else if (!strcasecmp(implements, "release"))
    {
        /* successful release (synchronous): now released, else: unchanged. */
        if (action_status == 0)
            set_lhsm_status(smi, attrs, STATUS_RELEASED);
        else
            /* (try to) update hsm_status on failure */
            lhsm_status(smi, id, attrs, attrs);

        *what_after = PA_UPDATE;
        return 0;
    }
    else if (!strcasecmp(implements, "removed") || !strcasecmp(implements, "deleted"))
    {
        /* successful removed (asynchronous): drop from DB */
        *what_after = (action_status != 0 ? PA_NONE : PA_RM_ONE);
        return 0;
    }
    else
        return -EINVAL;
}

/** changelog callback */
static int lhsm_cl_cb(struct sm_instance *smi, const CL_REC_TYPE *logrec,
                      const entry_id_t *id, const attr_set_t *attrs,
                       attr_set_t *refreshed_attrs, bool *getit)
{
    /* If this is a CREATE record, we know its status is NEW
     * (except if it is already set to another value) */
    if (logrec->cr_type == CL_CREATE)
    {
        if (!ATTR_MASK_STATUS_TEST(attrs, smi->smi_index))
        {
            /* new file, status is new */
            set_lhsm_status(smi, refreshed_attrs, STATUS_NEW);
            /* no need to retrieve it from filesystem */
            *getit = false;
        }
        /* else: file is already known. Preserve the known status. */

        /* FIXME RBHv3 manage no_archive, no_release, last_archive, last_restore */
    }
    else if ((logrec->cr_type == CL_MKDIR) || (logrec->cr_type == CL_RMDIR))
    {
        /* no status for directories */
        *getit = false;
    }
    else if (logrec->cr_type == CL_HSM)
    {
        switch (hsm_get_cl_event(logrec->cr_flags))
        {
            case HE_ARCHIVE:
                /* is it a successfull copy? */
                if (hsm_get_cl_error(logrec->cr_flags) == CLF_HSM_SUCCESS)
                {
                    /** @TODO RBHv3 remember last archive time */

                    /* if dirty flag is set in the changelog record, the entry is dirty,
                     * else, it is up to date. */
                    set_lhsm_status(smi, refreshed_attrs,
                        (hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY) ?
                        STATUS_MODIFIED : STATUS_SYNCHRO);
                    *getit = false;
                }
                else /* archive failed */
                {
                    /* Entry is probably still dirty. If dirty flag is not set,
                     * we need to ask the actual status */
                    if (hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY)
                    {
                        set_lhsm_status(smi, refreshed_attrs, STATUS_MODIFIED);
                        *getit = false;
                    }
                    else /* archive failed but entry is not dirty?
                          * retrieve the status from filesystem */
                        *getit = true;
                }
                break;

            case HE_RESTORE:
                if (hsm_get_cl_error(logrec->cr_flags) == CLF_HSM_SUCCESS)
                {
                    /** @TODO RBHv3 remember last restore time */

                    /* status is 'up-to-date' after a successful restore */
                    set_lhsm_status(smi, refreshed_attrs, STATUS_SYNCHRO);
                    *getit = false;
                }
                else /* failed restore */
                {
                    /* Entry status remains 'released' */
                    set_lhsm_status(smi, refreshed_attrs, STATUS_RELEASED);
                    *getit = false;
                }
                break;

            case HE_RELEASE:
                if (hsm_get_cl_error(logrec->cr_flags) != CLF_HSM_SUCCESS)
                {
                    /* release records are not expected to be erroneous */
                    DisplayLog(LVL_CRIT, LHSM_TAG,
                               "ERROR: Unexpected HSM release event with error %d",
                               hsm_get_cl_error(logrec->cr_flags));
                    /* make sure of actual entry status */
                    *getit = true;
                }
                else /* successful release */
                {
                    set_lhsm_status(smi, refreshed_attrs, STATUS_RELEASED);
                    *getit = false;
                }
                break;

            case HE_STATE:
                /* state changed: did it become dirty? */
                if (hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY)
                {
                    set_lhsm_status(smi, refreshed_attrs, STATUS_MODIFIED);
                    *getit = false;
                }
                else /* other status change: need to get it */
                    *getit = true;

                break;

            case HE_REMOVE:
            case HE_CANCEL:
                /* undetermined status after such an event */
                *getit = true;
                break;

            default:
                DisplayLog(LVL_CRIT, LHSM_TAG,
                           "ERROR: unknown HSM event: bitfield=%#x, event=%u",
                           logrec->cr_flags,
                           hsm_get_cl_event(logrec->cr_flags));
                /* skip */
                return EINVAL;
        }
    }
    else if (logrec->cr_type == CL_MTIME || logrec->cr_type == CL_TRUNC ||
              (logrec->cr_type == CL_CLOSE))
    {
        /* If file is modified or truncated, need to check its status
         * (probably modified) EXCEPT if its status is already 'modified' */
        if (!ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)
            || (!status_equal(smi, attrs, STATUS_NEW) &&
                !status_equal(smi, attrs, STATUS_MODIFIED)))
        {
            DisplayLog(LVL_DEBUG, LHSM_TAG,
                       "Getstatus needed because this is a %s event "
                       "and status is not already 'modified' or 'new': status=%s",
                       changelog_type2str(logrec->cr_type),
                       ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)?
                         STATUS_ATTR(attrs, smi->smi_index) :"<not set>");
            *getit = true;
        }
    }
    /* other records: keep default value for status need */
    return 0;
}

#if 0 /** FIXME LUSTRE_HSM specific? */

                if (logrec->cr_flags & CLF_UNLINK_HSM_EXISTS)
                    /* if CLF_UNLINK_HSM_EXISTS is set, we must clean something in the backend */
                    p_op->db_op_type = OP_TYPE_SOFT_REMOVE;
                else if (p_op->db_exists)
                    /* nothing in the backend, just clean the entry in DB */
                    p_op->db_op_type = OP_TYPE_REMOVE_LAST;
                else
                    /* ignore the record */
                    return STAGE_CHGLOG_CLR;
#endif

/** @TODO to be called in pipeline */

/** function to determine if a deleted entry must be inserted to SOFTRM table
 * @return <0 on error, 0 for false, 1 for true.
 */
static int lhsm_softrm_filter(struct sm_instance *smi, const entry_id_t *id,
                              const attr_set_t *attrs)
{
    if (ATTR_MASK_TEST(attrs, type)
        && !strcmp(ATTR(attrs, type), STR_TYPE_DIR))
    {
        DisplayLog(LVL_FULL, LHSM_TAG, "Removing directory entry (no rm in backend)");
        return 0;
    }
    else if (ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)
             && status_equal(smi, attrs, STATUS_NEW))
    {
        DisplayLog(LVL_DEBUG, LHSM_TAG, "Removing 'new' entry ("DFID"): no remove in backend",
                   PFID(id));
        return 0;
    }
    return 1;
}

/** Status manager for Lustre/HSM */
status_manager_t lhsm_sm = {
    .name = "lhsm",
    .flags = SM_SHARED | SM_DELETED | SM_MULTI_ACTION,
    .status_enum = lhsm_status_list,
    .status_count = STATUS_COUNT,

    /* This policy needs the ols status to process changelog callbacks.
     * As we don't know the actual index of the status manager instance (smi)
     * we set it to SMI_MASK(0). It is translater later by accessors to
     * its actual index.  */
    .status_needs_attrs_cached = ATTR_MASK_type | SMI_MASK(0),
    .status_needs_attrs_fresh = 0,

    .get_status_func = lhsm_status,
    .changelog_cb = lhsm_cl_cb,

    .check_action_name = lhsm_check_action_name,
    .action_cb = lhsm_action_callback,

    /* fields for managing deleted entries */
    .softrm_filter_mask = ATTR_MASK_type | SMI_MASK(0),
    .softrm_filter_func = lhsm_softrm_filter,
    /** needed attributes for undelete in addition to POSIX and fullpath: lhsm_status (to know the status of the 'undeleted' entry)
     * FIXME also need to store the 'archive_id'!
     */
    .softrm_table_mask = SMI_MASK(0),
    .undelete_func = NULL, /* FIXME to be implemented */

    /* XXX about full disaster recovery: must recreate all metadata (incl. symlinks => need link field)
     * not only the entries managed by the policy.
     * This was used to be done usung the contents of ENTRIES table.
     */

    /* XXX A status manager can load a configuration */
    /* XXX actions may need the same configuration... */
    .init_func = lhsm_init
};

const char *mod_get_name(void)
{
    return "lhsm";
}

status_manager_t *mod_get_status_manager(void)
{
    return &lhsm_sm;
}

action_func_t mod_get_action_by_name(const char *action_name)
{
    if (strcmp(action_name, "lhsm.archive") == 0)
        return lhsm_archive;
    else if (strcmp(action_name, "lhsm.release") == 0)
        return lhsm_release;
    else if (strcmp(action_name, "lhsm.hsm_remove") == 0)
        return lhsm_remove;
    else
        return NULL;
}
