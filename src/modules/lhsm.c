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
#include "db_schema.h" /* for common robinhood types: entry_id_t,
                          stripe_info_t... */
#include "status_manager.h"

#include <stdbool.h>
#include <glib.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <uuid/uuid.h>

/* config block name */
#define LHSM_BLOCK "lhsm_config"
/* tag for logs */
#define LHSM_TAG "lhsm"

#define DEFAULT_ARCHIVE_ID  0
#define ARCHIVE_PARAM "archive_id"

/* Length of a UUID as a string, without trailing NUL. */
#define UUID_XATTR_STRLEN 36

typedef struct lhsm_config_t {
    char **rebind_cmd;

    char uuid_xattr[XATTR_NAME_MAX + 1];
} lhsm_config_t;

/* lhsm config is global as the status manager is shared */
static lhsm_config_t config;

/**
 * Global static list of excluded variables for action parameters
 * serialization. */
static struct rbh_params *exclude_params = NULL;

/**
 * Get archive_id from action parameters.
 * @return archive_id on success, a negative value or error.
 */
static int get_archive_id(const action_params_t *params)
{
    int arch_id;
    const char *val = rbh_param_get(params, ARCHIVE_PARAM);

    if (val == NULL)
        return -ENOENT;

    arch_id = str2int(val);
    if (arch_id == -1) {
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
    if (rc) {
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
                       const attr_set_t *attrs, const action_params_t *params)
{
    struct hsm_user_request *req;
    int rc;
    char *mpath;
    unsigned int archive_id = DEFAULT_ARCHIVE_ID;   /* default */
    GString *args = NULL;
    const char *data = NULL;
    int data_len = 0;

    /* if archive_id is explicitely specified in action parameters, use it */
    rc = get_archive_id(params);
    if (rc == 0) {
        archive_id = rc;
    } else if (rc == -ENOENT) {
        /* for HSM_REMOVE, try to get it from previous attrs */
        if (action == HUA_REMOVE) {
            unsigned int *tmp;
            unsigned int idx;
            const sm_info_def_t *def;

            rc = sm_attr_get(NULL, attrs, "lhsm.archive_id", (void **)&tmp,
                             &def, &idx);
            if (rc == 0) {
                /* sanity check of returned type */
                if (def->db_type != DB_UINT)
                    DisplayLog(LVL_CRIT, LHSM_TAG,
                               "Unexpected type for 'lhsm.archive_id': %d",
                               def->db_type);
                else
                    archive_id = *tmp;
            }
        }
        /* all other cases: keep default */
    } else if (rc < 0)
        return rc;

    /* Serialize the parameters to pass them to the copytool.
     * exclude archive_id, which is for internal use. */
    args = g_string_new("");
    rc = rbh_params_serialize(params, args, exclude_params,
                              RBH_PARAM_CSV | RBH_PARAM_COMPACT);
    if (rc)
        goto free_args;

    if (!GSTRING_EMPTY(args)) {
        data = args->str;
        data_len = args->len + 1;
    }

    DisplayLog(LVL_DEBUG, LHSM_TAG,
               "action %s, fid=" DFID ", archive_id=%u, parameters='%s'",
               hsm_user_action2name(action), PFID(p_id), archive_id, args->str);

    req = llapi_hsm_user_request_alloc(1, data_len);
    if (!req) {
        rc = -errno;
        DisplayLog(LVL_CRIT, LHSM_TAG, "Cannot create HSM request: %s",
                   strerror(-rc));
        goto free_args;
    }

    req->hur_request.hr_action = action;
    req->hur_request.hr_archive_id = archive_id;
    req->hur_request.hr_flags = 0;

    req->hur_user_item[0].hui_fid = *p_id;
    req->hur_user_item[0].hui_extent.offset = 0;
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
                   "ERROR performing HSM request(%s, root=%s, fid=" DFID
                   "): %s", hsm_user_action2name(action), get_mount_point(NULL),
                   PFID(p_id), strerror(-rc));
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
    return lhsm_action(HUA_RELEASE, p_entry_id, p_attrs, params);
}

/** perform hsm_archive action */
static int lhsm_archive(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                        const action_params_t *params, post_action_e *after,
                        db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    /* 'after' is set in action callback */
    return lhsm_action(HUA_ARCHIVE, p_entry_id, p_attrs, params);
}

/** perform hsm_remove action */
static int lhsm_remove(const entry_id_t *p_entry_id, attr_set_t *p_attrs,
                       const action_params_t *params, post_action_e *after,
                       db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    /* 'after' is set in action callback */
    int rc = lhsm_action(HUA_REMOVE, p_entry_id, p_attrs, params);
    return rc;
}

/** set of managed status */
typedef enum {
    STATUS_NEW, /* file has no HSM flags (just created) */
    STATUS_MODIFIED,    /* file must be archived */
    STATUS_RESTORE_RUNNING, /* file is being retrieved */
    STATUS_ARCHIVE_RUNNING, /* file is being archived */
    STATUS_SYNCHRO, /* file has been synchronized in HSM, file can be purged */
    STATUS_RELEASED,    /* file is released (nothing to do). */

    STATUS_COUNT,   /* number of possible file status */
} hsm_status_t;

/* XXX /!\ Keep in sync with hsm_status_t */
static const char *lhsm_status_list[] = {
    [STATUS_NEW] = "new",
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

/** enum of specific attributes */
enum lhsm_info_e {
    ATTR_ARCHIVE_ID = 0,
    ATTR_NO_RELEASE,
    ATTR_NO_ARCHIVE,
    ATTR_LAST_ARCHIVE,
    ATTR_LAST_RESTORE,
    ATTR_UUID,
};

/** size of specific info to be stored in DB:
 * archive_id: hsm_user_state returns a 32bits interger for archive_id.
 * no_release: 0 or 1
 * no_archive: 0 or 1
 * last_archive: unix epoch
 * last_restore: unix epoch
 * uuid: 36 characters representing a 16 bytes UUID
 */
static sm_info_def_t lhsm_info[] = {
    [ATTR_ARCHIVE_ID] =
        {ARCHIVE_PARAM, "archid", DB_UINT, 0, {.val_uint = 0}, PT_INT},
    [ATTR_NO_RELEASE] =
        {"no_release", "norels", DB_BOOL, 0, {.val_bool = false}, PT_BOOL},
    [ATTR_NO_ARCHIVE] =
        {"no_archive", "noarch", DB_BOOL, 0, {.val_bool = false}, PT_BOOL},
    [ATTR_LAST_ARCHIVE] =
        {"last_archive", "lstarc", DB_UINT, 0, {.val_uint = 0}, PT_DURATION},
    [ATTR_LAST_RESTORE] =
        {"last_restore", "lstrst", DB_UINT, 0, {.val_uint = 0}, PT_DURATION},
    [ATTR_UUID] =
        {"uuid", "uuid", DB_TEXT, UUID_XATTR_STRLEN, {.val_str = NULL},
         PT_STRING},
};

static bool cfg_has_uuid(const lhsm_config_t *cfg)
{
    return cfg->uuid_xattr[0] != '\0';
}

/* Get the UUID for the fid.
 * Return 0 on success, an errno on failure. uuid must be at least 37
 * bytes long. */
static int get_uuid(const entry_id_t *id, char *uuid)
{
    char fid_path[RBH_PATH_MAX];
    int rc;

    rc = BuildFidPath(id, fid_path);
    if (rc)
        return rc;

    rc = lgetxattr(fid_path, config.uuid_xattr, uuid, UUID_XATTR_STRLEN + 1);
    if (rc == -1) {
        rc = errno;
        if (rc != ENODATA)
            DisplayLog(LVL_MAJOR, LHSM_TAG,
                       "Cannot get UUID for fid " DFID_NOBRACE " : %s",
                       PFID(id), strerror(rc));
        return rc;
    }

    if (rc != UUID_XATTR_STRLEN) {
        DisplayLog(LVL_MAJOR, LHSM_TAG,
                   "Invalid size %d for UUID for fid " DFID_NOBRACE,
                   rc, PFID(id));
        return E2BIG;
    }

    uuid[UUID_XATTR_STRLEN] = 0;

    return 0;
}

/* Get the UUID from the file and set the SM attribute. Do not return
 * an error if the file doesn't have a UUID, as it is better to still
 * have it up to date in the database than not at all. */
static void set_uuid_info(struct sm_instance *smi, const entry_id_t *id,
                          attr_set_t *refreshed_attrs)
{
    char *uuid;

    uuid = malloc(UUID_XATTR_STRLEN + 1);
    if (uuid == NULL)
        return;

    if (get_uuid(id, uuid) != 0) {
        free(uuid);
        return;
    }

    if (set_sm_info(smi, refreshed_attrs, ATTR_UUID, uuid) != 0)
        free(uuid);
}

/** get Lustre status and convert it to an internal scalar status */
static int lhsm_get_status(const char *path, hsm_status_t *p_status,
                           bool *no_release, bool *no_archive,
                           unsigned int *archive_id)
{
    struct hsm_user_state file_status;
    int rc;

    /* initialize outputs */
    *p_status = STATUS_NEW;
    *no_release = false;
    *no_archive = false;
    *archive_id = DEFAULT_ARCHIVE_ID;

    /* get status */
    rc = llapi_hsm_state_get(path, &file_status);

    if ((rc != 0) && (rc != -ENOENT) && (rc != -ESTALE))
        DisplayLog(LVL_DEBUG, LHSM_TAG, "llapi_hsm_state_get(%s)=%d", path, rc);
    if (rc != 0)
        return rc;

    /* archive_id */
    *archive_id = file_status.hus_archive_id;

    /* user flags */

    if (file_status.hus_states & HS_NORELEASE)
        *no_release = true;
    if (file_status.hus_states & HS_NOARCHIVE)
        *no_archive = true;

    /* clear them */
    file_status.hus_states &= ~(HS_NORELEASE | HS_NOARCHIVE);

    /* pending actions */

    if (file_status.hus_in_progress_action == HUA_ARCHIVE) {
        *p_status = STATUS_ARCHIVE_RUNNING;
        return 0;
    } else if (file_status.hus_in_progress_action == HUA_RESTORE) {
        *p_status = STATUS_RESTORE_RUNNING;
        return 0;
    } else if (file_status.hus_in_progress_action == HUA_RELEASE) {
        DisplayLog(LVL_DEBUG, LHSM_TAG, "Entry %s is being released", path);
    } else if (file_status.hus_in_progress_action == HUA_REMOVE) {
        DisplayLog(LVL_DEBUG, LHSM_TAG, "Entry %s is being removed", path);
    }

    /* status flags */
    if ((file_status.hus_states & HSM_FLAGS_MASK) == 0) {
        *p_status = STATUS_NEW;
    } else if (file_status.hus_states & HS_DIRTY) {
        *p_status = STATUS_MODIFIED;
    } else if (file_status.hus_states & HS_ARCHIVED) {
        /* "and not dirty" is ensured by the previous test */
        if (file_status.hus_states & HS_RELEASED) {
            /* file is archived in HSM, and released from Lustre */
            *p_status = STATUS_RELEASED;
        } else {
            /* file is up-to-date in HSM, and not released in Lustre */
            *p_status = STATUS_SYNCHRO;
        }
    } else if (file_status.hus_states & HS_EXISTS) {
        /* new file, not yet archived successfully */
        *p_status = STATUS_MODIFIED;
    } else {
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

/** helper to set the LHSM status in attribute structure */
static inline int set_lhsm_status(struct sm_instance *smi, attr_set_t *attrs,
                                  hsm_status_t status)
{
    return set_status_attr(smi, attrs, hsm_status2str(status));
}

/** get the HSM status of an entry */
static int lhsm_status(struct sm_instance *smi,
                       const entry_id_t *id, const attr_set_t *attrs,
                       attr_set_t *refreshed_attrs)
{
    int rc;
    char fid_path[RBH_PATH_MAX];
    hsm_status_t st;
    bool no_release = false, no_archive = false;
    unsigned int archive_id = DEFAULT_ARCHIVE_ID;

    if (ATTR_MASK_TEST(attrs, type) &&
        strcmp(ATTR(attrs, type), STR_TYPE_FILE) != 0) {
        /* not a file: no status */
        rc = 0;
        goto clean_status;
    }

    rc = BuildFidPath(id, fid_path);
    if (rc)
        goto clean_status;

    rc = lhsm_get_status(fid_path, &st, &no_release, &no_archive, &archive_id);
    if (rc)
        goto clean_status;

    rc = set_lhsm_status(smi, refreshed_attrs, st);
    if (rc)
        goto clean_status;

    /* save archive_id */
    rc = set_uint_info(smi, refreshed_attrs, ATTR_ARCHIVE_ID, archive_id);
    if (rc)
        goto clean_status;

    if (cfg_has_uuid(&config))
        set_uuid_info(smi, id, refreshed_attrs);

    /* update no_archive/no_release (non critical: ignore errors) */
    set_bool_info(smi, refreshed_attrs, ATTR_NO_ARCHIVE, no_archive);
    set_bool_info(smi, refreshed_attrs, ATTR_NO_RELEASE, no_release);

    return 0;

 clean_status:
    if (refreshed_attrs->attr_values.sm_status != NULL)
        /* don't free it as it contains a const char* */
        STATUS_ATTR(refreshed_attrs, smi->smi_index) = NULL;

    /* Clean the status from the mask */
    ATTR_MASK_STATUS_UNSET(refreshed_attrs, smi->smi_index);

    return rc;
}

/** helper to compare a LHSM status */
static bool status_equal(struct sm_instance *smi, const attr_set_t *attrs,
                         hsm_status_t status)
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

    if (!strcasecmp(implements, "archive")) {
        /* successful archive (asynchronous): now archive_running,
         * else (failed): unchanged. */
        if (action_status == 0)
            set_lhsm_status(smi, attrs, STATUS_ARCHIVE_RUNNING);
        else
            /* (try to) update hsm_status on failure */
            lhsm_status(smi, id, attrs, attrs);

        *what_after = PA_UPDATE;
        return 0;
    } else if (!strcasecmp(implements, "release")) {
        /* successful release (synchronous): now released, else: unchanged. */
        if (action_status == 0)
            set_lhsm_status(smi, attrs, STATUS_RELEASED);
        else
            /* (try to) update hsm_status on failure */
            lhsm_status(smi, id, attrs, attrs);

        *what_after = PA_UPDATE;
        return 0;
    } else if (!strcasecmp(implements, "removed")
               || !strcasecmp(implements, "deleted")) {
        /* successful removed (asynchronous): drop from DB */
        *what_after = (action_status != 0 ? PA_NONE : PA_RM_ONE);
        return 0;
    } else
        return -EINVAL;
}

/** changelog callback */
static int lhsm_cl_cb(struct sm_instance *smi, const CL_REC_TYPE *logrec,
                      const entry_id_t *id, const attr_set_t *attrs,
                      attr_set_t *refreshed_attrs, bool *getit,
                      proc_action_e *rec_action)
{
    /* If this is a CREATE record, we know its status is NEW
     * (except if it is already set to another value) */
    if (logrec->cr_type == CL_CREATE) {
        if (!ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)) {
            /* new file, status is new */
            set_lhsm_status(smi, refreshed_attrs, STATUS_NEW);
            /* no need to retrieve it from filesystem */
            *getit = false;
        }
        /* else: file is already known. Preserve the known status. */

        /* new entry: never archived or restored
         * (non-critical: ignore errors) */
        set_uint_info(smi, refreshed_attrs, ATTR_LAST_ARCHIVE, 0);
        set_uint_info(smi, refreshed_attrs, ATTR_LAST_RESTORE, 0);
        /* no flag is set at creation */
        set_bool_info(smi, refreshed_attrs, ATTR_NO_ARCHIVE, false);
        set_bool_info(smi, refreshed_attrs, ATTR_NO_RELEASE, false);
    } else if ((logrec->cr_type == CL_MKDIR) || (logrec->cr_type == CL_RMDIR)) {
        /* no status for directories */
        *getit = false;
    } else if (logrec->cr_type == CL_HSM) {
        switch (hsm_get_cl_event(logrec->cr_flags)) {
        case HE_ARCHIVE:
            /* is it a successful copy? */
            if (hsm_get_cl_error(logrec->cr_flags) == CLF_HSM_SUCCESS) {
                /* save last archive time (non-critical: ignore errors) */
                set_uint_info(smi, refreshed_attrs, ATTR_LAST_ARCHIVE,
                              cltime2sec(logrec->cr_time));

                /* Save UUID */
                if (cfg_has_uuid(&config))
                    set_uuid_info(smi, id, refreshed_attrs);

                /* if dirty flag is set in the changelog record,
                 * the entry is dirty, else, it is up to date. */
                set_lhsm_status(smi, refreshed_attrs,
                                (hsm_get_cl_flags(logrec->cr_flags) &
                                 CLF_HSM_DIRTY) ? STATUS_MODIFIED :
                                STATUS_SYNCHRO);
                *getit = false;
            } else {    /* archive failed */

                /* Entry is probably still dirty. If dirty flag is not set,
                 * we need to ask the actual status */
                if (hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY) {
                    set_lhsm_status(smi, refreshed_attrs, STATUS_MODIFIED);
                    *getit = false;
                } else  /* archive failed but entry is not dirty?
                         * retrieve the status from filesystem */
                    *getit = true;
            }
            break;

        case HE_RESTORE:
            if (hsm_get_cl_error(logrec->cr_flags) == CLF_HSM_SUCCESS) {
                /* save last restore time (non-critical: ignore errors) */
                set_uint_info(smi, refreshed_attrs, ATTR_LAST_RESTORE,
                              cltime2sec(logrec->cr_time));

                /* status is 'up-to-date' after a successful restore */
                set_lhsm_status(smi, refreshed_attrs, STATUS_SYNCHRO);
                *getit = false;
            } else {    /* failed restore */

                /* Entry status remains 'released' */
                set_lhsm_status(smi, refreshed_attrs, STATUS_RELEASED);
                *getit = false;
            }
            break;

        case HE_RELEASE:
            if (hsm_get_cl_error(logrec->cr_flags) != CLF_HSM_SUCCESS) {
                /* release records are not expected to be erroneous */
                DisplayLog(LVL_CRIT, LHSM_TAG,
                           "ERROR: Unexpected HSM release event with error %d",
                           hsm_get_cl_error(logrec->cr_flags));
                /* make sure of actual entry status */
                *getit = true;
            } else {    /* successful release */

                set_lhsm_status(smi, refreshed_attrs, STATUS_RELEASED);
                *getit = false;
            }
            break;

        case HE_STATE:
            /* state changed: did it become dirty? */
            if (hsm_get_cl_flags(logrec->cr_flags) & CLF_HSM_DIRTY) {
                set_lhsm_status(smi, refreshed_attrs, STATUS_MODIFIED);
                *getit = false;
            } else  /* other status change: need to get it */
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
                       logrec->cr_flags, hsm_get_cl_event(logrec->cr_flags));
            /* skip */
            return EINVAL;
        }
    } else if (logrec->cr_type == CL_MTIME || logrec->cr_type == CL_TRUNC ||
               (logrec->cr_type == CL_CLOSE)) {
        /* If file is modified or truncated, need to check its status
         * (probably modified) EXCEPT if its status is already 'modified' */
        if (!ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)
            || (!status_equal(smi, attrs, STATUS_NEW) &&
                !status_equal(smi, attrs, STATUS_MODIFIED))) {
            DisplayLog(LVL_DEBUG, LHSM_TAG,
                       "Getstatus needed because this is a %s event "
                       "and status is not already 'modified' or 'new': status=%s",
                       changelog_type2str(logrec->cr_type),
                       ATTR_MASK_STATUS_TEST(attrs, smi->smi_index) ?
                       STATUS_ATTR(attrs, smi->smi_index) : "<not set>");
            *getit = true;
        }
    } else if ((logrec->cr_type == CL_UNLINK)
               && (logrec->cr_flags & CLF_UNLINK_LAST)) {
        /* if CLF_UNLINK_HSM_EXISTS is set, we must clean something in the
         * backend.
         * always add the entry to the SOFTRM_TABLE.
         */
        if (logrec->cr_flags & CLF_UNLINK_HSM_EXISTS) {
            /* Don't care about softrm filter here as Lustre explicitely
             * indicates there is something to be cleaned in the backend. */
            *rec_action = PROC_ACT_SOFTRM_ALWAYS;
        } else  /* remove the entry from DB */
            *rec_action = PROC_ACT_RM_ALL;
    }

    /* other records: keep default value for status need */
    return 0;
}

/** function to determine if a deleted entry must be inserted to SOFTRM table
 */
static proc_action_e lhsm_softrm_filter(struct sm_instance *smi,
                                        const entry_id_t *id,
                                        const attr_set_t *attrs)
{
    if (ATTR_MASK_TEST(attrs, type)
        && strcmp(ATTR(attrs, type), STR_TYPE_FILE) != 0) {
        DisplayLog(LVL_FULL, LHSM_TAG,
                   "Removing non-file entry (no rm in backend)");
        return PROC_ACT_RM_ALL;
    } else if (ATTR_MASK_STATUS_TEST(attrs, smi->smi_index)
               && status_equal(smi, attrs, STATUS_NEW)) {
        DisplayLog(LVL_DEBUG, LHSM_TAG,
                   "Removing 'new' entry (" DFID "): no remove in backend",
                   PFID(id));
        return PROC_ACT_RM_ALL;
    }
    /* If we have a doubt, always insert to softrm.
     *  In the worst case, it's just a useless hsm_rm operation.
     */
    return PROC_ACT_SOFTRM_ALWAYS;
}

/** rebind an entry to a new fid in HSM backend */
static int lhsm_rebind(const entry_id_t *old_id, const entry_id_t *new_id,
                       const attr_set_t *new_attrs,
                       struct sm_instance *smi, unsigned int archive_id)
{
    const char descr[] = "rebind command";
    char **cmd = NULL;
    char *log_cmd;
    char tmp[256];  /* max length for fid */
    action_params_t cmd_params = { 0 };
    int rc;

    DisplayLog(LVL_EVENT, LHSM_TAG, "Rebinding " DFID " to " DFID " in archive",
               PFID(old_id), PFID(new_id));

    /* push archive_id, oldfid, newfid into command params */
    snprintf(tmp, sizeof(tmp), "%u", archive_id);
    rbh_param_set(&cmd_params, ARCHIVE_PARAM, tmp, true);
    snprintf(tmp, sizeof(tmp), DFID_NOBRACE, PFID(old_id));
    rbh_param_set(&cmd_params, "oldfid", tmp, true);
    snprintf(tmp, sizeof(tmp), DFID_NOBRACE, PFID(new_id));
    rbh_param_set(&cmd_params, "newfid", tmp, true);

    rc = subst_shell_params(config.rebind_cmd, descr, new_id, new_attrs,
                            &cmd_params, NULL, smi, true, &cmd);
    rbh_params_free(&cmd_params);

    if (rc) {
        log_cmd = concat_cmd(config.rebind_cmd);
        DisplayLog(LVL_MAJOR, LHSM_TAG, "Invalid rebind command: %s", log_cmd);
        free(log_cmd);
        return rc;
    }

    if (log_config.debug_level >= LVL_EVENT) {
        log_cmd = concat_cmd(cmd);
        DisplayLog(LVL_EVENT, LHSM_TAG, "Executing rebind command: %s",
                   log_cmd);
        free(log_cmd);
    }

    rc = execute_shell_command(cmd, cb_stderr_to_log, (void *)LVL_DEBUG);
    g_strfreev(cmd);

    return rc;
}

/**
 * Undelete function for Lustre/HSM.
 * Creates file in 'released' state, using the given attributes.
 * Then call directly an external command to rebind the old archived
 * entry with the new fid. As long as lustre can't transmit rebind
 * commands to copytools, robinhood directly calls a admin-defined
 * command to do this.
 */
static recov_status_t lhsm_undelete(struct sm_instance *smi,
                                    const entry_id_t *p_old_id,
                                    const attr_set_t *p_attrs_old_in,
                                    entry_id_t *p_new_id,
                                    attr_set_t *p_attrs_new,
                                    bool already_recovered)
{
    struct stat entry_stat = { 0 };
    unsigned int *tmp;
    unsigned int idx;
    const sm_info_def_t *def;
    unsigned int archive_id = DEFAULT_ARCHIVE_ID;
    int rc;
    const char *path;
    char *uuid = NULL;

    /* Lustre/HSM only archive files */
    if (ATTR_MASK_TEST(p_attrs_old_in, type) &&
        strcmp(ATTR(p_attrs_old_in, type), STR_TYPE_FILE) != 0) {
        return RS_NOBACKUP;
    }

    /* convert attrs from DB to a struct stat */
    rbh_attrs2stat(p_attrs_old_in, &entry_stat);

    if (!ATTR_MASK_TEST(p_attrs_old_in, fullpath)) {
        DisplayLog(LVL_MAJOR, LHSM_TAG, "Missing mandatory parameter "
                   "'fullpath' to import the file.");
        /** TODO create as <root>/.undelete/old_<fid> */
        return RS_ERROR;
    } else {
        path = ATTR(p_attrs_old_in, fullpath);
    }

    rc = sm_attr_get(smi, p_attrs_old_in, "lhsm.archive_id", (void **)&tmp,
                     &def, &idx);
    if (rc == 0) {
        /* sanity check of returned type */
        if (def->db_type != DB_UINT)
            DisplayLog(LVL_CRIT, LHSM_TAG,
                       "Unexpected type for 'lhsm.archive_id': %d",
                       def->db_type);
        else
            archive_id = *tmp;
    }

    rc = sm_attr_get(smi, p_attrs_old_in, "lhsm.uuid", (void **)&uuid,
                     &def, &idx);
    if (rc == 0) {
        /* sanity check of returned type */
        if (def->db_type != DB_TEXT) {
            DisplayLog(LVL_CRIT, LHSM_TAG,
                       "Unexpected type for 'lhsm.uuid': %d", def->db_type);
            free(uuid);
            uuid = NULL;
        }
    }

    /* create parent directory if it does not already exist */
    rc = create_parent_of(path, NULL);
    if (rc != 0 && rc != -EEXIST) {
        DisplayLog(LVL_CRIT, LHSM_TAG, "Failed to create parent directory for "
                   "file '%s': %s", path, strerror(-rc));
        free(uuid);
        return RS_ERROR;
    }

    /* create the file in 'released' state */
    rc = llapi_hsm_import(path, archive_id, &entry_stat, 0, -1, 0, 0, NULL,
                          p_new_id);
    if (rc) {
        DisplayLog(LVL_CRIT, LHSM_TAG, "Failed to import file '%s': %s", path,
                   strerror(-rc));
        free(uuid);
        return RS_ERROR;
    }

    /* Set the UUID back */
    if (uuid) {
        rc = lsetxattr(path, config.uuid_xattr, uuid, UUID_XATTR_STRLEN, 0);
        rc = rc ? errno : 0;
        free(uuid);
        if (rc) {
            DisplayLog(LVL_CRIT, LHSM_TAG,
                       "Failed to set UUID for file '%s': %s",
                       path, strerror(rc));
            return RS_ERROR;
        }
    }

    /* get the new entry attributes */
    if (lstat(path, &entry_stat)) {
        DisplayLog(LVL_CRIT, LHSM_TAG, "Failed to stat imported file '%s': %s",
                   path, strerror(errno));
        return RS_ERROR;
    }
    stat2rbh_attrs(&entry_stat, p_attrs_new, true);

    /** TODO If another status manager recovered it, just rebind in the
     * backend. */

    if (!cfg_has_uuid(&config)) {
        rc = lhsm_rebind(p_old_id, p_new_id, p_attrs_new, smi, archive_id);
        if (rc) {
            DisplayLog(LVL_CRIT, LHSM_TAG,
                       "Failed to rebind entry in backend: %s",
                       rc < 0 ? strerror(-rc) : "command failed");
            return RS_ERROR;
        }
    }

    return RS_FILE_OK;
}

#define DEFAULT_REBIND_CMD "lhsmtool_posix --archive={archive_id} " \
                                "--rebind {oldfid} {newfid} {fsroot}"

static void lhsm_cfg_set_default(void *module_config)
{
    lhsm_config_t *conf = module_config;
    GError *err_desc;
    int ac;

    if (!g_shell_parse_argv
        (DEFAULT_REBIND_CMD, &ac, &conf->rebind_cmd, &err_desc)) {
        DisplayLog(LVL_CRIT, __func__,
                   "Failed to parse default rebind_cmd '%s': %s",
                   DEFAULT_REBIND_CMD, err_desc->message);
        g_error_free(err_desc);
        conf->rebind_cmd = NULL;
        abort();
    }
    if (ac == 0) {
        g_strfreev(conf->rebind_cmd);
        conf->rebind_cmd = NULL;
    }
}

static void lhsm_cfg_write_default(FILE *output)
{
    print_begin_block(output, 0, LHSM_BLOCK, NULL);
    print_line(output, 1, "rebind_cmd: " DEFAULT_REBIND_CMD);
    print_end_block(output, 0);
}

#define UUID_CONFIG_BLOCK "uuid"

static int lhsm_cfg_read(config_file_t config, void *module_config,
                         char *msg_out)
{
    int rc;
    lhsm_config_t *conf = module_config;
    config_item_t block;
    config_item_t uuid_block;
    bool unique = true;

    const cfg_param_t hsm_params[] = {
        /* rebind_cmd can contain wildcards: {fsroot} {oldfid} {newfid}... */
        {"rebind_cmd", PT_CMD, 0, &conf->rebind_cmd, 0},
        END_OF_PARAMS
    };

    const cfg_param_t uuid_params[] = {
        {"xattr", PT_STRING, 0, conf->uuid_xattr, sizeof(conf->uuid_xattr)}
        ,
        END_OF_PARAMS
    };

    static const char *uuid_allowed[] = {
        "xattr", NULL
    };

    static const char *allowed_params[] = {
        "rebind_cmd", "uuid", NULL
    };

    /* get lhsm_config block */
    rc = get_cfg_block(config, LHSM_BLOCK, &block, msg_out);
    if (rc)
        return rc == ENOENT ? 0 : rc;   /* not mandatory */

    /* read std parameters */
    rc = read_scalar_params(block, LHSM_BLOCK, hsm_params, msg_out);
    if (rc)
        return rc;

    CheckUnknownParameters(block, LHSM_BLOCK, allowed_params);

    /* Read uuid block if present */
    uuid_block =
        rh_config_FindItemByName(config, LHSM_BLOCK "::" UUID_CONFIG_BLOCK,
                                 &unique);
    if (uuid_block) {
        rc = get_cfg_block(config, LHSM_BLOCK "::" UUID_CONFIG_BLOCK,
                           &uuid_block, msg_out);
        if (rc)
            return rc;

        rc = read_scalar_params(uuid_block, UUID_CONFIG_BLOCK, uuid_params,
                                msg_out);
        if (rc)
            return rc;

        CheckUnknownParameters(uuid_block, UUID_CONFIG_BLOCK, uuid_allowed);
    }

    return 0;
}

static void lhsm_cfg_write_template(FILE *output)
{
    print_begin_block(output, 0, LHSM_BLOCK, NULL);
    print_line(output, 1, "# command to rebind an entry in the backend");
    print_line(output, 1, "rebind_cmd = \"lhsmtool_posix "
               "--archive={archive_id} --hsm_root=/tmp/backend "
               "--rebind {oldfid} {newfid} {fsroot}\"");
    print_end_block(output, 0);
}

static void *lhsm_cfg_new(void)
{
    return calloc(1, sizeof(lhsm_config_t));
}

static void lhsm_cfg_free(void *cfg)
{
    if (cfg != NULL)
        free(cfg);
}

static int lhsm_cfg_set(void *cfg, bool reload)
{
    lhsm_config_t *new = cfg;

    if (!reload) {
        config = *new;
        return 0;
    }

    if (compare_cmd(new->rebind_cmd, config.rebind_cmd)) {
        DisplayLog(LVL_MAJOR, LHSM_TAG,
                   LHSM_BLOCK "::rebind_cmd changed in config file "
                   "but cannot be changed dynamically");
    }

    return 0;
}

static const mod_cfg_funcs_t lhsm_cfg_hdlr = {
    .module_name = "lhsm",
    .new = lhsm_cfg_new,
    .free = lhsm_cfg_free,
    .set_default = lhsm_cfg_set_default,
    .read = lhsm_cfg_read,
    .set_config = lhsm_cfg_set,
    .write_default = lhsm_cfg_write_default,
    .write_template = lhsm_cfg_write_template,
};

/** Status manager for Lustre/HSM */
static status_manager_t lhsm_sm = {
    .name = "lhsm",
    .flags = SM_SHARED | SM_DELETED | SM_MULTI_ACTION,
    .status_enum = lhsm_status_list,
    .status_count = STATUS_COUNT,
    .nb_info = G_N_ELEMENTS(lhsm_info),
    .info_types = lhsm_info,

    /* This policy needs the previous status to process changelog callbacks.
     * As we don't know the actual index of the status manager instance (smi)
     * we set it to SMI_MASK(0). It is translated later by accessors to
     * its actual index.  */
    .status_needs_attrs_cached = {.std = ATTR_MASK_type, .status = SMI_MASK(0)},
    .status_needs_attrs_fresh = {0},

    .get_status_func = lhsm_status,
    .changelog_cb = lhsm_cl_cb,

    .check_action_name = lhsm_check_action_name,
    .action_cb = lhsm_action_callback,

    /* fields for managing deleted entries */
    .softrm_filter_mask = {.std = ATTR_MASK_type, .status = SMI_MASK(0)},
    .softrm_filter_func = lhsm_softrm_filter,

    /** needed attributes for undelete in addition to POSIX and fullpath:
     * - lhsm_status: to know the original status of the 'undeleted' entry.
     * - archive_id: to know what archive the hsm_remove order must be sent to.
     * - uuid
     */
    .softrm_table_mask = {.status = SMI_MASK(0),
                          .sm_info = GENERIC_INFO_BIT(ATTR_ARCHIVE_ID) |
                          GENERIC_INFO_BIT(ATTR_UUID)
                          },
    .undelete_func = lhsm_undelete,

    /* XXX about full disaster recovery: must recreate all metadata
     * (incl. symlinks => need link field)
     * not only the entries managed by the policy.
     * This was used to be done using the contents of ENTRIES table.
     */

    .cfg_funcs = &lhsm_cfg_hdlr,
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
    else if (strcmp(action_name, "lhsm.hsm_remove") == 0
             || strcmp(action_name, "lhsm.remove") == 0)
        return lhsm_remove;
    else
        return NULL;
}
