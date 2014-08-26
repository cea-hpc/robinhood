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

#include "lhsm.h"
#include "rbh_logs.h"

/** get Lustre status and convert it to internal Robinhood status */
int lhsm_get_status(const char *path, hsm_status_t *p_status,
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
        DisplayLog(LVL_DEBUG, "HSMFlags", "llapi_hsm_state_get(%s)=%d", path,
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
        DisplayLog(LVL_DEBUG, "HSMFlags", "Entry %s is being released", path);
    }
    else if (file_status.hus_in_progress_action == HUA_REMOVE)
    {
        DisplayLog(LVL_DEBUG, "HSMFlags", "Entry %s is being removed", path);
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
        DisplayLog(LVL_MAJOR, "HSMFlags", "Entry %s has inconsistent or"
                   " unknown HSM flags %#X",
                   path, file_status.hus_states);
        return EINVAL;
    }

    /** @TODO what if special LOST flag is set??? */

    return 0;
}

#define ARCHIVE_HINT "archive_id"

/** Trigger a HSM action */
int lhsm_action(enum hsm_user_action action, const entry_id_t *p_id,
                const char *hints)
{
    struct hsm_user_request * req;
    int data_len = 0;
    int rc;
    char *mpath;
    char *new_hints = NULL;
    unsigned int archive_id = 0;

    /* Extract archive_id from hints. Don't propagate it to the copytool. */
    if (hints)
    {
        char *ptr = NULL;
        char *in, *next, *tmp_hints;
        ptrdiff_t archid_offset  = -1;

        data_len = strlen(hints)+1;
        in = tmp_hints = strdup(hints);
        while ((next = strtok_r(in, ",", &ptr)))
        {
            if (!strncmp(next, ARCHIVE_HINT"=", strlen(ARCHIVE_HINT)+1))
            {
                archid_offset = (next - tmp_hints);
                next += strlen(ARCHIVE_HINT)+1;
                archive_id = str2int(next);
                if ((int)archive_id == -1)
                {
                    DisplayLog(LVL_MAJOR, "HSMAction", "Invalid archive_id '%s': "
                               "index expected", next);
                    free(tmp_hints);
                    return -EINVAL;
                }
                break;
            }
            in = NULL;
        }
        /* this one was modified by strtok: drop it */
        free(tmp_hints);

        if (archid_offset != -1)
        {
            char *c_w, *c_r;
            /* remove archive_id from original hints */
            new_hints = strdup(hints);
            c_w = new_hints + archid_offset;
            c_r = strchr(c_w, ',');
            if (c_r == NULL)
            {
                /* no ',' after, truncate hints in place */
                *c_w = '\0';
            }
            else
            {
                /* skip ',' */
                c_r++;
                /* copy up to end of string */
                while (*c_r)
                {
                    *c_w = *c_r;
                    c_w++;
                    c_r++;
                }
                *c_w = '\0';
            }
            data_len = strlen(new_hints)+1;
            if (data_len == 1)
            {
                /* new hints are empty */
                free(new_hints);
                new_hints = NULL;
                data_len = 0;
            }
        }
    }

    req = llapi_hsm_user_request_alloc(1, data_len);

    if (!req)
    {
        rc = -errno;
        DisplayLog(LVL_CRIT, "HSMAction", "Cannot create HSM request: %s",
                   strerror(-rc));
        return rc;
    }

    req->hur_request.hr_action = action;
    req->hur_request.hr_archive_id = archive_id;

    req->hur_user_item[0].hui_fid = *p_id;
    req->hur_user_item[0].hui_extent.offset = 0 ;
    /* XXX for now, always transfer entire file */
    req->hur_user_item[0].hui_extent.length = -1LL;

    req->hur_request.hr_itemcount = 1;

    if (new_hints != NULL)
    {
        req->hur_request.hr_data_len = data_len;
        memcpy(hur_data(req), new_hints, data_len);
        free(new_hints);
    }
    else
    {
        req->hur_request.hr_data_len = 0;
    }

    /* make tmp copy as llapi_hsm_request arg is not const */
    mpath = strdup(get_mount_point(NULL));
    rc = llapi_hsm_request(mpath, req);
    free(mpath);
    free(req);

    if (rc)
        DisplayLog(LVL_CRIT, "HSMAction", "ERROR performing HSM request(%s,"
                   " root=%s, fid="DFID"): %s", hsm_user_action2name(action),
                   mpath, PFID(p_id), strerror(-rc));
    return rc;

}
