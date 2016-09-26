/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009 CEA/DAM
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

#define SHOOK_TAG   "Shook"

#include "rbh_misc.h"
#include "rbh_logs.h"
#include <shook_svr.h>

static file_status_t shook2rbh_status(shook_state st)
{
    switch (st) {
    case SS_ONLINE:
        return STATUS_SYNCHRO;  /* may also be dirty or archiving... */
    case SS_RELEASED:
        return STATUS_RELEASED;
    case SS_RELEASE_PEND:
        return STATUS_RELEASE_PENDING;
    case SS_RESTORE_PEND:
        return STATUS_RESTORE_RUNNING;
    case SS_LOST:
        return STATUS_UNKNOWN;
        /* handle restripe opreation as a restore */
    case SS_RESTRIPE_PEND:
        return STATUS_RESTORE_RUNNING;
    default:
        return (file_status_t)-1;
    }
}

/**
 * Get status of entry regarding 'shook' system
 * and convert it to robinhood status.
 * @return 0 on success, <0 on error.
 */
int ShookGetStatus(const char *path, file_status_t *p_status)
{
    shook_state st;
    int rc;

    if (shook_get_status(path, &st, FALSE) != 0) {
        rc = -errno;
        DisplayLog(LVL_CRIT, SHOOK_TAG, "ERROR getting state of %s: %s",
                   path, strerror(-rc));
        return rc;
    }

    if (st != SS_ONLINE)
        DisplayLog(LVL_FULL, SHOOK_TAG,
                   "shook indicates '%s' status is '%s'",
                   path, shook_attr_val[st]);

    *p_status = shook2rbh_status(st);
    if (*p_status == (file_status_t)-1) {
        DisplayLog(LVL_CRIT, SHOOK_TAG,
                   "ERROR getting state of %s: unknown status %d", path,
                   (int)st);
        return -EINVAL;
    }
    return 0;
}

int ShookRecoverById(const entry_id_t *p_id, file_status_t *p_status)
{
    int rc;
    shook_state st;

    rc = shook_recov_pending(get_fsname(), p_id, &st, 0);
    if (rc < 0)
        return rc;

    *p_status = shook2rbh_status(st);
    if (*p_status == (file_status_t)-1) {
        DisplayLog(LVL_CRIT, SHOOK_TAG,
                   "ERROR getting recovering " DFID ": unknown status %d",
                   PFID(p_id), (int)st);
        return -EINVAL;
    }
    return 0;
}
