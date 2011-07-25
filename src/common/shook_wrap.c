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

#include "RobinhoodMisc.h"
#include "RobinhoodLogs.h"
#include <shook_svr.h>

/**
 * Get status of entry regarding 'shook' system
 * and convert it to robinhood status.
 * @return 0 on success, <0 on error.
 */
int ShookGetStatus(const char * path, file_status_t * p_status)
{
    shook_state st;
    int rc;

    if (shook_get_status( path, &st, FALSE ) != 0)
    {
        rc = -errno;
        DisplayLog( LVL_CRIT, SHOOK_TAG, "ERROR getting state of %s: %s",
                    path, strerror(-rc) );
        return rc;
    }

    if (st != SS_ONLINE)
        DisplayLog( LVL_FULL, SHOOK_TAG,
                    "shook indicates '%s' status is '%s'",
                     path, shook_attr_val[st] );

    switch (st)
    {
        case SS_ONLINE:
            *p_status = STATUS_SYNCHRO; /* may also be dirty or archiving... */
            return 0;
        case SS_RELEASED:
            *p_status = STATUS_RELEASED;
            return 0;
        case SS_RELEASE_PEND:
            *p_status = STATUS_RELEASE_PENDING;
            return 0;
        case SS_RESTORE_PEND:
            *p_status = STATUS_RESTORE_RUNNING;
            return 0;
        case SS_LOST:
            *p_status = STATUS_UNKNOWN;
            return 0;
        default:
            DisplayLog( LVL_CRIT, SHOOK_TAG, "ERROR getting state of %s: unknown status %d",
                        path, (int)st );
            return -EINVAL;
    }
}
