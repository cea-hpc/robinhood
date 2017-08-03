/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2010-2014 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
#ifndef BACKUP_H
#define BACKUP_H

/** set of managed status */
typedef enum {
    STATUS_UNKNOWN = 0, /* undetermined status */
    STATUS_NEW, /* file does not exists in the backend */
    STATUS_MODIFIED, /* file has been modified since it was stored in
                      * the backend */
    STATUS_RESTORE_RUNNING, /* file is being retrieved */
    STATUS_ARCHIVE_RUNNING, /* file is being archived */
    STATUS_SYNCHRO, /* file has been synchronized in HSM, file can be purged */
    STATUS_RELEASED,    /* file is released (nothing to do). */
    STATUS_RELEASE_PENDING, /* file is being released */

    STATUS_COUNT    /* number of possible file status */
} file_status_t;

#ifdef HAVE_SHOOK
int rbh_shook_status(const char *path, file_status_t *p_status);
int rbh_shook_recov_by_id(const entry_id_t *p_id, file_status_t *p_status);
#endif

#endif
