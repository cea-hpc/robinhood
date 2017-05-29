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

/**
 * \file    chglog_reader.h
 * \author  Th. Leibovici
 * \brief   Interface for Lustre MDT Changelog processing.
 */

/**
 * \addtogroup CHANGE_LOGS
 * @{
 */
#ifndef _CHGLOG_READER_H
#define _CHGLOG_READER_H

#include "config_parsing.h"
#include "rbh_const.h"
#include "list_mgr.h"
#include <stdbool.h>

#define MDT_NAME_MAX  32
#define READER_ID_MAX 16

typedef struct mdt_def_t {
    char mdt_name[MDT_NAME_MAX];
    char reader_id[READER_ID_MAX];
} mdt_def_t;

/** Configuration for ChangeLog reader Module */
typedef struct chglog_reader_config_t {
    /** List of MDTs (used for opening ChangeLogs) */
    mdt_def_t *mdt_def;
    unsigned int mdt_count;

    /* nbr of changelog records to be agregated for llapi_changelog_clear() */
    int batch_ack_count;

    bool force_polling;
    time_t polling_interval;

    /* Maximum number of operations to keep in the internal queue. */
    int queue_max_size;

    /* Age of the opration we keep in the internal queue before we
     * push them to thepipeline. */
    time_t queue_max_age;

    /* Interval at which we have to check whether operation in the
     * internal queue have aged. */
    time_t queue_check_interval;

    /* Max delay to update last committed changelog record */
    time_t commit_update_max_delay;

    /* Max delta of record id to update last committed changelog record */
    int64_t commit_update_max_delta;

    /* Options suported by the MDS. LU-543 and LU-1331 are related to
     * events in changelog, where a rename is overriding a destination
     * file. */
    bool mds_has_lu543;
    bool mds_has_lu1331;

} chglog_reader_config_t;

/** start ChangeLog Readers
 * \param mdt_index -1 for all
 */
int cl_reader_start(run_flags_t flags, int mdt_index);

/** terminate ChangeLog Readers */
int cl_reader_terminate(void);

/** wait for ChangeLog Readers termination */
int cl_reader_wait(void);

/** Release last changelog records, and dump the final stats. */
int cl_reader_done(void);

/** dump changelog processing stats */
int cl_reader_dump_stats(void);

/** store changelog stats to db.
 * Must be called after cl_reader_dump_stats() as
 * cl_reader_store_stats() updates last_report time
 */
void cl_reader_store_stats(lmgr_t *lmgr);

/** config handlers */
extern mod_cfg_funcs_t cl_reader_cfg_hdlr;

#endif

/** @} */
