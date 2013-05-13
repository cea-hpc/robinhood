/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * \file tmp_fs_mgr_pipeline.h
 * \brief This file describes EntryProcessor pipeline for Temporary FS Manager.
 *
 * @TODO: adapt it to lustre fid
 */


#ifndef _PIPELINE_DEF_H
#define _PIPELINE_DEF_H

#ifdef HAVE_CHANGELOGS
typedef struct changelog_record
{
#ifdef HAVE_CHANGELOG_EXTEND_REC
    struct changelog_ext_rec * p_log_rec;
#else
    struct changelog_rec * p_log_rec;
#endif
    char          *mdt;
} changelog_record_t;
#endif

typedef struct op_extra_info__
{
#ifdef HAVE_CHANGELOGS
    /** changelog record info */
    changelog_record_t log_record;

    /** is this entry from changelog ?*/
    int is_changelog_record:1;
    int getattr_needed:1;
    int getpath_needed:1;
#endif

    /* indicates what extra info is to be retrieved for this entry */
    int            getstripe_needed:1;
} op_extra_info_t;

static void inline extra_info_init( op_extra_info_t * p_extra_info )
{
}

/* pipeline stages */
#define STAGE_GET_FID         0
#define STAGE_GET_INFO_DB     1
#define STAGE_GET_INFO_FS     2
#define STAGE_REPORTING       3
#define STAGE_DB_APPLY        4
#ifdef HAVE_CHANGELOGS
#define STAGE_CHGLOG_CLR      5
#define STAGE_RM_OLD_ENTRIES  6 /* special stage at the end of FS scan */
#else
#define STAGE_RM_OLD_ENTRIES  5 /* special stage at the end of FS scan */
#endif

#define PIPELINE_STAGE_COUNT (STAGE_RM_OLD_ENTRIES+1)

/* Pipeline implemented in entry_proc_pipeline */
extern pipeline_stage_t entry_proc_pipeline[PIPELINE_STAGE_COUNT];

#endif
