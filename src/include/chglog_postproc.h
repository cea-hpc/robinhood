/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/* TODO CASTOR-1998 Fix copyright header as appropriate */
/*
 * Copyright (C) 2008, 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

/**
 * \file  chglog_postproc.h
 * \brief changelog post-processor definitions
 */

#ifndef _CHGLOG_POSTPROC_H
#define _CHGLOG_POSTPROC_H

#include <stdbool.h>

struct chglog_postproc;
struct rh_list_head;

#define CL_PP_NAME_MAX 128

typedef bool (*chglog_post_proc_action_func_t)(struct rh_list_head *op_queue,
                                     unsigned int *op_queue_count,
                                     void *cpp_instance_data);

/** Changelog post-processor descriptor. */
typedef struct chglog_postproc {
    const char *name;
    chglog_post_proc_action_func_t action;
    void *instance_data;
} chglog_postproc_t;

/** Changelog post-processor instance. */
typedef struct cpp_instance {
    unsigned int       cpp_index;
    char              *name;
    bool               enabled;
    chglog_postproc_t *cpp;
} cpp_instance_t;

cpp_instance_t *create_cpp_instance(const char *cpp_name);

cpp_instance_t *cpp_by_name(const char *cpp_name);

#endif
