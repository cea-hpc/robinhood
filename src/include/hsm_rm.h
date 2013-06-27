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
 * \file  hsm_rm.h
 * \brief This module applies deferred remove operations to HSM.
 */
#ifndef _HSM_RM_H
#define _HSM_RM_H

#include "config_parsing.h"
#include <sys/types.h>

typedef struct hsm_rm_config_t
{
    time_t         runtime_interval;
    unsigned int   nb_threads_rm;
    unsigned int   rm_queue_size;
    unsigned int   max_rm;
} hsm_rm_config_t;

int            SetDefault_HSMRm_Config( void *module_config, char *msg_out );
int            Read_HSMRm_Config( config_file_t config,
                                  void *module_config, char *msg_out, int for_reload );
int            Reload_HSMRm_Config( void *module_config );
int            Write_HSMRm_ConfigTemplate( FILE * output );
int            Write_HSMRm_ConfigDefault( FILE * output );

int            Start_HSMRm( hsm_rm_config_t * config, int flags );
int            Wait_HSMRm( void );
void           Dump_HSMRm_Stats( void );

#endif
