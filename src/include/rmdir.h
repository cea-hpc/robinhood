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
 * \file  rmdir.h
 * \brief This module removes empty directories according to the policy specified in config file.
 */
#ifndef _RMDIR_H
#define _RMDIR_H

#include "config_parsing.h"
#include <sys/types.h>

typedef struct rmdir_config_t
{
    time_t         runtime_interval;
    unsigned int   nb_threads_rmdir;
    unsigned int   rmdir_queue_size;
} rmdir_config_t;

int            SetDefault_Rmdir_Config( void *module_config, char *msg_out );
int            Read_Rmdir_Config( config_file_t config,
                                  void *module_config, char *msg_out, int for_reload );
int            Reload_Rmdir_Config( void *module_config );
int            Write_Rmdir_ConfigTemplate( FILE * output );
int            Write_Rmdir_ConfigDefault( FILE * output );

int            Start_Rmdir( rmdir_config_t * config, int flags );
int            Wait_Rmdir( void );
void           Dump_Rmdir_Stats( void );

#endif
