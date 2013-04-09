/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2007, 2008, 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 *
 * \file    uidgidcache.h
 * \brief   Cache user and groups relative informations.
 *
 * Cache user and groups relative informations
 */
#ifndef UIDGID_CACHE_H
#define UIDGID_CACHE_H

#include "config.h"

#if HAVE_SYS_TYPES_H
#   include <sys/types.h>
#endif

#include "Memory.h"

#include <grp.h>
#include <pwd.h>

int            InitUidGid_Cache( void );

struct passwd *GetPwUid( uid_t owner );

struct group  *GetGrGid( gid_t gid );


#endif
