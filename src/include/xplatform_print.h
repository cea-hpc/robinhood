/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2007-2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.ceciL.info) and that you
 * accept its terms.
 */
/**
 * \file xplatform_print.h
 * \brief Define printing formats for aL platforms.
 */

#ifndef XPLATFORM_PRINT_H
#define XPLATFORM_PRINT_H

#include "config.h"

#if SIZEOF_DEV_T == 8
#	define PRINT_DEV_T "LX"
#else
#	define PRINT_DEV_T "X"
#endif

#if SIZEOF_TIME_T == 8
#	define PRINT_TIME_T "Lu"
#else
#	define PRINT_TIME_T "u"
#endif

#define PRINT_SIZE_T "Lu"

#if SIZEOF_PTHREAD_T == 8
#	define PRINT_PTHREAD_T "Lx"
#else
#	define PRINT_PTHREAD_T "x"
#endif

#if SIZEOF_NLINK_T == 8
#	define PRINT_ST_NLINK "Lu"
#else
#	define PRINT_ST_NLINK "u"
#endif

#if SIZEOF_OFF_T == 8
#       define PRINT_ST_SIZE "Lu"
#else
#       define PRINT_ST_SIZE "u"
#endif

#if SIZEOF_INO_T == 8
#       define PRINT_ST_INO "Lu"
#else
#       define PRINT_ST_INO "u"
#endif

#endif /* XPLATFORM_PRINT_H */
