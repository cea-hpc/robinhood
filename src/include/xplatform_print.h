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
#include <inttypes.h>

#if SIZEOF_DEV_T == 8
#	define PRI_DT PRIX64
#else
#	define PRI_DT PRIX32
#endif

#define PRI_TT "lu"

#define PRI_SZ "zu"

#if SIZEOF_PTHREAD_T == 8
#	define PRI_PTH "Lx"
#else
#	define PRI_PTH "x"
#endif

#if SIZEOF_NLINK_T == 8
#	define PRI_STNL "Lu"
#else
#	define PRI_STNL "u"
#endif

#define PRI_STSZ "zu"


#if SIZEOF_INO_T == 8
#if __WORDSIZE == 64
#       define PRI_STI "lu"
#else
#       define PRI_STI "Lu"
#endif
#else
#       define PRI_STI "u"
#endif

#endif /* XPLATFORM_PRINT_H */
