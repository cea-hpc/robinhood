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
 * \file StringAlloc.h
 * \brief Memory allocator sepcilized for strings <= maxpathlen.
 */
#ifndef __STRING_ALLOC_H
#define __STRING_ALLOC_H

#include <sys/types.h>

void           InitStringAllocator( void );

char          *GiveMeBufferFor( unsigned int strlength );

char          *ReallocBuffer( char *string, unsigned int strlength );

void           ReleaseBuffer( char *string );

#ifndef _BUDDY_MALLOC
void           StringAllocMemInfo( size_t * p_used, size_t * p_alloc );

/* for debug */
void           DumpStringAllocMap( void );
#endif

#endif
