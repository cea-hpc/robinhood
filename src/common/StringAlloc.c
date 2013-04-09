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
 *  Memory allocator specilized for strings <= maxpathlen
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "rbh_const.h"
#include <sys/param.h>
#include <unistd.h>
#include <pthread.h>
#include <stddef.h>
#include <string.h>

# include <stdio.h>

#ifdef _DEBUG_STRINGALLOC
# include <time.h>
#endif

#include "Memory.h"
#include "StringAlloc.h"

#ifndef _NO_STRINGALLOC

typedef struct string_desc__
{
    int            buffer_len;
    struct string_desc__ *p_next;
    char           string[0];

} string_desc_t;


/* array of free strings sorted by length
 * free_strings_tab[0] contains string with 0 to 7 chars
 *  + terminal character => 1 to 8 buffers.
 * free_strings_tab[1] contains string with 8 to 15 chars
 *  + terminal character => 9 to 16 buffers.
 * etc.
 */
#define STR_LEN_2_TAB_INDEX( _str_len )    ( _str_len/8 )
#define BUFF_LEN_2_TAB_INDEX( _buff_len )  ( (_buff_len - 1)/8 )
#define TAB_INDEX_2_BUFF_LEN( _index )     ( (_index+1)*8 )


#define INDEX_MAX  (RBH_PATH_MAX/8)

static string_desc_t *free_strings_tab[INDEX_MAX];

static pthread_mutex_t tab_lock = PTHREAD_MUTEX_INITIALIZER;



#define REALLOC_SIZE  (16*1024*1024)    /* 16Mo */

static char   *incr_buffer = NULL;
static unsigned incr_reste = 0;

static size_t  total_memory_alloc = 0;
static size_t  total_memory_used = 0;


static pthread_mutex_t incr_lock = PTHREAD_MUTEX_INITIALIZER;

static char   *IncrementalAlloc( unsigned int *size )
{
    char          *retour;
    unsigned int   align_size;

    /* align on 64 bits */
    align_size = ( *size + 7 ) & ~7;

    pthread_mutex_lock( &incr_lock );

    if ( ( align_size > incr_reste ) || ( incr_buffer == NULL ) )
    {
#ifdef _DEBUG_STRINGALLOC
        if ( incr_reste > 0 )
            printf( "Leaving a %u bytes buffer\n", incr_reste );

        printf( "Allocating a new memory page of %u bytes\n", REALLOC_SIZE );
#endif

        incr_buffer = ( caddr_t ) MemAlloc( REALLOC_SIZE );

        incr_reste = REALLOC_SIZE;

        total_memory_alloc += REALLOC_SIZE;
    }

    retour = incr_buffer;

    incr_buffer += align_size;
    incr_reste -= align_size;

    pthread_mutex_unlock( &incr_lock );

    *size = align_size;

    return retour;

}


void InitStringAllocator( void )
{
    unsigned int   u;

    for ( u = 0; u < INDEX_MAX; u++ )
    {
        free_strings_tab[u] = NULL;
    }
}

/* this limits the ability for a small block
 * to take a bigger block */
static inline int max_extend_limit_index( int index )
{
    return ( index + 2 );       /* i.e. 16 bytes */
}


char          *GiveMeBufferFor( unsigned int strlength )
{
    string_desc_t *desc = NULL;
    int            wanted = STR_LEN_2_TAB_INDEX( ( int ) strlength );
    int            index;
    unsigned int   buffer_size;

    if ( wanted < 0 )
        wanted = 0;
    if ( wanted >= INDEX_MAX )
        return NULL;

    pthread_mutex_lock( &tab_lock );

    /* we try taking the first fit */


    /* WARNING ! if we don't want to converge to RBH_PATH_MAX,
     * we should not allocate a too large buffer for a small one
     * thus, we stop at twice the wanted index + 16.
     */


    for ( index = wanted;
          ( index < INDEX_MAX )
          && ( index < max_extend_limit_index( wanted ) )
          && ( free_strings_tab[index] == NULL ); index++ ) /* do nothing */ ;

    /* we found a large enough string */

    if ( ( index < INDEX_MAX ) && ( index < max_extend_limit_index( wanted ) ) )
    {
        desc = free_strings_tab[index];
        free_strings_tab[index] = desc->p_next;

        total_memory_used += sizeof( string_desc_t ) + desc->buffer_len;

        pthread_mutex_unlock( &tab_lock );

        return desc->string;
    }

    /* nothing to do with the table anymore */
    pthread_mutex_unlock( &tab_lock );

    /* we didn't find, we create a slot */

    buffer_size = sizeof( string_desc_t ) + TAB_INDEX_2_BUFF_LEN( wanted );

    desc = ( string_desc_t * ) IncrementalAlloc( &buffer_size );

    /* effective buffer_len is aligned for 64 bits */
    desc->buffer_len = buffer_size - sizeof( string_desc_t );
    desc->p_next = NULL;

    pthread_mutex_lock( &tab_lock );
    total_memory_used += buffer_size;
    pthread_mutex_unlock( &tab_lock );

    return desc->string;
}


char          *ReallocBuffer( char *string, unsigned int strlength )
{
    string_desc_t *desc;
    char          *new;

    /* if string is NULL, return a new buffer */
    if ( string == NULL )
        return GiveMeBufferFor( strlength );

    desc = ( string_desc_t * ) ( string - sizeof( string_desc_t ) );

    /* if buffer is large enough, return the same */
    if ( desc->buffer_len > strlength )
        return string;

    /* else, alloc a new buffer, copy the old content, and release the old buffer */
    new = GiveMeBufferFor( strlength );
    memcpy( new, string, desc->buffer_len );

    ReleaseBuffer( string );
    return new;

}

void ReleaseBuffer( char *string )
{
    int            index;
    string_desc_t *desc;

    /* sanity check */
    if ( string == NULL )
        return;

    desc = ( string_desc_t * ) ( string - sizeof( string_desc_t ) );

    index = BUFF_LEN_2_TAB_INDEX( desc->buffer_len );

    if ( index < 0 )
        index = 0;
    if ( index >= INDEX_MAX )
        index = INDEX_MAX - 1;

    pthread_mutex_lock( &tab_lock );

    total_memory_used -= sizeof( string_desc_t ) + desc->buffer_len;

    desc->p_next = free_strings_tab[index];
    free_strings_tab[index] = desc;

    pthread_mutex_unlock( &tab_lock );

    return;
}

void StringAllocMemInfo( size_t * p_used, size_t * p_alloc )
{
    *p_used = total_memory_used;
    *p_alloc = total_memory_alloc;
}


/* for debug */
void DumpStringAllocMap( void )
{
    int            i;

    unsigned int   total_count = 0;
    size_t         total_size = 0;


    pthread_mutex_lock( &tab_lock );

    for ( i = 0; i < INDEX_MAX; i++ )
    {
        unsigned int   count = 0;
        size_t         size = 0;
        string_desc_t *desc;

        if ( free_strings_tab[i] )
        {
            for ( desc = free_strings_tab[i]; desc != NULL; desc = desc->p_next )
            {
                count++;
                size += sizeof( string_desc_t ) + desc->buffer_len;
            }

            printf( "strings[%d]: count=%u, size=%llu (avg=%d)\n", i, count,
                    ( unsigned long long ) size, ( int ) ( size / count ) );
        }

        total_count += count;
        total_size += size;
    }

    printf( "Total: count=%u, size=%llu (avg=%d)\n", total_count, ( unsigned long long ) total_size,
            ( int ) ( total_count ? total_size / total_count : 0 ) );

    pthread_mutex_unlock( &tab_lock );
}

#else

void InitStringAllocator( void )
{
    /* do nothing */
    return;
}

char          *GiveMeBufferFor( unsigned int strlength )
{
    return ( char * ) MemAlloc( strlength + 1 );
}

char          *ReallocBuffer( char *string, unsigned int strlength )
{
    return ( char * ) MemRealloc( string, strlength + 1 );
}

void ReleaseBuffer( char *string )
{
    MemFree( string );
    return;
}

#endif
