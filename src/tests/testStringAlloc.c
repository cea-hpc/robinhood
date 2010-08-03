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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "StringAlloc.h"
#include <stdio.h>
#include <time.h>
#include <pthread.h>

#define STRCOUNT (1*1000*1000)
#define STR_SIZE 128

#define NB_THREADS 1

void          *WorkerThread( void *args )
{
    unsigned int   i, j, k;
    long           top, total;

    unsigned int   my_count = STRCOUNT / NB_THREADS;

    char         **strtab = ( char ** ) calloc( my_count, sizeof( char * ) );

    size_t         used, alloc;

    for ( i = 0; i < my_count; i++ )
        strtab[i] = NULL;

    top = time( NULL );

    for ( i = 0; i < my_count; i++ )
    {
        unsigned int   size;

        /* allocates random string */
        size = rand(  ) % STR_SIZE;

        strtab[i] = GiveMeBufferFor( size );

        for ( j = 0; j < size; j++ )
            strtab[i][j] = 'a';
        strtab[i][size] = '\0';

    }

    printf( "First allocation in %ld seconds\n", time( NULL ) - top );

    total = time( NULL );

    StringAllocMemInfo( &used, &alloc );
    printf( "==== Memory Used/Alloc: %lu/%li\n", used, alloc );

    DumpStringAllocMap(  );


    for ( k = 0; k < 1000; k++ )
    {

        printf( "---> starting pass %u\n", k );
        top = time( NULL );

        /* now free everything */
        for ( i = 0; i < my_count; i++ )
            ReleaseBuffer( strtab[i] );

        printf( "\treleasing     = %ld seconds\n", time( NULL ) - top );

        top = time( NULL );

        /* reallocate some other strings */
        for ( i = 0; i < my_count; i++ )
        {
            unsigned int   size;

            /* allocates random string */
            size = rand(  ) % STR_SIZE;

            strtab[i] = GiveMeBufferFor( size );

            for ( j = 0; j < size; j++ )
                strtab[i][j] = 'a';
            strtab[i][size] = '\0';

        }

        printf( "\tre-allocating = %ld seconds\n", time( NULL ) - top );

        StringAllocMemInfo( &used, &alloc );
        printf( "==== Memory Used/Alloc: %lu/%li\n", used, alloc );

        DumpStringAllocMap(  );

    }

    printf( "15 pass in %ld seconds\n", time( NULL ) - total );

    pthread_exit( NULL );
    return 0;
}

int main( int argc, char **argv )
{
    pthread_attr_t thattrs;
    pthread_t      mythreads[NB_THREADS];
    int            i;
    void          *ret;

    srand( time( NULL ) + getpid(  ) );

    InitStringAllocator(  );

    pthread_attr_init( &thattrs );
    pthread_attr_setschedpolicy( &thattrs, PTHREAD_SCOPE_SYSTEM );
    pthread_attr_setdetachstate( &thattrs, PTHREAD_CREATE_JOINABLE );

    for ( i = 0; i < NB_THREADS; i++ )
        pthread_create( &mythreads[i], &thattrs, WorkerThread, NULL );

    for ( i = 0; i < NB_THREADS; i++ )
        pthread_join( mythreads[i], &ret );

    printf( "ALL THREADS FINISHED !\n" );
    return 0;

}
