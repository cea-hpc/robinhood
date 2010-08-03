/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "RobinhoodConfig.h"
#include "RobinhoodLogs.h"
#include <stdio.h>


#define ID_HASH_SIZE 7919

static inline unsigned int hash_id( entry_id_t * p_id, unsigned int modulo )
{
    unsigned int   val = 1;
    char          *buffer;
    unsigned int   index;
#ifdef FID_PK

    buffer = ( char * ) &( p_id->f_seq );

    for ( index = 0; index < sizeof( p_id->f_seq ); index++ )
        val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

    buffer = ( char * ) &( p_id->f_oid );

    for ( index = 0; index < sizeof( p_id->f_oid ); index++ )
        val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

#else
    buffer = ( char * ) &( p_id->device );

    for ( index = 0; index < sizeof( p_id->device ); index++ )
        val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

    buffer = ( char * ) &( p_id->inode );

    for ( index = 0; index < sizeof( p_id->inode ); index++ )
        val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );
#endif
    return val % modulo;
}

static inline unsigned int hash_operation( db_op_type_t op, db_tables_t table, int attr_mask )
{
    unsigned int   val = 1;
    char          *buffer;
    unsigned int   index;

    buffer = ( char * ) &op;

    for ( index = 0; index < sizeof( op ); index++ )
        val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

    buffer = ( char * ) &table;

    for ( index = 0; index < sizeof( table ); index++ )
        val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

    buffer = ( char * ) &attr_mask;

    for ( index = 0; index < sizeof( attr_mask ); index++ )
        val = ( val << 5 ) - val + ( unsigned int ) ( buffer[index] );

    return val % PREP_STMT_HASH_SIZE;
}

int main( int argc, char **argv )
{
    int            i, j;
    entry_id_t     id;
    int            mask;

    printf( "=== HASH of ENTRY Ids ===\n" );
    for ( i = 0; i < 5; i++ )
        for ( j = 0; j < 1000; j++ )
        {
#ifdef FID_PK
            id.f_seq = i;
            id.f_oid = j;
#else
            id.device = i;
            id.inode = j;
#endif
            printf( "hash(%u,%u)=%u\n", i, j, hash_id( &id, ID_HASH_SIZE ) );
        }

    printf( "=== HASH of DB operations ===\n" );

    for ( i = ( int ) OP_INSERT; i < ( int ) OP_SELECT; i++ )
        for ( j = ( int ) TAB_MAIN; j < ( int ) TAB_IDMAP; j++ )
            for ( mask = 1; mask < ( 1 << 4 ); mask <<= 1 )
            {
                printf( "hash_op(%u,%u,%X)=%u\n", i, j, mask,
                        hash_operation( ( db_op_type_t ) i, ( db_tables_t ) j, mask ) );
            }
    return 0;
}
