/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
#ifndef _LIST_MGR_COMMON_H
#define _LIST_MGR_COMMON_H

#include "list_mgr.h"
#include "listmgr_internal.h"

#define ASSIGN_UNION( _u, _type, _address ) do {            \
                    switch( _type )                         \
                    {                                       \
                      case DB_TEXT:                         \
                        _u.val_str = (char*)(_address);     \
                        break;                              \
                      case DB_INT:                          \
                        _u.val_int = *((int*)(_address));   \
                        break;                              \
                      case DB_UINT:                         \
                        _u.val_uint = *((unsigned int*)(_address));   \
                        break;                              \
                      case DB_BIGINT:                       \
                        _u.val_bigint = *((long long*)(_address));   \
                        break;                              \
                      case DB_BIGUINT:                      \
                        _u.val_biguint = *((unsigned long long*)(_address));  \
                        break;                              \
                      case DB_BOOL:                         \
                        _u.val_bool = *((int*)(_address));   \
                        break;                              \
                      default:                              \
                        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Unexpected type in ASSIGN_UNION: %d !!!", _type);\
                    }\
                    } while(0)

#define UNION_GET_VALUE( _u, _type, _address ) do {            \
                    switch( _type )                         \
                    {                                       \
                      case DB_TEXT:                         \
                        strcpy( (char*)(_address), _u.val_str ); \
                        break;                              \
                      case DB_INT:                          \
                        *((int*)(_address)) =  _u.val_int ;   \
                        break;                              \
                      case DB_UINT:                         \
                        *((unsigned int*)(_address)) = _u.val_uint;   \
                        break;                              \
                      case DB_BIGINT:                       \
                        *((long long*)(_address)) = _u.val_bigint ;   \
                        break;                              \
                      case DB_BIGUINT:                      \
                        *((unsigned long long*)(_address)) = _u.val_biguint;  \
                        break;                              \
                      case DB_BOOL:                         \
                        *((int*)(_address)) = _u.val_bool ;   \
                        break;                              \
                      default:                              \
                        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Unexpected type in UNION_GET_VALUE: %d !!!", _type);\
                    }\
                    } while(0)

/* precomputed masks for testing attr sets efficiently */
extern int     main_attr_set;
extern int     annex_attr_set;
extern int     gen_attr_set;
extern int     stripe_attr_set;
/* extern int     readonly_attr_set; => moved to listmgr.h */

void           init_attrset_masks(  );

#define main_fields( _attr_mask )      ( (_attr_mask) & main_attr_set )
#define annex_fields( _attr_mask )     ( (_attr_mask) & annex_attr_set )
#define gen_fields( _attr_mask )       ( (_attr_mask) & gen_attr_set )
#define stripe_fields( _attr_mask )    ( (_attr_mask) & stripe_attr_set )
#define readonly_fields( _attr_mask )  ( (_attr_mask) & readonly_attr_set )

#define is_read_only_field( _attr_index ) \
                ( field_infos[_attr_index].flags & GENERATED )

#define is_stripe_field( _attr_index ) \
                ( ( field_infos[_attr_index].db_type == DB_STRIPE_INFO ) || ( field_infos[_attr_index].db_type == DB_STRIPE_ITEMS ) )

#define is_main_field( _attr_index ) \
                ( (!annex_table || ( field_infos[_attr_index].flags & FREQ_ACCESS )) \
                  && !is_stripe_field( _attr_index ) \
                  && !(field_infos[_attr_index].flags & GENERATED) )

#define is_gen_field( _attr_index ) \
                ( field_infos[_attr_index].flags & GENERATED )

#define is_annex_field( _attr_index ) \
                ( annex_table && ( field_infos[_attr_index].flags & ( ANNEX_INFO | INIT_ONLY ) ) \
                  && !is_stripe_field( _attr_index ) \
                  && !(field_infos[_attr_index].flags & GENERATED) )

#ifdef _HSM_LITE
#define is_recov_field( _attr_index ) \
                ( (1 << _attr_index) & RECOV_ATTR_MASK )
#endif
#ifdef HAVE_RM_POLICY
#define is_softrm_field( _attr_index ) \
                ( (1 << _attr_index) & SOFTRM_MASK )
#endif

int            printdbtype( lmgr_t * p_mgr, char *str, db_type_t type, db_type_u * value_ptr );

int            parsedbtype( char *instr, db_type_t type, db_type_u * value_out );

typedef enum
{
    T_NONE = 0,                                  /* not set */
    T_MAIN,                                      /* fields in main table */
    T_ANNEX,                                     /* fiels in annex table */
    T_STRIPE_INFO,                               /* field in stripe info table */
    T_STRIPE_ITEMS,                              /* field in stripe items table */
#ifdef HAVE_RM_POLICY
    T_SOFTRM,                                    /* fields in softrm table */
#endif
#ifdef _HSM_LITE
    T_RECOV,                                     /* fields in recov table */
#endif
} table_enum;

void           add_source_fields_for_gen( int * attr_mask );
void           generate_fields( attr_set_t * p_set );

int            attrmask2fieldlist( char *str, int attr_mask, table_enum table, int leading_coma,
                                   int for_update );
int            attrset2valuelist( lmgr_t * p_mgr, char *str,
                                  const attr_set_t * p_set, table_enum table,
                                  int leading_coma, int prep_stmt );
int            attrset2updatelist( lmgr_t * p_mgr, char *str,
                                   const attr_set_t * p_set, table_enum table,
                                   int leading_coma );

int            prep_stmt_bind_attrs( prep_stmt_t prep, const attr_set_t * p_set, table_enum table,
                                     unsigned int start_index );

int            mk_result_bind_list( const attr_set_t * p_set, table_enum table,
                                    db_type_t * type_list, void **buff_list, size_t * size_list );

int            unset_null_results( attr_set_t * p_set, table_enum table, prep_stmt_t stmt,
                                   unsigned int shift );

char          *compar2str( filter_comparator_t compar );
int            filter2str( lmgr_t * p_mgr, char *str, const lmgr_filter_t * p_filter,
                           table_enum table, int leading_and, int prefix_table );

int            result2attrset( table_enum table, char **result_tab,
                               unsigned int res_count, attr_set_t * p_set );

int entry_id2pk( lmgr_t * p_mgr, const entry_id_t * p_id, int add_if_not_exists,
                 PK_PARG_T p_pk );
int pk2entry_id( lmgr_t * p_mgr, PK_ARG_T pk, entry_id_t * p_id );

/* those functions are used for begin/commit/rollback */
int            lmgr_begin( lmgr_t * p_mgr );
void           lmgr_rollback( lmgr_t * p_mgr );
int            lmgr_commit( lmgr_t * p_mgr );

/* to be called before closing a connection */
int            lmgr_flush_commit( lmgr_t * p_mgr );

#endif
