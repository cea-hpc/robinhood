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
#include "database.h"

#define ASSIGN_UNION( _u, _type, _address ) do {            \
                    switch( _type )                         \
                    {                                       \
                      case DB_ID:                           \
                        _u.val_id = *((entry_id_t*)(_address)); \
                        break;                              \
                      case DB_ENUM_FTYPE:                   \
                      case DB_TEXT:                         \
                        _u.val_str = (char*)(_address);     \
                        break;                              \
                      case DB_INT:                          \
                        _u.val_int = *((int*)(_address));   \
                        break;                              \
                      case DB_UINT:                         \
                        _u.val_uint = *((unsigned int*)(_address));   \
                        break;                              \
                      case DB_SHORT:                        \
                        _u.val_short = *((short*)(_address));   \
                        break;                              \
                      case DB_USHORT:                         \
                        _u.val_ushort = *((unsigned short*)(_address));  \
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
                      case DB_ID:                           \
                        *((entry_id_t*)(_address)) = _u.val_id; \
                        break;                              \
                      case DB_ENUM_FTYPE:                   \
                      case DB_TEXT:                         \
                        strcpy( (char*)(_address), _u.val_str ); \
                        break;                              \
                      case DB_INT:                          \
                        *((int*)(_address)) =  _u.val_int ;   \
                        break;                              \
                      case DB_UINT:                         \
                        *((unsigned int*)(_address)) = _u.val_uint;   \
                        break;                              \
                      case DB_SHORT:                          \
                        *((short*)(_address)) =  _u.val_short ;   \
                        break;                              \
                      case DB_USHORT:                       \
                        *((unsigned short*)(_address)) = _u.val_ushort;   \
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


#define DIFF_UNION( _diff, _type, _address1, _address2 ) do { \
                    db_type_u _u1, _u2;                     \
                    switch( _type )                         \
                    {                                       \
                      case DB_ID:                           \
                        _u1.val_id = *((entry_id_t*)(_address1)); \
                        _u2.val_id = *((entry_id_t*)(_address2)); \
                        _diff = !entry_id_equal( &_u1.val_id, &_u2.val_id); \
                        break;                              \
                      case DB_ENUM_FTYPE:                   \
                      case DB_TEXT:                         \
                        _u1.val_str = (char*)(_address1);   \
                        _u2.val_str = (char*)(_address2);   \
                        _diff = strcmp(_u1.val_str, _u2.val_str); \
                        break;                              \
                      case DB_INT:                          \
                        _u1.val_int = *((int*)(_address1));   \
                        _u2.val_int = *((int*)(_address2));   \
                        _diff = (_u1.val_int != _u2.val_int); \
                        break;                              \
                      case DB_UINT:                         \
                        _u1.val_uint = *((unsigned int*)(_address1)); \
                        _u2.val_uint = *((unsigned int*)(_address2)); \
                        _diff = (_u1.val_uint != _u2.val_uint); \
                        break;                              \
                      case DB_SHORT:                          \
                        _u1.val_short = *((short*)(_address1));   \
                        _u2.val_short = *((short*)(_address2));   \
                        _diff = (_u1.val_short != _u2.val_short); \
                        break;                               \
                      case DB_USHORT:                        \
                        _u1.val_ushort = *((unsigned short*)(_address1)); \
                        _u2.val_ushort = *((unsigned short*)(_address2)); \
                        _diff = (_u1.val_ushort != _u2.val_ushort); \
                        break;                              \
                      case DB_BIGINT:                       \
                        _u1.val_bigint = *((long long*)(_address1));   \
                        _u2.val_bigint = *((long long*)(_address2));   \
                        _diff = (_u1.val_bigint != _u2.val_bigint); \
                        break;                              \
                      case DB_BIGUINT:                      \
                        _u1.val_biguint = *((unsigned long long*)(_address1));  \
                        _u2.val_biguint = *((unsigned long long*)(_address2));  \
                        _diff = (_u1.val_biguint != _u2.val_biguint); \
                        break;                              \
                      case DB_BOOL:                         \
                        _u1.val_bool = *((int*)(_address1));   \
                        _u2.val_bool = *((int*)(_address2));   \
                        _diff = (_u1.val_bool != _u2.val_bool); \
                        break;                              \
                      default:                              \
                        DisplayLog( LVL_CRIT, LISTMGR_TAG, "Unexpected type in ASSIGN_UNION: %d !!!", _type);\
                    }\
                    } while(0)

/* precomputed masks for testing attr sets efficiently */
extern int     main_attr_set;
extern int     names_attr_set;
extern int     annex_attr_set;
extern int     gen_attr_set;
extern int     stripe_attr_set;
extern int     dir_attr_set;
extern int     slink_attr_set;
extern int     acct_attr_set;
extern int     acct_pk_attr_set;

/* extern int     readonly_attr_set; => moved to listmgr.h */

void           init_attrset_masks( const lmgr_config_t *lmgr_config );

#define main_fields( _attr_mask )      ( (_attr_mask) & main_attr_set )
#define names_fields( _attr_mask )      ( (_attr_mask) & names_attr_set )
#define annex_fields( _attr_mask )     ( (_attr_mask) & annex_attr_set )
#define gen_fields( _attr_mask )       ( (_attr_mask) & gen_attr_set )
#define stripe_fields( _attr_mask )    ( (_attr_mask) & stripe_attr_set )
#define readonly_fields( _attr_mask )  ( (_attr_mask) & readonly_attr_set )
#define dirattr_fields( _attr_mask )   ( (_attr_mask) & dir_attr_set )
#define slinkattr_fields( _attr_mask )   ( (_attr_mask) & slink_attr_set )

/* these 2 functions can only be used after init_attrset_masks() has been called */
#define is_acct_field( _attr_index ) \
                ( (1 << _attr_index) & acct_attr_set )

#define is_acct_pk( _attr_index ) \
                ( (1 << _attr_index) & acct_pk_attr_set )
/* ------------ */

#define is_read_only_field( _attr_index ) \
                ( (field_infos[_attr_index].flags & GENERATED) || \
                  (field_infos[_attr_index].flags & DIR_ATTR) || \
                  (field_infos[_attr_index].flags & FUNC_ATTR) )

#define is_stripe_field( _attr_index ) \
                ( ( field_infos[_attr_index].db_type == DB_STRIPE_INFO ) || \
                  ( field_infos[_attr_index].db_type == DB_STRIPE_ITEMS ) )

#define is_main_field( _attr_index ) \
                ( (!annex_table || ( field_infos[_attr_index].flags & FREQ_ACCESS )) \
                  && !is_stripe_field( _attr_index ) \
                  && !(field_infos[_attr_index].flags & GENERATED) \
                  && !(field_infos[_attr_index].flags & DIR_ATTR) \
                  && !(field_infos[_attr_index].flags & FUNC_ATTR) \
                  && !(field_infos[_attr_index].flags & DNAMES) )

#define is_gen_field( _attr_index ) \
                ( field_infos[_attr_index].flags & GENERATED )

#define is_indexed_field( _attr_index ) \
                ( field_infos[_attr_index].flags & INDEXED )

#define is_annex_field( _attr_index ) \
                ( annex_table && ( field_infos[_attr_index].flags & ( ANNEX_INFO | INIT_ONLY ) ) \
                  && !is_stripe_field( _attr_index ) \
                  && !(field_infos[_attr_index].flags & GENERATED) \
                  && !(field_infos[_attr_index].flags & DIR_ATTR) \
                  && !(field_infos[_attr_index].flags & FUNC_ATTR) )

#define is_names_field( _attr_index ) \
                ( field_infos[_attr_index].flags & DNAMES )

#define is_funcattr( _attr_index )  ( field_infos[_attr_index].flags & FUNC_ATTR )
#define is_dirattr( _attr_index )  ( field_infos[_attr_index].flags & DIR_ATTR )
#define is_slinkattr( _attr_index )  ( field_infos[_attr_index].flags & SLINK_ATTR )

#ifdef _HSM_LITE
#define is_recov_field( _attr_index ) \
                ( (1 << _attr_index) & RECOV_ATTR_MASK )
#endif
#ifdef HAVE_RM_POLICY
#define is_softrm_field( _attr_index ) \
                ( (1 << _attr_index) & SOFTRM_MASK )
#endif

int            printdbtype( lmgr_t * p_mgr, char *str, db_type_t type, const db_type_u * value_ptr );

int            parsedbtype( char *instr, db_type_t type, db_type_u * value_out );

typedef enum
{
    T_NONE = 0,                                  /* not set */
    T_MAIN,                                      /* fields in main table */
    T_DNAMES,                                    /* files in dir names table */
    T_ANNEX,                                     /* fiels in annex table */
    T_STRIPE_INFO,                               /* field in stripe info table */
    T_STRIPE_ITEMS,                              /* field in stripe items table */
    T_ACCT,                                      /* fields in accounting table */
    T_SOFTRM,                                    /* fields in softrm table (backup and HSM flavors only) */
    T_RECOV                                      /* fields in recov table (HSM flavors only) */
} table_enum;

static inline const char * table2name(table_enum table)
{
    switch(table)
    {
        case T_NONE: return NULL;
        case T_MAIN: return MAIN_TABLE;
        case T_DNAMES: return DNAMES_TABLE;
        case T_ANNEX: return ANNEX_TABLE;
        case T_STRIPE_INFO: return STRIPE_INFO_TABLE;
        case T_STRIPE_ITEMS: return STRIPE_ITEMS_TABLE;
        case T_ACCT: return ACCT_TABLE;
        case T_SOFTRM: return SOFT_RM_TABLE;
        case T_RECOV: return RECOV_TABLE;
   }
   return NULL;
}

typedef enum {
    ADD,
    SUBSTRACT
} operation_type;

void           add_source_fields_for_gen( int * attr_mask );
void           generate_fields( attr_set_t * p_set );

int            attrmask2fieldlist( char *str, int attr_mask, table_enum table, int leading_comma,
                                   int for_update, char *prefix, char *postfix );

int            attrmask2fieldcomparison( char *str, int attr_mask, table_enum table, const char *left_prefix,
                                   const char *right_prefix, const char *comparator, const char *separator );

int            attrmask2fieldoperation( char *str, int attr_mask, table_enum table, const char *prefix,
                                   operation_type operation );

int            attrset2valuelist( lmgr_t * p_mgr, char *str,
                                  const attr_set_t * p_set, table_enum table,
                                  int leading_coma );
int            attrset2updatelist(lmgr_t * p_mgr, char *str,
                                  const attr_set_t * p_set, table_enum table,
                                  int leading_coma, int generic_value);
int            mk_result_bind_list( const attr_set_t * p_set, table_enum table,
                                    db_type_t * type_list, void **buff_list, size_t * size_list );

char          *compar2str( filter_comparator_t compar );
int            filter2str( lmgr_t * p_mgr, char *str, const lmgr_filter_t * p_filter,
                           table_enum table, int leading_and, int prefix_table );

int            func_filter(lmgr_t * p_mgr, char* filter_str, const lmgr_filter_t * p_filter,
                           table_enum table, int leading_and, int prefix_table);

typedef enum
{
    FILTERDIR_NONE = 0,    /* no dir filter */
    FILTERDIR_EMPTY,       /* empty dir filter */
    FILTERDIR_OTHER,       /* other condition on directory attribute */
} filter_dir_e;

filter_dir_e dir_filter(lmgr_t * p_mgr, char* filter_str, const lmgr_filter_t * p_filter,
                        unsigned int * dir_attr_index, const char *prefix);

unsigned int  append_size_range_fields(char * str, int leading_comma, char *prefix);


int            result2attrset( table_enum table, char **result_tab,
                               unsigned int res_count, attr_set_t * p_set );

/* return the attr string for a dirattr */
const char * dirattr2str(unsigned int attr_index);

void entry_id2pk(const entry_id_t * p_id, PK_PARG_T p_pk);
int pk2entry_id( lmgr_t * p_mgr, PK_ARG_T pk, entry_id_t * p_id );

/* those functions are used for begin/commit/rollback */
int            lmgr_begin( lmgr_t * p_mgr );
void           lmgr_rollback( lmgr_t * p_mgr );
int            lmgr_commit( lmgr_t * p_mgr );

/* to be called before closing a connection */
int            lmgr_flush_commit( lmgr_t * p_mgr );

/** manage delayed retry of retryable errors
 * \return != 0 if the transaction must be restarted
 */
#define lmgr_delayed_retry(_l, _e) _lmgr_delayed_retry(_l, _e, __func__, __LINE__)
int _lmgr_delayed_retry(lmgr_t *lmgr, int errcode, const char *func, int line);

/* get/set variable in DB */
int lmgr_get_var(db_conn_t *pconn, const char *varname, char *value);
int lmgr_set_var(db_conn_t *pconn, const char *varname, const char *value);

int fullpath_attr2db(const char *attr, char *db);
void fullpath_db2attr(const char *db, char *attr);

static inline int sum_masks(attr_set_t **p_attrs, unsigned int count, int t_mask)
{
    int m = 0;
    unsigned int i;

    for (i = 0; i < count; i++)
    {
        m |= (p_attrs[i]->attr_mask & t_mask);
    }
    return m;
}

#endif
