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

#include "list_mgr.h"
#include "listmgr_common.h"
#include "listmgr_internal.h"
#include "database.h"
#include "Memory.h"
#include "RobinhoodLogs.h"
#include <stdio.h>
#include <stdlib.h>


typedef struct lmgr_report_t
{
    lmgr_t        *p_mgr;
    result_handle_t select_result;

    /* expected result content */
    db_type_t     *result_type_array;
    unsigned int   result_count;

    char         **str_tab;
} lmgr_report_t;


/* Return field string */
static inline const char *field_str( unsigned int index )
{
    return field_infos[index].field_name;
}

/**
 * Builds a report from database.
 */
struct lmgr_report_t *ListMgr_Report( lmgr_t * p_mgr, report_field_descr_t * report_desc_array,
                                      unsigned int report_descr_count,
                                      const lmgr_filter_t * p_filter,
                                      const lmgr_iter_opt_t * p_opt )
{
    unsigned int   i;

    /* A query consists of 5 parts:
     *  - List of fields to be selected
     *  - FROM clause with joins
     *  - WHERE clause (filters)
     *  - GROUP BY clause 
     *  - ORBER BY clause
     */
    char           fields[1024] = "";
    char           from[512] = "";
    char           where[2048] = "";
    char           having[2048] = "";
    char           group_by[512] = "";
    char           order_by[512] = "";
    char           query[4096] = "";

    char          *curr_field = fields;
    char          *curr_group_by = group_by;
    char          *curr_sort = order_by;
    char          *curr_query = query;
    char          *curr_where = where;
    char          *curr_having = having;

    char           attrname[128];
    char           attrstring[1024];

    lmgr_report_t *p_report;
    int            rc;

#define add_string(_buffer_, _ptr_, _s_ ) do { \
                    if ( _ptr_ != _buffer_ )   \
                    {                          \
                        _ptr_[0] = ',';        \
                        _ptr_[1] = ' ';        \
                        _ptr_+=2;              \
                    }                          \
                    strcpy( _ptr_, _s_ );      \
                    _ptr_ += strlen(_ptr_);    \
                } while(0)

#define field_type( _f_ )   (field_infos[_f_].db_type)

    int            main_table_flag = FALSE;
    int            annex_table_flag = FALSE;
    int            filter_main = 0;
    int            filter_annex = 0;

    /* allocate a new report structure */
    p_report = ( lmgr_report_t * ) MemAlloc( sizeof( lmgr_report_t ) );
    if ( !p_report )
        return NULL;

    p_report->p_mgr = p_mgr;

    p_report->result_type_array =
        ( db_type_t * ) MemCalloc( report_descr_count, sizeof( db_type_t ) );
    if ( !p_report->result_type_array )
        goto free_report;

    p_report->result_count = report_descr_count;

    /* initialy, no char * tab allocated */
    p_report->str_tab = NULL;

    /* expected result content */

    for ( i = 0; i < report_descr_count; i++ )
    {
        if ( report_desc_array[i].report_type != REPORT_COUNT ) /* no field for count */
        {
            /* in what table is this field ? */
            if ( is_main_field( report_desc_array[i].attr_index ) )
                main_table_flag = TRUE;
            else if ( is_annex_field( report_desc_array[i].attr_index ) )
                annex_table_flag = TRUE;
            else
            {
                /* Not supported yet */
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Error: report on attribute #%u is not supported (report item #%u).",
                            report_desc_array[i].attr_index, i );
                goto free_field_tab;
            }
        }

        sprintf( attrname, "attr%u", i );

        /* what kind of stat on this field ? */
        switch ( report_desc_array[i].report_type )
        {
        case REPORT_MIN:
            sprintf( attrstring, "MIN( %s ) as %s",
                     field_str( report_desc_array[i].attr_index ), attrname );
            add_string( fields, curr_field, attrstring );
            p_report->result_type_array[i] = field_type( report_desc_array[i].attr_index );
            break;
        case REPORT_MAX:
            sprintf( attrstring, "MAX( %s ) as %s",
                     field_str( report_desc_array[i].attr_index ), attrname );
            add_string( fields, curr_field, attrstring );
            p_report->result_type_array[i] = field_type( report_desc_array[i].attr_index );
            break;
        case REPORT_AVG:
            sprintf( attrstring, "ROUND(AVG( %s )) as %s",
                     field_str( report_desc_array[i].attr_index ), attrname );
            add_string( fields, curr_field, attrstring );
            p_report->result_type_array[i] = field_type( report_desc_array[i].attr_index );
            break;
        case REPORT_SUM:
            sprintf( attrstring, "SUM( %s ) as %s",
                     field_str( report_desc_array[i].attr_index ), attrname );
            add_string( fields, curr_field, attrstring );
            p_report->result_type_array[i] = field_type( report_desc_array[i].attr_index );
            break;
        case REPORT_COUNT:
            sprintf( attrstring, "COUNT(*) as %s", attrname );
            add_string( fields, curr_field, attrstring );
            p_report->result_type_array[i] = DB_UINT;
            break;
        case REPORT_COUNT_DISTINCT:
            sprintf( attrstring, "COUNT(DISTINCT(%s)) as %s",
                     field_str( report_desc_array[i].attr_index ), attrname );
            add_string( fields, curr_field, attrstring );
            p_report->result_type_array[i] = DB_UINT;
            break;
        case REPORT_GROUP_BY:
            sprintf( attrstring, "%s as %s", field_str( report_desc_array[i].attr_index ),
                     attrname );
            add_string( fields, curr_field, attrstring );
            add_string( group_by, curr_group_by, attrname );
            p_report->result_type_array[i] = field_type( report_desc_array[i].attr_index );
            break;
        }

        /* is this field sorted ? */

        if ( report_desc_array[i].sort_flag == SORT_ASC )
        {
            sprintf( attrstring, "%s ASC", attrname );
            add_string( order_by, curr_sort, attrstring );
        }
        else if ( report_desc_array[i].sort_flag == SORT_DESC )
        {
            sprintf( attrstring, "%s DESC", attrname );
            add_string( order_by, curr_sort, attrstring );
        }

        /* is this field filtered ? */

        if ( report_desc_array[i].filter )
        {
            printdbtype( p_mgr, attrstring, p_report->result_type_array[i],
                         &report_desc_array[i].filter_value );

            if ( report_desc_array[i].report_type != REPORT_GROUP_BY )
            {
                if ( having != curr_having )
                    curr_having += sprintf( curr_having, " AND " );
                curr_having +=
                    sprintf( curr_having, "(%s %s %s)", attrname,
                             compar2str( report_desc_array[i].filter_compar ), attrstring );
            }
            else
            {
                if ( where != curr_where )
                    curr_where += sprintf( curr_where, " AND " );
                curr_where +=
                    sprintf( curr_where, "(%s %s %s)",
                             field_str( report_desc_array[i].attr_index ),
                             compar2str( report_desc_array[i].filter_compar ), attrstring );
            }
        }

    }

    /* filter */

    if ( p_filter )
    {
        filter_main = filter2str( p_mgr, curr_where, p_filter, T_MAIN,
                                  ( where != curr_where ), TRUE );
        curr_where += strlen( curr_where );

        if ( filter_main )
            main_table_flag = TRUE;

        if ( annex_table )
        {
            filter_annex = filter2str( p_mgr, curr_where, p_filter, T_ANNEX,
                                       ( filter_main > 0 ), TRUE );
            curr_where += strlen( curr_where );

            if ( filter_annex )
                annex_table_flag = TRUE;
        }

        /* XXX ignore filters on stripe for now */
    }


    /* from clause */

    if ( main_table_flag && annex_table_flag )
        strcpy( from,
                MAIN_TABLE " LEFT JOIN " ANNEX_TABLE " ON " MAIN_TABLE ".id = " ANNEX_TABLE ".id" );
    else if ( main_table_flag )
        strcpy( from, MAIN_TABLE );
    else if ( annex_table_flag )
        strcpy( from, ANNEX_TABLE );


    /* Build the request */

    curr_query += sprintf( query, "SELECT %s FROM %s", fields, from );

    if ( where[0] )
    {
        curr_query += sprintf( curr_query, " WHERE %s", where );
    }

    if ( group_by[0] )
    {
        curr_query += sprintf( curr_query, " GROUP BY %s", group_by );
    }

    if ( having[0] )
    {
        curr_query += sprintf( curr_query, " HAVING %s", having );
    }


    if ( order_by[0] )
    {
        curr_query += sprintf( curr_query, " ORDER BY %s", order_by );
    }

    /* iterator opt */
    if ( p_opt && ( p_opt->list_count_max > 0 ) )
    {
        curr_query += sprintf( curr_query, " LIMIT %u", p_opt->list_count_max );
    }


#ifdef _DEBUG_DB
    printf( "Report is specified by: %s\n", query );
#endif

    /* execute request */
    rc = db_exec_sql( &p_mgr->conn, query, &p_report->select_result );
    if ( rc )
        goto free_field_tab;
    else
        return p_report;

/* error handlers */
  free_field_tab:
    MemFree( p_report->result_type_array );
  free_report:
    MemFree( p_report );
    return NULL;

}                               /* ListMgr_Report */


/**
 * Get next report entry.
 * @param p_value_count is IN/OUT parameter. IN: size of output array. OUT: nbr of fields set in array.
 */
int ListMgr_GetNextReportItem( struct lmgr_report_t *p_iter, db_value_t * p_value,
                               unsigned int *p_value_count )
{
    int            rc;
    unsigned int   i;

    if ( *p_value_count < p_iter->result_count )
        return DB_BUFFER_TOO_SMALL;

    if ( p_iter->str_tab == NULL )
    {
        p_iter->str_tab = ( char ** ) MemCalloc( p_iter->result_count, sizeof( char * ) );
        if ( !p_iter->str_tab )
            return DB_NO_MEMORY;
    }

    rc = db_next_record( &p_iter->p_mgr->conn, &p_iter->select_result, p_iter->str_tab,
                         p_iter->result_count );

    if ( rc )
        return rc;

    /* parse result values */
    for ( i = 0; i < p_iter->result_count; i++ )
    {
        if ( p_iter->str_tab[i] != NULL )
        {
            p_value[i].type = p_iter->result_type_array[i];
            if ( parsedbtype
                 ( p_iter->str_tab[i], p_iter->result_type_array[i],
                   &( p_value[i].value_u ) ) != 1 )
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Could not parse result field #%u: value='%s'", i, p_iter->str_tab[i] );
                return DB_INVALID_ARG;
            }
        }
        else
        {
            p_value[i].type = DB_TEXT;
            p_value[i].value_u.val_str = NULL;
        }
    }

    *p_value_count = p_iter->result_count;

    return DB_SUCCESS;
}


void ListMgr_CloseReport( struct lmgr_report_t *p_iter )
{
    db_result_free( &p_iter->p_mgr->conn, &p_iter->select_result );

    if ( p_iter->str_tab != NULL )
        MemFree( p_iter->str_tab );

    MemFree( p_iter->result_type_array );
    MemFree( p_iter );
}
