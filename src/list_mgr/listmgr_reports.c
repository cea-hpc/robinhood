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
#include <inttypes.h>


typedef struct lmgr_report_t
{
    lmgr_t        *p_mgr;
    result_handle_t select_result;

    /* expected result content */
    db_type_t     *result_type_array;
    unsigned int   result_count; /* report + profile */
    unsigned int   profile_count; /* profile only */
    unsigned int   ratio_count;  /* nbr of ratio field */
    unsigned int   profile_attr; /* profile attr (if profile_count > 0) */

    char         **str_tab;
} lmgr_report_t;


/* Return field string */
static inline const char *field_str( unsigned int index )
{
    if (index == (unsigned int)-1)
        return "id";
    else
        return field_infos[index].field_name;
}


#define add_string(_buffer_, _ptr_, _s_ ) do {  \
                    if ( _ptr_ != _buffer_ )    \
                    {                           \
                         strcpy( _ptr_, ", " ); \
                        _ptr_ += 2;             \
                    }                           \
                    strcpy( _ptr_, _s_ );       \
                    _ptr_ += strlen(_ptr_);     \
                } while(0)

#define field_type( _f_ )   (field_infos[_f_].db_type)

static void listmgr_fieldfilter( lmgr_report_t *p_report, lmgr_t * p_mgr,
                                    const report_field_descr_t *report_desc_array,
                                    char *attrstring, char *attrname,
                                    char *having, char **curr_having,
                                    char *where, char **curr_where , int i )
{
    /* is this field filtered ? */
    if ( report_desc_array[i].filter )
    {
        /* TODO support list filters (IN NOT and IN) */
        printdbtype( p_mgr, attrstring, p_report->result_type_array[i],
                     &report_desc_array[i].filter_value.value );

        if ( report_desc_array[i].report_type != REPORT_GROUP_BY )
        {
            /* sum, min, max, etc. are addressed by attr#n */
            if ( having != *curr_having )
                *curr_having += sprintf( *curr_having, " AND " );
            *curr_having +=
                sprintf( *curr_having, "(%s %s %s)", attrname,
                         compar2str( report_desc_array[i].filter_compar ), attrstring );
        }
        else
        {
            /* this is a real db field, can be filtered in a 'where' clause */
            if ( where != *curr_where )
                *curr_where += sprintf( *curr_where, " AND " );
            *curr_where +=
                sprintf( *curr_where, "(%s %s %s)",
                         field_str( report_desc_array[i].attr_index ),
                         compar2str( report_desc_array[i].filter_compar ), attrstring );
        }
    }
}


static void listmgr_optimizedstat( lmgr_report_t *p_report, lmgr_t * p_mgr,
                                    unsigned int report_descr_count,
                                    const report_field_descr_t *report_desc_array,
                                    const profile_field_descr_t * profile_descr,
                                    char *fields, char **curr_field,
                                    char *group_by, char **curr_group_by,
                                    char *order_by, char **curr_sort,
                                    char *having, char **curr_having,
                                    char *where, char **curr_where )
{
    char           attrstring[1024];
    int            i;
    char           attrname[128];

    /* sorting by ratio first */
    if (profile_descr && profile_descr->range_ratio_len > 0)
    {
        if ( profile_descr->attr_index == ATTR_INDEX_size )
        {
            if (profile_descr->range_ratio_sort == SORT_ASC)
                add_string( order_by, *curr_sort, "sizeratio ASC");
            else
                add_string( order_by, *curr_sort, "sizeratio DESC");
        }
    }

    for ( i = 0; i < report_descr_count; i++ )
    {
        sprintf( attrname, "attr%u", i );
        if( is_acct_pk( report_desc_array[i].attr_index ) || is_acct_field( report_desc_array[i].attr_index )
                            || report_desc_array[i].report_type == REPORT_COUNT )
        {
            switch ( report_desc_array[i].report_type )
            {
            case REPORT_MIN:
                sprintf( attrstring, "NULL as %s", attrname );
                add_string( fields, *curr_field, attrstring );
                p_report->result_type_array[i] = DB_TEXT;
                break;
            case REPORT_MAX:
                sprintf( attrstring, "NULL as %s", attrname );
                add_string( fields, *curr_field, attrstring );
                p_report->result_type_array[i] = DB_TEXT;
                break;
            case REPORT_AVG:
                sprintf( attrstring, "ROUND(SUM(%s)/SUM(" ACCT_FIELD_COUNT ")) as %s",
                         field_str( report_desc_array[i].attr_index ), attrname );
                add_string( fields, *curr_field, attrstring );
                p_report->result_type_array[i] = field_type( report_desc_array[i].attr_index );
                break;
            case REPORT_SUM:
                sprintf( attrstring, "SUM(%s) as %s",
                         field_str( report_desc_array[i].attr_index ), attrname );
                add_string( fields, *curr_field, attrstring );
                p_report->result_type_array[i] = field_type( report_desc_array[i].attr_index );
                break;
            case REPORT_COUNT:
                sprintf( attrstring, "SUM(" ACCT_FIELD_COUNT ") as %s", attrname );
                add_string( fields, *curr_field, attrstring );
                p_report->result_type_array[i] = DB_BIGUINT;
                break;
            case REPORT_COUNT_DISTINCT:
                sprintf(attrstring, "COUNT(DISTINCT(%s)) as %s",
                        field_str( report_desc_array[i].attr_index ), attrname);
                add_string( fields, *curr_field, attrstring );
                p_report->result_type_array[i] = DB_BIGUINT;
                break;
            case REPORT_GROUP_BY:
                sprintf( attrstring, "%s as %s", field_str( report_desc_array[i].attr_index ),
                         attrname );
                add_string( fields, *curr_field, attrstring );
                add_string( group_by, *curr_group_by, attrname );
                p_report->result_type_array[i] = field_type( report_desc_array[i].attr_index );
                break;
            }

            /* is this field sorted ? */

            if ( report_desc_array[i].sort_flag == SORT_ASC )
            {
                sprintf( attrstring, "%s ASC", attrname );
                add_string( order_by, *curr_sort, attrstring );
            }
            else if ( report_desc_array[i].sort_flag == SORT_DESC )
            {
                sprintf( attrstring, "%s DESC", attrname );
                add_string( order_by, *curr_sort, attrstring );
            }
        }
        else
        {
            sprintf( attrstring, "NULL as %s", attrname );
            add_string( fields, *curr_field, attrstring );
            p_report->result_type_array[i] = DB_TEXT;
        }
        listmgr_fieldfilter( p_report, p_mgr, report_desc_array, attrstring, attrname,
                            having, curr_having, where, curr_where, i );
    }
    if (profile_descr)
    {
        /* XXX only size profile in managed for now */
        if (profile_descr->attr_index == ATTR_INDEX_size)
        {
            for (i = 0; i < SZ_PROFIL_COUNT; i++)
            {
                (*curr_field) += sprintf(*curr_field, "%sSUM(%s)", (fields==(*curr_field))?"":",",
                                         sz_field[i]);
                p_report->result_type_array[i+report_descr_count] = DB_BIGUINT; /* count */
            }

            if (profile_descr->range_ratio_len > 0)
            {
                /* add ratio field and sort it */
                attrstring[0] = '\0';
                char *curr_attr = attrstring;
                for (i = 0; i < profile_descr->range_ratio_len; i++)
                {
                    if (attrstring != curr_attr)
                        curr_attr += sprintf(curr_attr, "+%s",
                                        sz_field[profile_descr->range_ratio_start + i]);
                    else
                        curr_attr += sprintf(curr_attr, "SUM(%s",
                                        sz_field[profile_descr->range_ratio_start + i]);
                }
                curr_attr += sprintf(curr_attr, ")/SUM("ACCT_FIELD_COUNT") as sizeratio");
                add_string( fields, *curr_field, attrstring );
            }
        }
    }

}


/**
 * Builds a report from database.
 */
struct lmgr_report_t *ListMgr_Report( lmgr_t * p_mgr,
                                      const report_field_descr_t * report_desc_array,
                                      unsigned int report_descr_count,
                                      const profile_field_descr_t * profile_descr,
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

    /* filters on NAMES or STRIPE_ITEMS
     * must be managed differently, as they
     * can create duplicates (non uniq id) */
    char           name_filter_str[1024] = "";
    char           stripe_filter_str[1024] = "";

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

    table_enum     query_tab;
    /* supported report fields: ENTRIES, ANNEX_INFO or ACCT */
    int            main_table_flag = FALSE;
    int            annex_table_flag = FALSE;
    int            acct_table_flag = FALSE;
    int            filter_main = 0;
    int            filter_annex = 0;
    int            filter_stripe_info = 0;
    int            filter_stripe_items = 0;
    int            filter_names = 0;
    int            filter_acct = 0;
    int            full_acct = TRUE;
    lmgr_iter_opt_t opt = {0};
    unsigned int   profile_len = 0;
    unsigned int   ratio = 0;

    /* check profile argument and increase output array if needed */
    if (profile_descr != NULL)
    {
        if (profile_descr->attr_index != ATTR_INDEX_size)
        {
            DisplayLog(LVL_CRIT, LISTMGR_TAG, "Profile on attribute #%u is not supported",
                       profile_descr->attr_index);
            return NULL;
        }
        profile_len = SZ_PROFIL_COUNT;
        if (profile_descr->range_ratio_len > 0)
            ratio = 1;
    }

    /* allocate a new report structure */
    p_report = ( lmgr_report_t * ) MemAlloc( sizeof( lmgr_report_t ) );
    if ( !p_report )
        return NULL;

    p_report->p_mgr = p_mgr;

    p_report->result_type_array =
        ( db_type_t * ) MemCalloc( report_descr_count + profile_len + ratio,
                                   sizeof( db_type_t ) );
    if ( !p_report->result_type_array )
        goto free_report;

    p_report->result_count = report_descr_count + profile_len + ratio;
    p_report->profile_count = profile_len;
    p_report->ratio_count = ratio;
    if (profile_descr != NULL)
        p_report->profile_attr = ATTR_INDEX_size;

    /* initialy, no char * tab allocated */
    p_report->str_tab = NULL;

    if (p_opt)
        opt = *p_opt;

    for ( i = 0; i < report_descr_count; i++ )
    {
        if ( ( report_desc_array[i].report_type != REPORT_COUNT ) &&
#ifdef ATTR_INDEX_dircount
                report_desc_array[i].attr_index != ATTR_INDEX_dircount &&
#endif
                !is_acct_field( report_desc_array[i].attr_index ) &&
                !is_acct_pk( report_desc_array[i].attr_index ) )
            full_acct = FALSE;
    }
    if ( p_filter )
    {
        if ( p_filter->filter_type == FILTER_SIMPLE )
        {
            for ( i = 0; i < p_filter->filter_simple.filter_count; i++ )
            {
                if ( !is_acct_pk( p_filter->filter_simple.filter_index[i] ) &&
                        !is_acct_field( p_filter->filter_simple.filter_index[i] ) )
                    full_acct = FALSE;
            }
        }
    }

    if ( full_acct && !opt.force_no_acct )
    {
        listmgr_optimizedstat( p_report, p_mgr, report_descr_count, report_desc_array,
                               profile_descr,
                               fields, &curr_field, group_by, &curr_group_by, order_by, &curr_sort,
                               having, &curr_having, where, &curr_where);
        acct_table_flag = TRUE;
    }
    else /* not only ACCT table */
    {
        /* sorting by ratio first */
        if (profile_descr && profile_descr->range_ratio_len > 0)
        {
            if ( profile_descr->attr_index == ATTR_INDEX_size )
            {
                if (profile_descr->range_ratio_sort == SORT_ASC)
                    add_string( order_by, curr_sort, "sizeratio ASC");
                else
                    add_string( order_by, curr_sort, "sizeratio DESC");
            }
        }

        for ( i = 0; i < report_descr_count; i++ )
        {
            /* no field for count or distinct count */
            if ( report_desc_array[i].report_type != REPORT_COUNT &&
                 report_desc_array[i].report_type != REPORT_COUNT_DISTINCT )
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
                p_report->result_type_array[i] = DB_BIGUINT;
                break;
            case REPORT_COUNT_DISTINCT:
                sprintf( attrstring, "COUNT(DISTINCT(%s)) as %s",
                field_str( report_desc_array[i].attr_index ), attrname );
                add_string( fields, curr_field, attrstring );
                p_report->result_type_array[i] = DB_BIGUINT;
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
            listmgr_fieldfilter( p_report, p_mgr, report_desc_array, attrstring, attrname,
                                having, &curr_having, where, &curr_where, i );
        }

        /* generate size profile */
        if (profile_descr != NULL)
        {
            if (profile_descr->attr_index == ATTR_INDEX_size)
            {
                add_string( fields, curr_field, "SUM(size=0)" );
                for (i=1; i < SZ_PROFIL_COUNT-1; i++)
                    curr_field += sprintf(curr_field, ",SUM("ACCT_SZ_VAL("size")"=%u)", i-1);
                curr_field += sprintf(curr_field, ",SUM("ACCT_SZ_VAL("size")">=%u)", i-1);

                for (i=0; i<SZ_PROFIL_COUNT; i++)
                    p_report->result_type_array[i+report_descr_count] = DB_BIGUINT;

                if (profile_descr->range_ratio_len > 0)
                {
                    /* add ratio field and sort it */
                    attrstring[0] = '\0';
                    char *curr_attr = attrstring;

                    curr_attr += sprintf(curr_attr, "SUM(size>=%Lu",
                                         SZ_MIN_BY_INDEX(profile_descr->range_ratio_start));

                    /* is the last range = 1T->inf ? */
                    if (profile_descr->range_ratio_start + profile_descr->range_ratio_len >= SZ_PROFIL_COUNT)
                        curr_attr += sprintf(curr_attr, ")");
                    else
                        curr_attr += sprintf(curr_attr, " and size<%Lu)",
                                         SZ_MIN_BY_INDEX(profile_descr->range_ratio_start+profile_descr->range_ratio_len));

                    curr_attr += sprintf(curr_attr, "/COUNT(*) as sizeratio");
                    add_string( fields, curr_field, attrstring );
                }
            }
        }

    }
    /* filter */

    if ( p_filter )
    {
        if ( ( full_acct && !opt.force_no_acct ) )
        {
            filter_acct = filter2str( p_mgr, curr_where, p_filter, T_ACCT,
                                      ( where != curr_where ), TRUE );
            curr_where += strlen( curr_where );
            if ( filter_acct )
                acct_table_flag =TRUE;
        }
        else
        {
            /* filter on main table? */
            filter_main = filter2str( p_mgr, curr_where, p_filter, T_MAIN,
                                      ( where != curr_where ), TRUE );
            curr_where += strlen( curr_where );

            if ( filter_main )
                main_table_flag = TRUE;

            /* filter on annex table? */
            if ( annex_table )
            {
                filter_annex = filter2str( p_mgr, curr_where, p_filter, T_ANNEX,
                                           (where != curr_where), TRUE );
                curr_where += strlen( curr_where );

                if ( filter_annex )
                    annex_table_flag = TRUE;
            }

            filter_stripe_info =
                filter2str( p_mgr, curr_where, p_filter, T_STRIPE_INFO,
                            (where != curr_where), TRUE );
            curr_where += strlen( curr_where );

           /*  filter on names table is particular as this may duplicate
             * entries when computing the report (multiple hardlinks) */
            filter_names = filter2str(p_mgr, name_filter_str, p_filter, T_DNAMES,
                                      FALSE, FALSE );

           /*  filter on stripe items table is particular as this may duplicate
             * entries when computing the report (multiple stripes) */
            filter_stripe_items = filter2str(p_mgr, stripe_filter_str, p_filter,
                                             T_STRIPE_ITEMS, FALSE, FALSE);
        }
    }

    /* FROM clause */

    if ( acct_table_flag )
    {
        strcpy( from, ACCT_TABLE );
        query_tab = T_ACCT;
    }
    else
    {
        const char * first_table = NULL;
        char * curr_from = from;
        if ( main_table_flag ) {
            strcpy(from, MAIN_TABLE);
            curr_from = from + strlen(from);
            first_table = MAIN_TABLE;
            query_tab = T_MAIN;
        }

        if ( annex_table_flag )
        {
            if (first_table)
                curr_from += sprintf(curr_from, " LEFT JOIN "ANNEX_TABLE" ON %s.id="ANNEX_TABLE".id",
                                     first_table);
            else
            {
                strcpy(from, ANNEX_TABLE);
                curr_from = from + strlen(from);
                first_table = ANNEX_TABLE;
                query_tab = T_ANNEX;
            }
        }
        if ( filter_stripe_info )
        {
            if (first_table)
                curr_from += sprintf(curr_from, " INNER JOIN "STRIPE_INFO_TABLE" ON %s.id="STRIPE_INFO_TABLE".id",
                                     first_table);
            else
            {
                strcpy(from, STRIPE_INFO_TABLE);
                curr_from = from + strlen(from);
                first_table = STRIPE_INFO_TABLE;
                query_tab = T_STRIPE_INFO;
            }
        }
        if (filter_names)
        {
            if (first_table)
                curr_from += sprintf(curr_from," INNER JOIN (SELECT DISTINCT(id)"
                                     " FROM "DNAMES_TABLE" WHERE %s) N"
                                     " ON %s.id=N.id", name_filter_str,
                                     first_table);
            else
            {
                DisplayLog(LVL_CRIT, LISTMGR_TAG, "Unexpected case: "DNAMES_TABLE
                           " table can't be the query table in %s()", __func__);
                goto free_field_tab;
            }
        }

        if (filter_stripe_items)
        {
            if (first_table)
                curr_from += sprintf(curr_from, " INNER JOIN (SELECT DISTINCT(id)"
                                     " FROM "STRIPE_ITEMS_TABLE" WHERE %s) SI"
                                     " ON %s.id=SI.id", stripe_filter_str,
                                     first_table);
            else
            {
                strcpy(from, STRIPE_ITEMS_TABLE);
                curr_from = from + strlen(from);
                strcpy(curr_where, stripe_filter_str);
                curr_where += strlen(curr_where);
                first_table = STRIPE_ITEMS_TABLE;
                query_tab = T_STRIPE_ITEMS;
                /* XXX the caller is supposed to select DISTINCT(id) in this case */
            }
        }
    }

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
    if ( opt.list_count_max > 0 )
    {
        curr_query += sprintf( curr_query, " LIMIT %u", opt.list_count_max );
    }


#ifdef _DEBUG_DB
    printf( "Report is specified by: %s\n", query );
#endif

retry:
    /* execute request (expect that ACCT table does not exists) */
    if (acct_table_flag)
        rc = db_exec_sql_quiet( &p_mgr->conn, query, &p_report->select_result );
    else
        rc = db_exec_sql( &p_mgr->conn, query, &p_report->select_result );

    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;

    /* if the ACCT table does exist, switch to standard mode */
    if ( acct_table_flag && (rc == DB_NOT_EXISTS))
    {
        lmgr_iter_opt_t new_opt;
        if (p_opt != NULL)
            new_opt = *p_opt;
        else
            new_opt.list_count_max = 0;

        new_opt.force_no_acct = TRUE;

        DisplayLog( LVL_EVENT, LISTMGR_TAG, "No accounting info: switching to standard query mode" );

        return ListMgr_Report( p_mgr, report_desc_array, report_descr_count,
                               profile_descr,
                               p_filter, &new_opt );
    }

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
 * @param p_profile OUT: output profile, if required.
 */
int ListMgr_GetNextReportItem( struct lmgr_report_t *p_iter, db_value_t * p_value,
                               unsigned int *p_value_count, profile_u *p_profile )
{
    int            rc;
    unsigned int   i;

    if ( *p_value_count < p_iter->result_count - p_iter->profile_count - p_iter->ratio_count )
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
    for ( i = 0; i < p_iter->result_count - p_iter->profile_count - p_iter->ratio_count; i++ )
    {
        if ( p_iter->str_tab[i] != NULL )
        {
            p_value[i].type = p_iter->result_type_array[i];
            if ( parsedbtype( p_iter->str_tab[i], p_iter->result_type_array[i],
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

    /* fill profile structure */
    if (p_profile && (p_iter->profile_count > 0))
    {
        if (p_iter->profile_attr == ATTR_INDEX_size)
        {
            db_type_u dbval;
            for (i=0; i < p_iter->profile_count; i++)
            {
                unsigned int idx = p_iter->result_count - p_iter->profile_count
                                   - p_iter->ratio_count + i ;
                if (p_iter->str_tab[idx] == NULL)
                {
                    p_profile->size.file_count[i] = 0;
                }
                else if (parsedbtype(p_iter->str_tab[idx], p_iter->result_type_array[idx],
                         &dbval) == 1)
                {
                    p_profile->size.file_count[i] = dbval.val_biguint;
                }
                else
                {
                    DisplayLog( LVL_CRIT, LISTMGR_TAG,
                                "Could not parse result field #%u: value='%s'",
                                idx, p_iter->str_tab[idx] );
                    return DB_INVALID_ARG;
                }
            }
        }
    }

    *p_value_count = p_iter->result_count - p_iter->profile_count - p_iter->ratio_count;

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

int ListMgr_EntryCount(lmgr_t * p_mgr, uint64_t *count)
{
    int            rc;
    result_handle_t result;
    char          *str_count = NULL;

    /* execute the request */
retry:
    rc = db_exec_sql( &p_mgr->conn, "SELECT COUNT(*) FROM " MAIN_TABLE, &result );
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    rc = db_next_record( &p_mgr->conn, &result, &str_count, 1 );
    if (rc)
        return rc;

    if ( sscanf( str_count, "%"SCNu64, count ) != 1 )
        rc = DB_REQUEST_FAILED;

    db_result_free( &p_mgr->conn, &result );
    return rc;
}

