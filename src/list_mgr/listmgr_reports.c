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
#include "rbh_logs.h"
#include "rbh_misc.h"
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

struct result {
    db_type_e type;
    int       flags;
};

typedef struct lmgr_report_t
{
    lmgr_t        *p_mgr;
    result_handle_t select_result;

    /* expected result content */
    struct result *result;
    unsigned int   result_count; /* report + profile */
    unsigned int   profile_count; /* profile only */
    unsigned int   ratio_count;  /* nbr of ratio field */
    unsigned int   profile_attr; /* profile attr (if profile_count > 0) */

    char         **str_tab;
} lmgr_report_t;


/* Return field string */
static inline const char *field_str( unsigned int index )
{
    /* count(id) for special COUNT attribute */
    if (index == ATTR_INDEX_FLG_COUNT)
        return "id";
    else
        return field_name(index);
}

/* Return field flag */
static inline int field_flag(unsigned int index)
{
    if (index == ATTR_INDEX_FLG_COUNT)
        return 0;
    else if (index < ATTR_COUNT)
        return field_infos[index].flags;
    else /* status, sm_info */
        return 0;
}

static inline void append_filter_cond(GString *str, lmgr_t *p_mgr, const char *attrname,
                                      const report_field_descr_t *desc,
                                      const struct result *res)
{
    if (!GSTRING_EMPTY(str))
        g_string_append(str, " AND ");

    g_string_append_printf(str, "(%s %s ", attrname,
                           compar2str(desc->filter_compar));

    /** TODO support list filters (IN NOT and IN) */
    printdbtype(&p_mgr->conn, str, res->type, &desc->filter_value.value);

    g_string_append(str, ")");
}

/** append a filter on a given report field */
static void listmgr_fieldfilter(const lmgr_report_t *p_report, lmgr_t *p_mgr,
                                const report_field_descr_t *report_desc_array,
                                const char *attrname,
                                GString *having, GString *where, int i)
{
    /* is this field filtered ? */
    if (report_desc_array[i].filter)
    {
        if (report_desc_array[i].report_type != REPORT_GROUP_BY)
        {
            /* sum, min, max, etc. are addressed by attr#n */
            append_filter_cond(having, p_mgr, attrname, &report_desc_array[i],
                               &p_report->result[i]);
        }
        else
        {
            /* this is a real db field, can be filtered in a 'where' clause */
            /* sum, min, max, etc. are addressed by attr#n */
            append_filter_cond(where, p_mgr, field_str(report_desc_array[i].attr_index),
                               &report_desc_array[i], &p_report->result[i]);
        }
    }
}

static inline void coma_if_needed(GString *str)
{
    if (!GSTRING_EMPTY(str))
        g_string_append(str, ",");
}

static inline void append_sort_order(GString *order_by, const char *name, sort_order_t sort)
{
    if (sort == SORT_ASC)
    {
        coma_if_needed(order_by);
        g_string_append_printf(order_by, "%s ASC", name);
    }
    else if (sort == SORT_DESC)
    {
        coma_if_needed(order_by);
        g_string_append_printf(order_by, "%s DESC", name);
    }
}

static void listmgr_optimizedstat(lmgr_report_t *p_report, lmgr_t *p_mgr,
                                  unsigned int report_descr_count,
                                  const report_field_descr_t *report_desc_array,
                                  const profile_field_descr_t *profile_descr,
                                  GString *fields, GString *group_by, GString *order_by,
                                  GString *having, GString *where)
{
    int            i;

    /* sorting by ratio first */
    if (profile_descr && profile_descr->range_ratio_len > 0)
    {
        if (profile_descr->attr_index == ATTR_INDEX_size)
            append_sort_order(order_by, "sizeratio", profile_descr->range_ratio_sort);
    }

    for (i = 0; i < report_descr_count; i++)
    {
        char  attrname[128];

        snprintf(attrname, sizeof(attrname), "attr%u", i);

        if ((report_desc_array[i].report_type == REPORT_COUNT)
            || is_acct_pk(report_desc_array[i].attr_index)
            || is_acct_field(report_desc_array[i].attr_index))
        {
            switch (report_desc_array[i].report_type)
            {
            case REPORT_MIN:
            case REPORT_MAX:
                coma_if_needed(fields);
                g_string_append_printf(fields, "NULL as %s", attrname);
                p_report->result[i].type = DB_TEXT;
                break;

            case REPORT_AVG:
                coma_if_needed(fields);
                g_string_append_printf(fields, "ROUND(SUM(%s)/SUM("ACCT_FIELD_COUNT")) as %s",
                         field_str(report_desc_array[i].attr_index), attrname);
                p_report->result[i].type = field_type(report_desc_array[i].attr_index);
                break;

            case REPORT_SUM:
                coma_if_needed(fields);
                g_string_append_printf(fields, "SUM(%s) as %s",
                                       field_str(report_desc_array[i].attr_index), attrname);
                p_report->result[i].type = field_type(report_desc_array[i].attr_index);
                break;

            case REPORT_COUNT:
                coma_if_needed(fields);
                g_string_append_printf(fields, "SUM("ACCT_FIELD_COUNT") as %s", attrname);
                p_report->result[i].type = DB_BIGUINT;
                break;

            case REPORT_COUNT_DISTINCT:
                coma_if_needed(fields);
                g_string_append_printf(fields, "COUNT(DISTINCT(%s)) as %s",
                            field_str(report_desc_array[i].attr_index), attrname);
                p_report->result[i].type = DB_BIGUINT;
                break;

            case REPORT_GROUP_BY:
                coma_if_needed(fields);
                g_string_append_printf(fields, "%s as %s",
                                       field_str(report_desc_array[i].attr_index), attrname);
                coma_if_needed(group_by);
                g_string_append(group_by, attrname);
                p_report->result[i].type = field_type(report_desc_array[i].attr_index);
                break;
            }

            /* is this field sorted ? */
            append_sort_order(order_by, attrname, report_desc_array[i].sort_flag);
        }
        else
        {
            coma_if_needed(fields);
            g_string_append_printf(fields, "NULL as %s", attrname);
            p_report->result[i].type = DB_TEXT;
        }

        listmgr_fieldfilter(p_report, p_mgr, report_desc_array, attrname,
                            having, where, i);

        p_report->result[i].flags = field_flag(report_desc_array[i].attr_index);
    }

    if (profile_descr)
    {
        /* XXX only size profile in managed for now */
        if (profile_descr->attr_index == ATTR_INDEX_size)
        {
            for (i = 0; i < SZ_PROFIL_COUNT; i++)
            {
                coma_if_needed(fields);
                g_string_append_printf(fields, "SUM(%s)", sz_field[i]);
                p_report->result[i+report_descr_count].type = DB_BIGUINT; /* count */
            }

            if (profile_descr->range_ratio_len > 0)
            {
                /* add ratio field and sort it */
                coma_if_needed(fields);
                /* case i == 0 */
                g_string_append_printf(fields, "SUM(%s",
                                       sz_field[profile_descr->range_ratio_start]);
                for (i = 1; i < profile_descr->range_ratio_len; i++)
                {
                    g_string_append_printf(fields, "+%s",
                                    sz_field[profile_descr->range_ratio_start + i]);
                }
                g_string_append(fields, ")/SUM("ACCT_FIELD_COUNT") as sizeratio");
            }
        }
    }
}

/** check if all fields and filters of a requested report and are in ACCT_TABLE */
static bool full_acct(const report_field_descr_t *report_desc_array,
                      unsigned int report_descr_count,
                      const lmgr_filter_t *p_filter)
{
    int i;

    for (i = 0; i < report_descr_count; i++)
    {
        if ((report_desc_array[i].report_type != REPORT_COUNT) &&
             report_desc_array[i].attr_index != ATTR_INDEX_dircount &&
             !is_acct_field(report_desc_array[i].attr_index) &&
             !is_acct_pk(report_desc_array[i].attr_index))
            return false;
    }

    if (!no_filter(p_filter))
    {
        if (p_filter->filter_type == FILTER_SIMPLE)
        {
            for (i = 0; i < p_filter->filter_simple.filter_count; i++)
            {
                if (!is_acct_pk(p_filter->filter_simple.filter_index[i]) &&
                        !is_acct_field(p_filter->filter_simple.filter_index[i]))
                    return false;
            }
        }
    }
    return true;
}


/**
 * Builds a report from database.
 */
struct lmgr_report_t *ListMgr_Report(lmgr_t *p_mgr,
                                     const report_field_descr_t *report_desc_array,
                                     unsigned int report_descr_count,
                                     const profile_field_descr_t *profile_descr,
                                     const lmgr_filter_t *p_filter,
                                     const lmgr_iter_opt_t *p_opt)
{
    unsigned int   i;
    char           attrname[128];
    lmgr_report_t *p_report;
    int            rc;
    table_enum     query_tab;
    /* supported report fields: ENTRIES, ANNEX_INFO or ACCT */
    bool           use_acct_table = false;
    lmgr_iter_opt_t opt = {0};
    unsigned int   profile_len = 0;
    unsigned int   ratio = 0;
    struct field_count fcnt = {0};
    GString           *req = NULL;
    GString           *fields = NULL;
    GString           *where = NULL;
    GString           *having = NULL;
    GString           *group_by = NULL;
    GString           *order_by = NULL;
    GString           *filter_name = NULL;


    /* check profile argument and increase output array if needed */
    if (profile_descr != NULL)
    {
        if (profile_descr->attr_index != ATTR_INDEX_size)
        {
            DisplayLog(LVL_CRIT, LISTMGR_TAG, "Profile on attribute '%s' (index=%u) is not supported",
                       field_name(profile_descr->attr_index), profile_descr->attr_index);
            return NULL;
        }
        profile_len = SZ_PROFIL_COUNT;
        if (profile_descr->range_ratio_len > 0)
            ratio = 1;
    }

    /* allocate a new report structure */
    p_report = (lmgr_report_t *) MemAlloc(sizeof(lmgr_report_t));
    if (!p_report)
        return NULL;

    p_report->p_mgr = p_mgr;

    p_report->result = (struct result *)MemCalloc(report_descr_count
                                 + profile_len + ratio, sizeof(struct result));
    if (!p_report->result)
        goto free_report;

    p_report->result_count = report_descr_count + profile_len + ratio;
    p_report->profile_count = profile_len;
    p_report->ratio_count = ratio;
    if (profile_descr != NULL)
        p_report->profile_attr = ATTR_INDEX_size;

    /* initially, no char * tab allocated */
    p_report->str_tab = NULL;

    if (p_opt)
        opt = *p_opt;

    fields = g_string_new(NULL);
    group_by = g_string_new(NULL);
    order_by = g_string_new(NULL);
    having = g_string_new(NULL);
    where = g_string_new(NULL);

    if (full_acct(report_desc_array, report_descr_count, p_filter)
        && !opt.force_no_acct)
    {
        listmgr_optimizedstat(p_report, p_mgr, report_descr_count, report_desc_array,
                              profile_descr, fields, group_by, order_by, having, where);
        use_acct_table = true;
    }
    else /* not only ACCT table */
    {
        /* sorting by ratio first */
        if (profile_descr && profile_descr->range_ratio_len > 0)
        {
            if (profile_descr->attr_index == ATTR_INDEX_size)
            {
                coma_if_needed(order_by);
                if (profile_descr->range_ratio_sort == SORT_ASC)
                    g_string_append(order_by, "sizeratio ASC");
                else
                    g_string_append(order_by, "sizeratio DESC");
            }
        }

        for (i = 0; i < report_descr_count; i++)
        {
            /* no field for count or distinct count */
            if (report_desc_array[i].report_type != REPORT_COUNT &&
                report_desc_array[i].report_type != REPORT_COUNT_DISTINCT)
            {
                /* in what table is this field ? */
                if (is_main_field(report_desc_array[i].attr_index))
                    fcnt.nb_main++;
                else if (is_annex_field(report_desc_array[i].attr_index))
                    fcnt.nb_annex++;
                else
                {
                    /* Not supported yet */
                    DisplayLog(LVL_CRIT, LISTMGR_TAG,
                                "Error: report on attribute '%s' (index=%u) is not supported (report item #%u).",
                                field_name(report_desc_array[i].attr_index), report_desc_array[i].attr_index, i);
                    rc = DB_NOT_SUPPORTED;
                    goto free_str;
                }
            }

            sprintf(attrname, "attr%u", i);

            /* what kind of stat on this field ? */
            switch (report_desc_array[i].report_type)
            {
            case REPORT_MIN:
                coma_if_needed(fields);
                g_string_append_printf(fields, "MIN(%s) as %s",
                          field_str(report_desc_array[i].attr_index), attrname);
                p_report->result[i].type = field_type(report_desc_array[i].attr_index);
                break;

            case REPORT_MAX:
                coma_if_needed(fields);
                g_string_append_printf(fields, "MAX(%s) as %s",
                          field_str(report_desc_array[i].attr_index), attrname);
                p_report->result[i].type = field_type(report_desc_array[i].attr_index);
                break;

            case REPORT_AVG:
                coma_if_needed(fields);
                g_string_append_printf(fields, "ROUND(AVG(%s)) as %s",
                          field_str(report_desc_array[i].attr_index), attrname);
                p_report->result[i].type = field_type(report_desc_array[i].attr_index);
                break;

            case REPORT_SUM:
                coma_if_needed(fields);
                g_string_append_printf(fields, "SUM(%s) as %s",
                         field_str(report_desc_array[i].attr_index), attrname);
                p_report->result[i].type = field_type(report_desc_array[i].attr_index);
                break;

            case REPORT_COUNT:
                coma_if_needed(fields);
                g_string_append_printf(fields, "COUNT(*) as %s", attrname);
                p_report->result[i].type = DB_BIGUINT;
                break;

            case REPORT_COUNT_DISTINCT:
                coma_if_needed(fields);
                g_string_append_printf(fields, "COUNT(DISTINCT(%s)) as %s",
                                       field_str(report_desc_array[i].attr_index), attrname);
                p_report->result[i].type = DB_BIGUINT;
                break;

            case REPORT_GROUP_BY:
                coma_if_needed(fields);
                g_string_append_printf(fields, "%s as %s", field_str(report_desc_array[i].attr_index),
                                       attrname);
                coma_if_needed(group_by);
                g_string_append(group_by, attrname);
                p_report->result[i].type = field_type(report_desc_array[i].attr_index);
                break;
            }

            /* is this field sorted ? */
            append_sort_order(order_by, attrname, report_desc_array[i].sort_flag);

            /* is this field filtered ? */
            listmgr_fieldfilter(p_report, p_mgr, report_desc_array, attrname,
                                having, where, i);

            p_report->result[i].flags = field_flag(report_desc_array[i].attr_index);
        }

        /* generate size profile */
        if (profile_descr != NULL)
        {
            if (profile_descr->attr_index == ATTR_INDEX_size)
            {
                coma_if_needed(fields);
                g_string_append(fields, "SUM(size=0)");

                for (i = 1; i < SZ_PROFIL_COUNT-1; i++)
                    g_string_append_printf(fields,
                            ",SUM("SZRANGE_FUNC"(size)=%u)", i-1);

                g_string_append_printf(fields, ",SUM("SZRANGE_FUNC"(size)>=%u)", SZ_PROFIL_COUNT-1);

                for (i = 0; i< SZ_PROFIL_COUNT; i++)
                    p_report->result[i+report_descr_count].type = DB_BIGUINT;

                if (profile_descr->range_ratio_len > 0)
                {
                    /* add ratio field and sort it */
                    coma_if_needed(fields);
                    g_string_append_printf(fields, "SUM(size>=%llu",
                                         SZ_MIN_BY_INDEX(profile_descr->range_ratio_start));

                    /* is the last range = 1T->inf ? */
                    if (profile_descr->range_ratio_start + profile_descr->range_ratio_len >= SZ_PROFIL_COUNT)
                        g_string_append(fields, ")");
                    else
                        g_string_append_printf(fields, " and size<%llu)",
                                         SZ_MIN_BY_INDEX(profile_descr->range_ratio_start
                                                         + profile_descr->range_ratio_len));

                    g_string_append(fields, "/COUNT(*) as sizeratio");
                }
            }
        }
    }

    /* process filter */
    if (!(no_filter(p_filter)))
    {
        if (full_acct(report_desc_array, report_descr_count, p_filter)
            && !opt.force_no_acct)
        {
            int filter_acct;

            /* filter on acct fields only */
            filter_acct = filter2str(p_mgr, where, p_filter, T_ACCT,
                                     (!GSTRING_EMPTY(where) ? AOF_LEADING_SEP : 0)
                                     | AOF_PREFIX);
            if (filter_acct > 0)
                use_acct_table = true;
        }
        else
        {
            /* process NAMES filters apart, as with must then join with DISTINCT(id) */
            filter_where(p_mgr, p_filter, &fcnt, where,
                         (!GSTRING_EMPTY(where) ? AOF_LEADING_SEP : 0)
                         | AOF_SKIP_NAME);

            filter_name = g_string_new(NULL);
            fcnt.nb_names = filter2str(p_mgr, filter_name, p_filter, T_DNAMES, 0);
        }
    }

    /* start building the whole request */
    req = g_string_new("SELECT ");
    g_string_append_printf(req, "%s FROM ", fields->str);

    /* FROM clause */
    if (use_acct_table)
    {
        g_string_append(req, ACCT_TABLE);
        query_tab = T_ACCT;
    }
    else
    {
        bool distinct;

        filter_from(p_mgr, &fcnt, req, &query_tab, &distinct, AOF_SKIP_NAME);

        if (filter_name != NULL && !GSTRING_EMPTY(filter_name))
        {
            g_string_append_printf(req, " INNER JOIN (SELECT DISTINCT(id)"
                            " FROM "DNAMES_TABLE" WHERE %s) N"
                            " ON %s.id=N.id", filter_name->str,
                            table2name(query_tab));
        /* FIXME: what if NAMES is the query tab? */
        }
        /* FIXME: do the same for stripe items */
    }

    /* Build the request */
    if (!GSTRING_EMPTY(where))
        g_string_append_printf(req, " WHERE %s", where->str);

    if (!GSTRING_EMPTY(group_by))
        g_string_append_printf(req, " GROUP BY %s", group_by->str);

    if (!GSTRING_EMPTY(having))
        g_string_append_printf(req, " HAVING %s", having->str);

    if (!GSTRING_EMPTY(order_by))
        g_string_append_printf(req, " ORDER BY %s", order_by->str);

    /* iterator opt */
    if (opt.list_count_max > 0)
        g_string_append_printf(req, " LIMIT %u", opt.list_count_max);


retry:
    /* execute request (expect that ACCT table does not exists) */
    if (use_acct_table)
        rc = db_exec_sql_quiet(&p_mgr->conn, req->str, &p_report->select_result);
    else
        rc = db_exec_sql(&p_mgr->conn, req->str, &p_report->select_result);

    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;

    /* if the ACCT table does exist, switch to standard mode */
    if (use_acct_table && (rc == DB_NOT_EXISTS))
    {
        lmgr_iter_opt_t new_opt;

        if (p_opt != NULL)
            new_opt = *p_opt;
        else
            new_opt.list_count_max = 0;

        new_opt.force_no_acct = true;

        DisplayLog(LVL_EVENT, LISTMGR_TAG, "No accounting info: switching to standard query mode");

        g_string_free(req, TRUE);
        g_string_free(fields, TRUE);
        g_string_free(group_by, TRUE);
        g_string_free(order_by, TRUE);
        g_string_free(having, TRUE);
        g_string_free(where, TRUE);
        if (filter_name != NULL)
            g_string_free(filter_name, TRUE);

        return ListMgr_Report(p_mgr, report_desc_array, report_descr_count,
                               profile_descr,
                               p_filter, &new_opt);
    }

free_str:
    /* these are always allocated */
    g_string_free(fields, TRUE);
    g_string_free(group_by, TRUE);
    g_string_free(order_by, TRUE);
    g_string_free(having, TRUE);
    g_string_free(where, TRUE);
    /* these may not be allocated */
    if (req != NULL)
        g_string_free(req, TRUE);
    if (filter_name != NULL)
        g_string_free(filter_name, TRUE);

    if (rc == DB_SUCCESS)
        return p_report;

/* error */
    MemFree(p_report->result);

free_report:
    MemFree(p_report);
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
            p_value[i].type = p_iter->result[i].type;
            if (parsedbtype(p_iter->str_tab[i], p_iter->result[i].type,
                            &(p_value[i].value_u)) != 1)
            {
                DisplayLog( LVL_CRIT, LISTMGR_TAG,
                            "Could not parse result field #%u: value='%s'", i, p_iter->str_tab[i] );
                return DB_INVALID_ARG;
            }
            if (p_iter->result[i].flags & SEPD_LIST)
                separated_db2list_inplace((char*)p_value[i].value_u.val_str);
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
                else if (parsedbtype(p_iter->str_tab[idx], p_iter->result[idx].type,
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


void ListMgr_CloseReport(struct lmgr_report_t *p_iter)
{
    db_result_free(&p_iter->p_mgr->conn, &p_iter->select_result);

    if (p_iter->str_tab != NULL)
        MemFree(p_iter->str_tab);

    MemFree(p_iter->result);
    MemFree(p_iter);
}

int ListMgr_EntryCount(lmgr_t * p_mgr, uint64_t *count)
{
    int rc;

    do {
        rc = lmgr_table_count(&p_mgr->conn, MAIN_TABLE, count);
    } while (rc != DB_SUCCESS && lmgr_delayed_retry(p_mgr, rc));

    return rc;
}
