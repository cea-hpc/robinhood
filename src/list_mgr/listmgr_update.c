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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "list_mgr.h"
#include "database.h"
#include "listmgr_common.h"
#include "listmgr_stripe.h"
#include "RobinhoodLogs.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>


int ListMgr_Update(lmgr_t * p_mgr, entry_id_t *p_id, attr_set_t *p_update_set)
{
    int            rc, main_count, annex_count;
    char           query[4096];
    char           fields[4096];
    char           annex_fields[4096];
    DEF_PK(pk);
    int            nb_tables = 0;

    /* read only fields in info mask? */
    if ( readonly_attr_set & p_update_set->attr_mask )
    {
        DisplayLog( LVL_MAJOR, LISTMGR_TAG, "Error: trying to update read only values: attr_mask=%#x",
                    readonly_attr_set & p_update_set->attr_mask );
        return DB_INVALID_ARG;
    }

    entry_id2pk(p_id, PTR_PK(pk));

    /* check how many tables are to be updated */
    if ( main_fields( p_update_set->attr_mask ) )
    {
        main_count = attrset2updatelist(p_mgr, fields, p_update_set, T_MAIN, FALSE, FALSE);
        if ( main_count < 0 )
            return -main_count;
        if ( main_count > 0 )
            nb_tables++;
    }
    else
        main_count = 0;

    /* For the NAMES tables. */
    nb_tables++;

    if ( annex_table && annex_fields( p_update_set->attr_mask ) )
    {
        annex_count = attrset2updatelist(p_mgr, annex_fields, p_update_set, T_ANNEX, FALSE, FALSE);
        if ( annex_count < 0 )
            return -annex_count;
        if ( annex_count > 0 )
            nb_tables++;
    }
    else
        annex_count = 0;


    if ( stripe_fields( p_update_set->attr_mask ) )
        nb_tables += 2;

    /* restart the whole transaction when the error is retryable */
retry:
    /* if only 1 table is impacted, switch to autocommit mode */
    if (nb_tables > 1)
    {
        /* @todo in the case of sqlite, we may want to do periodic commit
         * instead of systematic one. */
        rc = lmgr_begin(p_mgr);
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            return rc;
    }

    /* update main table */
    if (main_count > 0)
    {
        sprintf( query, "UPDATE " MAIN_TABLE " SET %s WHERE id="DPK, fields, pk );
        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;
    }

    /* update names table */
    if (ATTR_MASK_TEST(p_update_set, name) && ATTR_MASK_TEST(p_update_set, parent_id))
    {
        char          *fields_curr;
        char          *values_curr;
        char           values[4096];
        char          *set;

        strcpy( fields, "id" );
        sprintf( values, DPK, pk );
        fields_curr = fields + strlen( fields );
        values_curr = values + strlen( values );

        /* create field and values lists */
        attrmask2fieldlist( fields_curr, p_update_set->attr_mask, T_DNAMES, TRUE, FALSE, "", "" );
        attrset2valuelist( p_mgr, values_curr, p_update_set, T_DNAMES, TRUE );

        // FIXME this update operation may zero column content if some values are not specified
        set = query + sprintf(query, "INSERT INTO " DNAMES_TABLE "(%s, pkn) VALUES (%s, "HNAME_DEF") "
                "ON DUPLICATE KEY UPDATE id=VALUES(id)", fields, values);
        /* append the field values for 'ON DUPLICATE KEY...' */
        attrset2updatelist(p_mgr, set, p_update_set, T_DNAMES, TRUE, TRUE);

        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;
    }
    else if (ATTR_MASK_TEST(p_update_set, name) || ATTR_MASK_TEST(p_update_set, parent_id))
    {
        DisplayLog(LVL_DEBUG, LISTMGR_TAG, "WARNING: missing attribute to update name information"
                   " (entry "DPK"): name %s, parent_id %s", pk,
                   ATTR_MASK_TEST(p_update_set, name) ? "is set" : "is not set",
                   ATTR_MASK_TEST(p_update_set, parent_id) ? "is set" : "is not set");
    }

    /* update annex table (if any) */
    if ( annex_count > 0 )
    {
        sprintf( query, "UPDATE " ANNEX_TABLE " SET %s WHERE id="DPK, annex_fields, pk );
        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;
    }

    /* insert new stripe info if provided (and remove previous values if any) */
#ifdef _LUSTRE
    if ( ATTR_MASK_TEST( p_update_set, stripe_info ) )
    {
#ifdef HAVE_LLAPI_FSWAP_LAYOUTS
        int validator = ATTR(p_update_set, stripe_info).validator;
#else
        int validator = VALID(p_id);
#endif

        const stripe_items_t *p_items = NULL;

        if (ATTR_MASK_TEST(p_update_set, stripe_items))
            p_items = &ATTR(p_update_set, stripe_items);

        rc = update_stripe_info(p_mgr, pk, validator,
                                &ATTR(p_update_set, stripe_info), p_items, TRUE);
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;
    }
#endif

    if ( nb_tables > 1 )
    {
        rc = lmgr_commit(p_mgr);
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        if (!rc)
            p_mgr->nbop[OPIDX_UPDATE]++;
        return rc;
    }
    else
    {
        p_mgr->nbop[OPIDX_UPDATE]++;
        return DB_SUCCESS;
    }

  rollback:
    lmgr_rollback( p_mgr );
    return rc;
}


int ListMgr_MassUpdate( lmgr_t * p_mgr,
                        const lmgr_filter_t * p_filter, const attr_set_t * p_attr_set )
{
    char           query[4096];
    char           filter_str_main[2048];
    char           filter_str_annex[2048];
    char           filter_str_stripe_info[2048];
    char           filter_str_stripe_units[2048];
    int            filter_main = 0;
    int            filter_annex = 0;
    int            filter_stripe_info = 0;
    int            filter_stripe_units = 0;
    int            rc, count;
    int            impact_all = FALSE;
    int            filter_unic = FALSE;
    char           fields[2048];
    char           tmp_table_name[256];
    int            tmp_table_created = FALSE;

    filter_str_main[0] = '\0';
    filter_str_annex[0] = '\0';
    filter_str_stripe_info[0] = '\0';
    filter_str_stripe_units[0] = '\0';

    /* /!\ possible cases:
     * - simplest: the fields of the filter and the attributes to be changed are in the same table
     * - harder: the fields of the filter are in the same table and attributes are in another different table
     */

    /* 1) check the location of filters */

    if ( p_filter )
    {
        filter_main = filter2str( p_mgr, filter_str_main, p_filter, T_MAIN,
                                  FALSE, FALSE );

        if ( annex_table )
            filter_annex = filter2str( p_mgr, filter_str_annex, p_filter,
                                       T_ANNEX, FALSE, FALSE );
        else
            filter_annex = 0;

        filter_stripe_info =
            filter2str( p_mgr, filter_str_stripe_info, p_filter, T_STRIPE_INFO,
                        FALSE, FALSE );

        filter_stripe_units =
            filter2str( p_mgr, filter_str_stripe_units, p_filter,
                        T_STRIPE_ITEMS,FALSE, FALSE );

        if ( filter_main + filter_annex + filter_stripe_info + filter_stripe_units == 0 )
        {
            /* all records */
            impact_all = TRUE;
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Empty filter: all records will be affected" );
        }
        else if ( filter_main && !( filter_annex || filter_stripe_info || filter_stripe_units ) )
        {
            filter_unic = TRUE;
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Filter is only on " MAIN_TABLE " table" );
        }
        else if ( filter_annex && !( filter_main || filter_stripe_info || filter_stripe_units ) )
        {
            filter_unic = TRUE;
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Filter is only on " ANNEX_TABLE " table" );
        }
        else if ( filter_stripe_info && !( filter_main || filter_annex || filter_stripe_units ) )
        {
            filter_unic = TRUE;
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Filter is only on " STRIPE_INFO_TABLE " table" );
        }
        else if ( filter_stripe_units && !( filter_main || filter_annex || filter_stripe_info ) )
        {
            filter_unic = TRUE;
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Filter is only on " STRIPE_ITEMS_TABLE " table" );
        }
        else
            DisplayLog( LVL_FULL, LISTMGR_TAG,
                        "Filter on several tables: " MAIN_TABLE ":%d, "
                        ANNEX_TABLE ":%d, " STRIPE_INFO_TABLE ":%d, "
                        STRIPE_ITEMS_TABLE ":%d",
                        filter_main, filter_annex, filter_stripe_info, filter_stripe_units );
    }
    else
    {
        impact_all = TRUE;
        DisplayLog( LVL_FULL, LISTMGR_TAG, "Empty filter: all records will be affected" );
    }

    /* 2) check if attributes to be changed are in MAIN table */
    /* 3) if filters are in MAIN table too, make a simple update statement */
    /* 4) if not, make a compound request: update xxx set xxx WHERE pk in ( select pk from ... where ... ) */

retry:
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    /* perform updates on MAIN TABLE */
    count = attrset2updatelist(p_mgr, fields, p_attr_set, T_MAIN, FALSE, FALSE);
    if ( count < 0 )
        return -count;
    if ( count > 0 )
    {
        DisplayLog( LVL_FULL, LISTMGR_TAG, "Update on " MAIN_TABLE " table" );

        if ( impact_all )
        {
            /* all records are impacted */
            sprintf( query, "UPDATE " MAIN_TABLE " SET %s", fields );
        }
        else if ( filter_unic && filter_main )
        {
            /* update on the same table as filter */
            sprintf( query, "UPDATE " MAIN_TABLE " SET %s WHERE %s", fields, filter_str_main );
        }
        else if ( filter_unic )
        {
            /* update is on a different table than filter */
            if ( filter_annex )
            {
                if ( !tmp_table_created )
                {
                    /* create temp table */
                    sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
                             ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );

                    sprintf( query,
                             "CREATE TEMPORARY TABLE %s AS SELECT id FROM "
                             ANNEX_TABLE " WHERE %s", tmp_table_name, filter_str_annex );

                    rc = db_exec_sql( &p_mgr->conn, query, NULL );
                    if (lmgr_delayed_retry(p_mgr, rc))
                        goto retry;
                    else if (rc)
                        goto rollback;

                    tmp_table_created = TRUE;
                }

                DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: passing through unoptimized algorithm" );
                sprintf( query,
                         "UPDATE " MAIN_TABLE
                         " SET %s WHERE id IN (SELECT id FROM %s)", fields, tmp_table_name );

            }
            else if ( filter_stripe_units )
            {
                if ( !tmp_table_created )
                {
                    sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
                             ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );

                    /* create temp table */
                    sprintf( query,
                             "CREATE TEMPORARY TABLE %s AS SELECT id FROM "
                             STRIPE_ITEMS_TABLE " WHERE %s", tmp_table_name,
                             filter_str_stripe_units );

                    rc = db_exec_sql( &p_mgr->conn, query, NULL );
                    if (lmgr_delayed_retry(p_mgr, rc))
                        goto retry;
                    else if (rc)
                        goto rollback;

                    tmp_table_created = TRUE;
                }

                DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: passing through unoptimized algorithm" );

                sprintf( query,
                         "UPDATE " MAIN_TABLE
                         " SET %s WHERE id IN (SELECT id FROM %s)", fields, tmp_table_name );
            }
            else if ( filter_stripe_info )
            {
                if ( !tmp_table_created )
                {
                    sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
                             ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );

                    /* create temp table */
                    sprintf( query,
                             "CREATE TEMPORARY TABLE %s AS SELECT id FROM "
                             STRIPE_INFO_TABLE " WHERE %s", tmp_table_name,
                             filter_str_stripe_info );

                    rc = db_exec_sql( &p_mgr->conn, query, NULL );
                    if (lmgr_delayed_retry(p_mgr, rc))
                        goto retry;
                    else if (rc)
                        goto rollback;

                    tmp_table_created = TRUE;
                }

                DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: passing through unoptimized algorithm" );

                sprintf( query,
                         "UPDATE " MAIN_TABLE
                         " SET %s WHERE id IN (SELECT id FROM %s)", fields, tmp_table_name );
            }

        }
        else
        {
            /* @todo mixed filter :-s */
            rc = DB_NOT_SUPPORTED;
            goto rollback;
        }

        rc = db_exec_sql( &p_mgr->conn, query, NULL );
        if (lmgr_delayed_retry(p_mgr, rc))
            goto retry;
        else if (rc)
            goto rollback;
    }


    /* update on annex table ? */
    if ( annex_table )
    {
        count = attrset2updatelist(p_mgr, fields, p_attr_set, T_ANNEX, FALSE, FALSE);
        if ( count < 0 )
            return -count;
        if ( count > 0 )
        {
            DisplayLog( LVL_FULL, LISTMGR_TAG, "Update on " ANNEX_TABLE " table" );

            if ( impact_all )
            {
                /* all records are impacted */
                sprintf( query, "UPDATE " ANNEX_TABLE " SET %s", fields );
            }
            else if ( filter_unic && filter_annex )
            {
                /* update on the same table as filter */
                sprintf( query,
                         "UPDATE " ANNEX_TABLE " SET %s WHERE %s", fields, filter_str_annex );
            }
            else if ( filter_unic )
            {
                /* update is on a different table than filter */
                if ( filter_main )
                {
                    if ( !tmp_table_created )
                    {
                        /* create temp table */
                        sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
                                 ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );

                        sprintf( query,
                                 "CREATE TEMPORARY TABLE %s AS SELECT id FROM "
                                 MAIN_TABLE " WHERE %s", tmp_table_name, filter_str_main );

                        rc = db_exec_sql(&p_mgr->conn, query, NULL);
                        if (lmgr_delayed_retry(p_mgr, rc))
                            goto retry;
                        else if (rc)
                            goto rollback;

                        tmp_table_created = TRUE;
                    }

                    DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: passing through unoptimized algorithm" );

                    sprintf( query,
                             "UPDATE " ANNEX_TABLE
                             " SET %s WHERE id IN (SELECT id FROM %s)", fields, tmp_table_name );

                }
                else if ( filter_stripe_units )
                {

                    if ( !tmp_table_created )
                    {
                        /* create temp table */
                        sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
                                 ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );

                        sprintf( query,
                                 "CREATE TEMPORARY TABLE %s AS SELECT id FROM "
                                 STRIPE_ITEMS_TABLE " WHERE %s",
                                 tmp_table_name, filter_str_stripe_units );

                        rc = db_exec_sql( &p_mgr->conn, query, NULL );
                        if (lmgr_delayed_retry(p_mgr, rc))
                            goto retry;
                        else if (rc)
                            goto rollback;

                        tmp_table_created = TRUE;
                    }

                    DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: passing through unoptimized algorithm" );

                    sprintf( query,
                             "UPDATE " ANNEX_TABLE
                             " SET %s WHERE id IN (SELECT id FROM %s)", fields, tmp_table_name );
                }
                else if ( filter_stripe_info )
                {

                    if ( !tmp_table_created )
                    {
                        /* create temp table */
                        sprintf( tmp_table_name, "TMP_TABLE_%u_%u",
                                 ( unsigned int ) getpid(  ), ( unsigned int ) pthread_self(  ) );

                        sprintf( query,
                                 "CREATE TEMPORARY TABLE %s AS SELECT id FROM "
                                 STRIPE_INFO_TABLE " WHERE %s",
                                 tmp_table_name, filter_str_stripe_info );

                        rc = db_exec_sql( &p_mgr->conn, query, NULL );
                        if (lmgr_delayed_retry(p_mgr, rc))
                            goto retry;
                        else if (rc)
                            goto rollback;

                        tmp_table_created = TRUE;
                    }

                    DisplayLog( LVL_MAJOR, LISTMGR_TAG, "WARNING: passing through unoptimized algorithm" );

                    sprintf( query,
                             "UPDATE " ANNEX_TABLE
                             " SET %s WHERE id IN (SELECT id FROM %s)", fields, tmp_table_name );
                }
            }
            else
            {
                /* @todo mixed filter :-s */
                rc = DB_NOT_SUPPORTED;
                goto rollback;
            }

            rc = db_exec_sql( &p_mgr->conn, query, NULL );
            if (lmgr_delayed_retry(p_mgr, rc))
                goto retry;
            else if (rc)
                goto rollback;
        }
    }


    if ( ATTR_MASK_TEST( p_attr_set, stripe_info ) || ATTR_MASK_TEST( p_attr_set, stripe_items ) )
    {
        /* XXX is there a case where stripe info is to be updated massively ? */
        rc = DB_NOT_SUPPORTED;
        goto rollback;
    }

    if ( tmp_table_created )
    {
        rc = db_drop_component(&p_mgr->conn, DBOBJ_TABLE, tmp_table_name);
        if ( rc )
            goto rollback;
    }

    rc = lmgr_commit(p_mgr);


  rollback:
    lmgr_rollback( p_mgr );
    return rc;
}

int ListMgr_Replace(lmgr_t * p_mgr, entry_id_t *old_id, attr_set_t *old_attrs,
                    entry_id_t *new_id, attr_set_t *new_attrs,
                    int src_is_last, int update_target_if_exists)
{
    char query[4096];
    DEF_PK(oldpk);
    DEF_PK(newpk);
    int rc;

retry:
    rc = lmgr_begin(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        return rc;

    /* delete the old entry */
    rc = listmgr_remove_no_tx(p_mgr, old_id, old_attrs, src_is_last);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    /* create the new one */
    rc = listmgr_batch_insert_no_tx(p_mgr, &new_id, &new_attrs, 1,
                                    update_target_if_exists);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    /* update parent ids in NAMES table */
    entry_id2pk(old_id, PTR_PK(oldpk));
    entry_id2pk(new_id, PTR_PK(newpk));

    sprintf(query, "UPDATE "DNAMES_TABLE" SET parent_id="DPK" WHERE parent_id="DPK,
            newpk, oldpk);
    rc = db_exec_sql(&p_mgr->conn, query, NULL);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else if (rc)
        goto rollback;

    rc = lmgr_commit(p_mgr);
    if (lmgr_delayed_retry(p_mgr, rc))
        goto retry;
    else
        return rc;

rollback:
    lmgr_rollback(p_mgr);
    return rc;
}
