/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2009, 2010 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
*/

/**
 * policy management
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "policies.h"
#include "RobinhoodConfig.h"
#include "RobinhoodMisc.h"
#include <errno.h>

#define FILESETS_SECTION      "Filesets"
#define FILESET_BLOCK         "FileClass"

#define PURGEPOLICY_BLOCK     "purge_policies"
#define MIGRPOLICY_BLOCK      "migration_policies"
#define UNLINKPOLICY_BLOCK    "hsm_remove_policy"
#define UPDTPOLICY_BLOCK      "db_update_policy"

#define POLICY_BLOCK          "policy"

#define IGNORE_BLOCK          "ignore"
#define IGNORE_FC             "ignore_fileclass"
#define CONDITION_BLOCK       "condition"
#define DEFINITION_BLOCK      "definition"
#define BLACKLIST_BLOCK       "purge_penalty"

#define RMDIR_BLOCK      "rmdir_policy"
#define RM_RECURSE_BLOCK "recursive_rmdir"

#define RELOAD_TAG "PolicyReload"
#define CHK_TAG "PolicyCheck"

policies_t     policies = {
#ifdef HAVE_PURGE_POLICY
    .purge_policies = {NULL, 0, NULL, 0, 0},
#endif
#ifdef HAVE_MIGR_POLICY
    .migr_policies = {NULL, 0, 0},
#endif
#ifdef HAVE_RM_POLICY
    .unlink_policy = { FALSE, 86400 },
#endif
#ifdef HAVE_RMDIR_POLICY
    .rmdir_policy = {0, NULL, 0, NULL, 0, 0},
#endif
    .filesets = {NULL, 0, 0}
};




#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                                        sprintf( msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        return EFAULT; \
                                    }\
                                } while (0)

#define critical_err_check_goto(_ptr_, _blkname_, _label) do { if (!_ptr_) {\
                                        sprintf( msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        rc = EFAULT; \
                                        goto _label; \
                                    }\
                                } while (0)


/**
 * Compare 2 boolean expressions
 * @return TRUE if expression structure changed.
 * @return FALSE if they have the same structure,
 * @return  -1 on error.
 */
int compare_boolexpr( const bool_node_t * expr1, const bool_node_t * expr2 )
{
    if ( expr1->node_type != expr2->node_type )
        return 1;

    switch ( expr1->node_type )
    {
    case NODE_UNARY_EXPR:
        if ( expr1->content_u.bool_expr.bool_op != expr2->content_u.bool_expr.bool_op )
            return TRUE;
        return compare_boolexpr( expr1->content_u.bool_expr.expr1,
                                 expr2->content_u.bool_expr.expr1 );

    case NODE_BINARY_EXPR:
        if ( expr1->content_u.bool_expr.bool_op != expr2->content_u.bool_expr.bool_op )
            return TRUE;
        return ( compare_boolexpr
                 ( expr1->content_u.bool_expr.expr1, expr2->content_u.bool_expr.expr1 )
                 || compare_boolexpr( expr1->content_u.bool_expr.expr2,
                                      expr2->content_u.bool_expr.expr2 ) );

    case NODE_CONDITION:
        /* compare criteria */
        if ( expr1->content_u.condition->crit != expr2->content_u.condition->crit )
            return TRUE;
        /* compare operator, except for custom cmd and xattr */
        if ( ( expr1->content_u.condition->crit != CRITERIA_CUSTOM_CMD )
             && ( expr1->content_u.condition->crit != CRITERIA_XATTR )
             && ( expr1->content_u.condition->op != expr2->content_u.condition->op ) )
            return TRUE;
        /* same structure */
        return FALSE;
    }

    /* should not happen */
    return -1;
}                               /* compare_boolexpr */


/**
 * Update the numerical values of a boolean expression.
 * /!\ compare_boolexpr() must have returned 0 (else, unguarantied behavior).
 * @param tgt Boolean expression to be updated
 * @param src Boolean expression to take values from.
 * @return TRUE if expression values have been changed
 * @return FALSE if nothing has been changed
 */
int update_boolexpr( const bool_node_t * tgt, const bool_node_t * src )
{
    compare_triplet_t *p_triplet1;
    compare_triplet_t *p_triplet2;
    char           tmp_buff1[256];
    char           tmp_buff2[256];
    int            rc;

    switch ( tgt->node_type )
    {
    case NODE_UNARY_EXPR:
        return update_boolexpr( tgt->content_u.bool_expr.expr1, src->content_u.bool_expr.expr1 );

    case NODE_BINARY_EXPR:
        rc = update_boolexpr( tgt->content_u.bool_expr.expr1, src->content_u.bool_expr.expr1 );
        if ( update_boolexpr( tgt->content_u.bool_expr.expr2, src->content_u.bool_expr.expr2 ) )
            rc = TRUE;
        return rc;

    case NODE_CONDITION:

        p_triplet1 = tgt->content_u.condition;
        p_triplet2 = src->content_u.condition;

        switch ( p_triplet1->crit )
        {
        case CRITERIA_SIZE:
            if ( p_triplet1->val.size != p_triplet2->val.size )
            {
                FormatFileSize( tmp_buff1, 256, p_triplet1->val.size );
                FormatFileSize( tmp_buff2, 256, p_triplet2->val.size );

                DisplayLog( LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                            criteria2str( CRITERIA_SIZE ), op2str( p_triplet1->op ), tmp_buff1,
                            criteria2str( CRITERIA_SIZE ), op2str( p_triplet2->op ), tmp_buff2 );
                p_triplet1->val.size = p_triplet2->val.size;
                return TRUE;
            }
            else
                return FALSE;

            /* integer conditions */
        case CRITERIA_DEPTH:
        case CRITERIA_OST:

#ifdef ATTR_INDEX_dircount
        case CRITERIA_DIRCOUNT:
#endif
            if ( p_triplet1->val.integer != p_triplet2->val.integer )
            {
                DisplayLog( LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %d) -> (%s %s %d)",
                            criteria2str( p_triplet1->crit ), op2str( p_triplet1->op ),
                            p_triplet1->val.integer, criteria2str( p_triplet2->crit ),
                            op2str( p_triplet2->op ), p_triplet2->val.integer );
                p_triplet1->val.integer = p_triplet2->val.integer;
                return TRUE;
            }
            else
                return FALSE;

            /* duration conditions */
        case CRITERIA_LAST_ACCESS:
        case CRITERIA_LAST_MOD:
#ifdef ATTR_INDEX_last_archive
        case CRITERIA_LAST_ARCHIVE:
#endif
#ifdef ATTR_INDEX_last_restore
        case CRITERIA_LAST_RESTORE:
#endif
#ifdef ATTR_INDEX_creation_time
        case CRITERIA_CREATION:
#endif
            if ( p_triplet1->val.duration != p_triplet2->val.duration )
            {
                FormatDurationFloat( tmp_buff1, 256, p_triplet1->val.duration );
                FormatDurationFloat( tmp_buff2, 256, p_triplet2->val.duration );
                DisplayLog( LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                            criteria2str( p_triplet1->crit ), op2str( p_triplet1->op ), tmp_buff1,
                            criteria2str( p_triplet2->crit ), op2str( p_triplet2->op ), tmp_buff2 );
                p_triplet1->val.duration = p_triplet2->val.duration;
                return TRUE;
            }
            else
                return FALSE;

#ifdef ATTR_INDEX_type
        case CRITERIA_TYPE:
            if ( p_triplet1->val.type != p_triplet2->val.type )
            {
                DisplayLog( LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                            criteria2str( p_triplet1->crit ), op2str( p_triplet1->op ),
                            type2str( p_triplet1->val.type ), criteria2str( p_triplet2->crit ),
                            op2str( p_triplet2->op ), type2str( p_triplet2->val.type ) );
                p_triplet1->val.type = p_triplet2->val.type;
                return TRUE;
            }
            else
                return FALSE;
#endif

            /* unmodifiable conditions */
        case CRITERIA_TREE:
        case CRITERIA_PATH:
        case CRITERIA_FILENAME:
        case CRITERIA_OWNER:
        case CRITERIA_GROUP:
        case CRITERIA_POOL:
            if ( strcmp( p_triplet1->val.str, p_triplet2->val.str ) )
            {
                DisplayLog( LVL_MAJOR, RELOAD_TAG,
                            "Condition changed on attribute '%s' but this cannot be modified dynamically",
                            criteria2str( p_triplet1->crit ) );
            }
            return FALSE;

        case CRITERIA_CUSTOM_CMD:
            if ( strcmp( p_triplet1->val.str, p_triplet2->val.str ) )
            {
                DisplayLog( LVL_MAJOR, RELOAD_TAG,
                            "Command line changed for '%s' condition, but this cannot be modified dynamically",
                            criteria2str( p_triplet1->crit ) );
            }
            return FALSE;

        case CRITERIA_XATTR:
            if ( strcmp( p_triplet1->val.str, p_triplet2->val.str )
                 || strcmp( p_triplet1->xattr_name, p_triplet2->xattr_name ) )
            {
                DisplayLog( LVL_MAJOR, RELOAD_TAG,
                            "xattr condition changed, but it cannot be modified dynamically");
            }
            return FALSE;

        default:
            DisplayLog( LVL_CRIT, RELOAD_TAG,
                        "Unsupported attribute: %s", criteria2str( p_triplet1->crit ));
        }

        break;
    }

    /* should not happen */
    return -1;

}                               /* update_boolexpr */


/** Update whitelist rules */
/* @TODO support whitelist rules update on SIGHUP */
static void __attribute__(( __unused__ ))
            update_whitelist( whitelist_item_t * old_items, unsigned int old_count,
                              whitelist_item_t * new_items, unsigned int new_count,
                              const char *block_name )
{
    unsigned int   i;

    if ( old_count != new_count )
    {
        DisplayLog( LVL_MAJOR, RELOAD_TAG,
                    "Whitelist rules count changed in block '%s' but cannot be modified dynamically: whitelist update cancelled",
                    block_name );
        return;
    }

    /* compare whitelist boolean expression structure */
    for ( i = 0; i < new_count; i++ )
    {
        if ( ( old_items[i].attr_mask != new_items[i].attr_mask )
             || compare_boolexpr( &old_items[i].bool_expr,
                                  &new_items[i].bool_expr ) )
        {
            DisplayLog( LVL_MAJOR, RELOAD_TAG,
                        "Whitelist expression #%u changed in block '%s'. "
                        "Only numerical values can be modified dynamically. "
                        "Whitelist update cancelled", i, block_name );
            return;
        }
    }

    /* if they are all the same, update/check their values */

    for ( i = 0; i < new_count; i++ )
    {
        if ( update_boolexpr( &old_items[i].bool_expr, &new_items[i].bool_expr ) )
        {
            char           criteriastr[2048];
            BoolExpr2str( &old_items[i].bool_expr, criteriastr, 2048 );
            DisplayLog( LVL_EVENT, RELOAD_TAG,
                        "Whitelist expression #%u in block '%s' has been updated and is now: %s", i,
                        block_name, criteriastr );
        }
    }

    /* XXX attr_mask is unchanged, since we keep the same expression structures */

}


static void free_whitelist( whitelist_item_t * p_items, unsigned int count )
{
    unsigned int   i;

    /* free boolean expressions */
    for ( i = 0; i < count; i++ )
    {
        FreeBoolExpr( &p_items[i].bool_expr, FALSE );
    }

    if ( ( count > 0 ) && ( p_items != NULL ) )
        free( p_items );
}



#if 0
static int read_purge_policy( config_file_t config, purge_policies_t *policy, char *msg_out,
                             int for_reload )
{
    /* is current trigger list the default ? */
    unsigned int   blc_index;
    int            rc;

    /* get PURGE block */

    config_item_t  purge_block = rh_config_FindItemByName( config, PURGEPOLICY_BLOCK );

    /* not mandatory */
    if ( purge_block == NULL )
        return 0;

    /* initialize global attributes mask */
    policy->global_attr_mask = 0;

    /* read white list rules */

    for ( blc_index = 0; blc_index < rh_config_GetNbItems( purge_block ); blc_index++ )
    {
        char          *block_name;
        config_item_t  curr_item = rh_config_GetItemByIndex( purge_block, blc_index );
        critical_err_check( curr_item, PURGEPOLICY_BLOCK );

        if ( rh_config_ItemType( curr_item ) != CONFIG_ITEM_BLOCK )
        {
            strcpy( msg_out, "Only sub-block are expected in " PURGEPOLICY_BLOCK " block" );
            return EINVAL;
        }
        block_name = rh_config_GetBlockName( curr_item );
        critical_err_check( block_name, PURGEPOLICY_BLOCK );

        if ( !strcasecmp( block_name, WHITELIST_BLOCK ) )
        {
            if ( policy->whitelist_count == 0 )
                policy->whitelist_rules =
                    ( whitelist_item_t * ) malloc( sizeof( whitelist_item_t ) );
            else
                policy->whitelist_rules =
                    ( whitelist_item_t * ) realloc( policy->whitelist_rules,
                                                    ( policy->whitelist_count +
                                                      1 ) * sizeof( whitelist_item_t ) );

            policy->whitelist_count++;

            /* analyze boolean expression */
            rc = GetBoolExpr( curr_item, block_name,
                              &policy->whitelist_rules[policy->whitelist_count - 1].bool_expr,
                              &policy->whitelist_rules[policy->whitelist_count - 1].attr_mask,
                              msg_out );

            if ( rc )
                return rc;

#ifdef ATTR_INDEX_dircount
            if ( policy->whitelist_rules[policy->whitelist_count - 1].
                 attr_mask & ATTR_MASK_dircount )
            {
                sprintf( msg_out,
                         "Directory-only attribute 'dircount' is used in a non-directory policy. In block %s, line %d.",
                         block_name, rh_config_GetItemLine( curr_item ) );
                return EINVAL;
            }
#endif

            policy->global_attr_mask |=
                policy->whitelist_rules[policy->whitelist_count - 1].attr_mask;
        }
        else if ( !strcasecmp( block_name, BLACKLIST_BLOCK ) )
        {
            if ( policy->blacklist_count == 0 )
                policy->blacklist_rules =
                    ( blacklist_item_t * ) malloc( sizeof( blacklist_item_t ) );
            else
                policy->blacklist_rules =
                    ( blacklist_item_t * ) realloc( policy->blacklist_rules,
                                                    ( policy->blacklist_count +
                                                      1 ) * sizeof( blacklist_item_t ) );

            policy->blacklist_count++;

            rc = parse_penalty_block( curr_item, block_name,
                                      &policy->blacklist_rules[policy->blacklist_count - 1],
                                      msg_out );
            if ( rc )
                return rc;

            policy->global_attr_mask |=
                policy->blacklist_rules[policy->blacklist_count - 1].attr_mask;
        }
        else
        {
            sprintf( msg_out, "Unknown sub-block '%s' in " PURGEPOLICY_BLOCK " block", block_name );
            return EINVAL;
        }

    }

    return 0;
}



static int reload_purge_policy( purge_policy_t *policy )
{
    /* Reloading purge policies dynamically is quite complex:
     * 1) compare triggers: if they are the same (same count, same type)
     *    update their simple parameters: thresholds and check interval
     * 2) compare whitelist expressions count and structures.
     *    If the structure is the same (same parameters, comparators, etc.) only update
     *    the numerical values for conditions.
     * 3) do the same for penalty criterias.
     *    Also update penalty amount.
     */

    /** @TODO prevent from concurrent access when purge policy is beeing checked */

    update_whitelist( policies.purge_policy.whitelist_rules, policies.purge_policy.whitelist_count,
                      policy->whitelist_rules, policy->whitelist_count, PURGEPOLICY_BLOCK );

    update_blacklist( policies.purge_policy.blacklist_rules, policies.purge_policy.blacklist_count,
                      policy->blacklist_rules, policy->blacklist_count, PURGEPOLICY_BLOCK );

    /* XXX global_attr_mask is unchanged, since we keep the same expressions */

    /* free reloaded config structure (not used anymore) */
    free_whitelist( policy->whitelist_rules, policy->whitelist_count );
    free_blacklist( policy->blacklist_rules, policy->blacklist_count );

    return 0;
}


#endif

static int set_default_update_policy( updt_policy_t *policy, char *msg_out )
{
    policy->md.policy = UPDT_ALWAYS;
#ifdef _HAVE_FID
    policy->path.policy = UPDT_ON_EVENT_PERIODIC;
    policy->path.period_min = 0;
    policy->path.period_max = 3600;
#endif
    policy->fileclass.policy = UPDT_ALWAYS;

    return 0;
}

static int write_default_update_policy( FILE * output )
{
    print_begin_block( output, 0, UPDTPOLICY_BLOCK, NULL );
    print_line( output, 1, "md_update        : always;" );
#ifdef _HAVE_FID
    print_line( output, 1, "path_update      : on_event_periodic(0,1h);" );
#endif
    print_line( output, 1, "fileclass_update : always;" );
    print_end_block( output, 0 );

    return 0;
}

static int write_update_policy_template( FILE * output )
{
    print_begin_block( output, 0, UPDTPOLICY_BLOCK, NULL );
    print_line( output, 1, "# possible policies for refreshing metadata and path in database:" );
    print_line( output, 1, "#   never: get the information once, then never refresh it" );
    print_line( output, 1, "#   always: always update entry info when processing it" );
    print_line( output, 1, "#   on_event: only update on related event" );
    print_line( output, 1, "#   periodic(interval): only update periodically");
    print_line( output, 1, "#   on_event_periodic(min_interval,max_interval)= on_event + periodic" );
    fprintf( output, "\n" );
    print_line( output, 1, "# Updating of file metadata"  );
    print_line( output, 1, "md_update = always ;" );
#ifdef _HAVE_FID
    print_line( output, 1, "# Updating file path in database" );
    print_line( output, 1, "path_update = on_event_periodic(0,1h) ;" );
#endif
    print_line( output, 1, "# File classes matching"  );
    print_line( output, 1, "fileclass_update = always ;" );

    print_end_block( output, 0 );
    return 0;
}

static inline const char * update_policy2str( updt_policy_item_t * pol,
                                              char * buffer )
{
    char tmpbuf1[256];
    char tmpbuf2[256];
    switch(pol->policy)
    {
        case UPDT_NEVER:
            return "never";
        case UPDT_ALWAYS:
            return "always";
        case UPDT_ON_EVENT:
            return "on_event";
        case UPDT_ON_EVENT_PERIODIC:
            FormatDurationFloat( tmpbuf1, 256, pol->period_min );
            FormatDurationFloat( tmpbuf2, 256, pol->period_max );
            sprintf(buffer, "on_event_periodic(%s,%s)", tmpbuf1, tmpbuf2 );
            return buffer;
        case UPDT_PERIODIC:
            FormatDurationFloat( tmpbuf1, 256, pol->period_max );
            sprintf(buffer, "periodic(%s)", tmpbuf1 );
            return buffer;
        default:
            return "???";
    }
}

static int read_update_item( updt_policy_item_t *item,  const char *strpolicy,
                             char **options, unsigned int nb_options, char *msg_out )
{
    memset(item, 0, sizeof(updt_policy_item_t) );

    if ( !strcasecmp( strpolicy, "never" ) )
        item->policy = UPDT_NEVER;
    else if ( !strcasecmp( strpolicy, "always" ) )
        item->policy = UPDT_ALWAYS;
    else if ( !strcasecmp( strpolicy, "on_event" ) )
        item->policy = UPDT_ON_EVENT;
    else if ( !strcasecmp( strpolicy, "periodic" ) )
    {
        time_t timeval;

        item->policy = UPDT_PERIODIC;
        if (( nb_options != 1 ) || !options || !options[0] )
        {
            strcpy( msg_out,
                    "1 argument is expected for periodic update policy. "
                    "E.g. periodic(30s);" );
            return EINVAL;
        }
        /* read argument as a duration */
        timeval = str2duration( options[0] );
        if ( timeval == -1 )
        {
            sprintf( msg_out, "Invalid value for periodic update policy: "
                     "duration expected. E.g. periodic(5min);" );
            return EINVAL;
        }
        item->period_min = item->period_max = timeval;
    }
    else if ( !strcasecmp( strpolicy, "on_event_periodic" ) )
    {
        time_t timeval1, timeval2;

        item->policy = UPDT_ON_EVENT_PERIODIC;
        if (( nb_options != 2 ) || !options || !options[0] || !options[1] )
        {
            strcpy( msg_out,
                    "2 arguments are expected for on_event_periodic update policy. "
                    "E.g. on_event_periodic(1s,30s);" );
            return EINVAL;
        }
        /* read argument as a duration */
        timeval1 = str2duration( options[0] );
        timeval2 = str2duration( options[1] );
        if ( (timeval1 == -1) || (timeval2 == -1) )
        {
            sprintf( msg_out, "Invalid value for on_event_periodic update policy: "
                     "durations expected. E.g. on_event_periodic(1s,5min);" );
            return EINVAL;
        }
        item->period_min = timeval1;
        item->period_max = timeval2;
    }
    else
    {
        sprintf( msg_out, "Invalid update policy '%s' (expected: never, always, "
                 "on_event, periodic(<interval>), on_event_periodic(<intvl1>,<intvl2>)",
                 strpolicy );
        return EINVAL;
    }

    return 0;

}

static int read_update_policy( config_file_t config,  updt_policy_t *policy, char *msg_out,
                               int for_reload )
{
    int            rc;
    char           tmpstr[1024];
    char         **options = NULL;
    unsigned int   nb_options = 0;

    static const char *update_expect[] =
    {
        "md_update",
#ifdef _HAVE_FID
        "path_update",
#endif
        "fileclass_update",
        NULL
    };

    /* get db_update_policy block */
    config_item_t updt_block = rh_config_FindItemByName( config,
                                                         UPDTPOLICY_BLOCK );
    /* not mandatory */
    if ( updt_block == NULL )
    {
#ifdef _DEBUG_PARSING
        printf( "%s block not found in config file\n", UPDTPOLICY_BLOCK );
#endif
        return 0;
    }

    /* get parameters from this block */
    rc = GetStringParam( updt_block, UPDTPOLICY_BLOCK, "md_update",
                         STR_PARAM_NO_WILDCARDS, tmpstr, 1024,
                         &options, &nb_options, msg_out );
    if ( (rc != 0) && (rc != ENOENT) )
        return rc;
    else if ( rc != ENOENT )
    {
        /* parse the parameter */
        rc = read_update_item( &policy->md, tmpstr, options, nb_options, msg_out );
        if (rc)
            return rc;
    }

#ifdef _HAVE_FID
    rc = GetStringParam( updt_block, UPDTPOLICY_BLOCK, "path_update",
                         STR_PARAM_NO_WILDCARDS, tmpstr, 1024,
                         &options, &nb_options, msg_out );
    if ( (rc != 0) && (rc != ENOENT) )
        return rc;
    else if ( rc != ENOENT )
    {
        /* parse the parameter */
        rc = read_update_item( &policy->path, tmpstr, options, nb_options, msg_out );
        if (rc)
            return rc;
    }
#endif

    /* get parameters from this block */
    rc = GetStringParam( updt_block, UPDTPOLICY_BLOCK, "fileclass_update",
                         STR_PARAM_NO_WILDCARDS, tmpstr, 1024,
                         &options, &nb_options, msg_out );
    if ( (rc != 0) && (rc != ENOENT) )
        return rc;
    else if ( rc != ENOENT )
    {
        /* parse the parameter */
        rc = read_update_item( &policy->fileclass, tmpstr, options, nb_options,
                               msg_out );
        if (rc)
            return rc;

        if ( (policy->fileclass.policy == UPDT_ON_EVENT) ||
             (policy->fileclass.policy == UPDT_ON_EVENT_PERIODIC) )
        {
            sprintf( msg_out, "Policy not supported for fileclass update: "
                    "only 'never', 'always' or 'periodic' allowed" );
            return EINVAL;
        }
    }

    CheckUnknownParameters( updt_block, UPDTPOLICY_BLOCK, update_expect );
    return 0;
}

static int reload_update_policy( updt_policy_t *policy )
{
    char buff1[256];
    char buff2[256];

    if ( (policies.updt_policy.md.policy != policy->md.policy)
         || (policies.updt_policy.md.period_min != policy->md.period_min )
         || (policies.updt_policy.md.period_max != policy->md.period_max ) )
    {
        DisplayLog( LVL_EVENT, RELOAD_TAG, UPDTPOLICY_BLOCK "::md_update updated: %s->%s",
                    update_policy2str( &policies.updt_policy.md, buff1),
                    update_policy2str( &policy->md, buff2 ) );
        policies.updt_policy.md = policy->md;
    }

#ifdef _HAVE_FID
    if ( (policies.updt_policy.path.policy != policy->path.policy)
         || (policies.updt_policy.path.period_min != policy->path.period_min )
         || (policies.updt_policy.path.period_max != policy->path.period_max ) )
    {
        DisplayLog( LVL_EVENT, RELOAD_TAG, UPDTPOLICY_BLOCK "::path_update updated: %s->%s",
                    update_policy2str( &policies.updt_policy.path, buff1),
                    update_policy2str( &policy->path, buff2 ) );
        policies.updt_policy.path = policy->path;
    }
#endif

    if ( (policies.updt_policy.fileclass.policy != policy->fileclass.policy)
         || (policies.updt_policy.fileclass.period_min != policy->fileclass.period_min )
         || (policies.updt_policy.fileclass.period_max != policy->fileclass.period_max ) )
    {
        DisplayLog( LVL_EVENT, RELOAD_TAG, UPDTPOLICY_BLOCK "::fileclass_update updated: %s->%s",
                    update_policy2str( &policies.updt_policy.fileclass, buff1),
                    update_policy2str( &policy->fileclass, buff2 ) );
        policies.updt_policy.fileclass = policy->fileclass;
    }

    return 0;
}




#ifdef HAVE_RMDIR_POLICY

static int set_default_rmdir_policy( rmdir_policy_t *policy, char *msg_out )
{
    policy->age_rm_empty_dirs = 0;      /* disable empty dir removal */

    policy->whitelist_rules = NULL;
    policy->whitelist_count = 0;

    /* directory to be removed recursively (even if not empty) */
    policy->recursive_rmdir_rules = NULL;
    policy->recursive_rmdir_count = 0;

    policy->global_attr_mask = 0;

    return 0;

}

static int write_default_rmdir_policy( FILE * output )
{
    print_begin_block( output, 0, RMDIR_BLOCK, NULL );
    print_line( output, 1, "age_rm_empty_dirs    : 0 (disabled)" );
    print_line( output, 1, "whitelist rules      : NONE" );
    print_line( output, 1, "recursive_rmdir rules: NONE" );
    print_end_block( output, 0 );

    return 0;
}

static int write_rmdir_policy_template( FILE * output )
{
    print_begin_block( output, 0, RMDIR_BLOCK, NULL );
    print_line( output, 1, "# Remove directories that have been empty for more than 8 days" );
    print_line( output, 1, "age_rm_empty_dirs   = 8d ;" );
    fprintf( output, "\n" );
    print_line( output, 1, "# don't remove dirs that owns to 'root' or whose depth < 2" );
    print_begin_block( output, 1, IGNORE_BLOCK, NULL );
    print_line( output, 2, "owner == root" );
    print_line( output, 2, "or depth < 2" );
    print_end_block( output, 1 );
    fprintf( output, "\n" );
    print_line( output, 1, "# Recursively remove directories matching this condition" );
    print_line( output, 1, "# (/!\\ removes all the directory content)" );
    print_begin_block( output, 1, RM_RECURSE_BLOCK, NULL );
    print_line( output, 2, "path == /mnt/lustre/old/*" );
    print_line( output, 2, "and last_mod > 30d" );
    print_end_block( output, 1 );
    print_end_block( output, 0 );

    return 0;
}


static int read_rmdir_policy( config_file_t config,  rmdir_policy_t *policy, char *msg_out,
                             int for_reload )
{
    unsigned int   blc_index;
    int            rc, tmpval;

    static const char *rmdir_expect[] = { IGNORE_BLOCK, RM_RECURSE_BLOCK, "age_rm_empty_dirs", NULL };

    /* get RMDIR block */

    config_item_t  rmdir_block = rh_config_FindItemByName( config, RMDIR_BLOCK );

    /* not mandatory */
    if ( rmdir_block == NULL )
    {
#ifdef _DEBUG_PARSING
        printf( "%s block not found in config file\n", RMDIR_BLOCK );
#endif
        return 0;
    }

    /* initialize global attributes mask */
    policy->global_attr_mask = 0;

    /* read white list rules */

    for ( blc_index = 0; blc_index < rh_config_GetNbItems( rmdir_block ); blc_index++ )
    {
        char          *block_name;
        config_item_t  curr_item = rh_config_GetItemByIndex( rmdir_block, blc_index );
        critical_err_check( curr_item, RMDIR_BLOCK );

        if ( rh_config_ItemType( curr_item ) != CONFIG_ITEM_BLOCK )
            continue;

        block_name = rh_config_GetBlockName( curr_item );
        critical_err_check( block_name, RMDIR_BLOCK );

#ifdef _DEBUG_PARSING
        printf( "subblock found in %s: %s\n", RMDIR_BLOCK, block_name );
#endif

        if ( !strcasecmp( block_name, IGNORE_BLOCK ) )
        {
            if ( policy->whitelist_count == 0 )
                policy->whitelist_rules =
                    ( whitelist_item_t * ) malloc( sizeof( whitelist_item_t ) );
            else
                policy->whitelist_rules =
                    ( whitelist_item_t * ) realloc( policy->whitelist_rules,
                                                    ( policy->whitelist_count +
                                                      1 ) * sizeof( whitelist_item_t ) );

            policy->whitelist_count++;

            /* analyze boolean expression */
            rc = GetBoolExpr( curr_item, block_name,
                              &policy->whitelist_rules[policy->whitelist_count - 1].bool_expr,
                              &policy->whitelist_rules[policy->whitelist_count - 1].attr_mask,
                              msg_out );

            if ( rc )
                return rc;

            policy->global_attr_mask |=
                policy->whitelist_rules[policy->whitelist_count - 1].attr_mask;
        }
        else if ( !strcasecmp( block_name, RM_RECURSE_BLOCK ) )
        {
            if ( policy->recursive_rmdir_count == 0 )
                policy->recursive_rmdir_rules =
                    ( whitelist_item_t * ) malloc( sizeof( whitelist_item_t ) );
            else
                policy->recursive_rmdir_rules =
                    ( whitelist_item_t * ) realloc( policy->recursive_rmdir_rules,
                                                    (policy->recursive_rmdir_count + 1)
                                                    * sizeof( whitelist_item_t ) );

            policy->recursive_rmdir_count++;

            /* analyze boolean expression */
            rc = GetBoolExpr( curr_item, block_name,
                              &policy->recursive_rmdir_rules[policy->recursive_rmdir_count - 1].bool_expr,
                              &policy->recursive_rmdir_rules[policy->recursive_rmdir_count - 1].attr_mask,
                              msg_out );

            if ( rc )
                return rc;

            policy->global_attr_mask |=
                policy->recursive_rmdir_rules[policy->recursive_rmdir_count - 1].attr_mask;
        }
        else
        {
            sprintf( msg_out, "Unknown sub-block '%s' in " RMDIR_BLOCK " block", block_name );
            return EINVAL;
        }

    }

    /* retrieve age_rm_empty_dirs */

    rc = GetDurationParam( rmdir_block, RMDIR_BLOCK, "age_rm_empty_dirs",
                           INT_PARAM_POSITIVE, &tmpval, NULL, NULL, msg_out );

#ifdef _DEBUG_PARSING
    if ( rc )
        printf( "no value found for age_rm_empty_dirs\n" );
    else
        printf( "value found for age_rm_empty_dirs: %d\n", tmpval );
#endif

    if ( rc != 0 && rc != ENOENT )
        return rc;
    else if ( rc != ENOENT )
        policy->age_rm_empty_dirs = tmpval;

    CheckUnknownParameters( rmdir_block, RMDIR_BLOCK, rmdir_expect );

    return 0;
}

/* USED FOR NOW */
__attribute__((unused))
static int reload_rmdir_policy( rmdir_policy_t *policy )
{
    /** @TODO prevent from concurrent access when rmdir policy is beeing checked */

    update_whitelist( policies.rmdir_policy.whitelist_rules, policies.rmdir_policy.whitelist_count,
                      policy->whitelist_rules, policy->whitelist_count, RMDIR_BLOCK );

    update_whitelist( policies.rmdir_policy.recursive_rmdir_rules, policies.rmdir_policy.recursive_rmdir_count,
                      policy->recursive_rmdir_rules, policy->recursive_rmdir_count, RMDIR_BLOCK );

    /* check age_rm_empty_dirs parameter */

    if ( policies.rmdir_policy.age_rm_empty_dirs != policy->age_rm_empty_dirs )
    {
        char           tmp_buff1[256];
        char           tmp_buff2[256];

        FormatDurationFloat( tmp_buff1, 256, policies.rmdir_policy.age_rm_empty_dirs );
        FormatDurationFloat( tmp_buff2, 256, policy->age_rm_empty_dirs );
        DisplayLog( LVL_EVENT, RELOAD_TAG, RMDIR_BLOCK "::age_rm_empty_dirs updated: %s->%s",
                    tmp_buff1, tmp_buff2 );
        policies.rmdir_policy.age_rm_empty_dirs = policy->age_rm_empty_dirs;
    }

    return 0;
}

#endif /* HAVE_RMDIR_POLICY */

static int set_default_filesets( fileset_list_t * fslist, char *msg_out )
{
    fslist->fileset_list = NULL;
    fslist->fileset_count = 0;

    fslist->global_attr_mask = 0;
    return 0;
}

#ifdef HAVE_RM_POLICY
static int set_default_unlink_policy( unlink_policy_t * upol, char *msg_out )
{
    upol->hsm_remove = TRUE; /* hsm_remove enabled */
    upol->deferred_remove_delay = 86400; /* 1 day */
    return 0;
}

static int write_default_unlink_policy( FILE * output )
{
    print_begin_block( output, 0, UNLINKPOLICY_BLOCK, NULL );
    print_line( output, 1, "hsm_remove       :  enabled");
    print_line( output, 1, "deferred_remove_delay:  1d");
    print_end_block( output, 0 );
    return 0;
}

static int write_unlink_policy_template( FILE * output )
{
    print_begin_block( output, 0, UNLINKPOLICY_BLOCK, NULL );
    print_line( output, 1, "# set this parameter to 'off' for disabling HSM object removal" );
    print_line( output, 1, "hsm_remove = enabled;");
    print_line( output, 1, "# delay before impacting object removal in HSM" );
    print_line( output, 1, "deferred_remove_delay = 24h;");
    print_end_block( output, 0 );
    return 0;
}
#endif

static int set_default_policy( policy_list_t * policy, char *msg_out, policy_type_t policy_type )
{
    policy->whitelist_rules = NULL;
    policy->whitelist_count = 0;

    policy->ignore_list = NULL;
    policy->ignore_count = 0;

    policy->policy_list = NULL;
    policy->policy_count = 0;

    policy->global_attr_mask = 0;

    return 0;
}

static int write_default_filesets( FILE * output )
{
    print_begin_block( output, 0, FILESETS_SECTION, NULL );
    print_line( output, 1, "# none" );
    print_end_block( output, 0 );

    return 0;
}

static int write_default_policy( FILE * output, policy_type_t policy_type )
{
    if ( policy_type == PURGE_POLICY )
        print_begin_block( output, 0, PURGEPOLICY_BLOCK, NULL );
    else if ( policy_type == MIGR_POLICY )
        print_begin_block( output, 0, MIGRPOLICY_BLOCK, NULL );

    print_line( output, 1, "# none" );
    print_end_block( output, 0 );
    return 0;
}

static int write_template_filesets( FILE * output )
{
    fprintf( output, "#### File Class definitions ####\n\n" );

    print_begin_block( output, 0, FILESETS_SECTION, NULL );

    print_begin_block( output, 1, FILESET_BLOCK, "Experiment_A" );

    print_begin_block( output, 2, DEFINITION_BLOCK, NULL );
    print_line( output, 3, "tree == \"/mnt/lustre/dir_A\"" );
    print_end_block( output, 2 );

#ifdef HAVE_MIGR_POLICY
    print_line( output, 2, "migration_hints = \"cos=3,class={FileClass},priority=2\" ;" );
#endif
#ifdef _LUSTRE_HSM
    print_line(output, 2, "# target archive");
    print_line(output, 2, "archive_id = 1 ;");
#endif
    print_end_block( output, 1 );

    fprintf( output, "\n" );

    print_begin_block( output, 1, FILESET_BLOCK, "visualization" );

    print_begin_block( output, 2, DEFINITION_BLOCK, NULL );
    print_line( output, 3, "tree == \"/mnt/lustre/dir_*\"" );
    print_line( output, 3, "and" );
    print_line( output, 3, "xattr.user.tag_visu == 1" );
    print_end_block( output, 2 );

#ifdef HAVE_MIGR_POLICY
    print_line( output, 2, "migration_hints = \"cos=4,class={Fileclass},priority=5\";" );
#endif
#ifdef _LUSTRE_HSM
    fprintf(output, "\n");
    print_line(output, 2, "# target archive");
    print_line(output, 2, "archive_id = 2 ;");
#endif
    print_end_block( output, 1 );
    fprintf( output, "\n" );

#ifdef _LUSTRE
    print_begin_block( output, 1, FILESET_BLOCK, "pool_ssd" );

    print_begin_block( output, 2, DEFINITION_BLOCK, NULL );
    print_line( output, 3, "ost_pool == \"ssd*\"" );
    print_end_block( output, 2 );
    print_end_block( output, 1 );
    fprintf( output, "\n" );
    print_begin_block( output, 1, FILESET_BLOCK, "ost_set" );
    print_begin_block( output, 2, DEFINITION_BLOCK, NULL );
    print_line( output, 3, "# condition on ost_index is true");
    print_line( output, 3, "# if one of the storage objects of the file");
    print_line( output, 3, "# matches each condition:" );
    print_line( output, 3, "# ost_index == 15 or ost_index == 20");
    print_line( output, 3, "# => a part of the file must be on OST 15 or 20" );
    print_line( output, 3, "# ost_index == 15 and ost_index == 20");
    print_line( output, 3, "# => the file must have objects at least on OST 15 and 20" );
    print_line( output, 3, "# ost_index != 12 and ost_index != 13");
    print_line( output, 3, "# => the file must not have objects on OSTs 12 and 13" );
    print_line( output, 3, "ost_index == 1 or ost_index == 2 or");
    print_line( output, 3, "ost_index == 1 or ost_index == 2 or");
    print_line( output, 3, "ost_index == 3 or ost_index == 4");
    print_end_block( output, 2 );
    print_end_block( output, 1 );
    fprintf( output, "\n" );
#endif

    print_line( output, 1, "# defining fileclass as a union or intersection:" );
    print_begin_block( output, 1, FILESET_BLOCK, "visu_expA" );
    print_begin_block( output, 2, DEFINITION_BLOCK, NULL );
    print_line( output, 2, "visualization inter Experiment_A" );
    print_end_block( output, 2 );
    print_end_block( output, 1 );
    fprintf( output, "\n" );

    print_end_block( output, 0 );

    fprintf( output, "\n" );

    return 0;
}

#ifdef HAVE_MIGR_POLICY

static int write_migration_policy_template( FILE * output )
{

    fprintf( output, "#### Migration policies (archiving) ####\n\n" );

    print_begin_block( output, 0, MIGRPOLICY_BLOCK, NULL );


    print_begin_block( output, 1, POLICY_BLOCK, "standard_copy" );

    print_line( output, 2, "target_fileclass = experiment_A;" );
    print_line( output, 2, "target_fileclass = pool_ssd;" );
    fprintf( output, "\n" );

    print_line( output, 2, "# Copy a file 6hours after its creation if it as never been archived.");
    print_line( output, 2, "# For next changes, archive it daily.");
    print_line( output, 2, "# In all cases, do nothing when it has been modified too recently (-30min).");
    print_begin_block( output, 2, CONDITION_BLOCK, NULL );
    print_line( output, 3, "((last_archive == 0 and creation > 6h) " );
    print_line( output, 3, "  or last_archive > 1d)" );
    print_line( output, 3, "and last_mod > 30min");
    print_end_block( output, 2 );

#ifdef _LUSTRE_HSM
    fprintf(output, "\n");
    print_line(output, 2, "# target archive (/!\\ policy archive_id overrides fileset archive_id)");
    print_line(output, 2, "archive_id = 3 ;");
#endif

    print_end_block( output, 1 );
    fprintf( output, "\n" );

    print_begin_block( output, 1, POLICY_BLOCK, "visu_copy" );

    print_line( output, 2, "target_fileclass = visualization ;" );
    fprintf( output, "\n" );

    print_line( output, 2, "# copy those files quickly after they have been modified" );
    print_line( output, 2, "# or if they have been archived more that 6h ago" );
    print_line( output, 2, "# (if they are still beeing modified)" );

    print_begin_block( output, 2, CONDITION_BLOCK, NULL );
    print_line( output, 3, "last_mod > 1h" );
    print_line( output, 3, "or" );
    print_line( output, 3, "last_archive > 6h" );
    print_end_block( output, 2 );

    print_end_block( output, 1 );
    fprintf( output, "\n" );

    print_line( output, 1, "# Default migration policy." );
    print_line( output, 1, "# This applies to files that don't match previous fileclasses, i.e:" );
    print_line( output, 1, "#   - don't match the 'ignore' block" );
    print_line( output, 1, "#   - don't match a fileclass of 'ignore_fileclass' directives" );
    print_line( output, 1, "#   - don't match any 'target_fileclass' of migration policies above" );
    print_begin_block( output, 1, POLICY_BLOCK, "default" );

    print_begin_block( output, 2, CONDITION_BLOCK, NULL );
    print_line( output, 3, "last_mod > 12h" );
    print_end_block( output, 2 );

#ifdef _LUSTRE_HSM
    fprintf(output, "\n");
    print_line(output, 2, "# target archive");
    print_line(output, 2, "archive_id = 2 ;");
#endif

    print_end_block( output, 1 );


    print_end_block( output, 0 );
    fprintf( output, "\n" );

    return 0;
}
#endif

#ifdef HAVE_PURGE_POLICY
#ifndef _LUSTRE_HSM
/* Template for NON-lustre-HSM purposes */
static int write_purge_policy_template( FILE * output )
{
    print_begin_block( output, 0, PURGEPOLICY_BLOCK, NULL );

    print_line( output, 1, "# Do not purge files accessed recently, or whose size is 0," );
    print_line( output, 1, "# or located in \"/mnt/lustre/system_files\" directory." );
    print_begin_block( output, 1, IGNORE_BLOCK, NULL );
    print_line( output, 2, "last_access < 1h" );
    print_line( output, 2, "or size == 0" );
    print_line( output, 2, "or tree == \"/mnt/lustre/system_files\"" );
    print_end_block( output, 1 );

    fprintf( output, "\n" );

#ifdef _LUSTRE
    print_line( output, 1, "# do not purge files in FileClass \"pool_ssd\"" );
    print_line( output, 1, IGNORE_FC " = pool_ssd ;" );
    fprintf( output, "\n" );
#endif

    print_line( output, 1, "# Purge files of class 'Experiment_A' after 24h" );
    print_begin_block( output, 1, POLICY_BLOCK, "expA_1day" );

    print_line( output, 2, "target_fileclass = Experiment_A ;" );

    print_begin_block( output, 2, CONDITION_BLOCK, NULL );
    print_line( output, 3, "last_mod > 24h" );
    print_line( output, 3, "and" );
    print_line( output, 3, "last_access > 6h" );
    print_end_block( output, 2 );

    print_end_block( output, 1 );
    fprintf( output, "\n" );

    print_line( output, 1, "# Release files of class 'visualization' after 1 month," );
    print_line( output, 1, "# if they are not accessed for more that 3 days" );
    print_begin_block( output, 1, POLICY_BLOCK, "visu_1month" );

    print_line( output, 2, "target_fileclass = visualization ;" );

    print_begin_block( output, 2, CONDITION_BLOCK, NULL );
    print_line( output, 3, "last_mod > 30d" );
    print_line( output, 3, "and" );
    print_line( output, 3, "last_access > 3d" );
    print_end_block( output, 2 );

    print_end_block( output, 1 );
    fprintf( output, "\n" );

    print_line( output, 1, "# Default purge policy." );
    print_line( output, 1, "# This applies to files that don't match previous fileclasses, i.e:" );
    print_line( output, 1, "#   - don't match the 'ignore' block" );
    print_line( output, 1, "#   - don't match a fileclass of 'ignore_fileclass' directives" );
    print_line( output, 1, "#   - don't match any 'target_fileclass' of purge policies above" );
    print_begin_block( output, 1, POLICY_BLOCK, "default" );

    print_begin_block( output, 2, CONDITION_BLOCK, NULL );
    print_line( output, 3, "last_access > 12h" );
    print_end_block( output, 2 );

    print_end_block( output, 1 );

    print_end_block( output, 0 );
    fprintf( output, "\n" );

    return 0;
}

#else

static int write_purge_policy_template( FILE * output )
{
    fprintf( output, "#### Purge policies (space release) ####\n\n" );

    print_begin_block( output, 0, PURGEPOLICY_BLOCK, NULL );

    print_line( output, 1, "# do not purge files owned by \"foo\" or \"charlie\"" );
    print_begin_block( output, 1, IGNORE_BLOCK, NULL );
    print_line( output, 2, "owner == \"foo\"" );
    print_line( output, 2, "or" );
    print_line( output, 2, "owner == \"charlie\"" );
    print_end_block( output, 1 );
    fprintf( output, "\n" );

    print_line( output, 1, "# do not purge files in FileClass \"pool_ssd\"" );
    print_line( output, 1, IGNORE_FC " = pool_ssd ;" );
    fprintf( output, "\n" );

    print_begin_block( output, 1, POLICY_BLOCK, "Experiment_A_purge" );

    print_line( output, 2, "target_fileclass = experiment_A ;" );
    fprintf( output, "\n" );

    print_line( output, 2, "# purge files not accessed within the last 6 hours," );
    print_line( output, 2, "# and not copied-in within the last 12 hours" );
    print_line( output, 2, "# and copied-out more that 2 hours ago" );

    print_begin_block( output, 2, CONDITION_BLOCK, NULL );
    print_line( output, 3, "last_access > 6h" );
    print_line( output, 3, "and" );
    print_line( output, 3, "last_restore > 12h" );
    print_line( output, 3, "and" );
    print_line( output, 3, "last_archive > 2h" );
    print_end_block( output, 2 );

    print_end_block( output, 1 );
    fprintf( output, "\n" );

    print_begin_block( output, 1, POLICY_BLOCK, "visu_purge" );

    print_line( output, 2, "target_fileclass = visualization ;" );
    fprintf( output, "\n" );

    print_line( output, 2, "# purge files not accessed within the last day," );
    print_line( output, 2, "# or modified during the week" );

    print_begin_block( output, 2, CONDITION_BLOCK, NULL );
    print_line( output, 3, "last_access > 1d" );
    print_line( output, 3, "and" );
    print_line( output, 3, "last_mod > 7d" );
    print_end_block( output, 2 );

    print_end_block( output, 1 );
    fprintf( output, "\n" );

    print_line( output, 1, "# Default purge policy." );
    print_line( output, 1, "# This applies to files that don't match previous fileclasses, i.e:" );
    print_line( output, 1, "#   - don't match the 'ignore' block" );
    print_line( output, 1, "#   - don't match a fileclass of 'ignore_fileclass' directives" );
    print_line( output, 1, "#   - don't match any 'target_fileclass' of purge policies above" );
    print_begin_block( output, 1, POLICY_BLOCK, "default" );

    print_begin_block( output, 2, CONDITION_BLOCK, NULL );
    print_line( output, 3, "last_access > 12h" );
    print_end_block( output, 2 );

    print_end_block( output, 1 );

    print_end_block( output, 0 );

    fprintf( output, "\n" );

    return 0;
}
#endif /* HSM switch */
#endif /* purge policy */

/* get attribute mask for hints */
static int hints_mask(  char * hints )
{
    int mask = 0;
    char * pass_begin = hints;
    char * begin_var;
    char * end_var;
    char varname[128];

    do
    {
        /* look for a variable */
        begin_var = strchr( pass_begin, '{' );

        /* no more variables */
        if ( !begin_var )
            break;

        /* get matching '}' */
        end_var = strchr( begin_var, '}' );
        if (!end_var)
        {
           DisplayLog(LVL_CRIT,CHK_TAG, "ERROR: unmatched '{' in migration hints '%s'", hints);
           return -1;
        }

        memset( varname, 0, sizeof(varname) );
        strncpy(varname, begin_var+1, end_var-begin_var-1);

        if (!strcasecmp( varname, "path" ) )
           mask |= ATTR_MASK_fullpath;
        else if (!strcasecmp( varname, "name" ) )
           mask |= ATTR_MASK_name;
        else if (!strcasecmp( varname, "ost_pool" ) )
           mask |= ATTR_MASK_stripe_info;
        else if (strcasecmp( varname, "policy" ) &&
                 strcasecmp( varname, "fileclass" ) )
        {
            DisplayLog(LVL_CRIT,CHK_TAG, "ERROR: unknown parameter '%s' in hints '%s'", varname, hints);
            return -EINVAL;
        }

        pass_begin = end_var + 1;

    } while(1);

    return mask;

}


/** Read filesets block */
static int read_filesets( config_file_t config, fileset_list_t * fileset_list,
                          char *msg_out, int for_reload )
{
    unsigned int   i, j;
    int            rc;

    /* get Filesets block */

    config_item_t  fileset_block
        = rh_config_FindItemByName( config, FILESETS_SECTION );

    /* not mandatory */
    if ( fileset_block == NULL )
        return 0;

    /* initialize global attributes mask */
    fileset_list->global_attr_mask = 0;

    fileset_list->fileset_count = rh_config_GetNbItems( fileset_block );

    /* allocate the fileset list (1/sub-block) */
    if ( fileset_list->fileset_count > 0 )
    {
        fileset_list->fileset_list =
            ( fileset_item_t * ) calloc( fileset_list->fileset_count,
                                         sizeof( fileset_item_t ) );
        if ( fileset_list->fileset_list == NULL )
            return ENOMEM;
    }
    else
        fileset_list->fileset_list = NULL;

    for ( i = 0; i < fileset_list->fileset_count; i++ )
    {
        char          *block_name, *fsname;
        config_item_t  curr_class = rh_config_GetItemByIndex( fileset_block, i );
        critical_err_check_goto( curr_class, FILESETS_SECTION, clean_filesets );
        int definition_done;

        if ( rh_config_ItemType( curr_class ) != CONFIG_ITEM_BLOCK )
        {
            strcpy( msg_out,
                    "Only " FILESET_BLOCK " sub-blocks are expected in " FILESETS_SECTION
                    " section" );
            rc = EINVAL;
            goto clean_filesets;
        }
        block_name = rh_config_GetBlockName( curr_class );
        critical_err_check_goto( block_name, FILESETS_SECTION, clean_filesets );

        definition_done = FALSE;

        if ( !strcasecmp( block_name, FILESET_BLOCK ) )
        {

            /* get fileset name */
            fsname = rh_config_GetBlockId( curr_class );

            if ( ( fsname == NULL ) || ( strlen( fsname ) == 0 ) )
            {
                sprintf( msg_out, "Fileclass name expected for block "
                         FILESET_BLOCK ", line %d. E.g. " FILESET_BLOCK " user_files { ...",
                         rh_config_GetItemLine( curr_class ) );
                rc = EINVAL;
                goto clean_filesets;
            }


            /* check that class name is not already used */
            for ( j = 0; j < i; j++ )
            {
                if ( !strcasecmp( fsname, fileset_list->fileset_list[j].fileset_id ) )
                {
                    sprintf( msg_out, "Fileclass '%s' is already defined in block #%d.",
                             fileset_list->fileset_list[j].fileset_id, j + 1 );
                    rc = EINVAL;
                    goto clean_filesets;
                }
            }

            rh_strncpy(fileset_list->fileset_list[i].fileset_id, fsname, FILESET_ID_LEN);

            /* read file class block content */
            for ( j = 0; j < rh_config_GetNbItems( curr_class ); j++ )
            {
                config_item_t  sub_item = rh_config_GetItemByIndex( curr_class, j );
                critical_err_check_goto( sub_item, FILESET_BLOCK, clean_filesets );
                char          *subitem_name;

                switch ( rh_config_ItemType( sub_item ) )
                {
                    case CONFIG_ITEM_BLOCK:
                    {
                        subitem_name = rh_config_GetBlockName( sub_item );
                        critical_err_check_goto( subitem_name, FILESET_BLOCK, clean_filesets );

                        if ( strcasecmp( subitem_name, DEFINITION_BLOCK ) != 0 )
                        {
                            sprintf( msg_out,
                                     "'%s' sub-block unexpected in " FILESET_BLOCK " block, line %d.",
                                     subitem_name, rh_config_GetItemLine( sub_item ) );
                            rc = EINVAL;
                            goto clean_filesets;
                        }

                        /* check double definition */
                        if ( definition_done )
                        {
                            sprintf( msg_out, "Double fileclass definition in "
                                     FILESET_BLOCK " block, line %d.",
                                     rh_config_GetItemLine( sub_item ) );
                            rc = EINVAL;
                            goto clean_filesets;
                        }

                        /* 2 possible definition types expected: boolean expression
                         * or fileset union and/or intersection */
                        switch ( rh_config_ContentType( sub_item ) )
                        {
                            case CONFIG_ITEM_BOOL_EXPR:
                                /* analyze boolean expression */
                                rc = GetBoolExpr( sub_item, DEFINITION_BLOCK,
                                                  &fileset_list->fileset_list[i].definition,
                                                  &fileset_list->fileset_list[i].attr_mask, msg_out );
                                if ( rc )
                                    goto clean_filesets;
                                break;

                            case CONFIG_ITEM_SET:
                                /* Build a policy boolean expression from a
                                 * union/intersection or fileclasses */
                                rc = GetSetExpr( sub_item, DEFINITION_BLOCK,
                                                 &fileset_list->fileset_list[i].definition,
                                                 &fileset_list->fileset_list[i].attr_mask,
                                                 fileset_list, msg_out );
                                if ( rc )
                                    goto clean_filesets;
                                break;

                            default:
                                sprintf( msg_out, "Boolean expression or set-based definition expected in block '%s', "
                                         "line %d", subitem_name,
                                         rh_config_GetItemLine( ( config_item_t ) sub_item ) );
                                return EINVAL;
                        }

                        fileset_list->global_attr_mask |= fileset_list->fileset_list[i].attr_mask;
                        definition_done = TRUE;


                        if (  fileset_list->fileset_list[i].attr_mask & (
#ifdef ATTR_INDEX_last_archive
                            ATTR_MASK_last_archive |
#endif
#ifdef ATTR_INDEX_last_restore
                            ATTR_MASK_last_restore |
#endif
                            ATTR_MASK_last_access | ATTR_MASK_last_mod ) )
                        {
                           DisplayLog( LVL_MAJOR, CHK_TAG, "WARNING: in FileClass '%s', line %d: "
                                       "time-based conditions should be specified in policy condition instead of file class definition",
                                       fileset_list->fileset_list[i].fileset_id, rh_config_GetItemLine( sub_item ));
                        }
                        break;
                    }
                    case CONFIG_ITEM_VAR:
                    {
                        char          *value = NULL;
                        int            extra_args = FALSE;

                        rc = rh_config_GetKeyValue( sub_item, &subitem_name, &value, &extra_args );
                        if ( rc )
                            goto clean_filesets;

#ifdef HAVE_MIGR_POLICY
                        /* handle migration hints (if supported) */
                        if ( strcasecmp( subitem_name, "migration_hints" ) == 0 )
                        {
                            if ( extra_args )
                            {
                                sprintf( msg_out,
                                         "Unexpected arguments for migration_hints parameter, line %d.",
                                         rh_config_GetItemLine( sub_item ) );
                                rc = EINVAL;
                                goto clean_filesets;
                            }

                            /* get attribute mask for this hint */
                            rc = hints_mask( value );
                            if ( rc < 0 )
                                goto clean_filesets;
                            fileset_list->fileset_list[i].attr_mask |= rc;
                            fileset_list->global_attr_mask |= rc;

                            /* append migration hints */
                            if ( EMPTY_STRING( fileset_list->fileset_list[i].migration_hints ) )
                            {
                                if ( strlen( value ) > HINTS_LEN )
                                {
                                    sprintf( msg_out,
                                             "Value to large for migration_hints line %d (max: %d).",
                                             rh_config_GetItemLine( sub_item ), HINTS_LEN );
                                    rc = EOVERFLOW;
                                    goto clean_filesets;
                                }

                                strcpy( fileset_list->fileset_list[i].migration_hints, value );
                            }
                            else        /* append with ',' */
                            {
                                int            prev_len =
                                    strlen( fileset_list->fileset_list[i].migration_hints );
                                if ( prev_len + strlen( value ) + 1 > HINTS_LEN )
                                {
                                    sprintf( msg_out,
                                             "Value to large for migration_hints line %d (max: %d).",
                                             rh_config_GetItemLine( sub_item ), HINTS_LEN );
                                    rc = EOVERFLOW;
                                    goto clean_filesets;
                                }

                                fileset_list->fileset_list[i].migration_hints[prev_len] = ',';
                                strcpy( fileset_list->fileset_list[i].migration_hints + prev_len + 1,
                                        value );
                            }
                        }
                        /* only migration hints expected */
                        else
#endif
#ifdef _LUSTRE_HSM
                        /* manage archive_id */
                        if (!strcasecmp(subitem_name,"archive_id")
                            || !strcasecmp(subitem_name,"archive_num")) /* for backward compat. */
                        {
                            int tmp;

                            if ( extra_args )
                            {
                                sprintf(msg_out,
                                        "Unexpected arguments for archive_id parameter, line %d.",
                                        rh_config_GetItemLine(sub_item));
                                rc = EINVAL;
                                goto clean_filesets;
                            }
                            tmp = str2int( value );
                            if ( tmp <= 0 )
                            {
                                sprintf(msg_out,
                                        "Positive integer expected for archive_id parameter, line %d.",
                                        rh_config_GetItemLine(sub_item));
                                rc = EINVAL;
                                goto clean_filesets;
                            }
                            fileset_list->fileset_list[i].archive_id = tmp;
                        }
                        else
#endif
                        {
                            DisplayLog( LVL_CRIT, "Config Check",
                                        "WARNING: unknown parameter '%s' in block '%s' line %d",
                                        subitem_name, FILESET_BLOCK, rh_config_GetItemLine( sub_item ) );
/*                            sprintf( msg_out,
                                     "'%s' parameter unexpected in " FILESET_BLOCK " block, line %d.",
                                     subitem_name, rh_config_GetItemLine( sub_item ) );
                            rc = EINVAL;
                            goto clean_filesets; */
                        }
                        break;
                    }
                    default :
                        /* unexpected content */
                            sprintf( msg_out,
                                     "Unexpected item in "FILESET_BLOCK" block, line %d.",
                                     rh_config_GetItemLine( sub_item ) );
                            rc = EINVAL;
                            goto clean_filesets;
                } /* switch on item type */

            } /* loop on "fileclass" block content */

            if (!definition_done)
            {
                sprintf( msg_out,
                         "No definition in file class '%s', line %d", fsname,
                         rh_config_GetItemLine( curr_class ) );
                rc = ENOENT;
                goto clean_filesets;
            }

        }                       /* end of fileclass" block */
        else
        {
            sprintf( msg_out, "'%s' sub-block unexpected in %s section, line %d.",
                     block_name, FILESETS_SECTION, rh_config_GetItemLine( curr_class ));
            rc = EINVAL;
            goto clean_filesets;
        }


    }                           /* end of "filesets" section */

    return 0;

  clean_filesets:
    free( fileset_list->fileset_list );
    fileset_list->fileset_list = NULL;
    fileset_list->fileset_count = 0;
    return rc;
}


fileset_item_t *GetFilesetByName( fileset_list_t * filesets, const char *name )
{
    int            i;
    for ( i = 0; i < filesets->fileset_count; i++ )
    {
        if ( !strcasecmp( filesets->fileset_list[i].fileset_id, name ) )
            return &filesets->fileset_list[i];
    }
    return NULL;                /* not found */
}

#ifdef HAVE_RM_POLICY
/** Read filesets block */
static int read_unlink_policy( config_file_t config, unlink_policy_t * pol, char *msg_out,
                               int for_reload )
{
    int            rc;
    int            intval;

    static const char *allowed[] = {
        "hsm_remove", "deferred_remove_delay", NULL };

    /* get unlink policy block */

    config_item_t  param_block = rh_config_FindItemByName( config, UNLINKPOLICY_BLOCK );
    if ( param_block == NULL )
    {
        /* no error, because no parameter is mandatory */
        return 0;
    }

    /* check this is a block... */
    if ( rh_config_ItemType( param_block ) != CONFIG_ITEM_BLOCK )
    {
        strcpy( msg_out, "A block is expected for '" UNLINKPOLICY_BLOCK "' item" );
        return EINVAL;
    }

    /* parse parameters */
    rc = GetBoolParam( param_block, UNLINKPOLICY_BLOCK, "hsm_remove",
                       0, &intval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        pol->hsm_remove = intval;

    rc = GetDurationParam( param_block, UNLINKPOLICY_BLOCK, "deferred_remove_delay",
                           INT_PARAM_POSITIVE, &intval, NULL, NULL, msg_out );
    if ( ( rc != 0 ) && ( rc != ENOENT ) )
        return rc;
    else if ( rc != ENOENT )
        pol->deferred_remove_delay = intval;

    CheckUnknownParameters( param_block, UNLINKPOLICY_BLOCK, allowed );

    return 0;

}
#endif


static int parse_policy_block( config_item_t config_item,
                               const char *block_name,
                               policies_t * all_policies,
                               policy_item_t * output_policy,
                               policy_type_t policy_type, char *msg_out )
{
    char          *polname;
    int            is_default = FALSE;
    int            i, j, k, rc, mask;
    int            definition_done = FALSE;
    policy_list_t *this_list = NULL;

    /* initialize output */
    memset( output_policy, 0, sizeof( policy_item_t ) );

    /* get policy id */
    polname = rh_config_GetBlockId( config_item );
    critical_err_check( polname, block_name );

    /* check that this policy name is not already used for this type of policy */
#ifdef HAVE_PURGE_POLICY
    if ( policy_type == PURGE_POLICY )
        this_list = &all_policies->purge_policies;
#endif
#if defined(HAVE_PURGE_POLICY) && defined(HAVE_MIGR_POLICY)
    else
#endif
#ifdef HAVE_MIGR_POLICY
    if ( policy_type == MIGR_POLICY )
        this_list = &all_policies->migr_policies;
#endif
    else
    {
        sprintf( msg_out, "Unexpected policy type %d", policy_type );
        return EINVAL;
    }

    for ( i = 0; i < this_list->policy_count; i++ )
    {
        if ( !strcasecmp( polname, this_list->policy_list[i].policy_id ) )
        {
            sprintf( msg_out, "Policy name '%s' (line %d) is already used for another policy.",
                     polname, rh_config_GetItemLine( config_item ) );
            return EINVAL;
        }
    }

    /* is it a default block? */
    is_default = !strcasecmp( polname, "default" );

    /* save policy id */
    rh_strncpy(output_policy->policy_id, polname, POLICY_ID_LEN);

    /* read file block content */
    for ( i = 0; i < rh_config_GetNbItems( config_item ); i++ )
    {
        config_item_t  sub_item = rh_config_GetItemByIndex( config_item, i );
        critical_err_check( sub_item, block_name );
        char          *subitem_name;

        if ( rh_config_ItemType( sub_item ) == CONFIG_ITEM_BLOCK )
        {
            subitem_name = rh_config_GetBlockName( sub_item );
            critical_err_check( subitem_name, block_name );

            if ( strcasecmp( subitem_name, CONDITION_BLOCK ) != 0 )
            {
                sprintf( msg_out, "'%s' sub-block unexpected in %s block, line %d.",
                         subitem_name, block_name, rh_config_GetItemLine( sub_item ) );
                return EINVAL;
            }

            /* check double condition */
            if ( definition_done )
            {
                sprintf( msg_out, "Double condition in policy %s, line %d.",
                         polname, rh_config_GetItemLine( sub_item ) );
                return EINVAL;
            }

            /* analyze boolean expression */
            mask = 0;
            rc = GetBoolExpr( sub_item, CONDITION_BLOCK,
                              &output_policy->condition, &mask, msg_out );
            if ( rc )
                return rc;

            output_policy->attr_mask |= mask;
            definition_done = TRUE;
        }
        else                    /* not a block */
        {
            char          *value = NULL;
            int            extra_args = FALSE;
            fileset_item_t *fs;

            rc = rh_config_GetKeyValue( sub_item, &subitem_name, &value, &extra_args );
            if ( rc )
                return rc;

            /* expected : target filesets or migration hints (for migrations policies) */

            if ( !strcasecmp( subitem_name, "target_fileclass" ) )
            {
                if ( is_default )
                {
                    sprintf( msg_out, "No target_fileclass expected for default policy, line %d.",
                             rh_config_GetItemLine( sub_item ) );
                    /** @TODO free targets */
                    return EINVAL;
                }

                if ( extra_args )
                {
                    sprintf( msg_out,
                             "Unexpected arguments for %s parameter, line %d.",
                             subitem_name, rh_config_GetItemLine( sub_item ) );
                    return EINVAL;
                }

                /* get the associated fileset */
                fs = GetFilesetByName( &all_policies->filesets, value );

                if ( fs == NULL )
                {
                    sprintf( msg_out,
                             "Policy definition references unknown fileclass '%s', line %d.", value,
                             rh_config_GetItemLine( sub_item ) );
                    return EINVAL;
                }
#ifdef HAVE_PURGE_POLICY
                else if ( policy_type == PURGE_POLICY )
                    fs->has_purge_policy = TRUE;
#endif
#ifdef HAVE_MIGR_POLICY
                else if ( policy_type == MIGR_POLICY )
                    fs->has_migration_policy = TRUE;
#endif

                /* check that the fileset is not already referenced in the policy */
                for ( j = 0; j < this_list->policy_count; j++ )
                {
                    for ( k = 0; k < this_list->policy_list[j].target_count; k++ )
                        if ( fs == this_list->policy_list[j].target_list[k] )
                        {
                            sprintf( msg_out,
                                     "Fileclass '%s' is already a target of policy '%s'",
                                     value, this_list->policy_list[j].policy_id );
                            return EINVAL;
                        }
                }

                /* also check ignore_fileclass directives */
                for ( j = 0; j < this_list->ignore_count; j++ )
                {
                    if ( fs == this_list->ignore_list[j] )
                    {
                        sprintf( msg_out,
                                 "Fileclass '%s' is simultaneously ignored and referenced as a target for policy '%s'",
                                 value, polname );
                        return EINVAL;
                    }
                }

                /* append the fileset list */
                if ( output_policy->target_list == NULL )
                {
                    output_policy->target_list =
                        ( fileset_item_t ** ) malloc( sizeof( fileset_item_t * ) );
                    output_policy->target_count = 1;
                    output_policy->target_list[0] = fs;
                }
                else
                {
                    output_policy->target_list = ( fileset_item_t ** )
                        realloc( output_policy->target_list, ( output_policy->target_count + 1 ) *
                                 sizeof( fileset_item_t * ) );
                    output_policy->target_list[output_policy->target_count] = fs;
                    output_policy->target_count++;
                }

                /* add fileset mask to policy mask */
                output_policy->attr_mask |= fs->attr_mask;

            }
            else if ( !strcasecmp( subitem_name, "migration_hints" ) )
            {
                if ( policy_type != MIGR_POLICY )
                {
                    sprintf( msg_out, "migration_hints defined in a non-migration policy, line %d.",
                             rh_config_GetItemLine( sub_item ) );
                    return EINVAL;
                }

                if ( extra_args )
                {
                    sprintf( msg_out,
                             "Unexpected arguments for %s parameter, line %d.",
                             subitem_name, rh_config_GetItemLine( sub_item ) );
                    return EINVAL;
                }

                /* get attribute mask for this hint */
                rc = hints_mask( value );
                if ( rc < 0 )
                    return rc;
                output_policy->attr_mask |= rc;

                /* append migration hints */
                if ( EMPTY_STRING( output_policy->hints ) )
                {
                    if ( strlen( value ) > HINTS_LEN )
                    {
                        sprintf( msg_out, "String too large for migration_hints line %d (max: %d).",
                                 rh_config_GetItemLine( sub_item ), HINTS_LEN );
                        return EOVERFLOW;
                    }

                    strcpy( output_policy->hints, value );
                }
                else            /* append with ',' */
                {
                    int            prev_len = strlen( output_policy->hints );
                    if ( prev_len + strlen( value ) + 1 > HINTS_LEN )
                    {
                        sprintf( msg_out, "String too large for migration_hints line %d (max: %d).",
                                 rh_config_GetItemLine( sub_item ), HINTS_LEN );
                        return EOVERFLOW;
                    }

                    output_policy->hints[prev_len] = ',';
                    strcpy( output_policy->hints + prev_len + 1, value );
                }
            }
#ifdef _LUSTRE_HSM
            else if (!strcasecmp(subitem_name, "archive_id")
                     || !strcasecmp(subitem_name, "archive_num")) /* for backward compat */
            {
                int tmp;

                if ( policy_type != MIGR_POLICY )
                {
                    sprintf(msg_out, "archive_id defined in a non-migration policy, line %d.",
                            rh_config_GetItemLine(sub_item));
                    return EINVAL;
                }

                if ( extra_args )
                {
                    sprintf( msg_out,
                             "Unexpected arguments for %s parameter, line %d.",
                             subitem_name, rh_config_GetItemLine( sub_item ) );
                    return EINVAL;
                }

                tmp = str2int( value );
                if ( tmp <= 0 )
                {
                    sprintf(msg_out,
                            "Positive integer expected for archive_id parameter, line %d.",
                            rh_config_GetItemLine(sub_item));
                    return EINVAL;
                }
                output_policy->archive_id = tmp;
            }
#endif
            else
            {
                DisplayLog( LVL_CRIT, "Config Check",
                            "WARNING: unknown parameter '%s' in block '%s' line %d",
                            subitem_name, block_name, rh_config_GetItemLine( sub_item ) );
                /*sprintf( msg_out, "'%s' parameter unexpected in %s block, line %d.",
                         subitem_name, block_name, rh_config_GetItemLine( sub_item ) );
                return EINVAL;*/
            }


        }                       /* end of vars */
    }                           /* loop on "policy" block content */

    if ( !definition_done )
    {
        DisplayLog( LVL_MAJOR, CHK_TAG, "WARNING: in policy '%s', line %d: no condition specified!", polname,
                rh_config_GetItemLine( config_item ) );
    }

    return 0;

}





/* macro for preallocating array depending on configuration blocks in Read_Policy_ */
#define PREALLOC_ARRAY_CONFIG( _block_name_, _type_, _array_var, _goto_label )      \
    do {                                                                            \
        count = rh_config_CountItemNames( section, _block_name_ );                  \
        if ( count > 0 )                                                            \
        {                                                                           \
            policy_list->_array_var = (_type_ *)calloc( count, sizeof(_type_) );    \
            if ( policy_list->_array_var == NULL )                                  \
            {                                                                       \
                rc = ENOMEM;                                                        \
                goto _goto_label;                                                   \
            }                                                                       \
        }                                                                           \
        else if ( count == 0 )                                                      \
        {                                                                           \
            policy_list->_array_var = NULL;                                         \
        }                                                                           \
        else                                                                        \
        {                                                                           \
            rc = -1;                                                                \
            goto _goto_label;                                                       \
        }                                                                           \
    }                                                                               \
    while(0)


static int read_policy( config_file_t config, policies_t * policies, char *msg_out,
                        int for_reload, policy_type_t policy_type )
{
    /* is current trigger list the default ? */
    unsigned int   i, j, k;
    int            rc, count;

    policy_list_t *policy_list;
    config_item_t  section;
    char          *section_name;

/* macros for cleaner code */
#define curr_ign        policy_list->whitelist_count
#define curr_ign_fc     policy_list->ignore_count
#define curr_pol        policy_list->policy_count

#ifdef HAVE_PURGE_POLICY
    if ( policy_type == PURGE_POLICY )
    {
        policy_list = &policies->purge_policies;
        section_name = PURGEPOLICY_BLOCK;
    }
#endif
#if defined(HAVE_MIGR_POLICY) && defined(HAVE_PURGE_POLICY)
    else
#endif
#ifdef HAVE_MIGR_POLICY
    if ( policy_type == MIGR_POLICY )
    {
        policy_list = &policies->migr_policies;
        section_name = MIGRPOLICY_BLOCK;
    }
#endif
    else
    {
        strcpy(msg_out,"Policy type not supported for this purpose");
        return ENOTSUP;
    }

    /* initialize output */
    memset( policy_list, 0, sizeof( *policy_list ) );

    /* get policy section */

    section = rh_config_FindItemByName( config, section_name );

    /* not mandatory */
    if ( section == NULL )
        return 0;

    /* prealloc config arrays */
    PREALLOC_ARRAY_CONFIG( IGNORE_BLOCK, whitelist_item_t, whitelist_rules, err );
    PREALLOC_ARRAY_CONFIG( IGNORE_FC, fileset_item_t *, ignore_list, free_ignore );
    PREALLOC_ARRAY_CONFIG( "policy", policy_item_t, policy_list, free_ignore_fc );

    /* initialize global attributes mask */
    policy_list->global_attr_mask = 0;

    count = rh_config_GetNbItems( section );

    /* read sub-blocks */

    for ( i = 0; i < count; i++ )
    {
        char          *item_name;
        config_item_t  curr_item = rh_config_GetItemByIndex( section, i );
        critical_err_check_goto( curr_item, section_name, free_policy );

        if ( rh_config_ItemType( curr_item ) == CONFIG_ITEM_BLOCK )
        {
            item_name = rh_config_GetBlockName( curr_item );
            critical_err_check_goto( item_name, section_name, free_policy );

            if ( !strcasecmp( item_name, IGNORE_BLOCK ) )
            {
                /* analyze boolean expression */
                rc = GetBoolExpr( curr_item, item_name,
                                  &policy_list->whitelist_rules[curr_ign].bool_expr,
                                  &policy_list->whitelist_rules[curr_ign].attr_mask, msg_out );
                if ( rc )
                    goto free_policy;

                /* add expression attr mask to policy mask */
                policy_list->global_attr_mask |= policy_list->whitelist_rules[curr_ign].attr_mask;
                curr_ign++;
            }
            else if ( !strcasecmp( item_name, "policy" ) )
            {
                /* parse policy block */
                rc = parse_policy_block( curr_item, item_name, policies,
                                         &policy_list->policy_list[curr_pol],
                                         policy_type, msg_out );
                if ( rc )
                    goto free_policy;

                policy_list->global_attr_mask |= policy_list->policy_list[curr_pol].attr_mask;
                curr_pol++;
            }
#ifndef _LUSTRE_HSM
            else if ( !strcasecmp( item_name, BLACKLIST_BLOCK ) )
            {
                DisplayLog(LVL_CRIT, "PolicyCheck", "WARNING: "BLACKLIST_BLOCK" block parsing not implemented");
#if 0
                if ( policy->blacklist_count == 0 )
                    policy->blacklist_rules =
                        ( blacklist_item_t * ) malloc( sizeof( blacklist_item_t ) );
                else
                    policy->blacklist_rules =
                        ( blacklist_item_t * ) realloc( policy->blacklist_rules,
                                                        ( policy->blacklist_count +
                                                          1 ) * sizeof( blacklist_item_t ) );

                policy->blacklist_count++;

                rc = parse_penalty_block( curr_item, block_name,
                                          &policy->blacklist_rules[policy->blacklist_count - 1],
                                          msg_out );
                if ( rc )
                    return rc;

                policy->global_attr_mask |=
                    policy->blacklist_rules[policy->blacklist_count - 1].attr_mask;
#endif
            }
#endif
            else
            {
                sprintf( msg_out, "'%s' sub-block unexpected in %s block, line %d.",
                         item_name, section_name, rh_config_GetItemLine( curr_item ) );
                rc = EINVAL;
                goto free_policy;
            }
        }
        else                    /* not a block */
        {
            char          *value;
            int            extra_args = FALSE;

            rc = rh_config_GetKeyValue( curr_item, &item_name, &value, &extra_args );
            if ( rc )
                goto free_policy;

            /* only "ignore_fileclass" expected */
            if ( strcasecmp( item_name, IGNORE_FC ) != 0 )
            {
                sprintf( msg_out, "'%s' parameter unexpected in %s block, line %d.",
                         item_name, section_name, rh_config_GetItemLine( curr_item ) );
                rc = EINVAL;
                goto free_policy;
            }

            if ( extra_args )
            {
                sprintf( msg_out,
                         "Unexpected arguments for %s parameter, line %d.",
                         item_name, rh_config_GetItemLine( curr_item ) );
                rc = EINVAL;
                goto free_policy;
            }

            /* find fileset in policy */
            policy_list->ignore_list[curr_ign_fc] = GetFilesetByName( &policies->filesets, value );

            if ( policy_list->ignore_list[curr_ign_fc] == NULL )
            {
                sprintf( msg_out, "Policy definition references unknown fileclass '%s', line %d.",
                         value, rh_config_GetItemLine( curr_item ) );
                rc = EINVAL;
                goto free_policy;
            }
#ifdef HAVE_PURGE_POLICY
            else if ( policy_type == PURGE_POLICY )
                policy_list->ignore_list[curr_ign_fc]->has_purge_policy = TRUE;
#endif
#ifdef HAVE_MIGR_POLICY
            else if ( policy_type == MIGR_POLICY )
                policy_list->ignore_list[curr_ign_fc]->has_migration_policy = TRUE;
#endif
            else
            {
                sprintf( msg_out, "Unexpected policy type %d", policy_type );
                rc = EINVAL;
                goto free_policy;
            }


            /* check that the fileset is not already referenced in a policy */
            for ( j = 0; j < curr_pol; j++ )
            {
                for ( k = 0; k < policy_list->policy_list[j].target_count; k++ )
                    if ( policy_list->ignore_list[curr_ign_fc] ==
                         policy_list->policy_list[j].target_list[k] )
                    {
                        sprintf( msg_out,
                                 "Fileclass '%s' is simultaneously ignored and referenced as a target for policy '%s'",
                                 value, policy_list->policy_list[j].policy_id );
                        rc = EINVAL;
                        goto free_policy;
                    }
            }

            /* add fileset attr mask to policy mask */
            policy_list->global_attr_mask |= policy_list->ignore_list[curr_ign_fc]->attr_mask;
            curr_ign_fc++;

        }                       /* end of vars */

    }                           /* end of section content */

    return 0;

  free_policy:
    if ( policy_list->policy_list )
        free( policy_list->policy_list );
  free_ignore_fc:
    if ( policy_list->ignore_list )
        free( policy_list->ignore_list );
  free_ignore:
    if ( curr_ign > 0 )
        free_whitelist( policy_list->whitelist_rules, curr_ign );
    else if ( policy_list->whitelist_rules )
        free( policy_list->whitelist_rules );
  err:
    return rc;

}



static int reload_policies( policies_t * policies )
{
#if 0
    purge_policy_t *policy = ( purge_policy_t * ) module_config;

    /* Reloading purge policies dynamically is quite complex:
     * 1) compare triggers: if they are the same (same count, same type)
     *    update their simple parameters: thresholds and check interval
     * 2) compare whitelist expressions count and structures.
     *    If the structure is the same (same parameters, comparators, etc.) only update
     *    the numerical values for conditions.
     */

    /** @TODO prevent from concurrent access when purge policy is beeing checked */

    update_whitelist( policies.purge_policy.whitelist_rules, policies.purge_policy.whitelist_count,
                      policy->whitelist_rules, policy->whitelist_count, PURGEPOLICY_BLOCK );

    /* XXX global_attr_mask is unchanged, since we keep the same expressions */

    /* free reloaded config structure (no used anymore) */
    free_whitelist( policy->whitelist_rules, policy->whitelist_count );

#endif
    reload_update_policy( &policies->updt_policy );
    return 0;
}


/* wrapper for reading policies */
int SetDefault_Policies( void *module_config, char *msg_out )
{
    policies_t    *policy = ( policies_t * ) module_config;
    int            rc;

    rc = set_default_filesets( &policy->filesets, msg_out );
    if ( rc )
        return rc;
#ifdef HAVE_PURGE_POLICY
    rc = set_default_policy( &policy->purge_policies, msg_out, PURGE_POLICY );
    if ( rc )
        return rc;
#endif
#ifdef HAVE_MIGR_POLICY
    rc = set_default_policy( &policy->migr_policies, msg_out, MIGR_POLICY );
    if ( rc )
        return rc;
#endif
#ifdef HAVE_RM_POLICY
    rc = set_default_unlink_policy( &policy->unlink_policy, msg_out );
    if ( rc )
        return rc;
#endif

#ifdef HAVE_RMDIR_POLICY
    rc = set_default_rmdir_policy( &policy->rmdir_policy, msg_out );
    if ( rc )
        return rc;
#endif

    rc = set_default_update_policy( &policy->updt_policy, msg_out );
    if ( rc )
        return rc;

    return 0;
}

int Read_Policies( config_file_t config, void *module_config, char *msg_out, int for_reload )
{
    policies_t    *policy = ( policies_t * ) module_config;
    int            rc;

    rc = read_filesets( config, &policy->filesets, msg_out, for_reload );
    if ( rc )
        return rc;
#ifdef HAVE_PURGE_POLICY
    rc = read_policy( config, policy, msg_out, for_reload, PURGE_POLICY );
    if ( rc )
        return rc;
#endif
#ifdef HAVE_MIGR_POLICY
    rc = read_policy( config, policy, msg_out, for_reload, MIGR_POLICY );
    if ( rc )
        return rc;
#endif
#ifdef HAVE_RM_POLICY
    rc = read_unlink_policy( config, &policy->unlink_policy, msg_out, for_reload );
    if ( rc )
        return rc;
#endif

#ifdef HAVE_RMDIR_POLICY
    rc = read_rmdir_policy( config, &policy->rmdir_policy, msg_out, for_reload );
    if ( rc )
        return rc;
#endif

    rc = read_update_policy( config, &policy->updt_policy, msg_out, for_reload );
    if ( rc )
        return rc;

    return 0;

}

int Reload_Policies( void *module_config )
{
    policies_t    *policy = ( policies_t * ) module_config;
    int rc;

/*    @TODO reload policies */
    rc = reload_policies( policy );

    return rc;

}

int Write_Policy_Template( FILE * output )
{
    int            rc;

    rc = write_template_filesets( output );
    if ( rc )
        return rc;
#ifdef HAVE_MIGR_POLICY
    rc = write_migration_policy_template( output );
    if ( rc )
        return rc;
#endif

#ifdef HAVE_RM_POLICY
    rc = write_unlink_policy_template( output );
    if ( rc )
        return rc;
#endif

#ifdef HAVE_PURGE_POLICY
    rc = write_purge_policy_template( output );
    if ( rc )
        return rc;
#endif

#ifdef HAVE_RMDIR_POLICY
    rc = write_rmdir_policy_template( output );
    if ( rc )
        return rc;
#endif

    rc = write_update_policy_template( output );
    if ( rc )
        return rc;

    return 0;
}

int Write_Policy_Default( FILE * output )
{
    int            rc;

    rc = write_default_filesets( output );
    if ( rc )
        return rc;
#ifdef HAVE_MIGR_POLICY
    rc = write_default_policy( output, MIGR_POLICY );
    if ( rc )
        return rc;
#endif

#ifdef HAVE_RM_POLICY
    rc = write_default_unlink_policy( output );
    if ( rc )
        return rc;
#endif

#ifdef HAVE_RMDIR_POLICY
    rc = write_default_rmdir_policy( output );
    if ( rc )
        return rc;
#endif

#ifdef HAVE_PURGE_POLICY
    rc = write_default_policy( output, PURGE_POLICY );
    if ( rc )
        return rc;
#endif

    rc = write_default_update_policy( output );
    if ( rc )
        return rc;

    return 0;
}
