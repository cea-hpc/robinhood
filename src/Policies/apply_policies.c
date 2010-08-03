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
 * functions for applying policies to entries
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "policies.h"
#include "RobinhoodLogs.h"
#include "RobinhoodMisc.h"
#include "RobinhoodConfig.h"
#include "uidgidcache.h"

#include <string.h>
#include <libgen.h>
#include <fnmatch.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#ifdef HAVE_ATTR_XATTR_H
#include <attr/xattr.h>
#endif

#define POLICY_TAG "Policy"

/* macros for displaying entries */
#ifdef _HAVE_FID
#define F_ENT_ID DFID
#define P_ENT_ID(_id, _attr)    PFID( _id )
#else
#define F_ENT_ID    "'%s'"
#define P_ENT_ID(_id, _attr)    ATTR( _attr, fullpath )
#endif



static char   *ExtractParentDir( const char *file_path, char *out_buff )
{

    char           buff[MAXPATHLEN];
    char          *dir;

    strncpy( buff, file_path, MAXPATHLEN );

    dir = dirname( buff );

    strcpy( out_buff, dir );

    return out_buff;
}

static int TestPathRegexp( const char *regexp, const char *to_be_tested, int is_child_path )
{
    char full_path[MAXPATHLEN];
    const char * full_regexp = regexp;

    /* is the regexp relative ? */
    if ( !IS_ABSOLUTE_PATH(regexp) )
    {
        /* add root path to the path to be tested */
        sprintf( full_path, "%s/%s", global_config.fs_path, regexp );
        full_regexp = full_path;
    }

    if ( !fnmatch( full_regexp, to_be_tested, FNM_PATHNAME | ( is_child_path ? FNM_LEADING_DIR : 0 )) )
    {
#ifdef _DEBUG_POLICIES
        printf("MATCH regexp='%s', path='%s', tree = %d\n", full_regexp, to_be_tested, is_child_path );
#endif
        return TRUE;
    }
#ifdef _DEBUG_POLICIES
    printf("NO MATCH regexp='%s', path='%s', tree = %d\n", full_regexp, to_be_tested, is_child_path );
#endif

    return FALSE;

}                               /* TestRegexp */


static int TestRegexp( const char *regexp, const char *to_be_tested )
{
    return !fnmatch(regexp, to_be_tested, 0);
}


static inline int size_compare( unsigned long long size1, compare_direction_t comp,
                                unsigned long long size2 )
{
    switch ( comp )
    {
    case COMP_GRTHAN:
        return ( size1 > size2 );
    case COMP_GRTHAN_EQ:
        return ( size1 >= size2 );
    case COMP_LSTHAN:
        return ( size1 < size2 );
    case COMP_LSTHAN_EQ:
        return ( size1 <= size2 );
    case COMP_EQUAL:
        return ( size1 == size2 );
    case COMP_DIFF:
        return ( size1 != size2 );
    default:
        DisplayLog( LVL_CRIT, POLICY_TAG, "Invalid comparator for size (%d)", comp );
        return FALSE;
    }
}

static inline int int_compare( int int1, compare_direction_t comp, int int2 )
{
    switch ( comp )
    {
    case COMP_GRTHAN:
        return ( int1 > int2 );
    case COMP_GRTHAN_EQ:
        return ( int1 >= int2 );
    case COMP_LSTHAN:
        return ( int1 < int2 );
    case COMP_LSTHAN_EQ:
        return ( int1 <= int2 );
    case COMP_EQUAL:
        return ( int1 == int2 );
    case COMP_DIFF:
        return ( int1 != int2 );
    default:
        DisplayLog( LVL_CRIT, POLICY_TAG, "Invalid comparator for int (%d)", comp );
        return FALSE;
    }
}

#define BOOL2POLICY(_rc_) ((_rc_)?POLICY_MATCH:POLICY_NO_MATCH)

#define CHECK_ATTR( _pset_, _attr_, _no_trace ) do {                                    \
                                           if ( !ATTR_MASK_TEST( _pset_, _attr_ ) )     \
                                           {                                            \
                                               if (!(_no_trace))                        \
                                                   DisplayLog( LVL_DEBUG, POLICY_TAG,   \
                                                       "Missing attribute '%s' for evaluating boolean expression", (#_attr_) ); \
                                               return POLICY_MISSING_ATTR;              \
                                           }                                            \
                                     } while (0)


static filter_comparator_t Policy2FilterComparator( compare_direction_t comp )
{
    switch( comp )
    {
        case COMP_GRTHAN: return MORETHAN_STRICT;
        case COMP_GRTHAN_EQ: return MORETHAN;
        case COMP_LSTHAN: return LESSTHAN_STRICT;
        case COMP_LSTHAN_EQ: return LESSTHAN;
        case COMP_EQUAL: return EQUAL;
        case COMP_DIFF: return NOTEQUAL;
        case COMP_LIKE: return LIKE;
        case COMP_UNLIKE: return UNLIKE;

        default:
            /* Error */
            DisplayLog( LVL_CRIT, POLICY_TAG, "ERROR: unknown comparator %d", comp );
            return (filter_comparator_t) -1;
    }
}

/* return the opposite direction */
static compare_direction_t oppose_compare( compare_direction_t comp )
{
    switch( comp )
    {
        case COMP_GRTHAN: return COMP_LSTHAN;
        case COMP_GRTHAN_EQ: return COMP_LSTHAN_EQ;
        case COMP_LSTHAN: return COMP_GRTHAN;
        case COMP_LSTHAN_EQ: return COMP_GRTHAN_EQ;

        /* reflexive operation */
        case COMP_EQUAL:
        case COMP_DIFF:
        case COMP_LIKE:
        case COMP_UNLIKE:
            return comp;

        default:
            /* Error */
            DisplayLog( LVL_CRIT, POLICY_TAG, "ERROR: unknown comparator %d", comp );
            return (compare_direction_t) -1;
    }
}

static const char * Policy2ListMgrType( obj_type_t type )
{
     switch ( type )
        {
        case TYPE_LINK: return STR_TYPE_LINK;
        case TYPE_DIR: return STR_TYPE_DIR;
        case TYPE_FILE: return STR_TYPE_FILE;
        case TYPE_CHR: return STR_TYPE_CHR;
        case TYPE_BLK: return STR_TYPE_BLK;
        case TYPE_FIFO: return STR_TYPE_FIFO;
        case TYPE_SOCK: return STR_TYPE_SOCK;
        default: return NULL;
        }
}

/**
 * Convert criteria to ListMgr data
 * \param p_comp        IN: the condition to be converted
 * \param p_attr_index  OUT: related attribute index
 * \param p_compar      OUT: listmgr comparator
 * \param db_type_u     OUT: value
 * \param p_must_release OUT: set to TRUE if the db_type_u.val_str string must be released
 * \return -1 if this is not a criteria stored in DB.
 */
int CriteriaToFilter(const compare_triplet_t * p_comp, int * p_attr_index,
                     filter_comparator_t * p_compar, db_type_u * p_value,
                     int * p_must_release)
{
    int len;
    char * new_str;
    int add_root = FALSE;

    *p_must_release = FALSE;

    switch( p_comp->crit )
    {
    case CRITERIA_TREE:

        /* is the path relative ? */
        if ( !IS_ABSOLUTE_PATH(p_comp->val.str) )
            add_root = TRUE;

        /* fullpath like 'tree/% ' */

       *p_attr_index = ATTR_INDEX_fullpath;

        if ( (p_comp->op == COMP_LIKE) || (p_comp->op == COMP_EQUAL) )
            *p_compar = LIKE;
        else if ( (p_comp->op == COMP_UNLIKE) || (p_comp->op == COMP_DIFF) )
            *p_compar = UNLIKE;

    
        len = strlen(p_comp->val.str);

        if ( add_root )
            len += strlen(global_config.fs_path) + 1; /* root path + '/' */

        new_str = MemAlloc( len+3 ); /* 3 => '/' '%' '\0' */
        *p_must_release = TRUE;

        if ( add_root )
            sprintf(new_str, "%s/%s", global_config.fs_path, p_comp->val.str);
        else
            strcpy( new_str, p_comp->val.str );

        /* is a / needed ? */
        if ( !FINAL_SLASH(new_str) )
        {
            new_str[len] = '/';
            len++;
        }
        /* add db 'wildcard' */
        new_str[len]='*';
        len++;
        new_str[len]='\0';

        p_value->val_str = new_str;
        break;

    case CRITERIA_PATH: /* fullpath 'path' */

        *p_attr_index = ATTR_INDEX_fullpath;
        *p_compar = Policy2FilterComparator( p_comp->op );

        if ( !IS_ABSOLUTE_PATH(p_comp->val.str) )
        {
            /* add root path */
            len = strlen(p_comp->val.str) + strlen(global_config.fs_path) + 1;
            new_str = MemAlloc( len + 1 ); /* +1 for \0 */
            *p_must_release = TRUE;
            sprintf(new_str, "%s/%s", global_config.fs_path, p_comp->val.str);
            p_value->val_str = new_str;
        }
        else
            p_value->val_str = p_comp->val.str;

        break;

    case CRITERIA_FILENAME: /* name like 'filename' */

        *p_attr_index = ATTR_INDEX_name;
        *p_compar = Policy2FilterComparator( p_comp->op );
        p_value->val_str = p_comp->val.str;

        break;

#ifdef ATTR_INDEX_type
    case CRITERIA_TYPE: /* type = 'type' */

        *p_attr_index = ATTR_INDEX_type;
        *p_compar = Policy2FilterComparator( p_comp->op );
        p_value->val_str = Policy2ListMgrType( p_comp->val.type );
        break;
#endif
    case CRITERIA_OWNER: /* owner like 'owner' */
        *p_attr_index = ATTR_INDEX_owner;
        *p_compar = Policy2FilterComparator( p_comp->op );
        p_value->val_str = p_comp->val.str;
        break;

    case CRITERIA_GROUP:
        *p_attr_index = ATTR_INDEX_gr_name;
        *p_compar = Policy2FilterComparator( p_comp->op );
        p_value->val_str = p_comp->val.str;
        break;

    case CRITERIA_SIZE:
        *p_attr_index = ATTR_INDEX_size;
        *p_compar = Policy2FilterComparator( p_comp->op );
        p_value->val_biguint = p_comp->val.size;
        break;

    case CRITERIA_DEPTH:
        *p_attr_index = ATTR_INDEX_depth;
        *p_compar = Policy2FilterComparator( p_comp->op );
        p_value->val_uint = p_comp->val.integer;
        break;

#ifndef _LUSTRE_HSM
    case CRITERIA_DIRCOUNT:
        *p_attr_index = ATTR_INDEX_dircount;
        *p_compar = Policy2FilterComparator( p_comp->op );
        p_value->val_uint = p_comp->val.integer;
        break;
#endif
    case CRITERIA_LAST_ACCESS:
        *p_attr_index = ATTR_INDEX_last_access;

    /*   last_access > 2h <=> access_time < time(NULL) - 2h */

        *p_compar = Policy2FilterComparator( oppose_compare( p_comp->op ) );
        p_value->val_uint = time(NULL) - p_comp->val.duration;
        break;


    case CRITERIA_LAST_MOD:
        *p_attr_index = ATTR_INDEX_last_mod;
        *p_compar = Policy2FilterComparator( oppose_compare( p_comp->op ) );
        p_value->val_uint = time(NULL) - p_comp->val.duration;
        break;

#ifdef _LUSTRE_HSM
    case CRITERIA_LAST_ARCHIVE:
        *p_attr_index = ATTR_INDEX_last_archive;
        *p_compar = Policy2FilterComparator( oppose_compare( p_comp->op ) );
        p_value->val_uint = time(NULL) - p_comp->val.duration;
        break;

    case CRITERIA_LAST_RESTORE:
        *p_attr_index = ATTR_INDEX_last_restore;
        *p_compar = Policy2FilterComparator( oppose_compare( p_comp->op ) );
        p_value->val_uint = time(NULL) - p_comp->val.duration;
        break;
#endif
    case CRITERIA_POOL:
        *p_attr_index = ATTR_INDEX_stripe_info;
        *p_compar = Policy2FilterComparator( p_comp->op );
        p_value->val_str = p_comp->val.str;
        break;

    case CRITERIA_XATTR:
    case CRITERIA_CUSTOM_CMD:
    default:
        *p_attr_index = -1;
        return -1;
    }

    return 0;
}



static policy_match_t eval_condition( const entry_id_t * p_entry_id,
                                      const attr_set_t * p_entry_attr,
                                      const compare_triplet_t * p_triplet,
                                      int no_warning )
{
    char           tmpbuff[MAXPATHLEN];
    char          *rep;
    const char * typedb;
    int            rc;

    switch ( p_triplet->crit )
    {
    case CRITERIA_TREE:
        /* fullpath is required */
        CHECK_ATTR( p_entry_attr, fullpath, no_warning );

        rep = ExtractParentDir( ATTR( p_entry_attr, fullpath ), tmpbuff );
        rc = TestPathRegexp( p_triplet->val.str, rep, TRUE );

#ifdef _DEBUG_POLICIES
        if ( rc )
            printf( "%s (dir %s) matches tree %s\n", ATTR( p_entry_attr, fullpath ), rep,
                    p_triplet->val.str );
#endif

        if ( p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE )
            return BOOL2POLICY( rc );
        else
            return BOOL2POLICY( !rc );

    case CRITERIA_PATH:
        /* fullpath is required */
        CHECK_ATTR( p_entry_attr, fullpath, no_warning );

        rc = TestPathRegexp( p_triplet->val.str, ATTR( p_entry_attr, fullpath ), FALSE );

        if ( p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE )
            return BOOL2POLICY( rc );
        else
            return BOOL2POLICY( !rc );

    case CRITERIA_FILENAME:

        /* filename is required */
        CHECK_ATTR( p_entry_attr, name, no_warning );

        rc = TestRegexp( p_triplet->val.str, ATTR( p_entry_attr, name ) );

#ifdef _DEBUG_POLICIES
        if ( rc )
            printf( "%s matches filename %s\n", ATTR( p_entry_attr, name ), p_triplet->val.str );
#endif

        if ( p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE )
            return BOOL2POLICY( rc );
        else
            return BOOL2POLICY( !rc );

#ifdef ATTR_INDEX_type
    case CRITERIA_TYPE:

        /* type is required */
        CHECK_ATTR( p_entry_attr, type, no_warning );

        typedb = Policy2ListMgrType( p_triplet->val.type );
        if ( typedb == NULL )
            return POLICY_ERR;
        else
            rc = !strcmp( ATTR( p_entry_attr, type ), typedb );

        if ( p_triplet->op == COMP_EQUAL )
            return BOOL2POLICY( rc );
        else
            return BOOL2POLICY( !rc );
#endif

    case CRITERIA_OWNER:
        /* owner is required */
        CHECK_ATTR( p_entry_attr, owner, no_warning );

        rc = TestRegexp( p_triplet->val.str, ATTR( p_entry_attr, owner ) );

        if ( p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE )
            return BOOL2POLICY( rc );
        else
            return BOOL2POLICY( !rc );

    case CRITERIA_GROUP:
        /* group is required */
        CHECK_ATTR( p_entry_attr, gr_name, no_warning );

        rc = TestRegexp( p_triplet->val.str, ATTR( p_entry_attr, gr_name ) );

        if ( p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE )
            return BOOL2POLICY( rc );
        else
            return BOOL2POLICY( !rc );

    case CRITERIA_SIZE:
        /* size is required */
        CHECK_ATTR( p_entry_attr, size, no_warning );

        rc = size_compare( ATTR( p_entry_attr, size ), p_triplet->op, p_triplet->val.size );
        return BOOL2POLICY( rc );

    case CRITERIA_DEPTH:
        /* depth is required */
        CHECK_ATTR( p_entry_attr, depth, no_warning );

        rc = int_compare( ATTR( p_entry_attr, depth ), p_triplet->op, p_triplet->val.integer );
        return BOOL2POLICY( rc );

#ifndef _LUSTRE_HSM
    case CRITERIA_DIRCOUNT:
        /* dircount is required */
        CHECK_ATTR( p_entry_attr, dircount, no_warning );

        rc = int_compare( ATTR( p_entry_attr, dircount ), p_triplet->op, p_triplet->val.integer );
        return BOOL2POLICY( rc );
#endif

    case CRITERIA_LAST_ACCESS:
        /* last_access is required */
        CHECK_ATTR( p_entry_attr, last_access, no_warning );

        rc = int_compare( time( NULL ) - ATTR( p_entry_attr, last_access ), p_triplet->op,
                          p_triplet->val.duration );
        return BOOL2POLICY( rc );

    case CRITERIA_LAST_MOD:
        /* last_mod required */

        CHECK_ATTR( p_entry_attr, last_mod, no_warning );

        rc = int_compare( time( NULL ) - ATTR( p_entry_attr, last_mod ), p_triplet->op,
                          p_triplet->val.duration );
        return BOOL2POLICY( rc );

#ifdef _LUSTRE_HSM
    case CRITERIA_LAST_ARCHIVE:
        /* last_archive is required */
        CHECK_ATTR( p_entry_attr, last_archive, no_warning );

        rc = int_compare( time( NULL ) - ATTR( p_entry_attr, last_archive ), p_triplet->op,
                          p_triplet->val.duration );
        return BOOL2POLICY( rc );

        break;

    case CRITERIA_LAST_RESTORE:
        /* restore time is required */
        CHECK_ATTR( p_entry_attr, last_restore, no_warning );

        rc = int_compare( time( NULL ) - ATTR( p_entry_attr, last_restore ), p_triplet->op,
                          p_triplet->val.duration );
        return BOOL2POLICY( rc );

        break;
#endif
    case CRITERIA_POOL:
        /* pool name is required */
        CHECK_ATTR( p_entry_attr, stripe_info, no_warning );

        rc = TestRegexp( p_triplet->val.str, ATTR( p_entry_attr, stripe_info ).pool_name );

#ifdef _DEBUG_POLICIES
        if ( rc )
            printf( "'%s' pool matches '%s'\n", ATTR( p_entry_attr, stripe_info ).pool_name,
                    p_triplet->val.str );
#endif

        if ( p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE )
            return BOOL2POLICY( rc );
        else
            return BOOL2POLICY( !rc );


    case CRITERIA_XATTR:
#ifdef HAVE_ATTR_XATTR_H
    {
        const char * entry_path;
        char value[1024];
#if (!defined (_LUSTRE) || !defined(_HAVE_FID))
         /* fullpath needed to get xattr, except if fids are supported */
        CHECK_ATTR( p_entry_attr, fullpath, no_warning );
#else
        if (p_entry_id)
        {
            /* use fid path */
            rc = BuildFidPath( p_entry_id, tmpbuff );
            if ( rc )
               return POLICY_ERR; 
            entry_path = tmpbuff;
        }
        else if (ATTR_MASK_TEST(p_entry_attr, fullpath))
        {
            /* use posix path */
            entry_path = ATTR(p_entry_attr, fullpath);
        }
        else
        {
            if (!no_warning)
                DisplayLog( LVL_DEBUG, POLICY_TAG,
                    "Missing fid or fullpath to evaluate boolean expression on xattr" );
            return POLICY_MISSING_ATTR;
        }
#endif

        /* retrieve xattr value */
        rc = lgetxattr( entry_path, p_triplet->xattr_name, value, 1024 );
        if ( rc < 0 )
        {
            if (errno == ENOATTR)
                /* empty string == no attribute */
                strcpy( value, "" );
            else if  (errno == ENOTSUP )
            {
                DisplayLog(LVL_CRIT, POLICY_TAG, "Error: condition on extended attribute " 
                           "whereas this feature is not supported by the filesystem");
                return POLICY_ERR;
            }
            else
            {
                DisplayLog(LVL_CRIT, POLICY_TAG, "Error getting xattr '%s' on '%s' : %s",
                           p_triplet->xattr_name, entry_path, strerror(errno) );
                return POLICY_ERR;
            }
        }
        else
        {
            /* security: set byte n+1 to '\0', to avaoid overflows if attr is not a string */
            if ( rc < 1024 )
                value[rc] = '\0';
        }

        DisplayLog(LVL_FULL, POLICY_TAG, "<xattr>.%s = \"%s\" (%s)", p_triplet->xattr_name, value, entry_path );

        /* compare attribute value */

        rc = TestRegexp( p_triplet->val.str, value );

        if ( p_triplet->op == COMP_EQUAL || p_triplet->op == COMP_LIKE )
            return BOOL2POLICY( rc );
        else
            return BOOL2POLICY( !rc );

        break;
    }
#else
    DisplayLog(LVL_CRIT, POLICY_TAG, "robinhood has not been compiled with xattr support: cannot evaluate condition" );
    return POLICY_ERR;
    break;
#endif

    case CRITERIA_CUSTOM_CMD:
        /* fullpath is required if fids are not available */
        /* @TODO */
        break;
    }

    return POLICY_ERR;

}


/* function for testing a boolean expression on a given entry */
static policy_match_t _EntryMatches( const entry_id_t * p_entry_id, const attr_set_t * p_entry_attr,
                                     bool_node_t * p_node, int no_warning ) 
{
    policy_match_t rc;

    if ( !p_entry_id || !p_entry_attr || !p_node )
        return POLICY_ERR;

    switch ( p_node->node_type )
    {
    case NODE_UNARY_EXPR:

        /* BOOL_NOT is the only supported unary operator */
        if ( p_node->content_u.bool_expr.bool_op != BOOL_NOT )
            return POLICY_ERR;

        rc = _EntryMatches( p_entry_id, p_entry_attr, p_node->content_u.bool_expr.expr1, no_warning );

        if ( rc == POLICY_MATCH )
            return POLICY_NO_MATCH;
        else if ( rc == POLICY_NO_MATCH )
            return POLICY_MATCH;
        else
            return rc;

        break;

    case NODE_BINARY_EXPR:
        /* always test the first expression */
        rc = _EntryMatches( p_entry_id, p_entry_attr, p_node->content_u.bool_expr.expr1, no_warning );

        /* in some cases, we can stop here */
        if ( ( p_node->content_u.bool_expr.bool_op == BOOL_OR ) && ( rc == POLICY_MATCH ) )
            return POLICY_MATCH;
        else if ( ( p_node->content_u.bool_expr.bool_op == BOOL_AND ) && ( rc == POLICY_NO_MATCH ) )
            return POLICY_NO_MATCH;
        else if ( rc != POLICY_MATCH && rc != POLICY_NO_MATCH )
            return rc;

        /* compute the second expression */
        return _EntryMatches( p_entry_id, p_entry_attr, p_node->content_u.bool_expr.expr2, no_warning );

        break;

    case NODE_CONDITION:
        /* It's now time to test the value ! */
        return eval_condition( p_entry_id, p_entry_attr, p_node->content_u.condition, no_warning );

        break;
    }

    return POLICY_ERR;

}

policy_match_t EntryMatches( const entry_id_t * p_entry_id, const attr_set_t * p_entry_attr,
                             bool_node_t * p_node )
{
    return _EntryMatches( p_entry_id, p_entry_attr, p_node, FALSE );
}

static policy_match_t _IsWhitelisted( const entry_id_t * p_entry_id, const attr_set_t * p_entry_attr,
                              policy_type_t policy_type, int no_warning )
{
    unsigned int   i, count;
    policy_match_t rc = POLICY_NO_MATCH;
    whitelist_item_t *list;
    fileset_item_t **fs_list;

    switch ( policy_type )
    {
    case PURGE_POLICY:
        count = policies.purge_policies.whitelist_count;
        list = policies.purge_policies.whitelist_rules;
        break;
#ifdef HAVE_MIGR_POLICY
    case MIGR_POLICY:
        count = policies.migr_policies.whitelist_count;
        list = policies.migr_policies.whitelist_rules;
        break;
#endif
#ifdef HAVE_RMDIR_POLICY
    case RMDIR_POLICY:
        count = policies.rmdir_policy.whitelist_count;
        list = policies.rmdir_policy.whitelist_rules;
        break;
#endif
    default:
        DisplayLog( LVL_CRIT, POLICY_TAG, "Unsupported policy type %d", policy_type );
        return POLICY_ERR;
    }

    for ( i = 0; i < count; i++ )
    {
        switch ( _EntryMatches( p_entry_id, p_entry_attr, &list[i].bool_expr, no_warning ) )
        {
        case POLICY_MATCH:
            return POLICY_MATCH;
        case POLICY_MISSING_ATTR:
            if ( !no_warning )
            {
                char buff[1024];
                BoolExpr2str( &list[i].bool_expr, buff, 1024 );
                DisplayLog( LVL_MAJOR, POLICY_TAG, "Attribute is missing for checking whitelist rule '%s'", buff );
            }
            if ( rc != POLICY_ERR )
                rc = POLICY_MISSING_ATTR;
            break;
        case POLICY_ERR:
        {
            char buff[1024];
            BoolExpr2str( &list[i].bool_expr, buff, 1024 );
            DisplayLog( LVL_CRIT, POLICY_TAG, "An error occured while checking this whitelist rule: %s", buff );
            rc = POLICY_ERR;
            break;
        }
        case POLICY_NO_MATCH:
            /* continue testing other whitelist rules */
            break;
        }
    }

#ifdef HAVE_RMDIR_POLICY
    if ( policy_type == RMDIR_POLICY )
        return rc;
#endif

    /* Check if file belongs to a whitelisted fileset */

    switch ( policy_type )
    {
    case PURGE_POLICY:
        count = policies.purge_policies.ignore_count;
        fs_list = policies.purge_policies.ignore_list;
        break;
#ifdef HAVE_MIGR_POLICY
    case MIGR_POLICY:
        count = policies.migr_policies.ignore_count;
        fs_list = policies.migr_policies.ignore_list;
        break;
#endif
    default:
        DisplayLog( LVL_CRIT, POLICY_TAG, "Unsupported policy type %d", policy_type );
        return POLICY_ERR;
    }

    for ( i = 0; i < count; i++ )
    {
        switch ( _EntryMatches( p_entry_id, p_entry_attr, &fs_list[i]->definition, no_warning ) )
        {
        case POLICY_MATCH:
            return POLICY_MATCH;
        case POLICY_MISSING_ATTR:
            if ( !no_warning )
                DisplayLog( LVL_MAJOR, POLICY_TAG,
                        "Attribute is missing for checking ignore_fileclass rule" );
            if ( rc != POLICY_ERR )
                rc = POLICY_MISSING_ATTR;
            break;
        case POLICY_ERR:
            DisplayLog( LVL_CRIT, POLICY_TAG,
                        "An error occured when checking ignore_fileclass rule" );
            rc = POLICY_ERR;
            break;
        case POLICY_NO_MATCH:
            /* continue testing other whitelist rules */
            break;
        }
    }

    return rc;
}

policy_match_t IsWhitelisted( const entry_id_t * p_entry_id, const attr_set_t * p_entry_attr,
                              policy_type_t policy_type )
{
    return _IsWhitelisted( p_entry_id, p_entry_attr, policy_type, FALSE );
}

/** @TODO XXX is this usefull? */
#if 0
char          *FilesetMatch( const entry_id_t * p_entry_id, const attr_set_t * p_entry_attr )
{
    unsigned int   i;
    int            rc = POLICY_NO_MATCH;

    for ( i = 0; i < policies.filesets.fileset_count; i++ )
    {
        switch ( EntryMatches
                 ( p_entry_id, p_entry_attr, &policies.filesets.fileset_list[i].definition ) )
        {
        case POLICY_MATCH:
            return policies.filesets.fileset_list[i].fileset_id;
        case POLICY_MISSING_ATTR:
            DisplayLog( LVL_MAJOR, POLICY_TAG, "Attribute is missing for checking fileset" );
            if ( rc != POLICY_ERR )
                rc = POLICY_MISSING_ATTR;
            break;
        case POLICY_ERR:
            DisplayLog( LVL_CRIT, POLICY_TAG, "An error occured when checking fileset" );
            rc = POLICY_ERR;
            break;
        case POLICY_NO_MATCH:
            /* continue testing other file classes */
            break;
        }
    }

    return NULL;

7}


fileset_item_t *GetFilesetById( const char *fileset_id )
{
    unsigned int   i;

    for ( i = 0; i < policies.filesets.fileset_count; i++ )
    {
        if ( !strcmp( policies.filesets.fileset_list[i].fileset_id, fileset_id ) )
            return &policies.filesets.fileset_list[i];
    }

    return NULL;

}
#endif

/** get the first matching policy case for the given file */
policy_item_t *GetPolicyCase( const entry_id_t * p_entry_id,
                              const attr_set_t * p_entry_attr, policy_type_t policy_type,
                              fileset_item_t ** pp_fileset )
{
    int            count, i, j;
    int            default_index = -1;
    policy_item_t *pol_list;

    switch ( policy_type )
    {
    case PURGE_POLICY:
        pol_list = policies.purge_policies.policy_list;
        count = policies.purge_policies.policy_count;
        break;
#ifdef HAVE_MIGR_POLICY
    case MIGR_POLICY:
        pol_list = policies.migr_policies.policy_list;
        count = policies.migr_policies.policy_count;
        break;
#endif
    default:
        DisplayLog( LVL_CRIT, POLICY_TAG, "Unsupported policy type %d", policy_type );
        return NULL;
    }

    /* for each policy (except default), check target filesets.
     *   - if a fileset matches, return the associated policy.
     *   - else, return defaut policy, if it is specified.
     *   - else, write a warning.
     */
    for ( i = 0; i < count; i++ )
    {

#ifdef _DEBUG_POLICIES
        printf( "Checking policy %s...\n", pol_list[i].policy_id );
#endif

        if ( !strcasecmp( pol_list[i].policy_id, "default" ) )
        {
            /* remember index of default policy */
            default_index = i;
            continue;
        }

        /* check filesets */

        for ( j = 0; j < pol_list[i].target_count; j++ )
        {

#ifdef _DEBUG_POLICIES
            printf( "    Checking file class %s\n", pol_list[i].target_list[j]->fileset_id );
#endif

            switch ( EntryMatches
                     ( p_entry_id, p_entry_attr, &pol_list[i].target_list[j]->definition ) )
            {
            case POLICY_MATCH:
                DisplayLog( LVL_FULL, POLICY_TAG,
                            "Entry " F_ENT_ID " matches target file class '%s' of policy '%s'",
                            P_ENT_ID( p_entry_id, p_entry_attr ), pol_list[i].target_list[j]->fileset_id,
                            pol_list[i].policy_id );
                if ( pp_fileset )
                    *pp_fileset = pol_list[i].target_list[j];
                return &pol_list[i];

            case POLICY_NO_MATCH:
                break;

            case POLICY_MISSING_ATTR:
                DisplayLog( LVL_MAJOR, POLICY_TAG, "Attributes are missing to check if entry " F_ENT_ID
                            " matches file class '%s' (in policy '%s')", P_ENT_ID( p_entry_id, p_entry_attr ),
                            pol_list[i].target_list[j]->fileset_id, pol_list[i].policy_id );
                break;

            default:
                DisplayLog( LVL_CRIT, POLICY_TAG, "Error while checking if entry " F_ENT_ID
                            " matches file class '%s' (in policy '%s')", P_ENT_ID( p_entry_id, p_entry_attr ),
                            pol_list[i].target_list[j]->fileset_id, pol_list[i].policy_id );
            }
        }
    }

    /* => entry matches no fileset in any policy */
    if ( pp_fileset )
        *pp_fileset = NULL;


    /* if there a default ? */
    if ( default_index != -1 )
        return &pol_list[default_index];

    /* entry matches no policy  */
    DisplayLog( LVL_MAJOR, POLICY_TAG, "Warning: entry " F_ENT_ID " matches no %s policy.",
                P_ENT_ID( p_entry_id, p_entry_attr ), ( policy_type == PURGE_POLICY ) ? "purge" : "migration" );

    return NULL;

}

/**
 *  Check if an entry has a chance to be matched in any policy condition.
 */
policy_match_t PolicyMatchAllConditions( const entry_id_t * p_entry_id,
                                         const attr_set_t * p_entry_attr,
                                         policy_type_t policy_type )
{
    int            could_not_match = FALSE;

    int            count, i, j;
    int            default_index = -1;
    policy_item_t *pol_list;

    /* if it MATCHES any whitelist condition, return NO_MATCH
     * else, it could potentially match a policy, so we must test them.
     */
    switch ( _IsWhitelisted( p_entry_id, p_entry_attr, policy_type, TRUE ) )
    {
    case POLICY_MATCH:
        return POLICY_NO_MATCH;
    case POLICY_MISSING_ATTR:
        could_not_match = TRUE;
        break;
    case POLICY_NO_MATCH:
        break;
    default:
        return POLICY_ERR;
    }

    switch ( policy_type )
    {
    case PURGE_POLICY:
        pol_list = policies.purge_policies.policy_list;
        count = policies.purge_policies.policy_count;
        break;
#ifdef HAVE_MIGR_POLICY
    case MIGR_POLICY:
        pol_list = policies.migr_policies.policy_list;
        count = policies.migr_policies.policy_count;
        break;
#endif
    default:
        DisplayLog( LVL_CRIT, POLICY_TAG, "Unsupported policy type %d", policy_type );
        return POLICY_ERR;
    }

    /* for each policy:
     * - if we get NO_MATCH for all filesets of the policy,
     *   no need to check the condition.
     * - if we get MATCH for any fileset of the policy,
     *   only test this condition.
     * - if we get MISSING_ATTR  for any fileset of the policy,
     *   test the condition.
     *
     * - if we get MATCH for the condition, return MATCH or MISSING_ATTR if we got some previously
     * - if we get NO_MATCH for the condition, this policy cannot be matched.
     * - if we get MISSING_ATTR for the condition, return MISSING_ATTR.
     */

    for ( i = 0; i < count; i++ )
    {
        int            match = FALSE;
        int            all_no_match = ( pol_list[i].target_count > 0 ? TRUE : FALSE );
        int            missing_attr = FALSE;

#ifdef _DEBUG_POLICIES
        printf( "Checking policy %s...\n", pol_list[i].policy_id );
#endif

        if ( !strcasecmp( pol_list[i].policy_id, "default" ) )
        {
            /* remember index of default policy */
            default_index = i;
            continue;
        }

        /* check filesets */

        for ( j = 0; j < pol_list[i].target_count; j++ )
        {

#ifdef _DEBUG_POLICIES
            printf( "    Checking file class %s\n", pol_list[i].target_list[j]->fileset_id );
#endif

            switch ( _EntryMatches
                     ( p_entry_id, p_entry_attr, &pol_list[i].target_list[j]->definition, TRUE ) )
            {
            case POLICY_MATCH:
                DisplayLog( LVL_FULL, POLICY_TAG,
                            "Entry matches target file class '%s' of policy '%s'",
                            pol_list[i].target_list[j]->fileset_id, pol_list[i].policy_id );
                all_no_match = FALSE;
                match = TRUE;
                break;

            case POLICY_NO_MATCH:
                break;

            case POLICY_MISSING_ATTR:
                all_no_match = FALSE;
                missing_attr = TRUE;
                DisplayLog( LVL_FULL, POLICY_TAG, "Attributes are missing to check if entry"
                            " matches file class '%s' (in policy '%s')",
                            pol_list[i].target_list[j]->fileset_id, pol_list[i].policy_id );
                break;

            default:
                DisplayLog( LVL_CRIT, POLICY_TAG, "Error while checking if entry"
                            " matches file class '%s' (in policy '%s')",
                            pol_list[i].target_list[j]->fileset_id, pol_list[i].policy_id );
                return POLICY_ERR;
            }

            /* if entry matches or an attribute is missing, we can test the condition */
            if ( match || missing_attr )
                break;
        }

#ifdef _DEBUG_POLICIES
        printf
            ( "Summary for target filesets of policy %s: match=%d, missing_attr=%d, all_no_match=%d\n",
              pol_list[i].policy_id, match, missing_attr, all_no_match );
#endif
        /**
         * - if we get MATCH for any fileset of the policy,
         *   only test this condition.
         * - if we get MISSING_ATTR  for any fileset of the policy,
         *   test the condition.
         */

        /* if we got NO_MATCH for all filesets of the policy, no need to check the condition. */
        if ( all_no_match )
            continue;

        /* test the condition of the policy */

        /* - if we get MATCH for the condition, return MATCH or MISSING_ATTR if we got some previously
         * - if we get NO_MATCH for the condition, this policy cannot be matched.
         * - if we get MISSING_ATTR for the condition, return MISSING_ATTR.
         */
        switch ( _EntryMatches( p_entry_id, p_entry_attr, &pol_list[i].condition, TRUE ) )
        {
        case POLICY_NO_MATCH:
            /* the entry cannot match this item */
            break;
        case POLICY_MATCH:
            /* return MATCH if we add not missing attrs previously,
             * MISSING_ATTR else.
             */
            DisplayLog( LVL_DEBUG, POLICY_TAG,
                        "Entry matches the condition for policy '%s'.", pol_list[i].policy_id );
            if ( missing_attr || could_not_match )
                return POLICY_MISSING_ATTR;
            else
                return POLICY_MATCH;
            break;
        case POLICY_MISSING_ATTR:
            return POLICY_MISSING_ATTR;
        default:
            DisplayLog( LVL_MAJOR, POLICY_TAG,
                        "Error checking if entry matches the condition for policy '%s'.",
                        pol_list[i].policy_id );
            return POLICY_ERR;
        }

        /*  if we get MATCH for any fileset of the policy,
         *  only test this condition. */
        if ( match )
            return POLICY_NO_MATCH;

    }

    /* at this point, we have no chance to match policies,
     * now check default case.
     */

    if ( default_index != -1 )
    {

        /* - if we get MATCH for the condition, return MATCH or MISSING_ATTR if we got some previously
         * - if we get NO_MATCH for the condition, no policy is matched.
         * - if we get MISSING_ATTR for the condition, return MISSING_ATTR.
         */
        switch ( _EntryMatches( p_entry_id, p_entry_attr, &pol_list[default_index].condition, TRUE ) )
        {
        case POLICY_NO_MATCH:
            return POLICY_NO_MATCH;
            break;
        case POLICY_MATCH:
            /* return MATCH if we add not missing attrs previously,
             * MISSING_ATTR else.
             */
            DisplayLog( LVL_DEBUG, POLICY_TAG, "Entry matches the condition for default policy" );
            if ( could_not_match )
                return POLICY_MISSING_ATTR;
            else
                return POLICY_MATCH;
            break;
        case POLICY_MISSING_ATTR:
            return POLICY_MISSING_ATTR;
        default:
            DisplayLog( LVL_MAJOR, POLICY_TAG,
                        "Error checking if entry matches the condition for default" );
            return POLICY_ERR;
        }

    }

    /* not matched */
    return POLICY_NO_MATCH;

}


#ifdef ATTR_INDEX_whitelisted
/**
 * check whitelist condition for file or directory entries
 */
int check_policies( const entry_id_t * p_id, attr_set_t * p_attrs )
{
    policy_match_t isok;

    if ( ATTR_MASK_TEST( p_attrs, type )
         && !strcmp( ATTR( p_attrs, type ), STR_TYPE_DIR ) )
    {
        /* generate needed fields */
        ListMgr_GenerateFields( p_attrs, policies.rmdir_policy.global_attr_mask );
        
        isok = IsWhitelisted( p_id, p_attrs, RMDIR_POLICY );

        if ( isok == POLICY_MATCH )
        {
            ATTR_MASK_SET( p_attrs, whitelisted );
            ATTR( p_attrs, whitelisted ) = TRUE;
        }
        else if ( isok == POLICY_NO_MATCH )
        {
            ATTR_MASK_SET( p_attrs, whitelisted );
            ATTR( p_attrs, whitelisted ) = FALSE;
        }
    }
    else                    /* non directory object */
    {
        /* generate needed fields */
        ListMgr_GenerateFields( p_attrs, policies.purge_policies.global_attr_mask );

        isok = IsWhitelisted( p_id, p_attrs, PURGE_POLICY );

        if ( isok == POLICY_MATCH )
        {
            ATTR_MASK_SET( p_attrs, whitelisted );
            ATTR( p_attrs, whitelisted ) = TRUE;
        }
        else if ( isok == POLICY_NO_MATCH )
        {
            ATTR_MASK_SET( p_attrs, whitelisted );
            ATTR( p_attrs, whitelisted ) = FALSE;
        }
    }

    return 0;
}
#endif


#ifdef HAVE_MIGR_POLICY

static const char *empty_str = "";

/* Analyse and replace params betweens brackets.
 * \param hints malloc'ated string.
 *        It can be reallocated or freed in this function.
 */
char * analyze_hints_params(  char * hints,
                          const policy_item_t * policy,
                          const fileset_item_t * fileset,
                          const entry_id_t * p_entry_id,
                          const attr_set_t * p_entry_attr )
{
    int error = FALSE;
    char * pass_begin = hints;
    char * begin_var;
    char * end_var;
    const char * value;

    do
    {
        char * new_str = NULL;

        /* look for a variable */
        begin_var = strchr( pass_begin, '{' );

        /* no more variables */
        if ( !begin_var )
            break;

        *begin_var = '\0';
        begin_var++;

        /* get matching '}' */
        end_var = strchr( begin_var, '}' );
        if (!end_var)
        {
           DisplayLog(LVL_CRIT,POLICY_TAG, "ERROR: unmatched '{' in migration hints '%s'", hints);
           error = TRUE;
           break;
        }

        *end_var = '\0';
        end_var++;

        value = NULL;

        /* compute final length, depending on variable name */
        if (!strcasecmp( begin_var, "policy" ) )
           value = policy->policy_id;
        else if (!strcasecmp( begin_var, "fileclass" ) )
           value = fileset->fileset_id;
        else if (!strcasecmp( begin_var, "path" ) )
        {
           if ( !ATTR_MASK_TEST(p_entry_attr, fullpath) )
           {
                DisplayLog(LVL_CRIT,POLICY_TAG, "ERROR: {path} parameter cannot be replaced in hints '%s'", hints);
                error = TRUE;
                break;
           }
           value = ATTR(p_entry_attr, fullpath); 
        }
        else if (!strcasecmp( begin_var, "name" ) )
        {
           if ( !ATTR_MASK_TEST(p_entry_attr, name) )
           {
                DisplayLog(LVL_CRIT,POLICY_TAG, "ERROR: {name} parameter cannot be replaced in hints '%s'", hints);
                error = TRUE;
                break;
           }
           value = ATTR(p_entry_attr, name); 
        }
        else if (!strcasecmp( begin_var, "ost_pool" ) )
        {
           if ( !ATTR_MASK_TEST(p_entry_attr, stripe_info) )
           {
                DisplayLog(LVL_CRIT,POLICY_TAG, "ERROR: {ost_pool} parameter cannot be replaced in hints '%s'", hints);
                error = TRUE;
                break;
           }
           value = ATTR(p_entry_attr, stripe_info).pool_name; 
        }
        else
        {
            DisplayLog(LVL_CRIT,POLICY_TAG, "ERROR: unknown parameter '%s' in hints '%s'", begin_var, hints);
            error = TRUE;
            break;
        }

        /* allocate a new string if var length < value length */
        new_str = malloc( strlen(pass_begin)+strlen(value)+strlen(end_var)+1 );

        sprintf(new_str, "%s%s%s", pass_begin, value, end_var );

        free(pass_begin);
        pass_begin = new_str;
        
    } while(1);

    return pass_begin;

}



char          *build_migration_hints( const policy_item_t * policy, const fileset_item_t * fileset,
                                      const entry_id_t * p_entry_id,
                                      const attr_set_t * p_entry_attr )
{
    unsigned int   policy_len = 0;
    unsigned int   fileset_len = 0;
    unsigned int   separ = 0;
    char          *output;

    /* what is hint len? */
    if ( policy )
        policy_len = strlen( policy->hints );
    if ( fileset )
        fileset_len = strlen( fileset->migration_hints );

    /* count separator */
    if ( policy_len && fileset_len )
        separ = 1;

    if ( policy_len + fileset_len + separ > 0 )
    {
        output = MemAlloc( policy_len + fileset_len + separ + 1 );
        if ( output == NULL )
        {
            DisplayLog( LVL_CRIT, POLICY_TAG, "Error: could not allocate memory in %s",
                        __FUNCTION__ );
            return NULL;
        }
        if ( policy_len )
            strcpy( output, policy->hints );
        if ( separ )
            output[policy_len] = ',';
        if ( fileset_len )
            strcpy( output + policy_len + separ, fileset->migration_hints );

        /* replace {xxx} parameters */
        output = analyze_hints_params( output, policy, fileset, p_entry_id, p_entry_attr );
    }
    else
        output = NULL;


    return output;

}

void free_migration_hints( char *hints )
{
    if ( hints == NULL )
        return;
    else if ( hints == empty_str )
        return;
    else
        MemFree( hints );
}

#endif



#ifndef GIGABYTE
#define GIGABYTE (1024*1024*1024)
#endif


