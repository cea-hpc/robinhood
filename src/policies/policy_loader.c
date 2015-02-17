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

#include "policy_rules.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "rbh_modules.h"
#include "rbh_cfg_helpers.h"
#include "status_manager.h"
#include <errno.h>
#include <fnmatch.h>

#define FILESETS_SECTION      "Filesets"
#define FILESET_BLOCK         "FileClass"

#define POLICY_DECLARATION    "define_policy"

#define OLD_PURGEPOLICY_BLOCK     "purge_policies"
#define OLD_MIGRPOLICY_BLOCK      "migration_policies"
#define OLD_UNLINKPOLICY_BLOCK    "hsm_remove_policy"

#define OLD_RULE_BLOCK        "policy"
#define RULE_BLOCK            "rule"

#define OLD_POLICIES_BLOCK    "policies"
#define POLICIES_BLOCK        "rules"

#define IGNORE_BLOCK          "ignore"
#define IGNORE_FC             "ignore_fileclass"
#define CONDITION_BLOCK       "condition"
#define DEFINITION_BLOCK      "definition"
#define SCOPE_BLOCK           "scope"

//#define RMDIR_BLOCK      "rmdir_policy"
#define RM_RECURSE_BLOCK "recursive_rmdir"

#define RELOAD_TAG  "PolicyReload"
#define CHK_TAG     "PolicyCheck"
#define LOADER_TAG  "PolicyLoader"

static const policies_t  policy_initializer = {0};
policies_t policies = {0};


#define critical_err_check(_ptr_, _blkname_) do { if (!_ptr_) {\
                                        sprintf(msg_out, "Internal error reading %s block in config file", _blkname_); \
                                        return EFAULT; \
                                    }\
                                } while (0)

#define critical_err_check_goto(_ptr_, _blkname_, _label) do { if (!_ptr_) {\
                                        sprintf(msg_out, "Internal error reading %s block in config file", _blkname_); \
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
int compare_boolexpr(const bool_node_t * expr1, const bool_node_t * expr2)
{
    if (expr1->node_type != expr2->node_type)
        return 1;

    switch (expr1->node_type)
    {
    case NODE_UNARY_EXPR:
        if (expr1->content_u.bool_expr.bool_op != expr2->content_u.bool_expr.bool_op)
            return true;

        return compare_boolexpr(expr1->content_u.bool_expr.expr1,
                                 expr2->content_u.bool_expr.expr1);

    case NODE_BINARY_EXPR:
        if (expr1->content_u.bool_expr.bool_op != expr2->content_u.bool_expr.bool_op)
            return true;

        return (compare_boolexpr
                 (expr1->content_u.bool_expr.expr1, expr2->content_u.bool_expr.expr1)
                 || compare_boolexpr(expr1->content_u.bool_expr.expr2,
                                      expr2->content_u.bool_expr.expr2));

    case NODE_CONDITION:
        /* compare criteria */
        if (expr1->content_u.condition->crit != expr2->content_u.condition->crit)
            return true;

        /* compare operator, except for custom cmd and xattr */
        if ((expr1->content_u.condition->crit != CRITERIA_XATTR)
             && (expr1->content_u.condition->op != expr2->content_u.condition->op))
            return true;

        /* same structure */
        return false;
    }

    /* should not happen */
    RBH_BUG("Unexpected node_type in boolean expression");
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
bool update_boolexpr(const bool_node_t * tgt, const bool_node_t * src)
{
    compare_triplet_t *p_triplet1;
    compare_triplet_t *p_triplet2;
    char           tmp_buff1[256];
    char           tmp_buff2[256];
    bool           rc;

    switch (tgt->node_type)
    {
    case NODE_UNARY_EXPR:
        return update_boolexpr(tgt->content_u.bool_expr.expr1, src->content_u.bool_expr.expr1);

    case NODE_BINARY_EXPR:
        rc = update_boolexpr(tgt->content_u.bool_expr.expr1, src->content_u.bool_expr.expr1);
        if (update_boolexpr(tgt->content_u.bool_expr.expr2, src->content_u.bool_expr.expr2))
            rc = true;
        return rc;

    case NODE_CONDITION:

        p_triplet1 = tgt->content_u.condition;
        p_triplet2 = src->content_u.condition;

        switch (p_triplet1->crit)
        {
        case CRITERIA_SIZE:
            if (p_triplet1->val.size != p_triplet2->val.size)
            {
                FormatFileSize(tmp_buff1, 256, p_triplet1->val.size);
                FormatFileSize(tmp_buff2, 256, p_triplet2->val.size);

                DisplayLog(LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                            criteria2str(CRITERIA_SIZE), op2str(p_triplet1->op), tmp_buff1,
                            criteria2str(CRITERIA_SIZE), op2str(p_triplet2->op), tmp_buff2);
                p_triplet1->val.size = p_triplet2->val.size;
                return true;
            }
            else
                return false;

            /* integer conditions */
        case CRITERIA_DEPTH:
        case CRITERIA_OST:

#ifdef ATTR_INDEX_dircount
        case CRITERIA_DIRCOUNT:
#endif
            if (p_triplet1->val.integer != p_triplet2->val.integer)
            {
                DisplayLog(LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %d) -> (%s %s %d)",
                            criteria2str(p_triplet1->crit), op2str(p_triplet1->op),
                            p_triplet1->val.integer, criteria2str(p_triplet2->crit),
                            op2str(p_triplet2->op), p_triplet2->val.integer);
                p_triplet1->val.integer = p_triplet2->val.integer;
                return true;
            }
            else
                return false;

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
            if (p_triplet1->val.duration != p_triplet2->val.duration)
            {
                FormatDurationFloat(tmp_buff1, 256, p_triplet1->val.duration);
                FormatDurationFloat(tmp_buff2, 256, p_triplet2->val.duration);
                DisplayLog(LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                            criteria2str(p_triplet1->crit), op2str(p_triplet1->op), tmp_buff1,
                            criteria2str(p_triplet2->crit), op2str(p_triplet2->op), tmp_buff2);
                p_triplet1->val.duration = p_triplet2->val.duration;
                return true;
            }
            else
                return false;

#ifdef ATTR_INDEX_type
        case CRITERIA_TYPE:
            if (p_triplet1->val.type != p_triplet2->val.type)
            {
                DisplayLog(LVL_EVENT, RELOAD_TAG,
                            "Criteria value updated: (%s %s %s) -> (%s %s %s)",
                            criteria2str(p_triplet1->crit), op2str(p_triplet1->op),
                            type2str(p_triplet1->val.type), criteria2str(p_triplet2->crit),
                            op2str(p_triplet2->op), type2str(p_triplet2->val.type));
                p_triplet1->val.type = p_triplet2->val.type;
                return true;
            }
            else
                return false;
#endif

            /* unmodifiable conditions */
        case CRITERIA_TREE:
        case CRITERIA_PATH:
        case CRITERIA_FILENAME:
        case CRITERIA_OWNER:
        case CRITERIA_GROUP:
        case CRITERIA_POOL:
            if (strcmp(p_triplet1->val.str, p_triplet2->val.str))
            {
                DisplayLog(LVL_MAJOR, RELOAD_TAG,
                            "Condition changed on attribute '%s' but this cannot be modified dynamically",
                            criteria2str(p_triplet1->crit));
            }
            return false;

        case CRITERIA_XATTR:
            if (strcmp(p_triplet1->val.str, p_triplet2->val.str)
                 || strcmp(p_triplet1->xattr_name, p_triplet2->xattr_name))
            {
                DisplayLog(LVL_MAJOR, RELOAD_TAG,
                            "xattr condition changed, but it cannot be modified dynamically");
            }
            return false;

        default:
            DisplayLog(LVL_CRIT, RELOAD_TAG,
                        "Unsupported attribute: %s", criteria2str(p_triplet1->crit));
        }

        break;
    }

    /* should not happen */
    RBH_BUG("Unexpected node_type in boolean expression");
    return -1;
}                               /* update_boolexpr */


/** Update whitelist rules */
/* @TODO support whitelist rules update on SIGHUP */
static void __attribute__((__unused__))
            update_whitelist(whitelist_item_t * old_items, unsigned int old_count,
                              whitelist_item_t * new_items, unsigned int new_count,
                              const char *block_name)
{
    unsigned int   i;

    if (old_count != new_count)
    {
        DisplayLog(LVL_MAJOR, RELOAD_TAG,
                    "Whitelist rules count changed in block '%s' but cannot be modified dynamically: whitelist update cancelled",
                    block_name);
        return;
    }

    /* compare whitelist boolean expression structure */
    for (i = 0; i < new_count; i++)
    {
        if ((old_items[i].attr_mask != new_items[i].attr_mask)
             || compare_boolexpr(&old_items[i].bool_expr,
                                  &new_items[i].bool_expr))
        {
            DisplayLog(LVL_MAJOR, RELOAD_TAG,
                        "Whitelist expression #%u changed in block '%s'. "
                        "Only numerical values can be modified dynamically. "
                        "Whitelist update cancelled", i, block_name);
            return;
        }
    }

    /* if they are all the same, update/check their values */

    for (i = 0; i < new_count; i++)
    {
        if (update_boolexpr(&old_items[i].bool_expr, &new_items[i].bool_expr))
        {
            char           criteriastr[2048];
            BoolExpr2str(&old_items[i].bool_expr, criteriastr, 2048);
            DisplayLog(LVL_EVENT, RELOAD_TAG,
                        "Whitelist expression #%u in block '%s' has been updated and is now: %s", i,
                        block_name, criteriastr);
        }
    }

    /* XXX attr_mask is unchanged, since we keep the same expression structures */

}


static void free_whitelist(whitelist_item_t * p_items, unsigned int count)
{
    unsigned int   i;

    /* free boolean expressions */
    for (i = 0; i < count; i++)
    {
        FreeBoolExpr(&p_items[i].bool_expr, false);
    }

    if ((count > 0) && (p_items != NULL))
        free(p_items);
}

static int parse_policy_decl(config_item_t config_blk, const char *block_name,
                             policy_descr_t *policy, bool *manage_deleted,
                             char *msg_out)
{
    int          rc, i;
    const char  *name;
    char         tmpstr[1024];
    bool         has_scope = false;
    uint64_t     mask;
    char       **extra = NULL;
    unsigned int extra_cnt = 0;

    static const char *expect[] =
    {
        "status_manager", "scope", "default_action",
        "default_lru_sort_attr",
        NULL
    };

    name = rh_config_GetBlockId(config_blk);
    if (!name)
    {
        strcpy(msg_out, "Missing name for '"POLICY_DECLARATION"' block "
               "(ex: "POLICY_DECLARATION" my_policy { ...");
        return ENOENT;
    }

    /* @TODO check the policy name is not already used! */

    /* parse the parameter */
    if (strlen(name) > POLICY_NAME_LEN - 1)
    {
        sprintf(msg_out, "Policy name is too long (max: %u).", POLICY_NAME_LEN - 1);
        return EINVAL;
    }
    rh_strncpy(policy->name, name, sizeof(policy->name));

    rc = GetStringParam(config_blk, block_name, "default_action",
                        PFLG_NO_WILDCARDS | PFLG_MANDATORY,
                        tmpstr, sizeof(tmpstr), &extra, &extra_cnt, msg_out);
    if (rc)
        return rc;
    if (!strcasecmp(tmpstr, "cmd"))
    {
        /* external command */
        /* 1 single argument expected */
        if (extra_cnt != 1)
        {
            strcpy(msg_out, "A single argument is expected for cmd. E.g.: default_action = cmd(\"myscript.sh\");");
            return EINVAL;
        }
        /* absolute path expected */
        else if (extra[0][0] != '/')
        {
            strcpy(msg_out, "An absolute path is expected for default_action::cmd");
            return EINVAL;
        }
        rh_strncpy(policy->default_action.action_u.command, extra[0],
                   sizeof(policy->default_action.action_u.command));
        policy->default_action.type = ACTION_COMMAND;
    }
    else
    {
        if (extra_cnt != 0)
        {
            strcpy(msg_out, "No extra argument is expected for default_action");
            return EINVAL;
        }
        policy->default_action.action_u.function = module_get_action_by_name(tmpstr);
        if (policy->default_action.action_u.function == NULL)
        {
            sprintf(msg_out, "default_action: unknown function '%s'", tmpstr);
            return EINVAL;
        }
        policy->default_action.type = ACTION_FUNCTION;
    }

    rc = GetStringParam(config_blk, block_name, "default_lru_sort_attr",
                        PFLG_NO_WILDCARDS | PFLG_MANDATORY, tmpstr, sizeof(tmpstr), NULL, NULL,
                        msg_out);
    if (rc)
        return rc;
    /* is it a time attribute? */
    rc = str2lru_attr(tmpstr);
    if (rc == -1)
    {
        strcpy(msg_out, "time attribute expected for 'default_lru_sort_attr': "
               ALLOWED_LRU_ATTRS_STR"...");
        return EINVAL;
    }
    else
        policy->default_lru_sort_attr = rc;

    extra = NULL;
    extra_cnt = 0;
    rc = GetStringParam(config_blk, block_name, "status_manager",
                        PFLG_MANDATORY | PFLG_NO_WILDCARDS, tmpstr, sizeof(tmpstr),
                        &extra, &extra_cnt, msg_out);
    if (rc == ENOENT)
        strcat(msg_out, "\nIf you don't need a status manager, you should explicitely specify: status_manager=none");
    if (rc != 0)
        return rc;

    if (!strcasecmp(tmpstr, "none"))
    {
        policy->status_mgr = NULL;
        if (extra_cnt > 0)
        {
            sprintf(msg_out, "No argument expected after 'status_manager = none': found '%s'",
                    extra[0]);
            return EINVAL;
        }
    }
    else
    {
        policy->status_mgr = create_sm_instance(policy->name, tmpstr);
        if (policy->status_mgr == NULL)
        {
            sprintf(msg_out, "Could not load status manager '%s'", tmpstr);
            return EINVAL;
        }

        /* does this policy manage deleted entries? */
        if (extra_cnt == 1 && (!strcasecmp(extra[0], "removed")
                             || !strcasecmp(extra[0], "deleted")))
        {
            /* the status manager must handle them */
            if (!smi_manage_deleted(policy->status_mgr))
            {
                sprintf(msg_out, "'%s' is specified for status manager '%s' whereas it cannot handle deleted entries.",
                        extra[0], tmpstr);
                return EINVAL;
            }
            policy->manage_deleted = true;
            *manage_deleted = true;
        }
        else if (extra_cnt != 0)
        {
            sprintf(msg_out, "Too many arguments (%u) found for status manager '%s'",
                    extra_cnt, tmpstr);
            return EINVAL;
        }
    }

    /* parse sub blocks */
    for (i = 0; i < rh_config_GetNbItems(config_blk); i++)
    {
        config_item_t  sub_item = rh_config_GetItemByIndex(config_blk, i);
        critical_err_check(sub_item, block_name);
        char          *subitem_name;

        if (rh_config_ItemType(sub_item) == CONFIG_ITEM_BLOCK)
        {
            subitem_name = rh_config_GetBlockName(sub_item);
            critical_err_check(subitem_name, block_name);

            if (strcasecmp(subitem_name, SCOPE_BLOCK) != 0)
            {
                sprintf(msg_out, "'%s' sub-block unexpected in %s block, line %d.",
                         subitem_name, block_name, rh_config_GetItemLine(sub_item));
                return EINVAL;
            }

            /* check double declaration */
            if (has_scope)
            {
                sprintf(msg_out, "Double scope declaration in policy %s, line %d.",
                        policy->name, rh_config_GetItemLine(sub_item));
                return EINVAL;
            }

            /* analyze boolean expression */
            /* pass the status manager instance to interpret status condition
             * depending on the context */
            mask = 0;
            rc = GetBoolExpr(sub_item, SCOPE_BLOCK, &policy->scope, &mask,
                             msg_out, policy->status_mgr);
            if (rc)
                return rc;

            policy->scope_mask = mask;
            has_scope = true;
        }
    }

    if (!has_scope)
    {
        sprintf(msg_out, "Missing mandatory parameter 'scope' in block '%s'",
                block_name);
        return ENOENT;
    }

    CheckUnknownParameters(config_blk, block_name, expect);
    return 0;
}


static int read_policy_definitions(config_file_t config, policies_t *pol,
                                   char *msg_out)
{
    unsigned int blc_index;
    int rc;

    for (blc_index = 0; blc_index < rh_config_GetNbBlocks(config); blc_index++)
    {
        char *block_name;
        config_item_t  curr_item = rh_config_GetBlockByIndex(config, blc_index);
        critical_err_check(curr_item, "root");

        if (rh_config_ItemType(curr_item) != CONFIG_ITEM_BLOCK)
            continue;

        block_name = rh_config_GetBlockName(curr_item);
        critical_err_check(block_name, "root");

        if (!strcasecmp(block_name, POLICY_DECLARATION))
        {
            bool manage_deleted = false;

            if (pol->policy_count == 0)
                pol->policy_list = (policy_descr_t *)malloc(sizeof(policy_descr_t));
            else
                pol->policy_list = (policy_descr_t *)realloc(pol->policy_list,
                        (pol->policy_count + 1) * sizeof(policy_descr_t));

            memset(&pol->policy_list[pol->policy_count], 0, sizeof(policy_descr_t));

            pol->policy_count ++;

            /* analyze policy declaration */
            rc = parse_policy_decl(curr_item, block_name,
                                   &pol->policy_list[pol->policy_count - 1],
                                   &manage_deleted, msg_out);
            if (rc)
                return rc;

            if (manage_deleted)
                pol->manage_deleted = 1;
        }
    }
    return 0;
}

#ifdef HAVE_RMDIR_POLICY

static void set_default_rmdir_policy(rmdir_policy_t *policy)
{
    policy->age_rm_empty_dirs = 0;      /* disable empty dir removal */

    policy->whitelist_rules = NULL;
    policy->whitelist_count = 0;

    /* directory to be removed recursively (even if not empty) */
    policy->recursive_rmdir_rules = NULL;
    policy->recursive_rmdir_count = 0;

    policy->global_attr_mask = 0;
}

static int write_default_rmdir_policy(FILE * output)
{
    print_begin_block(output, 0, RMDIR_BLOCK, NULL);
    print_line(output, 1, "age_rm_empty_dirs    : 0 (disabled)");
    print_line(output, 1, "whitelist rules      : NONE");
    print_line(output, 1, "recursive_rmdir rules: NONE");
    print_end_block(output, 0);

    return 0;
}

static int write_rmdir_policy_template(FILE * output)
{
    print_begin_block(output, 0, RMDIR_BLOCK, NULL);
    print_line(output, 1, "# Remove directories that have been empty for more than 8 days");
    print_line(output, 1, "age_rm_empty_dirs   = 8d ;");
    fprintf(output, "\n");
    print_line(output, 1, "# don't remove dirs that owns to 'root' or whose depth < 2");
    print_begin_block(output, 1, IGNORE_BLOCK, NULL);
    print_line(output, 2, "owner == root");
    print_line(output, 2, "or depth < 2");
    print_end_block(output, 1);
    fprintf(output, "\n");
    print_line(output, 1, "# Recursively remove directories matching this condition");
    print_line(output, 1, "# (/!\\ removes all the directory content)");
    print_begin_block(output, 1, RM_RECURSE_BLOCK, NULL);
    print_line(output, 2, "path == /mnt/lustre/old/*");
    print_line(output, 2, "and last_mod > 30d");
    print_end_block(output, 1);
    print_end_block(output, 0);

    return 0;
}


static int read_rmdir_policy(config_file_t config,  rmdir_policy_t *policy, char *msg_out)
{
    unsigned int   blc_index;
    int            rc, tmpval;

    static const char *rmdir_expect[] = { IGNORE_BLOCK, RM_RECURSE_BLOCK, "age_rm_empty_dirs", NULL };

    /* get RMDIR block */

    config_item_t  rmdir_block = rh_config_FindItemByName(config, RMDIR_BLOCK);

    /* not mandatory */
    if (rmdir_block == NULL)
    {
#ifdef _DEBUG_PARSING
        printf("%s block not found in config file\n", RMDIR_BLOCK);
#endif
        return 0;
    }

    /* initialize global attributes mask */
    policy->global_attr_mask = 0;

    /* read white list rules */

    for (blc_index = 0; blc_index < rh_config_GetNbItems(rmdir_block); blc_index++)
    {
        char          *block_name;
        config_item_t  curr_item = rh_config_GetItemByIndex(rmdir_block, blc_index);
        critical_err_check(curr_item, RMDIR_BLOCK);

        if (rh_config_ItemType(curr_item) != CONFIG_ITEM_BLOCK)
            continue;

        block_name = rh_config_GetBlockName(curr_item);
        critical_err_check(block_name, RMDIR_BLOCK);

#ifdef _DEBUG_PARSING
        printf("subblock found in %s: %s\n", RMDIR_BLOCK, block_name);
#endif

        if (!strcasecmp(block_name, IGNORE_BLOCK))
        {
            if (policy->whitelist_count == 0)
                policy->whitelist_rules =
                    (whitelist_item_t *) malloc(sizeof(whitelist_item_t));
            else
                policy->whitelist_rules =
                    (whitelist_item_t *) realloc(policy->whitelist_rules,
                                                    (policy->whitelist_count +
                                                      1) * sizeof(whitelist_item_t));

            policy->whitelist_count++;

            /* analyze boolean expression */
            /* XXX allow using 'status' in ignore statement? */
            rc = GetBoolExpr(curr_item, block_name,
                              &policy->whitelist_rules[policy->whitelist_count - 1].bool_expr,
                              &policy->whitelist_rules[policy->whitelist_count - 1].attr_mask,
                              msg_out, NULL);

            if (rc)
                return rc;

            policy->global_attr_mask |=
                policy->whitelist_rules[policy->whitelist_count - 1].attr_mask;
        }
        else if (!strcasecmp(block_name, RM_RECURSE_BLOCK))
        {
            if (policy->recursive_rmdir_count == 0)
                policy->recursive_rmdir_rules =
                    (whitelist_item_t *) malloc(sizeof(whitelist_item_t));
            else
                policy->recursive_rmdir_rules =
                    (whitelist_item_t *) realloc(policy->recursive_rmdir_rules,
                                                    (policy->recursive_rmdir_count + 1)
                                                    * sizeof(whitelist_item_t));

            policy->recursive_rmdir_count++;

            /* analyze boolean expression */
            rc = GetBoolExpr(curr_item, block_name,
                              &policy->recursive_rmdir_rules[policy->recursive_rmdir_count - 1].bool_expr,
                              &policy->recursive_rmdir_rules[policy->recursive_rmdir_count - 1].attr_mask,
                              msg_out);

            if (rc)
                return rc;

            policy->global_attr_mask |=
                policy->recursive_rmdir_rules[policy->recursive_rmdir_count - 1].attr_mask;
        }
        else
        {
            sprintf(msg_out, "Unknown sub-block '%s' in " RMDIR_BLOCK " block", block_name);
            return EINVAL;
        }

    }

    /* retrieve age_rm_empty_dirs */

    rc = GetDurationParam(rmdir_block, RMDIR_BLOCK, "age_rm_empty_dirs",
                           INT_PARAM_POSITIVE, &tmpval, NULL, NULL, msg_out);

#ifdef _DEBUG_PARSING
    if (rc)
        printf("no value found for age_rm_empty_dirs\n");
    else
        printf("value found for age_rm_empty_dirs: %d\n", tmpval);
#endif

    if (rc != 0 && rc != ENOENT)
        return rc;
    else if (rc != ENOENT)
        policy->age_rm_empty_dirs = tmpval;

    CheckUnknownParameters(rmdir_block, RMDIR_BLOCK, rmdir_expect);

    return 0;
}

#if 0 /** FIXME RBHv3 UNUSED FOR NOW */
__attribute__((unused))
static int reload_rmdir_policy(rmdir_policy_t *policy)
{
    /** @TODO prevent from concurrent access when rmdir policy is beeing checked */

    update_whitelist(policies.rmdir_policy.whitelist_rules, policies.rmdir_policy.whitelist_count,
                      policy->whitelist_rules, policy->whitelist_count, RMDIR_BLOCK);

    update_whitelist(policies.rmdir_policy.recursive_rmdir_rules, policies.rmdir_policy.recursive_rmdir_count,
                      policy->recursive_rmdir_rules, policy->recursive_rmdir_count, RMDIR_BLOCK);

    /* check age_rm_empty_dirs parameter */

    if (policies.rmdir_policy.age_rm_empty_dirs != policy->age_rm_empty_dirs)
    {
        char           tmp_buff1[256];
        char           tmp_buff2[256];

        FormatDurationFloat(tmp_buff1, 256, policies.rmdir_policy.age_rm_empty_dirs);
        FormatDurationFloat(tmp_buff2, 256, policy->age_rm_empty_dirs);
        DisplayLog(LVL_EVENT, RELOAD_TAG, RMDIR_BLOCK "::age_rm_empty_dirs updated: %s->%s",
                    tmp_buff1, tmp_buff2);
        policies.rmdir_policy.age_rm_empty_dirs = policy->age_rm_empty_dirs;
    }

    return 0;
}

#endif /* HAVE_RMDIR_POLICY */

#ifdef HAVE_RM_POLICY
static void set_default_unlink_policy(unlink_policy_t * upol)
{
    upol->hsm_remove = true; /* hsm_remove enabled */
    upol->deferred_remove_delay = 86400; /* 1 day */
}

static int write_default_unlink_policy(FILE * output)
{
    print_begin_block(output, 0, UNLINKPOLICY_BLOCK, NULL);
    print_line(output, 1, "hsm_remove       :  enabled");
    print_line(output, 1, "deferred_remove_delay:  1d");
    print_end_block(output, 0);
    return 0;
}

static int write_unlink_policy_template(FILE * output)
{
    print_begin_block(output, 0, UNLINKPOLICY_BLOCK, NULL);
    print_line(output, 1, "# set this parameter to 'off' for disabling HSM object removal");
    print_line(output, 1, "hsm_remove = enabled;");
    print_line(output, 1, "# delay before impacting object removal in HSM");
    print_line(output, 1, "deferred_remove_delay = 24h;");
    print_end_block(output, 0);
    return 0;
}
#endif

#endif /* 0 */

static int write_default_filesets(FILE *output)
{
    print_begin_block(output, 0, FILESETS_SECTION, NULL);
    print_line(output, 1, "# none");
    print_end_block(output, 0);

    return 0;
}

#if 0
static int write_default_policy(FILE * output, policy_type_t policy_type)
{
    if (policy_type == PURGE_POLICY)
        print_begin_block(output, 0, PURGEPOLICY_BLOCK, NULL);
    else if (policy_type == MIGR_POLICY)
        print_begin_block(output, 0, MIGRPOLICY_BLOCK, NULL);

    print_line(output, 1, "# none");
    print_end_block(output, 0);
    return 0;
}
#endif

static int write_template_filesets(FILE * output)
{
    fprintf(output, "#### File Class definitions ####\n\n");

    print_begin_block(output, 0, FILESETS_SECTION, NULL);

    print_begin_block(output, 1, FILESET_BLOCK, "Experiment_A");

    print_begin_block(output, 2, DEFINITION_BLOCK, NULL);
    print_line(output, 3, "tree == \"/mnt/lustre/dir_A\"");
    print_end_block(output, 2);

#ifdef HAVE_MIGR_POLICY
    print_line(output, 2, "migration_hints = \"cos=3,class={FileClass},priority=2\" ;");
#endif
#ifdef _LUSTRE_HSM
    print_line(output, 2, "# target archive");
    print_line(output, 2, "lhsm_archive_hints = \"archive_id=1\" ;");
#endif
    print_end_block(output, 1);

    fprintf(output, "\n");

    print_begin_block(output, 1, FILESET_BLOCK, "visualization");

    print_begin_block(output, 2, DEFINITION_BLOCK, NULL);
    print_line(output, 3, "tree == \"/mnt/lustre/dir_*\"");
    print_line(output, 3, "and");
    print_line(output, 3, "xattr.user.tag_visu == 1");
    print_end_block(output, 2);

#ifdef HAVE_MIGR_POLICY
    print_line(output, 2, "migration_hints = \"cos=4,class={Fileclass},priority=5\";");
#endif
#ifdef _LUSTRE_HSM
    fprintf(output, "\n");
    print_line(output, 2, "# target archive");
    print_line(output, 2, "lhsm_archive_hints = \"archive_id=2\" ;");
#endif
    print_end_block(output, 1);
    fprintf(output, "\n");

#ifdef _LUSTRE
    print_begin_block(output, 1, FILESET_BLOCK, "pool_ssd");

    print_begin_block(output, 2, DEFINITION_BLOCK, NULL);
    print_line(output, 3, "ost_pool == \"ssd*\"");
    print_end_block(output, 2);
    print_end_block(output, 1);
    fprintf(output, "\n");
    print_begin_block(output, 1, FILESET_BLOCK, "ost_set");
    print_begin_block(output, 2, DEFINITION_BLOCK, NULL);
    print_line(output, 3, "# condition on ost_index is true");
    print_line(output, 3, "# if one of the storage objects of the file");
    print_line(output, 3, "# matches each condition:");
    print_line(output, 3, "# ost_index == 15 or ost_index == 20");
    print_line(output, 3, "# => a part of the file must be on OST 15 or 20");
    print_line(output, 3, "# ost_index == 15 and ost_index == 20");
    print_line(output, 3, "# => the file must have objects at least on OST 15 and 20");
    print_line(output, 3, "# ost_index != 12 and ost_index != 13");
    print_line(output, 3, "# => the file must not have objects on OSTs 12 and 13");
    print_line(output, 3, "ost_index == 1 or ost_index == 2 or");
    print_line(output, 3, "ost_index == 1 or ost_index == 2 or");
    print_line(output, 3, "ost_index == 3 or ost_index == 4");
    print_end_block(output, 2);
    print_end_block(output, 1);
    fprintf(output, "\n");
#endif

    print_line(output, 1, "# defining fileclass as a union or intersection:");
    print_begin_block(output, 1, FILESET_BLOCK, "visu_expA");
    print_begin_block(output, 2, DEFINITION_BLOCK, NULL);
    print_line(output, 2, "visualization inter Experiment_A");
    print_end_block(output, 2);
    print_end_block(output, 1);
    fprintf(output, "\n");

    print_end_block(output, 0);

    fprintf(output, "\n");

    return 0;
}

#if 0
#ifdef HAVE_MIGR_POLICY

static int write_migration_policy_template(FILE * output)
{

    fprintf(output, "#### Migration policies (archiving) ####\n\n");

    print_begin_block(output, 0, MIGRPOLICY_BLOCK, NULL);


    print_begin_block(output, 1, POLICY_BLOCK, "standard_copy");

    print_line(output, 2, "target_fileclass = experiment_A;");
    print_line(output, 2, "target_fileclass = pool_ssd;");
    fprintf(output, "\n");

    print_line(output, 2, "# Copy a file 6hours after its creation if it as never been archived.");
    print_line(output, 2, "# For next changes, archive it daily.");
    print_line(output, 2, "# In all cases, do nothing when it has been modified too recently (-30min).");
    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "((last_archive == 0 and creation > 6h) ");
    print_line(output, 3, "  or last_archive > 1d)");
    print_line(output, 3, "and last_mod > 30min");
    print_end_block(output, 2);

#ifdef _LUSTRE_HSM
    fprintf(output, "\n");
    print_line(output, 2, "# target archive (/!\\ policy archive_id overrides fileset archive_id)");
    print_line(output, 2, "lhsm_archive_hints = "archive_id=3" ;");
#endif

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_begin_block(output, 1, POLICY_BLOCK, "visu_copy");

    print_line(output, 2, "target_fileclass = visualization ;");
    fprintf(output, "\n");

    print_line(output, 2, "# copy those files quickly after they have been modified");
    print_line(output, 2, "# or if they have been archived more that 6h ago");
    print_line(output, 2, "# (if they are still beeing modified)");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_mod > 1h");
    print_line(output, 3, "or");
    print_line(output, 3, "last_archive > 6h");
    print_end_block(output, 2);

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_line(output, 1, "# Default migration policy.");
    print_line(output, 1, "# This applies to files that don't match previous fileclasses, i.e:");
    print_line(output, 1, "#   - don't match the 'ignore' block");
    print_line(output, 1, "#   - don't match a fileclass of 'ignore_fileclass' directives");
    print_line(output, 1, "#   - don't match any 'target_fileclass' of migration policies above");
    print_begin_block(output, 1, POLICY_BLOCK, "default");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_mod > 12h");
    print_end_block(output, 2);

#ifdef _LUSTRE_HSM
    fprintf(output, "\n");
    print_line(output, 2, "# target archive");
    print_line(output, 2, "lhsm_archive_hints = "archive_id=2" ;");
#endif

    print_end_block(output, 1);


    print_end_block(output, 0);
    fprintf(output, "\n");

    return 0;
}
#endif

#ifdef HAVE_PURGE_POLICY
#ifndef _LUSTRE_HSM
/* Template for NON-lustre-HSM purposes */
static int write_purge_policy_template(FILE * output)
{
    print_begin_block(output, 0, PURGEPOLICY_BLOCK, NULL);

    print_line(output, 1, "# Do not purge files accessed recently, or whose size is 0,");
    print_line(output, 1, "# or located in \"/mnt/lustre/system_files\" directory.");
    print_begin_block(output, 1, IGNORE_BLOCK, NULL);
    print_line(output, 2, "last_access < 1h");
    print_line(output, 2, "or size == 0");
    print_line(output, 2, "or tree == \"/mnt/lustre/system_files\"");
    print_end_block(output, 1);

    fprintf(output, "\n");

#ifdef _LUSTRE
    print_line(output, 1, "# do not purge files in FileClass \"pool_ssd\"");
    print_line(output, 1, IGNORE_FC " = pool_ssd ;");
    fprintf(output, "\n");
#endif

    print_line(output, 1, "# Purge files of class 'Experiment_A' after 24h");
    print_begin_block(output, 1, POLICY_BLOCK, "expA_1day");

    print_line(output, 2, "target_fileclass = Experiment_A ;");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_mod > 24h");
    print_line(output, 3, "and");
    print_line(output, 3, "last_access > 6h");
    print_end_block(output, 2);

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_line(output, 1, "# Release files of class 'visualization' after 1 month,");
    print_line(output, 1, "# if they are not accessed for more that 3 days");
    print_begin_block(output, 1, POLICY_BLOCK, "visu_1month");

    print_line(output, 2, "target_fileclass = visualization ;");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_mod > 30d");
    print_line(output, 3, "and");
    print_line(output, 3, "last_access > 3d");
    print_end_block(output, 2);

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_line(output, 1, "# Default purge policy.");
    print_line(output, 1, "# This applies to files that don't match previous fileclasses, i.e:");
    print_line(output, 1, "#   - don't match the 'ignore' block");
    print_line(output, 1, "#   - don't match a fileclass of 'ignore_fileclass' directives");
    print_line(output, 1, "#   - don't match any 'target_fileclass' of purge policies above");
    print_begin_block(output, 1, POLICY_BLOCK, "default");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_access > 12h");
    print_end_block(output, 2);

    print_end_block(output, 1);

    print_end_block(output, 0);
    fprintf(output, "\n");

    return 0;
}

#else

static int write_purge_policy_template(FILE * output)
{
    fprintf(output, "#### Purge policies (space release) ####\n\n");

    print_begin_block(output, 0, PURGEPOLICY_BLOCK, NULL);

    print_line(output, 1, "# do not purge files owned by \"foo\" or \"charlie\"");
    print_begin_block(output, 1, IGNORE_BLOCK, NULL);
    print_line(output, 2, "owner == \"foo\"");
    print_line(output, 2, "or");
    print_line(output, 2, "owner == \"charlie\"");
    print_end_block(output, 1);
    fprintf(output, "\n");

    print_line(output, 1, "# do not purge files in FileClass \"pool_ssd\"");
    print_line(output, 1, IGNORE_FC " = pool_ssd ;");
    fprintf(output, "\n");

    print_begin_block(output, 1, POLICY_BLOCK, "Experiment_A_purge");

    print_line(output, 2, "target_fileclass = experiment_A ;");
    fprintf(output, "\n");

    print_line(output, 2, "# purge files not accessed within the last 6 hours,");
    print_line(output, 2, "# and not copied-in within the last 12 hours");
    print_line(output, 2, "# and copied-out more that 2 hours ago");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_access > 6h");
    print_line(output, 3, "and");
    print_line(output, 3, "last_restore > 12h");
    print_line(output, 3, "and");
    print_line(output, 3, "last_archive > 2h");
    print_end_block(output, 2);

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_begin_block(output, 1, POLICY_BLOCK, "visu_purge");

    print_line(output, 2, "target_fileclass = visualization ;");
    fprintf(output, "\n");

    print_line(output, 2, "# purge files not accessed within the last day,");
    print_line(output, 2, "# or modified during the week");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_access > 1d");
    print_line(output, 3, "and");
    print_line(output, 3, "last_mod > 7d");
    print_end_block(output, 2);

    print_end_block(output, 1);
    fprintf(output, "\n");

    print_line(output, 1, "# Default purge policy.");
    print_line(output, 1, "# This applies to files that don't match previous fileclasses, i.e:");
    print_line(output, 1, "#   - don't match the 'ignore' block");
    print_line(output, 1, "#   - don't match a fileclass of 'ignore_fileclass' directives");
    print_line(output, 1, "#   - don't match any 'target_fileclass' of purge policies above");
    print_begin_block(output, 1, POLICY_BLOCK, "default");

    print_begin_block(output, 2, CONDITION_BLOCK, NULL);
    print_line(output, 3, "last_access > 12h");
    print_end_block(output, 2);

    print_end_block(output, 1);

    print_end_block(output, 0);

    fprintf(output, "\n");

    return 0;
}
#endif /* HSM switch */
#endif /* purge policy */
#endif /* 0 */

/* get attribute mask for hints */
static int hints_mask(const char *hints)
{
    int mask = 0;
    const char *pass_begin = hints;
    const char *begin_var;
    const char *end_var;
    char varname[128];

    do
    {
        /* look for a variable */
        begin_var = strchr(pass_begin, '{');

        /* no more variables */
        if (!begin_var)
            break;

        /* get matching '}' */
        end_var = strchr(begin_var, '}');
        if (!end_var)
        {
           DisplayLog(LVL_CRIT,CHK_TAG, "ERROR: unmatched '{' in policy hints '%s'", hints);
           return -1;
        }

        memset(varname, 0, sizeof(varname));
        strncpy(varname, begin_var+1, end_var-begin_var-1);

        if (!strcasecmp(varname, "path"))
           mask |= ATTR_MASK_fullpath;
        else if (!strcasecmp(varname, "name"))
           mask |= ATTR_MASK_name;
        else if (!strcasecmp(varname, "ost_pool"))
           mask |= ATTR_MASK_stripe_info;
        else if (strcasecmp(varname, "policy") &&
                 strcasecmp(varname, "fileclass"))
        {
            DisplayLog(LVL_CRIT,CHK_TAG, "ERROR: unknown parameter '%s' in hints '%s'", varname, hints);
            return -EINVAL;
        }

        pass_begin = end_var + 1;

    } while(1);

    return mask;

}

/* add hints for the given policy and fileset */
static int append_policy_hint(fileset_item_t *fset, policy_descr_t *p,
                              const char *hint, char *msg_out, int cfg_line)
{
    int i, m;

    /* get attribute mask for this hint */
    m = hints_mask(hint);
    if (m < 0)
        return EINVAL;

    fset->hints_attr_mask |= m;
    p->rules.run_attr_mask |= m;

    /* is there already a hint for this policy? */
    for (i = 0; i < fset->hints_count; i++)
    {
        if (fset->action_hints[i].policy == p)
        {
            /* append ',' + hint */
            int prev_len = strlen(fset->action_hints[i].hint_str);

            if (snprintf(fset->action_hints[i].hint_str + prev_len,
                         HINTS_LEN - prev_len, ",%s", hint) >= HINTS_LEN - prev_len)
            {
                sprintf(msg_out, "String too long for %s_hints line %d (max: %d).",
                        p->name, cfg_line, HINTS_LEN);
                return EOVERFLOW;
            }
            return 0;
        }
    }
    /* not found, must increase hint list */
    fset->action_hints = (action_hint_t *)realloc(fset->action_hints,
                        (fset->hints_count + 1)* sizeof(action_hint_t));
    if (fset->action_hints == NULL)
    {
        strcpy(msg_out, "Cannot allocate memory");
        return ENOMEM;
    }

    fset->hints_count++;
    i = fset->hints_count-1;
    fset->action_hints[i].policy = p;
    if (strlen(hint) > HINTS_LEN)
    {
        sprintf(msg_out, "String too long for %s_hints line %d (max: %d).",
                p->name, cfg_line, HINTS_LEN);
        return EOVERFLOW;
    }
    strcpy(fset->action_hints[i].hint_str, hint);
    return 0;
}

/** test if the variable name is a policy hint */
static inline bool match_policy_action_hints(const char *s)
{
    return !fnmatch("*_hints", s, FNM_CASEFOLD);
}

/** parse and check a policy action hint from a fileset config item */
static int parse_policy_action_hints(policies_t *p_policies, const char *hint_name,
                                     const char *value, fileset_item_t *curr_fset,
                                     char *msg_out, int cfg_line)
{
#define MAX_HINT_NAME_LEN 1024
    char buff[MAX_HINT_NAME_LEN];
    char *c;
    int i;

    rh_strncpy(buff, hint_name, MAX_HINT_NAME_LEN);
    c = strrchr(buff, '_');
    if (c == NULL)
        RBH_BUG("parse_policy_action_hints() called for an item that doesn't satisfy match_policy_action_hints()");
    *c = '\0';
    /* check the policy name exists */
    for (i = 0; i < p_policies->policy_count; i++)
    {
        if (!strcasecmp(p_policies->policy_list[i].name, buff))
            return append_policy_hint(curr_fset, &p_policies->policy_list[i],
                                      value, msg_out, cfg_line);
    }
    sprintf(msg_out, "No policy declaration found matching policy '%s', processing parameter '%s' line %d.",
            buff, hint_name, cfg_line);
    return ENOENT; /* policy not found */
}

static void free_filesets(policies_t *p_policies)
{
    int i;
    for (i = 0; i < p_policies->fileset_count; i++)
    {
        if(p_policies->fileset_list[i].action_hints != NULL)
            free(p_policies->fileset_list[i].action_hints);
    }
    free(p_policies->fileset_list);
    p_policies->fileset_list = NULL;
    p_policies->fileset_count = 0;
}


/** Read filesets block */
static int read_filesets(config_file_t config, policies_t *p_policies,
                         char *msg_out)
{
    unsigned int   i, j;
    int            rc;

    /* get Filesets block */

    config_item_t  fileset_block
        = rh_config_FindItemByName(config, FILESETS_SECTION);

    /* not mandatory */
    if (fileset_block == NULL)
        return 0;

    /* initialize global attributes mask */
    p_policies->global_fileset_mask = 0;

    p_policies->fileset_count = rh_config_GetNbItems(fileset_block);

    /* allocate the fileset list (1/sub-block) */
    if (p_policies->fileset_count > 0)
    {
        p_policies->fileset_list =
            (fileset_item_t *) calloc(p_policies->fileset_count,
                                         sizeof(fileset_item_t));
        if (p_policies->fileset_list == NULL)
            return ENOMEM;
    }
    else
        p_policies->fileset_list = NULL;

    for (i = 0; i < p_policies->fileset_count; i++)
    {
        char          *block_name, *fsname;
        bool           definition_done;
        config_item_t  curr_class = rh_config_GetItemByIndex(fileset_block, i);

        critical_err_check_goto(curr_class, FILESETS_SECTION, clean_filesets);

        if (rh_config_ItemType(curr_class) != CONFIG_ITEM_BLOCK)
        {
            strcpy(msg_out,
                    "Only " FILESET_BLOCK " sub-blocks are expected in " FILESETS_SECTION
                    " section");
            rc = EINVAL;
            goto clean_filesets;
        }
        block_name = rh_config_GetBlockName(curr_class);
        critical_err_check_goto(block_name, FILESETS_SECTION, clean_filesets);

        definition_done = false;

        if (!strcasecmp(block_name, FILESET_BLOCK))
        {

            /* get fileset name */
            fsname = rh_config_GetBlockId(curr_class);

            if ((fsname == NULL) || (strlen(fsname) == 0))
            {
                sprintf(msg_out, "Fileclass name expected for block "
                         FILESET_BLOCK ", line %d. E.g. " FILESET_BLOCK " user_files { ...",
                         rh_config_GetItemLine(curr_class));
                rc = EINVAL;
                goto clean_filesets;
            }

            /* check that class name is not already used */
            for (j = 0; j < i; j++)
            {
                if (!strcasecmp(fsname, p_policies->fileset_list[j].fileset_id))
                {
                    sprintf(msg_out, "Fileclass '%s' is already defined in block #%d.",
                            p_policies->fileset_list[j].fileset_id, j + 1);
                    rc = EINVAL;
                    goto clean_filesets;
                }
            }

            rh_strncpy(p_policies->fileset_list[i].fileset_id, fsname,
                       FILESET_ID_LEN);
            /* set default */
            p_policies->fileset_list[i].matchable = 1;

            /* read file class block content */
            for (j = 0; j < rh_config_GetNbItems(curr_class); j++)
            {
                config_item_t  sub_item = rh_config_GetItemByIndex(curr_class, j);
                critical_err_check_goto(sub_item, FILESET_BLOCK, clean_filesets);
                char          *subitem_name;

                switch (rh_config_ItemType(sub_item))
                {
                    case CONFIG_ITEM_BLOCK:
                    {
                        subitem_name = rh_config_GetBlockName(sub_item);
                        critical_err_check_goto(subitem_name, FILESET_BLOCK, clean_filesets);

                        if (strcasecmp(subitem_name, DEFINITION_BLOCK) != 0)
                        {
                            sprintf(msg_out,
                                     "'%s' sub-block unexpected in " FILESET_BLOCK " block, line %d.",
                                     subitem_name, rh_config_GetItemLine(sub_item));
                            rc = EINVAL;
                            goto clean_filesets;
                        }

                        /* check double definition */
                        if (definition_done)
                        {
                            sprintf(msg_out, "Double fileclass definition in "
                                     FILESET_BLOCK " block, line %d.",
                                     rh_config_GetItemLine(sub_item));
                            rc = EINVAL;
                            goto clean_filesets;
                        }

                        /* 2 possible definition types expected: boolean expression
                         * or fileset union and/or intersection */
                        switch (rh_config_ContentType(sub_item))
                        {
                            case CONFIG_ITEM_BOOL_EXPR:
                                /* analyze boolean expression */
                                rc = GetBoolExpr(sub_item, DEFINITION_BLOCK,
                                                 &p_policies->fileset_list[i].definition,
                                                 &p_policies->fileset_list[i].attr_mask,
                                                 msg_out, NULL);
                                if (rc)
                                    goto clean_filesets;
                                break;

                            case CONFIG_ITEM_SET:
                                /* Build a policy boolean expression from a
                                 * union/intersection or fileclasses */
                                rc = GetSetExpr(sub_item, DEFINITION_BLOCK,
                                                 &p_policies->fileset_list[i].definition,
                                                 &p_policies->fileset_list[i].attr_mask,
                                                 p_policies, msg_out);
                                if (rc)
                                    goto clean_filesets;
                                break;

                            default:
                                sprintf(msg_out, "Boolean expression or set-based definition expected in block '%s', "
                                         "line %d", subitem_name,
                                         rh_config_GetItemLine((config_item_t) sub_item));
                                return EINVAL;
                        }

                        p_policies->global_fileset_mask |= p_policies->fileset_list[i].attr_mask;
                        definition_done = true;

                        if (p_policies->fileset_list[i].attr_mask & (
#ifdef ATTR_INDEX_last_archive
                            ATTR_MASK_last_archive |
#endif
#ifdef ATTR_INDEX_last_restore
                            ATTR_MASK_last_restore |
#endif
                            ATTR_MASK_last_access | ATTR_MASK_last_mod))
                        {
                           DisplayLog(LVL_MAJOR, CHK_TAG, "WARNING: in FileClass '%s', line %d: "
                                       "time-based conditions should be specified in policy condition instead of file class definition",
                                       p_policies->fileset_list[i].fileset_id, rh_config_GetItemLine(sub_item));
                        }
                        break;
                    }
                    case CONFIG_ITEM_VAR:
                    {
                        char          *value = NULL;
                        int            extra_args = 0;

                        rc = rh_config_GetKeyValue(sub_item, &subitem_name, &value, &extra_args);
                        if (rc)
                            goto clean_filesets;

                        /* is the variable of the form <policy_name>_hints ? */
                        if (match_policy_action_hints(subitem_name))
                        {
                            if (extra_args)
                            {
                                sprintf(msg_out,
                                        "Unexpected arguments for hints parameter, line %d.",
                                        rh_config_GetItemLine(sub_item));
                                rc = EINVAL;
                                goto clean_filesets;
                            }

                            rc = parse_policy_action_hints(p_policies, subitem_name,
                                    value, &p_policies->fileset_list[i], msg_out,
                                    rh_config_GetItemLine(sub_item));
                            if (rc)
                                goto clean_filesets;
                        }
                        else if (!strcasecmp(subitem_name, "report"))
                        {
                            if (extra_args)
                            {
                                sprintf(msg_out,
                                        "Unexpected arguments for 'report' parameter, line %d.",
                                        rh_config_GetItemLine(sub_item));
                                rc = EINVAL;
                                goto clean_filesets;
                            }
                            int tmp =str2bool(value);
                            if (tmp == -1)
                            {
                                sprintf(msg_out,
                                        "Boolean expected for 'report' parameter, line %d.",
                                        rh_config_GetItemLine(sub_item));
                                rc = EINVAL;
                                goto clean_filesets;
                            }
                            p_policies->fileset_list[i].matchable = tmp;
                        }
#ifdef _LUSTRE_HSM
                        /* manage archive_id */
                        else if (!strcasecmp(subitem_name,"archive_id")
                            || !strcasecmp(subitem_name,"archive_num")) /* for backward compat. */
                        {
                            sprintf(msg_out, "%s parameter is deprecated (line %u). Specify archive_id as policy hint, e.g. <policy>_hints=\"archive_id=<idx>\".",
                                    subitem_name, rh_config_GetItemLine(sub_item));
                            rc = EINVAL;
                            goto clean_filesets;
                        }
                        else
#endif
                        {
                            DisplayLog(LVL_CRIT, "Config Check",
                                        "WARNING: unknown parameter '%s' in block '%s' line %d",
                                        subitem_name, FILESET_BLOCK, rh_config_GetItemLine(sub_item));
/*                            sprintf(msg_out,
                                     "'%s' parameter unexpected in " FILESET_BLOCK " block, line %d.",
                                     subitem_name, rh_config_GetItemLine(sub_item));
                            rc = EINVAL;
                            goto clean_filesets; */
                        }
                        break;
                    }
                    default :
                        /* unexpected content */
                            sprintf(msg_out,
                                     "Unexpected item in "FILESET_BLOCK" block, line %d.",
                                     rh_config_GetItemLine(sub_item));
                            rc = EINVAL;
                            goto clean_filesets;
                } /* switch on item type */

            } /* loop on "fileclass" block content */

            if (!definition_done)
            {
                sprintf(msg_out,
                         "No definition in file class '%s', line %d", fsname,
                         rh_config_GetItemLine(curr_class));
                rc = ENOENT;
                goto clean_filesets;
            }

        }                       /* end of fileclass" block */
        else
        {
            sprintf(msg_out, "'%s' sub-block unexpected in %s section, line %d.",
                     block_name, FILESETS_SECTION, rh_config_GetItemLine(curr_class));
            rc = EINVAL;
            goto clean_filesets;
        }


    }                           /* end of "filesets" section */

    return 0;

clean_filesets:
    free_filesets(p_policies);
    return rc;
}


fileset_item_t *get_fileset_by_name(const policies_t *p_policies, const char *name)
{
    int            i;
    for (i = 0; i < p_policies->fileset_count; i++)
    {
        if (!strcasecmp(p_policies->fileset_list[i].fileset_id, name))
            return &p_policies->fileset_list[i];
    }
    return NULL;                /* not found */
}

#if 0
#ifdef HAVE_RM_POLICY
/** Read filesets block */
static int read_unlink_policy(config_file_t config, unlink_policy_t * pol, char *msg_out)
{
    int            rc;
    int            intval;

    static const char *allowed[] = {
        "hsm_remove", "deferred_remove_delay", NULL };

    /* get unlink policy block */

    config_item_t  param_block = rh_config_FindItemByName(config, UNLINKPOLICY_BLOCK);
    if (param_block == NULL)
    {
        /* no error, because no parameter is mandatory */
        return 0;
    }

    /* check this is a block... */
    if (rh_config_ItemType(param_block) != CONFIG_ITEM_BLOCK)
    {
        strcpy(msg_out, "A block is expected for '" UNLINKPOLICY_BLOCK "' item");
        return EINVAL;
    }

    /* parse parameters */
    rc = GetBoolParam(param_block, UNLINKPOLICY_BLOCK, "hsm_remove",
                       0, &intval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        pol->hsm_remove = intval;

    rc = GetDurationParam(param_block, UNLINKPOLICY_BLOCK, "deferred_remove_delay",
                           INT_PARAM_POSITIVE, &intval, NULL, NULL, msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc != ENOENT)
        pol->deferred_remove_delay = intval;

    CheckUnknownParameters(param_block, UNLINKPOLICY_BLOCK, allowed);

    return 0;

}
#endif
#endif


/** parse a rule config block and fill rule_item_t structure
 * @param[in]  all_policies  Needed to check fileset definition
 * @param[in]  policy        Needed to uild specific parameter name like '<policy>_hints',
  *                          check status manager properties...
 * @param[in]  policy_rules  Needed to check other rule names in the policy
 * @param[out] rule          The rule structure to fill-in
 */
static int parse_rule_block(config_item_t config_item,
                            const char *block_name,
                            const policies_t *all_policies,
                            const policy_descr_t *policy,
                            const policy_rules_t *policy_rules,
                            rule_item_t *rule, char *msg_out)
{
    char          *rule_name;
    bool           is_default = false;
    int            i, j, k, rc;
    uint64_t       mask;
    bool           definition_done = false;

    /* initialize output */
    memset(rule, 0, sizeof(rule_item_t));

    /* get policy id */
    rule_name = rh_config_GetBlockId(config_item);
    critical_err_check(rule_name, block_name);

    /* check that this rule name is not already used in this policy */
    for (i = 0; i < policy_rules->rule_count; i++)
    {
        if (!strcasecmp(rule_name, policy_rules->rules[i].rule_id))
        {
            sprintf(msg_out, "Rule name '%s' (line %d) is already used by another rule in the policy.",
                    rule_name, rh_config_GetItemLine(config_item));
            return EINVAL;
        }
    }

    /* is it a default block? */
    is_default = !strcasecmp(rule_name, "default");

    /* save policy id */
    rh_strncpy(rule->rule_id, rule_name, sizeof(rule->rule_id));

    /* read file block content */
    for (i = 0; i < rh_config_GetNbItems(config_item); i++)
    {
        config_item_t  sub_item = rh_config_GetItemByIndex(config_item, i);
        critical_err_check(sub_item, block_name);
        char          *subitem_name;

        if (rh_config_ItemType(sub_item) == CONFIG_ITEM_BLOCK)
        {
            subitem_name = rh_config_GetBlockName(sub_item);
            critical_err_check(subitem_name, block_name);

            /* TODO add support for LUA rules */
            if (strcasecmp(subitem_name, CONDITION_BLOCK) != 0)
            {
                sprintf(msg_out, "'%s' sub-block unexpected in %s block, line %d.",
                         subitem_name, block_name, rh_config_GetItemLine(sub_item));
                return EINVAL;
            }

            /* check double condition */
            if (definition_done)
            {
                sprintf(msg_out, "Double condition in policy %s, line %d.",
                         rule_name, rh_config_GetItemLine(sub_item));
                return EINVAL;
            }

            /* analyze boolean expression */
            /* allow using 'status' related info in conditions */
            mask = 0;
            rc = GetBoolExpr(sub_item, CONDITION_BLOCK, &rule->condition,
                             &mask, msg_out, policy->status_mgr);
            if (rc)
                return rc;

            rule->attr_mask |= mask;
            definition_done = true;
        }
        else                    /* not a block */
        {
            char          *value = NULL;
            int            extra_args = 0;
            fileset_item_t *fs;
            char hint_name[POLICY_NAME_LEN + sizeof("_hints") + 1];

            sprintf(hint_name, "%s_hints", policy->name);

            rc = rh_config_GetKeyValue(sub_item, &subitem_name, &value, &extra_args);
            if (rc)
                return rc;

            /* expected : target filesets or migration hints (for migrations policies) */

            if (!strcasecmp(subitem_name, "target_fileclass"))
            {
                if (is_default)
                {
                    sprintf(msg_out, "No target_fileclass expected for default policy, line %d.",
                             rh_config_GetItemLine(sub_item));
                    /** @TODO free targets */
                    return EINVAL;
                }

                if (extra_args)
                {
                    sprintf(msg_out,
                             "Unexpected arguments for %s parameter, line %d.",
                             subitem_name, rh_config_GetItemLine(sub_item));
                    return EINVAL;
                }

                /* get the associated fileset */
                fs = get_fileset_by_name(all_policies, value);

                if (fs == NULL)
                {
                    sprintf(msg_out,
                            "Policy rule references unknown fileclass '%s', line %d.", value,
                            rh_config_GetItemLine(sub_item));
                    return EINVAL;
                }
                fs->used_in_policy = 1;

                /* note: matchable is only for the fileclass in DB.
                 * allow using non-matchable in policies */

                /* check that the fileset is not already referenced in the policy */
                for (j = 0; j < policy_rules->rule_count; j++)
                {
                    for (k = 0; k < policy_rules->rules[j].target_count; k++)
                        if (fs == policy_rules->rules[j].target_list[k])
                        {
                            sprintf(msg_out,
                                     "Fileclass '%s' is already a target of policy rule '%s'",
                                     value, policy_rules->rules[j].rule_id);
                            return EINVAL;
                        }
                }

                /* also check ignore_fileclass directives */
                for (j = 0; j < policy_rules->ignore_count; j++)
                {
                    if (fs == policy_rules->ignore_list[j])
                    {
                        sprintf(msg_out,
                                 "Fileclass '%s' is simultaneously ignored and referenced as a target for policy '%s'",
                                 value, rule_name);
                        return EINVAL;
                    }
                }

                /* append the fileset list */
                rule->target_count++;
                rule->target_list = (fileset_item_t **)realloc(rule->target_list,
                                        rule->target_count * sizeof(fileset_item_t *));
                rule->target_list[rule->target_count-1] = fs;

                /* add fileset mask to policy mask */
                rule->attr_mask |= fs->attr_mask;
            }
            /* allowed syntaxes:    hints, <policyname>_hints, action_hints */
            else if (!strcasecmp(subitem_name, "hints") ||
                     !strcasecmp(subitem_name, "action_hints") ||
                     !strcasecmp(subitem_name, hint_name))
            {
                if (extra_args)
                {
                    sprintf(msg_out,
                             "Unexpected arguments for %s parameter, line %d.",
                             subitem_name, rh_config_GetItemLine(sub_item));
                    return EINVAL;
                }

                /* get attribute mask for this hint */
                rc = hints_mask(value);
                if (rc < 0)
                    return rc;
                rule->attr_mask |= rc;

                /* append hints */
                if (EMPTY_STRING(rule->action_hints))
                {
                    if (strlen(value) > HINTS_LEN)
                    {
                        sprintf(msg_out, "String too large for %s line %d (max: %d).",
                                subitem_name, rh_config_GetItemLine(sub_item), HINTS_LEN);
                        return EOVERFLOW;
                    }

                    strcpy(rule->action_hints, value);
                }
                else            /* append with ',' */
                {
                    int            prev_len = strlen(rule->action_hints);
                    if (prev_len + strlen(value) + 1 > HINTS_LEN)
                    {
                        sprintf(msg_out, "String too large for %s line %d (max: %d).",
                                subitem_name, rh_config_GetItemLine(sub_item), HINTS_LEN);
                        return EOVERFLOW;
                    }

                    rule->action_hints[prev_len] = ',';
                    strcpy(rule->action_hints + prev_len + 1, value);
                }
            }
#ifdef _LUSTRE_HSM
            /* archive_id is now managed as a hint */
            else if (!strcasecmp(subitem_name, "archive_id")
                     || !strcasecmp(subitem_name, "archive_num")) /* for backward compat */
            {
                sprintf(msg_out, "%s parameter is deprecated (line %u). Specify archive_id in %s, e.g. %s=\"archive_id=<idx>\".",
                        subitem_name, rh_config_GetItemLine(sub_item), hint_name, hint_name);
                return EINVAL;
            }
#endif
            else
            {
                DisplayLog(LVL_CRIT, "Config Check",
                            "WARNING: unknown parameter '%s' in block '%s' line %d",
                            subitem_name, block_name, rh_config_GetItemLine(sub_item));
                /*sprintf(msg_out, "'%s' parameter unexpected in %s block, line %d.",
                         subitem_name, block_name, rh_config_GetItemLine(sub_item));
                return EINVAL;*/
            }


        }                       /* end of vars */
    }                           /* loop on "policy" block content */

    if (!definition_done)
    {
        DisplayLog(LVL_MAJOR, CHK_TAG, "WARNING: in policy '%s', line %d: no condition specified!",
                   rule_name, rh_config_GetItemLine(config_item));
    }

    return 0;

}

static void free_policy_rules(policy_rules_t *rules)
{
if (rules->rules)
    /** FIXME rules contents must also be freed */
    free(rules->rules);
if (rules->ignore_list)
    free(rules->ignore_list);
if (rules->whitelist_count > 0)
    free_whitelist(rules->whitelist_rules, rules->whitelist_count);
else if (rules->whitelist_rules) /* preallocated? */
    free(rules->whitelist_rules);

    rules->rules = NULL;
    rules->ignore_list = NULL;
    rules->whitelist_rules = NULL;
    rules->whitelist_count = 0;
}

static void free_policy_descr(policy_descr_t *descr)
{
    /** FIXME free scope + sm_instance (+default action parameters) */
    free_policy_rules(&descr->rules);
}

/* macro for preallocating array depending on configuration blocks in Read_Policy_ */
#define PREALLOC_ARRAY_CONFIG(_block_name_, _type_, _array_var, _goto_label)      \
    do {                                                                          \
        count = rh_config_CountItemNames(section, _block_name_);                  \
        if (count > 0)                                                            \
        {                                                                         \
            rules->_array_var = (_type_ *)calloc(count, sizeof(_type_));          \
            if (rules->_array_var == NULL)                                        \
            {                                                                     \
                rc = ENOMEM;                                                      \
                goto _goto_label;                                                 \
            }                                                                     \
        }                                                                         \
        else if (count == 0)                                                      \
        {                                                                         \
            rules->_array_var = NULL;                                             \
        }                                                                         \
        else                                                                      \
        {                                                                         \
            rc = -1;                                                              \
            goto _goto_label;                                                     \
        }                                                                         \
    }                                                                             \
    while(0)


static int read_policy(config_file_t config, const policies_t *p_policies, char *msg_out,
                       policy_descr_t *policy_descr)
{
    unsigned int   i, j, k;
    int            rc, count;

    policy_rules_t *rules;
    config_item_t   section;
    /* 16: strlen("_policy") + aligned padding */
    char            section_name[POLICY_NAME_LEN+16] = "";

/* macros for cleaner code */
    rules = &policy_descr->rules;
#define curr_ign        rules->whitelist_count
#define curr_ign_fc     rules->ignore_count
#define curr_rule       rules->rule_count

    /* initialize output */
    memset(rules, 0, sizeof(*rules));

    /* check if the new name exists first */
    snprintf(section_name, sizeof(section_name)-1, "%s_%s",
             policy_descr->name, POLICIES_BLOCK);
    section_name[sizeof(section_name)-1] = '\0';

    /* get policy section */
    section = rh_config_FindItemByName(config, section_name);

    if (section == NULL)
    {
        snprintf(section_name, sizeof(section_name)-1, "%s_%s",
                 policy_descr->name, OLD_POLICIES_BLOCK);
        section_name[sizeof(section_name)-1] = '\0';

        /* get policy section */
        section = rh_config_FindItemByName(config, section_name);

        if (section != NULL)
            /* Deprecation warning */
            DisplayLog(LVL_MAJOR, LOADER_TAG, "WARNING: '*_"OLD_POLICIES_BLOCK
                       "' block names are deprecated. Rename '%s' block to "
                       "'%s_"POLICIES_BLOCK"'.", section_name,
                       policy_descr->name);
        else
            /* not mandatory */
            return 0;
    }

    /* prealloc config arrays */
    PREALLOC_ARRAY_CONFIG(IGNORE_BLOCK, whitelist_item_t, whitelist_rules, err);
    PREALLOC_ARRAY_CONFIG(IGNORE_FC, fileset_item_t *, ignore_list, err);

    /* don't use PREALLOC_ARRAY_CONFIG for rules, as we accept old rule name (policy)  */
    //PREALLOC_ARRAY_CONFIG(RULE_BLOCK, rule_item_t, rules, free_ignore_fc);
    count = rh_config_CountItemNames(section, RULE_BLOCK)+
                rh_config_CountItemNames(section, OLD_RULE_BLOCK);
    if (count > 0)
    {
        rules->rules = (rule_item_t *)calloc(count, sizeof(rule_item_t));
        if (rules->rules == NULL)
        {
            rc = ENOMEM;
            goto err;
        }
    }
    else if (count == 0)
    {
        rules->rules = NULL;
    }
    else
    {
        rc = -1;
        goto err;
    }

    count = rh_config_GetNbItems(section);

    /* read sub-blocks */
    for (i = 0; i < count; i++)
    {
        char          *item_name;
        config_item_t  curr_item = rh_config_GetItemByIndex(section, i);
        critical_err_check_goto(curr_item, section_name, err);

        if (rh_config_ItemType(curr_item) == CONFIG_ITEM_BLOCK)
        {
            item_name = rh_config_GetBlockName(curr_item);
            critical_err_check_goto(item_name, section_name, err);

            if (!strcasecmp(item_name, IGNORE_BLOCK))
            {
                /* analyze boolean expression */
                /* allow using status related info in ignore statement? */
                rc = GetBoolExpr(curr_item, item_name,
                                 &rules->whitelist_rules[curr_ign].bool_expr,
                                 &rules->whitelist_rules[curr_ign].attr_mask,
                                 msg_out, policy_descr->status_mgr);
                if (rc)
                    goto err;

                /* add expression attr mask to policy mask */
                rules->run_attr_mask |= rules->whitelist_rules[curr_ign].attr_mask;
                curr_ign++;
            }
            /* allow 'policy' or 'rule' */
            else if (!strcasecmp(item_name, OLD_RULE_BLOCK) || !strcasecmp(item_name, RULE_BLOCK))
            {
                /* parse 'policy' block */
                rc = parse_rule_block(curr_item, item_name, p_policies,
                                      policy_descr, rules,
                                      &rules->rules[curr_rule],
                                      msg_out);
                if (rc)
                    goto err;

                rules->run_attr_mask |= rules->rules[curr_rule].attr_mask;
                curr_rule++;
            }
            else
            {
                sprintf(msg_out, "'%s' sub-block unexpected in %s block, line %d.",
                         item_name, section_name, rh_config_GetItemLine(curr_item));
                rc = EINVAL;
                goto err;
            }
        }
        else                    /* not a block */
        {
            char          *value;
            int            extra_args = 0;

            rc = rh_config_GetKeyValue(curr_item, &item_name, &value, &extra_args);
            if (rc)
                goto err;

            /* only "ignore_fileclass" expected */
            if (strcasecmp(item_name, IGNORE_FC) != 0)
            {
                sprintf(msg_out, "'%s' parameter unexpected in %s block, line %d.",
                         item_name, section_name, rh_config_GetItemLine(curr_item));
                rc = EINVAL;
                goto err;
            }

            if (extra_args)
            {
                sprintf(msg_out,
                         "Unexpected arguments for %s parameter, line %d.",
                         item_name, rh_config_GetItemLine(curr_item));
                rc = EINVAL;
                goto err;
            }

            /* find fileset in policy */
            rules->ignore_list[curr_ign_fc] =
                get_fileset_by_name(p_policies, value);

            if (rules->ignore_list[curr_ign_fc] == NULL)
            {
                sprintf(msg_out, "Policy definition references unknown fileclass '%s', line %d.",
                         value, rh_config_GetItemLine(curr_item));
                rc = EINVAL;
                goto err;
            }

            rules->ignore_list[curr_ign_fc]->used_in_policy = 1;

            /* check that the fileset is not already referenced in a policy */
            for (j = 0; j < curr_rule; j++)
            {
                for (k = 0; k < rules->rules[j].target_count; k++)
                    if (rules->ignore_list[curr_ign_fc] ==
                         rules->rules[j].target_list[k])
                    {
                        sprintf(msg_out,
                                 "Fileclass '%s' is simultaneously ignored and referenced as a target for policy '%s'",
                                 value, rules->rules[j].rule_id);
                        rc = EINVAL;
                        goto err;
                    }
            }

            /* add fileset attr mask to policy mask */
            rules->run_attr_mask |= rules->ignore_list[curr_ign_fc]->attr_mask;
            curr_ign_fc++;

        }                       /* end of vars */

    }                           /* end of section content */

    return 0;

  err:
    free_policy_rules(rules);

    return rc;
}

/** @TODO manage SM config + SM init */

static int reload_policies(policies_t *p_policies)
{
    if (p_policies->policy_count != policies.policy_count)
    {
        /* policy count changed */
        DisplayLog(LVL_MAJOR, RELOAD_TAG, "Policy count changed %u->%u: "
                   "program restart required (skipping policy rules update).",
                   policies.policy_count, p_policies->policy_count);
    }

    /* TODO reload all policies */

#if 0
    purge_policy_t *policy = (purge_policy_t *) module_config;

    /* Reloading purge policies dynamically is quite complex:
     * 1) compare triggers: if they are the same (same count, same type)
     *    update their simple parameters: thresholds and check interval
     * 2) compare whitelist expressions count and structures.
     *    If the structure is the same (same parameters, comparators, etc.) only update
     *    the numerical values for conditions.
     */

    /** @TODO prevent from concurrent access when purge policy is beeing checked */

    update_whitelist(policies.purge_policy.whitelist_rules, policies.purge_policy.whitelist_count,
                      policy->whitelist_rules, policy->whitelist_count, PURGEPOLICY_BLOCK);

    /* XXX global_attr_mask is unchanged, since we keep the same expressions */

    /* free reloaded config structure (no used anymore) */
    free_whitelist(policy->whitelist_rules, policy->whitelist_count);

#endif
    return 0;
}

static int set_policies(void *cfg,  bool reload)
{
    policies_t *p_policies = (policies_t *)cfg;

    if (reload)
        return reload_policies(p_policies);
    else
        policies = *p_policies;
    return 0;
}

static void set_default_policies(void *module_config)
{
    policies_t    *pol = (policies_t *)module_config;

    *pol = policy_initializer;
}

static int read_policies(config_file_t config, void *cfg, char *msg_out)
{
    policies_t    *pol = (policies_t *)cfg;
    int            rc, i;

    memset(pol, 0, sizeof(*pol));

    /* read policy declarations, allocate policy descriptors */
    rc = read_policy_definitions(config, pol, msg_out);
    if (rc)
        return rc;

    /* load fileset definitions, and check fileset hints against defined policies */
    rc = read_filesets(config, pol, msg_out);
    if (rc)
        return rc;

    /* iterate on declared policies */
    for (i = 0; i < pol->policy_count; i++)
    {
        rc = read_policy(config, pol, msg_out, &pol->policy_list[i]);
        if (rc)
            return rc;
    }

    return 0;
}

static void write_policy_template(FILE *output)
{
    write_template_filesets(output);

// FIXME write policy templates
/*
    print_line(output, 1, "# default sort order for the policy (this is");
    print_line(output, 1, "# overridden by policy parameters::lru_sort_attr)");
    print_line(output, 1, "#default_lru_sort_attr = last_access ;");

    print_line(output, 1, "# Default action for this policy.");
    print_line(output, 1, "# The syntax to call built-in functions is <module_name>.<action_name>");
    print_line(output, 1, "# e.g. common.copy, common.unlink, lhsm.archive, lhsm.release...");
    print_line(output, 1, "default_action = common.unlink ;");
    print_line(output, 1, "# To call a custom script instead, use the following syntax:");
    print_line(output, 1, "# default_action = cmd(\"/usr/bin/move_to_trash.sh {path}\") ;");
    print_line(output, 1, "# Special parameters can passed to the command:");
    print_line(output, 1, "#    {path}: posix path to the entry");
#ifdef _LUSTRE
#   ifdef _HAVE_FID
    print_line(output, 1, "#    {fid}: fid of the entry");
#   endif
    print_line(output, 1, "#    {fsname}: Lustre fsname");
#endif
    print_line(output, 1, "#    {hints}: pass action_hints to the command");
    fprintf(output, "\n");
*/
}

static void write_policy_default(FILE * output)
{
    write_default_filesets(output);
// FIXME write policy defaults
}

static void *policies_cfg_new(void)
{
    return calloc(1, sizeof(policies_t));
}
static void policies_cfg_free(void *arg)
{
    policies_t *cfg = (policies_t*)arg;
    int i;

    if (cfg == NULL)
        return;

    for (i = 0; i < cfg->policy_count; i++)
        free_policy_descr(&cfg->policy_list[i]);

    free(cfg->policy_list);
    cfg->policy_list = NULL;
    cfg->policy_count = 0;

    free_filesets(cfg);
    free(cfg);
}


mod_cfg_funcs_t policies_cfg_hdlr = {
    .module_name = "policies",
    .new         = policies_cfg_new,
    .free        = policies_cfg_free,
    .set_default = set_default_policies,
    .read        = read_policies,
    .set_config  = set_policies,
    .write_default  = write_policy_default,
    .write_template = write_policy_template
};

bool policy_exists(const char *name, int *index)
{
    int i;
    for (i = 0; i < policies.policy_count; i++)
    {
        if (!strcasecmp(name, policies.policy_list[i].name))
        {
            if (index != NULL)
                *index = i;
            return true;
        }
    }
    return false;
}
