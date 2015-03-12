/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright 2015 Cray Inc., All rights reserved.
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

#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "rbh_params.h"
#include "rbh_logs.h"
#include "global_config.h"
#include "rbh_misc.h"

/* avoid linking with all robinhood libs */
log_config_t log_config = { .debug_level = LVL_DEBUG };
global_config_t global_config = { .fs_path = "somefspath" };

void DisplayLogFn(log_level debug_level, const char *tag, const char *format, ...)
{
    if (LVL_DEBUG >= debug_level)
    {
        va_list args;

        va_start(args, format);
        vprintf(format, args);
        va_end(args);
        printf("\n");
    }
}

const char *config_file_path(void) { return "someconfigfile"; }
const char *get_fsname(void) { return "somefsname"; }

/* Will substitute values in first column with values in second
 * column. */
static const char *vars[] = {
    "foo", "barbar",
    "hello", "bye",
    "marco", "polo",
    "double", "{marco}",
    "triple", "{double}",
    "explorer", "marco",
    "quote1", "a'b",
    NULL, NULL
};

static const char descr[] = "my test string";

static void test_subst_params(void)
{
    char *newcmd;
    const char *cmd;
    const attr_set_t attrs = {
        .attr_mask = ATTR_INDEX_name | ATTR_INDEX_fullpath,
        .attr_values = {
            .name = "somename",
            .fullpath = "somepath",
        }
    };
    entry_id_t id = { 0x1234, 0x5678, 0xabcd };

    /*
     * Basic checks. No variable.
     */

    /* Empty string */
    newcmd = subst_params("", descr, NULL, NULL, NULL, NULL, false);
    if (strcmp(newcmd, ""))
        abort();
    g_free(newcmd);

    /* Nothing interresting */
    cmd = "hello";
    newcmd = subst_params(cmd, descr, NULL, NULL, NULL, NULL, false);
    assert(strcmp(newcmd, cmd) == 0);
    g_free(newcmd);

    /* Empty variable */
    /* should fail if empty string is not a param */
    newcmd = subst_params("{}", descr, NULL, NULL, NULL, NULL, false, true);
    assert(newcmd == NULL);

    /* case of 'rbh-find': {} is expended to path */
    newcmd = subst_params("{}", descr, NULL, NULL, NULL, find_vars, false, true);
    assert_str_equal(newcmd, "somepath");
    g_free(newcmd);

    /* One unknown variable */
    newcmd = subst_params("{hello}", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /* One unknown variable with text before */
    newcmd = subst_params("qwerty{hello}", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /* One unknown variable with text after */
    newcmd = subst_params("{hello}cvbn", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /* One unknown variable with text around */
    newcmd = subst_params("qwerty{hello}cvbn", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /* two unknown variables */
    newcmd = subst_params("{azerty}{hello}", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /* two unknown variables with text around */
    newcmd = subst_params("jgds{azerty}lgkfhd{hello}iub", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /* string with lone { */
    /* TODO: should that be an error? It's inconsistent with next test. */
    newcmd = subst_params("qwerty{hellocvbn", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /* string with lone } */
    cmd = "qwerty}hellocvbn";
    newcmd = subst_params(cmd, descr, NULL, NULL, NULL, NULL, false);
    assert(strcmp(newcmd, cmd) == 0);
    g_free(newcmd);

    /* string with 2 { */
    newcmd = subst_params("qwerty{{hellocvbn", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /* string with 2 { */
    newcmd = subst_params("qwerty{ghfd{hellocvbn", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /* string with inverted {} */
    newcmd = subst_params("qwerty}ghfd{hellocvbn", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /* string with 2 { and 1 { */
    newcmd = subst_params("qwerty{ghfd{hello}cvbn", descr, NULL, NULL, NULL, NULL, false);
    assert(newcmd == NULL);

    /*
     * With standard variables
     */

    /* One standard variable */
    newcmd = subst_params("{name}", descr, NULL, &attrs, NULL, NULL, false);
    assert(strcmp(newcmd, attrs.attr_values.name) == 0);
    g_free(newcmd);

#if 0
    /* BUG? id is not set. Can that happen? */
    newcmd = subst_params("{fid}", descr, NULL, &attrs, NULL, NULL, false);
    assert(strcmp(newcmd, attrs.attr_values.name) == 0);
    g_free(newcmd);
#endif

    newcmd = subst_params("{fid}", "", &id, &attrs, NULL, NULL, false);
    assert(strcmp(newcmd, "0x1234:0x5678:0xabcd") == 0);
    g_free(newcmd);

    newcmd = subst_params("{fsname}", "", &id, &attrs, NULL, NULL, false);
    assert(strcmp(newcmd, "somefsname") == 0);
    g_free(newcmd);

    newcmd = subst_params("{fsroot}", "", &id, &attrs, NULL, NULL, false);
    assert(strcmp(newcmd, "somefspath") == 0);
    g_free(newcmd);

    newcmd = subst_params("{cfg}", "", &id, &attrs, NULL, NULL, false);
    assert(strcmp(newcmd, "someconfigfile") == 0);
    g_free(newcmd);

    /* Partial standard variable name */
    newcmd = subst_params("{nam}", descr, NULL, &attrs, NULL, NULL, false);
    assert(newcmd == NULL);

    /* Standard variable name with an extra letter */
    newcmd = subst_params("{namee}", descr, NULL, &attrs, NULL, NULL, false);
    assert(newcmd == NULL);

    /* Two standard variables */
    newcmd = subst_params("{name} {fullpath}", descr, NULL, &attrs, NULL, NULL, false);
    assert(strcmp(newcmd, "somename somepath") == 0);
    g_free(newcmd);

    /*
     * With some real variables.
     */

    /* Simple replacement */
    newcmd = subst_params("{foo}", descr, NULL, NULL, NULL, vars, false);
    assert(strcmp(newcmd, "barbar") == 0);
    g_free(newcmd);

    newcmd = subst_params("{hello}", descr, NULL, NULL, NULL, vars, false);
    assert(strcmp(newcmd, "bye") == 0);
    g_free(newcmd);

    newcmd = subst_params("{marco}", descr, NULL, NULL, NULL, vars, false);
    assert(strcmp(newcmd, "polo") == 0);
    g_free(newcmd);

    /* With quotes */
    newcmd = subst_params("{foo}", descr, NULL, NULL, NULL, vars, true);
    assert(strcmp(newcmd, "'barbar'") == 0);
    g_free(newcmd);

    newcmd = subst_params("{quote1}", descr, NULL, NULL, NULL, vars, false);
    assert(strcmp(newcmd, "a'b") == 0);
    g_free(newcmd);

    newcmd = subst_params("{quote1}", descr, NULL, NULL, NULL, vars, true);
    assert(strcmp(newcmd, "'a'\\''b'") == 0);
    g_free(newcmd);

    newcmd = subst_params("az {quote1}", descr, NULL, NULL, NULL, vars, true);
    assert(strcmp(newcmd, "az 'a'\\''b'") == 0);
    g_free(newcmd);

    newcmd = subst_params("{quote1} sx", descr, NULL, NULL, NULL, vars, true);
    assert(strcmp(newcmd, "'a'\\''b' sx") == 0);
    g_free(newcmd);

    newcmd = subst_params("az {quote1} sx", descr, NULL, NULL, NULL, vars, true);
    assert(strcmp(newcmd, "az 'a'\\''b' sx") == 0);
    g_free(newcmd);

    /* Non-existent variable in first column, but present in 2nd column */
    newcmd = subst_params("{barbar}", descr, NULL, NULL, NULL, vars, false);
    assert(newcmd == NULL);

    newcmd = subst_params("{bye}", descr, NULL, NULL, NULL, vars, false);
    assert(newcmd == NULL);

    newcmd = subst_params("{polo}", descr, NULL, NULL, NULL, vars, false);
    assert(newcmd == NULL);

    /* 2 variables */
    newcmd = subst_params("{foo} {hello}", descr, NULL, NULL, NULL, vars, false);
    assert(strcmp(newcmd, "barbar bye") == 0);
    g_free(newcmd);

    /* twice the same variable */
    newcmd = subst_params("{hello}{hello}", descr, NULL, NULL, NULL, vars, false);
    assert(strcmp(newcmd, "byebye") == 0);
    g_free(newcmd);

    /* 3 variables */
    newcmd = subst_params("{marco}{hello}{foo}", descr, NULL, NULL, NULL, vars, false);
    assert(strcmp(newcmd, "polobyebarbar") == 0);
    g_free(newcmd);

    /* 2 + 3 variables */
    newcmd = subst_params("A{marco} {hello} {marco}d{hello}w{hello}", descr, NULL, NULL, NULL, vars, false);
    assert(strcmp(newcmd, "Apolo bye polodbyewbye") == 0);
    g_free(newcmd);

    /* 5 times the same variable */
    newcmd = subst_params("{marco}{marco}{marco}{marco}{marco}", descr, NULL, NULL, NULL, vars, false);
    assert(strcmp(newcmd, "polopolopolopolopolo") == 0);
    g_free(newcmd);

    /*
     * Mix between standard and user variables
     */

    /* One of each */
    newcmd = subst_params("{marco} {fullpath}", descr, NULL, &attrs, NULL, vars, false);
    assert(strcmp(newcmd, "polo somepath") == 0);
    g_free(newcmd);

    /* Several of each */
    newcmd = subst_params("{marco} {fullpath} {hello} {name} ", descr, NULL, &attrs, NULL, vars, false);
    assert(strcmp(newcmd, "polo somepath bye somename ") == 0);
    g_free(newcmd);

#if 0
    /*
     * Multiple substitutions
     */

    /* "{triple}" -> "{double}" --> "{marco}" --> "polo" */
    /* TODO: is it an abuse -- should that work? Is it something we want to work? */
    newcmd = subst_params("{triple} ", descr, NULL, &attrs, NULL, vars, false);
    assert(strcmp(newcmd, "polo") == 0);
    g_free(newcmd);
#endif

#if 0
    /* "{{explorer}}" -> "{marco}" --> "polo" */
    /* TODO: is it an abuse -- should that work? Is it something we want to work? */
    newcmd = subst_params("{{explorer}} ", descr, NULL, &attrs, NULL, vars, false);
    assert(strcmp(newcmd, "polo") == 0);
    g_free(newcmd);
#endif
}

static void test_param_mask(void)
{
    uint64_t newmask;

    /* no variable */
    newmask = params_mask("", descr);
    assert(newmask == 0);

    /* Empty variable */
    newmask = params_mask("{}", descr);
    assert(newmask == (uint64_t)-1LL);

    /* Known std parameter */
    newmask = params_mask("{name}", descr);
    assert(newmask == ATTR_MASK_name);

    /* Known std parameter but not an attribute */
    newmask = params_mask("{fid}", descr);
    assert(newmask == 0);

    /* Unknown parameter */
    newmask = params_mask("{marco}", descr);
    assert(newmask == 0);

    /*
     * With extra parameters
     */

    /* no variable */
    newmask = params_mask("", descr);
    assert(newmask == 0);

    /* Empty variable */
    newmask = params_mask("{}", descr);
    assert(newmask == (uint64_t)-1LL);

    /* Known std parameter */
    newmask = params_mask("{name}", descr);
    assert(newmask == ATTR_MASK_name);

    /* Known std parameter but not an attribute */
    newmask = params_mask("{fid}", descr);
    assert(newmask == 0);

    /* extra parameter */
    newmask = params_mask("{marco}", descr);
    assert(newmask == 0);

    /* twice the same variable */
    newmask = params_mask("{fullpath}{fullpath}", descr);
    assert(newmask == ATTR_MASK_fullpath);

    /* 2 variables */
    newmask = params_mask("wertyu{fullpath}fghj {name}", descr);
    assert(newmask == (ATTR_MASK_fullpath | ATTR_MASK_name));

    /* 1 known variable and 1 unknown */
    newmask = params_mask("wertyu{fullpath}fghj {namee}", descr);
    assert(newmask == ATTR_MASK_fullpath);

    /* 5 mixed variables */
    newmask = params_mask("{hello}wertyu{fullpath}{marco}fghj {name}{fid}", descr);
    assert(newmask == (ATTR_MASK_fullpath | ATTR_MASK_name));
}

int main(int argc, char **argv)
{
    test_subst_params();
    test_param_mask();

    printf("good\n");

    return 0;
}
