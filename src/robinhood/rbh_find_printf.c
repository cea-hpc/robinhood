/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright 2016 Cray Inc. All Rights Reserved.
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

#include <glib.h>

#include "cmd_helpers.h"
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "status_manager.h"

#include "rbh_find.h"

/**
 * Handle the printf option for rbh-find
 *
 * The format string is split into chunks with only one % in
 * each. That format in the chunk is then adapted to be fed to
 * printf() like functions. The bulk of the processing such as looking
 * for directives, finding their arguments, validating them, ... is
 * done only once.
 *
 * For instance a format like
 *   "file is %p and its archive is %R{lhsm.archid}u (neat!)"
 * becomes the 2 following chunks:
 *   "file is %s and its archive is "
 *   "%u (neat!)"
 * Their type of argument is stored in one fchunk each.
 */

struct fchunk {

    char directive;            /* main directive (e.g. "%p" -> 'p') */
    char sub_directive;        /* sub directive (e.g. "%Rf" -> 'f') */

    GString *format;

    /* For directives that refer to a status module attribute (for
     * instance "%R{lhsm.archid}"). */
    const sm_instance_t *smi;
    unsigned int attr_index;
    char attr_format;           /* how to print the attribute
                                 * (s or u) */
};

/* Extract the field length from a printf specifier. e.g. "%-20s" will
 * set the field to "-20" and return the next position in the string. */
static const char *extract_field_width(const char *str, GString *format)
{
    if (*str == '-')
    {
        g_string_append_c(format, '-');
        str++;
    }

    while (*str && *str >= '0' && *str <= '9')
    {
        g_string_append_c(format, *str);
        str++;
    }

    return str;
}

/* Extract a module attribute name. Sets the smi and attribute index
 * in the chunk. Return NULL when the module doesn't exist. Typical
 * input string: "{lhsm.archive_id}". */
static const char *extract_mod_attr(const char *str, struct fchunk *chunk)
{
    const char *start = str;
    const char *end;
    char *name = NULL;
    char *p;
    int rc;
    const sm_info_def_t *ppdef;

    if (*start != '{')
        goto err;

    start++;
    end = start;

    /* Find the closing bracket */
    while (*end && *end != '}')
        end++;

    if (*end != '}')
        goto err;

    name = strndup(start, end - start);
    if (name == NULL)
        goto err;

    /* Find the dot so we can get the module name */
    p = strchr(name, '.');
    if (p == NULL)
        goto err;

    *p = 0;

    chunk->smi = smi_by_name(name);
    if (chunk->smi == NULL)
        goto err;

    rc = sm_attr_get(chunk->smi, NULL, p+1, NULL, &ppdef, &chunk->attr_index);
    if (rc)
        goto err;

    free(name);

    return end + 1;

err:
    free(name);
    return NULL;
}

/* Analyze a format string, and find the next chunk. Each argument is
 * transformed into its real printf type. For instance "%s" means the
 * size, and is stored as an large integer in the database, needs to
 * be displayed as an "%zu". */
static const char *extract_chunk(const char *str, struct fchunk *chunk)
{
    const char *directive = NULL;

    while (*str)
    {
        if (*str != '%')
        {
            if (*str == '\\')
            {
                str++;

                switch(*str)
                {
                case '\\':
                    g_string_append_c(chunk->format, '\\');
                    break;

                case 'n':
                    g_string_append_c(chunk->format, '\n');
                    break;

                case 't':
                    g_string_append_c(chunk->format, '\t');
                    break;

                case 0:
                    DisplayLog(LVL_CRIT, FIND_TAG, "Error: lone \\ at end of format string");
                    return NULL;

                default:
                    DisplayLog(LVL_CRIT, FIND_TAG, "Error: unrecognized escape code \\%c", *str);
                    return NULL;
                }
            }
            else
            {
                g_string_append_c(chunk->format, *str);
            }

            str++;
            continue;
        }

        if (directive)
        {
            /* Already have a directive. Stop here. */
            return str;
        }

        str++;

        if (*str == 0)
        {
            DisplayLog(LVL_CRIT, FIND_TAG, "Error: lone %% at end of format string");
            return NULL;
        }

        /* Ignore %% as it is a valid printf directive, which saves a
         * chunk. */
        if (*str == '%')
        {
            g_string_append(chunk->format, "%%");
            str++;
            continue;
        }

        /* Found a new directive */
        directive = str;

        g_string_append_c(chunk->format, '%');
        str = extract_field_width(str, chunk->format);
        if (str == NULL) {
            DisplayLog(LVL_CRIT, FIND_TAG, "Error: invalid length field in format string at %s", str);
            return NULL;
        }

        chunk->directive = *str;

        switch (*str)
        {
        case 'b':
            disp_mask.std |= ATTR_MASK_blocks;
            g_string_append(chunk->format, "zu");
            break;

        case 'd':
            disp_mask.std |= ATTR_MASK_depth;
            g_string_append_c(chunk->format, 'u');
            break;

        case 'f':
            disp_mask.std |= ATTR_MASK_name;
            g_string_append_c(chunk->format, 's');
            break;

        case 'g':
            disp_mask.std |= ATTR_MASK_gr_name;
            g_string_append_c(chunk->format, 's');
            break;

        case 'M':
            disp_mask.std |= ATTR_MASK_mode;
            g_string_append_c(chunk->format, 's');
            break;

        case 'm':
            disp_mask.std |= ATTR_MASK_mode;
            g_string_append_c(chunk->format, 'o');
            break;

        case 'n':
            disp_mask.std |= ATTR_MASK_nlink;
            g_string_append_c(chunk->format, 'u');
            break;

        case 'p':
            g_string_append_c(chunk->format, 's');
            break;

        case 's':
            disp_mask.std |= ATTR_MASK_size;
            g_string_append(chunk->format, "zu");
            break;

        case 'u':
            disp_mask.std |= ATTR_MASK_owner;
            g_string_append_c(chunk->format, 's');
            break;

        case 'Y':
            disp_mask.std |= ATTR_MASK_type;
            g_string_append_c(chunk->format, 's');
            break;

        case 'y':
            disp_mask.std |= ATTR_MASK_type;
            g_string_append_c(chunk->format, 'c');
            break;

        case 'R':
            str++;
            chunk->sub_directive = *str;

            switch (*str)
            {
            case 'c':
                disp_mask.std |= ATTR_MASK_fileclass;
                g_string_append_c(chunk->format, 's');
                break;

            case 'f':
                g_string_append_c(chunk->format, 's');
                break;

            case 'm':
                /* Field from module followed by format. e.g. "%Rm{lhsm.archid}u". */
                str++;

                str = extract_mod_attr(str, chunk);
                if (str == NULL)
                {
                    DisplayLog(LVL_CRIT, FIND_TAG,
                               "Error: cannot extract module attribute name, or invalid name");
                    return NULL;
                }

                disp_mask.sm_info |= smi_info_bit(chunk->smi, chunk->attr_index);

                chunk->attr_index = attr2sminfo_index(chunk->attr_index);

                /* The format for that attribute */
                switch(*str)
                {
                case 'u':
                    g_string_append_c(chunk->format, 'u');
                    chunk->attr_format = 'u';
                    break;

                case 's':
                    g_string_append_c(chunk->format, 's');
                    chunk->attr_format = 's';
                    break;

                default:
                    DisplayLog(LVL_CRIT, FIND_TAG, "Error: unrecognized format %%R%c", *str);
                    break;
                }
                break;

            case 'o':
                disp_mask.std |= ATTR_MASK_stripe_items;
                g_string_append_c(chunk->format, 's');
                break;

            case 'p':
                disp_mask.std |= ATTR_MASK_parent_id;
                g_string_append_c(chunk->format, 's');
                break;

            case 0:
                DisplayLog(LVL_CRIT, FIND_TAG, "Error: lone %%R at end of format string");
                return NULL;

            default:
                DisplayLog(LVL_CRIT, FIND_TAG, "Error: unrecognized format %%R%c", *str);
                return NULL;
            }
            break;

        case 0:
            DisplayLog(LVL_CRIT, FIND_TAG, "Error: lone %% at end of format string");
            return NULL;

        default:
            DisplayLog(LVL_CRIT, FIND_TAG, "Error: unrecognized format %%%c", *str);
            return NULL;
        }

        str++;
    }


    return str;
}

/* Output the desired information for one file. */
void printf_entry(GArray *chunks, const wagon_t *id, const attr_set_t *attrs)
{
    int i;

    for (i = 0; i < chunks->len; i++)
    {
        struct fchunk *chunk = &g_array_index(chunks, struct fchunk, i);
        const char *format = chunk->format->str;

        switch (chunk->directive)
        {
        case 0:
            printf(format);
            break;

        case 'b':
            printf(format, ATTR(attrs, blocks));
            break;

        case 'd':
            printf(format, ATTR(attrs, depth));
            break;

        case 'f':
            printf(format, ATTR(attrs, name));
            break;

        case 'g':
            printf(format, ATTR(attrs, gr_name));
            break;

        case 'm':
            printf(format, ATTR(attrs, mode));
            break;

        case 'M':
        {
            char mode_str[9];

            mode_str[8] = 0;
            mode_string(ATTR(attrs, mode), mode_str);

            printf(format, mode_str);
        }
            break;

        case 'n':
            printf(format, ATTR(attrs, nlink));
            break;

        case 'p':
            printf(format, ATTR(attrs, fullpath));
            break;

        case 's':
            printf(format, ATTR(attrs, size));
            break;

        case 'u':
            printf(format, ATTR(attrs, owner));
            break;

        case 'Y':
        {
            const char * type;

            if (!ATTR_MASK_TEST(attrs, type))
                type = "?";
            else
                type = type2char(ATTR(attrs, type));

            printf(format, type);
        }
            break;

        case 'y':
        {
            char type;

            if (!ATTR_MASK_TEST(attrs, type))
                type = '?';
            else
                type = type2onechar(ATTR(attrs, type));

            printf(format, type);
        }
            break;

        case 'R':
            /* Robinhood specifiers */
            switch (chunk->sub_directive)
            {
            case 'c':
                printf(format,
                       class_format(ATTR_MASK_TEST(attrs, fileclass)?
                                    ATTR(attrs, fileclass) : NULL));
                break;

            case 'f':
            {
                char fid_str[FID_NOBRACE_LEN+1];

                sprintf(fid_str, DFID_NOBRACE, PFID(&id->id));
                printf(format, fid_str);
            }
                break;

            case 'm':
                if (ATTR_MASK_INFO_TEST(attrs, chunk->smi, chunk->attr_index))
                {
                    if (chunk->attr_format == 'u')
                        printf(format, *(unsigned int *)SMI_INFO(attrs, chunk->smi, chunk->attr_index));
                    else if (chunk->attr_format == 's')
                        printf(format, SMI_INFO(attrs, chunk->smi, chunk->attr_index));
                }
                else
                {
                    if (chunk->attr_format == 'u')
                        printf(format, -1);
                    else if (chunk->attr_format == 's')
                        printf(format, "[n/a]");
                }
                break;

            case 'o':
#ifdef _LUSTRE
                if (ATTR_MASK_TEST(attrs, stripe_items) &&
                    (ATTR(attrs, stripe_items).count > 0))
                {
                    char ostbuf[24576] = "";

                    /* Note: it would be nice to have FormatStripeList
                     * work on GString. */
                    FormatStripeList(ostbuf, sizeof(ostbuf), &ATTR(attrs, stripe_items), true);
                    printf(format, ostbuf);
                }
#endif
                break;

            case 'p':
            {
                char fid_str[FID_NOBRACE_LEN+1];

                sprintf(fid_str, DFID_NOBRACE, PFID(&ATTR(attrs, parent_id)));
                printf(format, fid_str);

                break;
            }
            }
            break;
        }
    }
}

/* Release the ressources allocated by prepare_printf_format. */
void free_printf_formats(GArray *chunks)
{
    int i;

    for (i = 0; i < chunks->len; i++)
    {
        struct fchunk *chunk = &g_array_index(chunks, struct fchunk, i);

        g_string_free(chunk->format, TRUE);
    }

    g_array_unref(chunks);
}

/* Split the format string and store the result in an array. Validate
 * each chunk at the same time.
 * Return the array on success, or NULL on error.
 *
 * This function is called only once per format string.
 */
GArray *prepare_printf_format(const char *format)
{
    struct fchunk chunk;
    GArray *chunks;

    chunks = g_array_sized_new(FALSE, FALSE, sizeof(struct fchunk), 10);

    while (*format)
    {
        chunk.format = g_string_sized_new(50);

        format = extract_chunk(format, &chunk);
        g_array_append_val(chunks, chunk);

        if (format == NULL)
            goto free;
    }

    return chunks;

free:
    free_printf_formats(chunks);

    return NULL;
}

