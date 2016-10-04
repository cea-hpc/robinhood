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

#include <ctype.h>

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
 *   "file is %p and its archive is %R{lhsm.archive_id} (neat!)"
 * becomes the 2 following chunks:
 *   "file is %s and its archive is "
 *   "%u (neat!)"
 * Their type of argument is stored in one fchunk each.
 */

struct fchunk {
    unsigned int directive;     /* main directive (e.g. "%p" -> 'p') */
    unsigned int sub_directive; /* sub directive (e.g. "%Rf" -> 'f') */

    GString *format;

    /* A simple time format directive will be stored directly in
     * 'format', while a whole strftime format string is stored in
     * 'time_format'. This is an enhancement over find's printf
     * option. */
    GString *time_format;

    /* For directives that refer to a status module attribute (for
     * instance "%R{lhsm.archive_id}"). */
    const sm_instance_t *smi;
    unsigned int attr_index; /**< absolute attr index */
    unsigned int rel_sm_info_index; /**< relative index of sm_info attr */
    const sm_info_def_t *def;
};

/* The SM status cannot be retrieved or read like the other SM
 * attributes. So make a special case for it. */
#define SUB_DIRECTIVE_STATUS 0x7876

/* Escape a file name to create a valid string. Valid filenames
 * characters are all except NULL and /. But not everything else is
 * printable. This function returns a statically allocated string
 * which will be overwritten by subsequent calls. */
static const char *escape_name(const char *fullname)
{
    const unsigned char *src = (const unsigned char *)fullname;
    static GString *dest;

    if (dest == NULL)
        dest = g_string_sized_new(100);
    else
        g_string_truncate(dest, 0);

    while (*src) {
        if (isprint(*src) && *src != '\\')
            g_string_append_c(dest, *src);
        else
            g_string_append_printf(dest, "\\%03o", *src);

        src++;
    }

    return dest->str;
}

/* Extract the field length from a printf specifier. e.g. "%-20s" will
 * set the field to "-20" and return the next position in the string. */
static const char *extract_field_width(const char *str, GString *format)
{
    if (*str == '-') {
        g_string_append_c(format, '-');
        str++;
    }

    while (*str && *str >= '0' && *str <= '9') {
        g_string_append_c(format, *str);
        str++;
    }

    return str;
}

/* Extract a module attribute name. Sets the smi and attribute index
 * in the chunk. Return NULL when the module doesn't exist. Typical
 * input string: "{lhsm.archive_id}". If not NULL, the returned
 * pointer points to '}'. */
static const char *extract_mod_attr(const char *str, struct fchunk *chunk)
{
    const char *start = str;
    const char *end;
    char *name = NULL;
    char *p;
    int rc;

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

    rc = sm_attr_get(chunk->smi, NULL, p + 1, NULL,
                     &chunk->def, &chunk->attr_index);
    if (rc)
        goto err;

    free(name);

    return end;

 err:
    free(name);
    return NULL;
}

/* Append a time format, for %RA, %RC and %RM. */
static const char *append_time_format(const char *str, struct fchunk *chunk)
{
    str++;
    if (*str == 0) {
        DisplayLog(LVL_CRIT, FIND_TAG,
                   "Error: incomplete time format at end of format string");
        return NULL;
    }

    if (*str == '{') {
        /* Format is in a substring. */
        chunk->time_format = g_string_sized_new(50);
        g_string_append_c(chunk->format, 's');

        str++;

        /* Copy until the closing bracket */
        while (*str && *str != '}') {
            g_string_append_c(chunk->time_format, *str);
            str++;
        }

        if (*str != '}') {
            DisplayLog(LVL_CRIT, FIND_TAG, "Error: invalid string format");
            return NULL;
        }
    } else if (*str == 'E' || *str == 'O') {
        /* Format starts with an strftime format modifier. Next
         * character is the directive. */
        g_string_append_c(chunk->format, *str);
        str++;
        if (*str == 0) {
            DisplayLog(LVL_CRIT, FIND_TAG,
                       "Error: incomplete time format at end of format string");
            return NULL;
        }

        g_string_append_c(chunk->format, *str);
    } else {
        /* Straight directive. */
        g_string_append_c(chunk->format, *str);
    }

    return str;
}

/* Analyze a format string, and find the next chunk. Each argument is
 * transformed into its real printf type. For instance "%s" means the
 * size, and is stored as an large integer in the database, needs to
 * be displayed as an "%zu". */
static const char *extract_chunk(const char *str, struct fchunk *chunk)
{
    unsigned int directive = 0;

    while (*str) {
        if (*str != '%') {
            if (*str == '\\') {
                str++;

                switch (*str) {
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
                    DisplayLog(LVL_CRIT, FIND_TAG,
                               "Error: lone \\ at end of format string");
                    return NULL;

                default:
                    DisplayLog(LVL_CRIT, FIND_TAG,
                               "Error: unrecognized escape code \\%c", *str);
                    return NULL;
                }
            } else {
                g_string_append_c(chunk->format, *str);
            }

            str++;
            continue;
        }

        if (directive) {
            /* Already have a directive. Stop here. */
            return str;
        }

        str++;

        if (*str == 0) {
            DisplayLog(LVL_CRIT, FIND_TAG,
                       "Error: lone %% at end of format string");
            return NULL;
        }

        /* Ignore %% as it is a valid printf directive, which saves a
         * chunk. */
        if (*str == '%') {
            g_string_append(chunk->format, "%%");
            str++;
            continue;
        }

        /* Found a new directive */
        g_string_append_c(chunk->format, '%');
        str = extract_field_width(str, chunk->format);
        if (str == NULL) {
            DisplayLog(LVL_CRIT, FIND_TAG,
                       "Error: invalid length field in format string at %s",
                       str);
            return NULL;
        }

        directive = *str;
        chunk->directive = directive;

        switch (*str) {
        case 'A':
            disp_mask.std |= ATTR_MASK_last_access;
            str = append_time_format(str, chunk);
            if (str == NULL)
                return NULL;
            break;

        case 'b':
            disp_mask.std |= ATTR_MASK_blocks;
            g_string_append(chunk->format, "zu");
            break;

        case 'C':
            disp_mask.std |= ATTR_MASK_last_mdchange;
            str = append_time_format(str, chunk);
            if (str == NULL)
                return NULL;
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
            disp_mask.std |= ATTR_MASK_gid;
            if (global_config.uid_gid_as_numbers)
                g_string_append_c(chunk->format, 'd');
            else
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

        case 'T':
            disp_mask.std |= ATTR_MASK_last_mod;
            str = append_time_format(str, chunk);
            if (str == NULL)
                return NULL;
            break;

        case 'u':
            disp_mask.std |= ATTR_MASK_uid;
            if (global_config.uid_gid_as_numbers)
                g_string_append_c(chunk->format, 'd');
            else
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

            switch (*str) {
            case 'C':
                disp_mask.std |= ATTR_MASK_creation_time;
                str = append_time_format(str, chunk);
                if (str == NULL)
                    return NULL;
                break;

            case 'c':
                disp_mask.std |= ATTR_MASK_fileclass;
                g_string_append_c(chunk->format, 's');
                break;

            case 'f':
                g_string_append_c(chunk->format, 's');
                break;

            case 'm':
                /* Module name and attribute followed by
                 * format. e.g. "%Rm{lhsm.archive_id}". */
                str++;

                str = extract_mod_attr(str, chunk);
                if (str == NULL) {
                    DisplayLog(LVL_CRIT, FIND_TAG,
                               "Error: cannot extract module attribute name, or invalid name");
                    return NULL;
                }

                attr_mask_set_index(&disp_mask, chunk->attr_index);

                if (strcmp(chunk->def->user_name, "status") == 0) {
                    /* status is a special case. Change the directive
                     * for print_entry(). */
                    chunk->sub_directive = SUB_DIRECTIVE_STATUS;
                    g_string_append_c(chunk->format, 's');

                    break;
                }

                chunk->rel_sm_info_index = attr2sminfo_index(chunk->attr_index)
                                           - chunk->smi->sm_info_offset;

                /* The format for that attribute */
                switch (chunk->def->db_type) {
                case DB_UINT:
                case DB_BOOL:
                    g_string_append_c(chunk->format, 'u');
                    break;

                case DB_INT:
                    g_string_append_c(chunk->format, 'i');
                    break;

                case DB_TEXT:
                    g_string_append_c(chunk->format, 's');
                    break;

                default:
                    DisplayLog(LVL_CRIT, FIND_TAG,
                               "Error: unsupported database format %d",
                               chunk->def->db_type);
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
                DisplayLog(LVL_CRIT, FIND_TAG,
                           "Error: lone %%R at end of format string");
                return NULL;

            default:
                DisplayLog(LVL_CRIT, FIND_TAG,
                           "Error: unrecognized format %%R%c", *str);
                return NULL;
            }
            break;

        case 0:
            DisplayLog(LVL_CRIT, FIND_TAG,
                       "Error: lone %% at end of format string");
            return NULL;

        default:
            DisplayLog(LVL_CRIT, FIND_TAG, "Error: unrecognized format %%%c",
                       *str);
            return NULL;
        }

        str++;
    }

    return str;
}

static void printf_date(const struct fchunk *chunk, time_t date)
{
    char str[1000];
    struct tm *tmp;
    size_t sret;

    tmp = localtime(&date);
    if (tmp == NULL) {
        printf("(none)");
        return;
    }

    if (chunk->time_format)
        sret = strftime(str, sizeof(str), chunk->time_format->str, tmp);
    else
        sret = strftime(str, sizeof(str), chunk->format->str, tmp);

    if (sret >= sizeof(str) - 1) {
        /* Overflow. 1000 bytes should be big enough for that to never
         * happen in any locale. */
        printf("(date output truncated)");
    } else if (sret == 0) {
        /* According to the man page, a return of 0 is either an error
         * or an empty string. In both cases, don't print anything. */
    } else {
        if (chunk->time_format)
            printf(chunk->format->str, str);
        else
            printf("%s", str);
    }
}

/**
 * Output the desired information for one file.
 */
void printf_entry(GArray *chunks, const wagon_t *id, const attr_set_t *attrs)
{
    int i;

    for (i = 0; i < chunks->len; i++) {
        struct fchunk *chunk = &g_array_index(chunks, struct fchunk, i);
        const char *format = chunk->format->str;

        switch (chunk->directive) {
        case 0:
            printf(format);
            break;

        case 'A':
            printf_date(chunk, ATTR(attrs, last_access));
            break;

        case 'b':
            printf(format, ATTR(attrs, blocks));
            break;

        case 'C':
            printf_date(chunk, ATTR(attrs, last_mdchange));
            break;

        case 'd':
            printf(format, ATTR(attrs, depth));
            break;

        case 'f':
            printf(format, ATTR(attrs, name));
            break;

        case 'g':
            if (global_config.uid_gid_as_numbers)
                printf(format, ATTR(attrs, gid).num);
            else
                printf(format, ATTR(attrs, gid).txt);
            break;

        case 'm':
            printf(format, ATTR(attrs, mode));
            break;

        case 'M':
            {
                char mode_str[10];  /* mask + final '\0' */

                mode_str[9] = 0;
                mode_string(ATTR(attrs, mode), mode_str);

                printf(format, mode_str);
            }
            break;

        case 'n':
            printf(format, ATTR(attrs, nlink));
            break;

        case 'p':
            if (prog_options.escaped)
                printf(format, escape_name(id->fullname));
            else
                printf(format, id->fullname);
            break;

        case 's':
            printf(format, ATTR(attrs, size));
            break;

        case 'T':
            printf_date(chunk, ATTR(attrs, last_mod));
            break;

        case 'u':
            if (global_config.uid_gid_as_numbers)
                printf(format, ATTR(attrs, uid).num);
            else
                printf(format, ATTR(attrs, uid).txt);
            break;

        case 'Y':
            {
                const char *type;

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
            switch (chunk->sub_directive) {
            case 'C':
                printf_date(chunk, ATTR(attrs, creation_time));
                break;

            case 'c':
                printf(format,
                       class_format(ATTR_MASK_TEST(attrs, fileclass) ?
                                    ATTR(attrs, fileclass) : NULL));
                break;

            case 'f':
                {
                    char fid_str[RBH_FID_LEN];

                    sprintf(fid_str, DFID_NOBRACE, PFID(&id->id));
                    printf(format, fid_str);
                }
                break;

            case 'm':
                if (ATTR_MASK_INFO_TEST(attrs, chunk->smi,
                                        chunk->rel_sm_info_index)) {
                    switch (chunk->def->db_type) {
                    case DB_UINT:
                        printf(format,
                               *(unsigned int *)SMI_INFO(attrs, chunk->smi,
                                                         chunk->
                                                         rel_sm_info_index));
                        break;

                    case DB_INT:
                        printf(format,
                               *(int *)SMI_INFO(attrs, chunk->smi,
                                                chunk->rel_sm_info_index));
                        break;

                    case DB_BOOL:
                        printf(format,
                               *(bool *)SMI_INFO(attrs, chunk->smi,
                                                 chunk->rel_sm_info_index));
                        break;

                    case DB_TEXT:
                        printf(format,
                               SMI_INFO(attrs, chunk->smi,
                                        chunk->rel_sm_info_index));
                        break;

                    default:
                        break;
                    }
                } else {
                    switch (chunk->def->db_type) {
                    case DB_UINT:
                    case DB_INT:
                        printf(format, 0);
                        break;

                    case DB_TEXT:
                        printf(format, "[n/a]");
                        break;

                    default:
                        break;
                    }
                }
                break;

#ifdef _LUSTRE
            case 'o':
                if (ATTR_MASK_TEST(attrs, stripe_items) &&
                    (ATTR(attrs, stripe_items).count > 0)) {
                    GString *osts = g_string_new("");

                    append_stripe_list(osts, &ATTR(attrs, stripe_items), true);
                    printf(format, osts->str);
                    g_string_free(osts, TRUE);
                }
                break;

            case 'p':
                {
                    char fid_str[RBH_FID_LEN];

                    sprintf(fid_str, DFID_NOBRACE,
                            PFID(&ATTR(attrs, parent_id)));
                    printf(format, fid_str);

                    break;
                }
#endif

            case SUB_DIRECTIVE_STATUS:
                {
                    unsigned int smi_index = chunk->smi->smi_index;

                    if (ATTR_MASK_STATUS_TEST(attrs, smi_index))
                        printf(format, STATUS_ATTR(attrs, smi_index));
                    else
                        printf(format, "[n/a]");

                    break;
                }

            }
            break;
        }
    }
}

/**
 * Release the ressources allocated by prepare_printf_format.
 */
void free_printf_formats(GArray *chunks)
{
    int i;

    for (i = 0; i < chunks->len; i++) {
        struct fchunk *chunk = &g_array_index(chunks, struct fchunk, i);

        g_string_free(chunk->format, TRUE);
        if (chunk->time_format)
            g_string_free(chunk->time_format, TRUE);
    }

    g_array_unref(chunks);
}

/**
 * Split the format string and store the result in an array. Validate
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

    while (*format) {
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
