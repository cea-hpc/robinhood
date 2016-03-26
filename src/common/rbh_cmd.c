/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2016 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */
/**
 * \file  rbh_cmd.h
 * \brief External command execution.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "rbh_misc.h"
#include "rbh_logs.h"

#include <assert.h>
#include <unistd.h>

#define TAG "ExecCmd"

/**
 * When executing an external processes, two I/O channels are open on its
 * stdout / stderr streams.  Everytime a line is read from these channels
 * we call a user-provided function back.
 */
struct io_chan_arg {
    int              ident;
    parse_cb_t       cb;
    void            *udata;
    struct exec_ctx *exec_ctx;
};

/**
 * GMainLoop exposes a refcount but it is not related to running and stopping
 * the loop. Because we can have several users of the loop (child process
 * termination watcher, stdout watcher, stderr watcher), we need to wait for
 * all of them to complete before calling g_main_loop_quit(). Use custom
 * reference counting for this purpose.
 */
struct exec_ctx {
    GMainLoop   *loop;
    int          ref;
    int          rc;
};

static inline void ctx_incref(struct exec_ctx *ctx)
{
    assert(ctx->ref >= 0);
    ctx->ref++;
}

static inline void ctx_decref(struct exec_ctx *ctx)
{
    assert(ctx->ref > 0);
    if (--ctx->ref == 0)
        g_main_loop_quit(ctx->loop);
}

/** convert process return code to errno-like value */
static int child_status2errno(int status, const char **msg)
{
    int rc;

    if (WIFEXITED(status))
    {
        rc = WEXITSTATUS(status);
        /* handle shell special return values */
        switch (rc) {
        case 0:
            *msg = "no error";
            return 0;
        case 126:
            *msg = "permissions problem or command is not an executable";
            return -EPERM;
        case 127:
            *msg = "command not found";
            return -ENOENT;
        case 128:
            *msg = "invalid argument to exit";
            return -EINVAL;
        default:
            *msg = "non-zero exit status";
            /* return code to caller as-is */
            return rc;
        }
    }

    if (WIFSIGNALED(status))
    {
        *msg = "command terminated by signal";
        return -EINTR;
    }

    *msg = "unexpected error";
    return -EIO;
}

/**
 * External process termination handler.
 */
static void watch_child_cb(GPid pid, gint status, gpointer data)
{
    struct exec_ctx *ctx = data;
    const char      *err = "";

    DisplayLog(LVL_DEBUG, TAG, "Child %d terminated with %d", pid, status);

    if (status != 0) {
        ctx->rc = child_status2errno(status, &err);
        DisplayLog(LVL_DEBUG, TAG, "Command failed (%d): %s", ctx->rc,
                   err);
    }

    g_spawn_close_pid(pid);
    ctx_decref(ctx);
}

/**
 * IO channel watcher.
 * Read one line from the current channel and forward it to the user function.
 *
 * Return true as long as the channel has to stay registered, false otherwise.
 */
static gboolean readline_cb(GIOChannel *channel, GIOCondition cond, gpointer ud)
{
    struct io_chan_arg  *args  = ud;
    GError              *error = NULL;
    gchar               *line;
    gsize                size;
    GIOStatus            res;

    /* The channel is closed, no more data to read */
    if (cond == G_IO_HUP)
    {
        g_io_channel_unref(channel);
        ctx_decref(args->exec_ctx);
        return false;
    }

    res = g_io_channel_read_line(channel, &line, &size, NULL, &error);
    if (res != G_IO_STATUS_NORMAL)
    {
        DisplayLog(LVL_MAJOR, TAG, "Cannot read from child: %s",
                   error->message);
        g_error_free(error);
        g_io_channel_unref(channel);
        ctx_decref(args->exec_ctx);
        return false;
    }
    if (args->cb != NULL)
        args->cb(args->udata, line, size, args->ident);
    g_free(line);
    return true;
}

/**
 * Execute synchronously an external command, read its output and invoke
 * a user-provided filter function on every line of it.
 */
int execute_shell_command(const char *cmd, parse_cb_t cb_func, void *cb_arg)
{
    struct exec_ctx   ctx = { 0 };
    GPid              pid;
    gint              ac;
    gchar           **av = NULL;
    GError           *err_desc = NULL;
    GSpawnFlags       flags = G_SPAWN_SEARCH_PATH | G_SPAWN_DO_NOT_REAP_CHILD;
    GIOChannel       *out_chan = NULL;
    GIOChannel       *err_chan = NULL;
    int               p_stdout;
    int               p_stderr;
    bool              success;
    int               rc = 0;

    success = g_shell_parse_argv(cmd, &ac, &av, &err_desc);
    if (!success)
    {
        rc = -EINVAL;
        DisplayLog(LVL_MAJOR, TAG, "Cannot parse '%s': %s", cmd,
                   err_desc->message);
        goto out_err_free;
    }

    ctx.loop = g_main_loop_new(NULL, false);
    ctx.ref  = 0;
    ctx.rc   = 0;

    DisplayLog(LVL_DEBUG, TAG, "Spawning external command \"%s\"", cmd);

    success = g_spawn_async_with_pipes(NULL,   /* Working dir */
                                       av,     /* Parameters */
                                       NULL,   /* Environment */
                                       flags,  /* Execution directives */
                                       NULL,   /* Child setup function */
                                       NULL,   /* Child setup arg */
                                       &pid,   /* Child PID */
                                       NULL,      /* STDIN (unused) */
                                       cb_func ? &p_stdout : NULL, /* STDOUT */
                                       cb_func ? &p_stderr : NULL, /* STDERR */
                                       &err_desc);
    if (!success)
    {
        rc = -ECHILD;
        DisplayLog(LVL_MAJOR, TAG, "Failed to execute \"%s\": %s",
                   cmd, err_desc->message);
        goto out_free;
    }

    /* register a watcher in the loop, thus increase refcount of our exec_ctx */
    ctx_incref(&ctx);
    g_child_watch_add(pid, watch_child_cb, &ctx);

    if (cb_func != NULL)
    {
        struct io_chan_arg  out_args = {
            .ident    = STDOUT_FILENO,
            .cb       = cb_func,
            .udata    = cb_arg,
            .exec_ctx = &ctx
        };
        struct io_chan_arg  err_args = {
            .ident    = STDERR_FILENO,
            .cb       = cb_func,
            .udata    = cb_arg,
            .exec_ctx = &ctx
        };

        out_chan = g_io_channel_unix_new(p_stdout);
        err_chan = g_io_channel_unix_new(p_stderr);

        /* instruct the refcount system to close the channels when unused */
        g_io_channel_set_close_on_unref(out_chan, true);
        g_io_channel_set_close_on_unref(err_chan, true);

        /* update refcount for the two watchers */
        ctx_incref(&ctx);
        ctx_incref(&ctx);

        if ((g_io_channel_set_encoding(out_chan, NULL, &err_desc)
                != G_IO_STATUS_NORMAL) ||
            (g_io_channel_set_encoding(err_chan, NULL, &err_desc)
                != G_IO_STATUS_NORMAL))
        {
            if (err_desc->code == G_CONVERT_ERROR_NO_MEMORY)
                rc = -ENOMEM;
            else
                rc = -EINVAL;

            DisplayLog(LVL_MAJOR, TAG, "Could not set channel encoding: %s",
                       err_desc->message);
            goto out_free;
        }

        g_io_add_watch(out_chan, G_IO_IN | G_IO_HUP, readline_cb, &out_args);
        g_io_add_watch(err_chan, G_IO_IN | G_IO_HUP, readline_cb, &err_args);
    }

    g_main_loop_run(ctx.loop);

out_free:
    g_main_loop_unref(ctx.loop);
    g_strfreev(av);

out_err_free:
    if (err_desc)
        g_error_free(err_desc);

    return rc ? rc : ctx.rc;
}

/** template callback to redirect stderr to robinhood log (arg = (void*)log_level) */
int cb_stderr_to_log(void *arg, char *line, size_t size, int stream)
{
    log_level lvl = (log_level)arg;
    int       len;

    if (line == NULL)
        return -EINVAL;

    /* only log 'stderr' */
    if (stream != STDERR_FILENO)
        return 0;

    if (log_config.debug_level < lvl)
        return 0;

    len = strnlen(line, size);
    /* terminate the string */
    if (len >= size)
        line[len - 1] = '\0';

    /* remove '\n' */
    if ((len > 0) && (line[len - 1] == '\n'))
        line[len - 1] = '\0';

    DisplayLogFn(lvl, TAG, line);
    return 0;
}
