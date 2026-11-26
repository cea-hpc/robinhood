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
#ifdef HAVE_POSIX_SPAWN_ADDCLOSEFROM
#include <spawn.h>
#include <errno.h>

extern char **environ;
#endif

#define TAG "ExecCmd"

/**
 * When executing an external processes, two I/O channels are open on its
 * stdout / stderr streams.  Every time a line is read from these channels
 * we call a user-provided function back.
 */
struct io_chan_arg {
    int         ident;
    parse_cb_t  cb;
    void       *udata;
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
    GMainLoop    *loop;
    GMainContext *gctx;
    int           ref;
    int           rc;
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

    if (WIFEXITED(status)) {
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

    if (WIFSIGNALED(status)) {
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
        DisplayLog(LVL_DEBUG, TAG, "Command failed (%d): %s", ctx->rc, err);
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
static gboolean readline_cb(GIOChannel *channel, GIOCondition cond,
                            gpointer ud)
{
    struct io_chan_arg  *args = ud;
    GError              *error = NULL;
    gchar               *line;
    gsize                size;
    GIOStatus            res;

    /* The channel is closed, no more data to read */
    if (cond == G_IO_HUP) {
        g_io_channel_unref(channel);
        ctx_decref(args->exec_ctx);
        return false;
    }

    res = g_io_channel_read_line(channel, &line, &size, NULL, &error);
    if (res != G_IO_STATUS_NORMAL) {
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
 * Wrapper to set io channel encoding to NULL
 */
static int iochan_null_enc(GIOChannel *chan)
{
    GError *err_desc = NULL;
    int rc = 0;

    if (g_io_channel_set_encoding(chan, NULL, &err_desc)
            != G_IO_STATUS_NORMAL) {
/* G_CONVERT_ERROR_NO_MEMORY exists since glib 2.40 */
#if GLIB_CHECK_VERSION(2,40,0)
        if (err_desc->code == G_CONVERT_ERROR_NO_MEMORY)
            rc = -ENOMEM;
        else
#endif
            rc = -EINVAL;

        DisplayLog(LVL_MAJOR, TAG, "Could not set channel encoding: %s",
                   err_desc->message);
        g_error_free(err_desc);
    }

    return rc;
}

/**
 * g_child_watch_add will bind the source to the "main" main context,
 * g_main_context_get_default(), which is not what we want
 */
static int g_child_watch_add_tothread(GPid pid,
                                      GChildWatchFunc function, gpointer data)
{
    GSource *source;
    guint id;

    g_return_val_if_fail(function != NULL, 0);
    g_return_val_if_fail(pid > 0, 0);

    source = g_child_watch_source_new(pid);

    g_source_set_callback(source, (GSourceFunc) function, data, NULL);
    id = g_source_attach(source, g_main_context_get_thread_default());
    g_source_unref(source);

    return id;
}

static int g_io_add_watch_tothread(GIOChannel *channel,
                                   GIOCondition condition,
                                   GIOFunc func, gpointer user_data)
{
    GSource *source;
    guint id;

    g_return_val_if_fail(channel != NULL, 0);

    source = g_io_create_watch(channel, condition);

    g_source_set_callback(source, (GSourceFunc) func, user_data, NULL);

    id = g_source_attach(source, g_main_context_get_thread_default());
    g_source_unref(source);

    return id;
}

#ifdef HAVE_POSIX_SPAWN_ADDCLOSEFROM
/**
 * Create a child process with its stdout/stderr connected to pipes, using
 * posix_spawn() rather than the fork()+exec() that g_spawn_async_with_pipes()
 * performs internally.
 *
 * Rationale: glibc fork() runs the pthread_atfork handlers, which take every
 * malloc arena lock, and it clones the caller's page tables. In a policy
 * daemon with a large resident heap and dozens of worker threads, each
 * per-entry fork therefore stalls every thread that touches malloc for the
 * duration of the fork, capping aggregate action throughput near 1/fork_time
 * *per process*, independent of nb_threads. posix_spawn() uses
 * clone(CLONE_VM | CLONE_VFORK) and runs no atfork handlers, so the worker
 * threads keep running while the child is created.
 *
 * Semantics kept compatible with the previous g_spawn call:
 *   - PATH search (posix_spawnp, ~ G_SPAWN_SEARCH_PATH);
 *   - no fds >= 3 inherited by the child (addclosefrom_np, glibc >= 2.34),
 *     matching g_spawn's default fd-closing behaviour;
 *   - the caller reaps the child (~ G_SPAWN_DO_NOT_REAP_CHILD): still done by
 *     the GLib child watch below.
 *
 * Returns 0 on success, sets *pid, and (when want_pipes) sets the p_out and
 * p_err outputs to the read ends of the child's stdout and stderr pipes.
 * Returns -errno on failure.
 */
static int spawn_with_pipes(char **argv, bool want_pipes,
                            GPid *pid, int *p_out, int *p_err)
{
    posix_spawn_file_actions_t fa;
    int   out_pipe[2] = { -1, -1 };
    int   err_pipe[2] = { -1, -1 };
    pid_t child;
    int   rc;

    rc = posix_spawn_file_actions_init(&fa);
    if (rc)
        return -rc;

    if (want_pipes) {
        if (pipe(out_pipe) < 0 || pipe(err_pipe) < 0) {
            rc = errno;
            goto out;
        }
        /* in the child, wire the pipe write ends onto stdout/stderr */
        posix_spawn_file_actions_adddup2(&fa, out_pipe[1], STDOUT_FILENO);
        posix_spawn_file_actions_adddup2(&fa, err_pipe[1], STDERR_FILENO);
    }

    /* close every inherited fd >= 3 in the child (including the spare pipe
     * fds just dup'd onto 1/2). Matches g_spawn's default; needs glibc 2.34+ */
    posix_spawn_file_actions_addclosefrom_np(&fa, STDERR_FILENO + 1);

    rc = posix_spawnp(&child, argv[0], &fa, NULL, argv, environ);

out:
    posix_spawn_file_actions_destroy(&fa);

    if (rc) {
        if (out_pipe[0] >= 0) close(out_pipe[0]);
        if (out_pipe[1] >= 0) close(out_pipe[1]);
        if (err_pipe[0] >= 0) close(err_pipe[0]);
        if (err_pipe[1] >= 0) close(err_pipe[1]);
        return -rc;
    }

    if (want_pipes) {
        /* parent keeps the read ends, closes the write ends */
        close(out_pipe[1]);
        close(err_pipe[1]);
        *p_out = out_pipe[0];
        *p_err = err_pipe[0];
    }

    *pid = child;
    return 0;
}
#endif /* HAVE_POSIX_SPAWN_ADDCLOSEFROM */

/**
 * Execute synchronously an external command, read its output and invoke
 * a user-provided filter function on every line of it.
 */
int execute_shell_command(char **cmd, parse_cb_t cb_func, void *cb_arg)
{
    struct exec_ctx     ctx = { 0 };
    GPid                pid = -1;
    GError             *err_desc = NULL;
    GIOChannel         *out_chan = NULL;
    GIOChannel         *err_chan = NULL;
    struct io_chan_arg  out_args;
    struct io_chan_arg  err_args;
    char               *log_cmd;
    int                 p_stdout = -1;
    int                 p_stderr = -1;
    int                 rc = 0;
#ifndef HAVE_POSIX_SPAWN_ADDCLOSEFROM
    GSpawnFlags         flags = G_SPAWN_SEARCH_PATH | G_SPAWN_DO_NOT_REAP_CHILD;
    bool                success;
#endif

    ctx.gctx = g_main_context_new();
    g_main_context_push_thread_default(ctx.gctx);
    ctx.loop = g_main_loop_new(ctx.gctx, false);
    ctx.ref = 0;
    ctx.rc = 0;

    DisplayLog(LVL_DEBUG, TAG, "Spawning external command \"%s\"", cmd[0]);

#ifdef HAVE_POSIX_SPAWN_ADDCLOSEFROM
    rc = spawn_with_pipes(cmd, cb_func != NULL, &pid, &p_stdout, &p_stderr);
    if (rc) {
        log_cmd = concat_cmd(cmd);
        DisplayLog(LVL_MAJOR, TAG, "Failed to execute \"%s\": %s",
                   log_cmd, strerror(-rc));
        free(log_cmd);
        goto out_free;
    }
#else
    success = g_spawn_async_with_pipes(NULL,    /* Working dir */
                                       cmd, /* Parameters */
                                       NULL,    /* Environment */
                                       flags,   /* Execution directives */
                                       NULL,    /* Child setup function */
                                       NULL,    /* Child setup arg */
                                       &pid,    /* Child PID */
                                       NULL,    /* STDIN (unused) */
                                       cb_func ? &p_stdout : NULL,  /* STDOUT */
                                       cb_func ? &p_stderr : NULL,  /* STDERR */
                                       &err_desc);
    if (!success) {
        rc = -ECHILD;
        log_cmd = concat_cmd(cmd);
        DisplayLog(LVL_MAJOR, TAG, "Failed to execute \"%s\": %s",
                   log_cmd, err_desc->message);
        free(log_cmd);
        goto out_free;
    }
#endif

    /* register a watcher in the loop, thus increase refcount of our exec_ctx */
    ctx_incref(&ctx);
    g_child_watch_add_tothread(pid, watch_child_cb, &ctx);

    if (cb_func != NULL) {
        out_args.ident    = STDOUT_FILENO;
        out_args.cb       = cb_func;
        out_args.udata    = cb_arg;
        out_args.exec_ctx = &ctx;
        err_args.ident    = STDERR_FILENO;
        err_args.cb       = cb_func;
        err_args.udata    = cb_arg;
        err_args.exec_ctx = &ctx;

        out_chan = g_io_channel_unix_new(p_stdout);
        err_chan = g_io_channel_unix_new(p_stderr);

        /* instruct the refcount system to close the channels when unused */
        g_io_channel_set_close_on_unref(out_chan, true);
        g_io_channel_set_close_on_unref(err_chan, true);

        if ((rc = iochan_null_enc(out_chan)) ||
            (rc = iochan_null_enc(err_chan)))
            goto out_free;

        /* update refcount for the two watchers */
        ctx_incref(&ctx);
        ctx_incref(&ctx);

        g_io_add_watch_tothread(out_chan, G_IO_IN | G_IO_HUP,
                                readline_cb, &out_args);
        g_io_add_watch_tothread(err_chan, G_IO_IN | G_IO_HUP,
                                readline_cb, &err_args);
    }

    g_main_loop_run(ctx.loop);

 out_free:
    g_main_loop_unref(ctx.loop);
    g_main_context_pop_thread_default(ctx.gctx);
    g_main_context_unref(ctx.gctx);

    if (err_desc)
        g_error_free(err_desc);

    return rc ? rc : ctx.rc;
}

/**
 * Template callback to redirect stderr to robinhood log
 * @param arg (void*)log_level.
 */
int cb_stderr_to_log(void *arg, char *line, size_t size, int stream)
{
    log_level lvl = (log_level) arg;
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

    DisplayLogFn(lvl, TAG, "%s", line);
    return 0;
}
