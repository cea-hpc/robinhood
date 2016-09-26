/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2015 CEA/DAM
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
#include "rbh_logs.h"
#include "rbh_misc.h"
#include "rbh_modules.h"
#include "mod_internal.h"
#include "policy_rules.h"
#include "status_manager.h"
#include "Memory.h"
#include <unistd.h>
#include <utime.h>
#include <fcntl.h>
#include <sys/sendfile.h>
#include <zlib.h>

struct copy_params_t {
    const char *name;
    copy_flags_e flag;
} copy_params[] = {
    {
    "compress", CP_COMPRESS},   /* compress target */
    {
    "nosync", CP_NO_SYNC},  /* don't sync when the copy ends */
    {
    "copyback", CP_COPYBACK},   /* revert copy way: tgt->src */
    {
    NULL, 0}
};

/** helper to set file attributes from a struct stat */
static int file_clone_attrs(const char *tgt, const struct stat *st)
{
    struct utimbuf tbuf;

    if (lchown(tgt, st->st_uid, st->st_gid))
        return -errno;
    if (chmod(tgt, st->st_mode & 07777))
        return -errno;

    tbuf.actime = st->st_atime;
    tbuf.modtime = st->st_mtime;

    if (utime(tgt, &tbuf))
        return -errno;

    return 0;
}

copy_flags_e params2flags(const action_params_t *params)
{
    copy_flags_e flg = 0;
    struct copy_params_t *curr_param;

    if (params == NULL)
        return 0;

    for (curr_param = copy_params; curr_param->name != NULL; curr_param++) {
        const char *val = rbh_param_get(params, curr_param->name);
        if ((val != NULL) && (str2bool(val) != 0))
            flg |= curr_param->flag;
    }

    return flg;
}

struct copy_info {
    const char *src;
    const char *dst;
    int src_fd;
    int dst_fd;
    struct stat src_st;
};

static int flush_data(int srcfd, int dstfd, copy_flags_e flags)
{
    posix_fadvise(srcfd, 0, 0, POSIX_FADV_DONTNEED);
    if (!(flags & CP_NO_SYNC)) {
        if (fdatasync(dstfd) < 0)
            return -errno;
    }
    posix_fadvise(dstfd, 0, 0, POSIX_FADV_DONTNEED);
    return 0;
}

static inline bool compress_src(copy_flags_e flags)
{
    return (flags & CP_COMPRESS) && (flags & CP_COPYBACK);
}

static inline bool uncompress_src(copy_flags_e flags)
{
    return (flags & CP_COMPRESS) && !(flags & CP_COPYBACK);
}

static int builtin_copy_standard(const struct copy_info *cp_nfo,
                                 copy_flags_e flags)
{
    int srcfd, dstfd;
    struct stat dst_st;
    int rc = 0;
    size_t io_size;
    ssize_t r = 0, w = 0;
    char *io_buff = NULL;
    gzFile gz = NULL;
    int gzerr, err_close = 0;

    if (compress_src(flags)) {
        srcfd = dup(cp_nfo->src_fd);
        dstfd = cp_nfo->dst_fd;

        gz = gzdopen(srcfd, "rb");
        if (gz == NULL) {
            DisplayLog(LVL_MAJOR, CP_TAG,
                       "Failed to initialize decompression stream");
            close(srcfd);
            return -EIO;
        }
    } else if (uncompress_src(flags)) {
        srcfd = cp_nfo->src_fd;
        dstfd = dup(cp_nfo->dst_fd);

        gz = gzdopen(dstfd, "wb");
        if (gz == NULL) {
            DisplayLog(LVL_MAJOR, CP_TAG,
                       "Failed to initialize decompression stream");
            close(dstfd);
            return -EIO;
        }
    } else {
        /* Uncompressed regular copy */
        srcfd = cp_nfo->src_fd;
        dstfd = cp_nfo->dst_fd;
    }

    /* needed to get the biggest IO size of source and destination. */
    if (fstat(dstfd, &dst_st)) {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Failed to stat %s: %s",
                   cp_nfo->dst, strerror(-rc));
        goto out_close;
    }

    io_size = MAX2(cp_nfo->src_st.st_blksize, dst_st.st_blksize);
    DisplayLog(LVL_DEBUG, CP_TAG, "using IO size = %" PRI_SZ, io_size);

    io_buff = MemAlloc(io_size);
    if (!io_buff) {
        rc = -ENOMEM;
        goto out_close;
    }

    /* Do the copy */
    do {
        if (compress_src(flags))
            r = gzread(gz, io_buff, io_size);
        else
            r = read(srcfd, io_buff, io_size);

        if (r <= 0)
            break;

        if (uncompress_src(flags))
            w = gzwrite(gz, io_buff, r);
        else
            w = write(dstfd, io_buff, r);

        if (w < 0) {
            rc = -errno;
            DisplayLog(LVL_MAJOR, CP_TAG, "Copy error (%s -> %s): %s",
                       cp_nfo->src, cp_nfo->dst, strerror(-rc));
            goto out_free;
        } else if (w < r) {
            DisplayLog(LVL_MAJOR, CP_TAG, "Short write on %s, aborting copy",
                       cp_nfo->dst);
            rc = -EAGAIN;
            goto out_free;
        }
    } while (r > 0);

    if (r < 0) {    /* error */
        rc = -errno;
        goto out_free;
    }
    /* else (r == 0): EOF */

    /* need to flush the compression buffer before system sync */
    if (compress_src(flags)) {
        if (gzflush(gz, Z_FINISH) != Z_OK) {
            DisplayLog(LVL_MAJOR, CP_TAG, "compression error for %s: %s",
                       cp_nfo->dst, gzerror(gz, &gzerr));
            rc = -EIO;
            goto out_free;
        }
    }

    /* Free the kernel buffer cache as we don't expect to read the files again.
     * This can be done immediately for the read file.
     * For the written file, we need to flush it to disk to ensure
     * that it is correctly archived and to allow freeing the buffer cache. */
    rc = flush_data(srcfd, dstfd, flags);
    if (rc)
        goto out_free;

 out_free:
    MemFree(io_buff);

 out_close:
    if (flags & CP_COMPRESS)
        err_close = (gzclose(gz) != Z_OK);

    if (err_close && rc == 0) {
        rc = errno ? -errno : -EIO;
        DisplayLog(LVL_MAJOR, CP_TAG, "close failed on %s: %s",
                   cp_nfo->src, "error closing compression stream");
    }

    return rc;
}

static int builtin_copy_sendfile(const struct copy_info *cp_nfo,
                                 copy_flags_e flags)
{
    int rc;
    int srcfd = cp_nfo->src_fd;
    int dstfd = cp_nfo->dst_fd;
    size_t fsize = cp_nfo->src_st.st_size;

#if HAVE_FALLOCATE
    rc = fallocate(dstfd, 0, 0, fsize);
    if (rc) {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Failed to fallocate %s: %s",
                   cp_nfo->dst, strerror(-rc));
        goto out;
    }
#endif

    rc = sendfile(dstfd, srcfd, NULL, fsize);
    if (rc) {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Failed to sendfile(%s->%s): %s",
                   cp_nfo->src, cp_nfo->dst, strerror(-rc));
        goto out;
    }

    rc = flush_data(srcfd, dstfd, flags);
    if (rc)
        goto out;

 out:
    return rc;
}

int builtin_copy(const char *src, const char *dst, int dst_oflags,
                 bool save_attrs, copy_flags_e flags)
{
    struct copy_info cp_nfo;
    int rc, err_close = 0;

    cp_nfo.src = src;
    cp_nfo.dst = dst;

    DisplayLog(LVL_DEBUG, "Mod",
               "builtin_copy('%s', '%s', oflg=%#x, save_attrs=%d, flags=%#x)",
               src, dst, dst_oflags, save_attrs, flags);

    cp_nfo.src_fd = open(src, O_RDONLY | O_NOATIME);
    if (cp_nfo.src_fd < 0) {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Can't open %s for read: %s", src,
                   strerror(-rc));
        return rc;
    }

    if (fstat(cp_nfo.src_fd, &cp_nfo.src_st)) {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Failed to stat %s: %s", src,
                   strerror(-rc));
        goto close_src;
    }

    cp_nfo.dst_fd = open(dst, dst_oflags, cp_nfo.src_st.st_mode & 07777);
    if (cp_nfo.dst_fd < 0) {
        rc = -errno;
        DisplayLog(LVL_MAJOR, CP_TAG, "Can't open %s for write: %s",
                   dst, strerror(-rc));
        goto close_src;
    }

    if (flags & CP_COMPRESS)
        rc = builtin_copy_standard(&cp_nfo, flags);
    else if (flags & CP_USE_SENDFILE)
        rc = builtin_copy_sendfile(&cp_nfo, flags);
    else
        rc = builtin_copy_standard(&cp_nfo, flags);

    err_close = close(cp_nfo.dst_fd);
    if (err_close && (rc == 0)) {
        rc = errno ? -errno : -EIO;
        DisplayLog(LVL_MAJOR, CP_TAG, "close failed on %s: %s",
                   dst, strerror(-rc));
    }

 close_src:
    close(cp_nfo.src_fd);

    if (rc == 0 && save_attrs)
        rc = file_clone_attrs(dst, &cp_nfo.src_st);

    return rc;
}

/** collect the first line of output */
static int cb_collect_stdout(void *arg, char *line, size_t size, int stream)
{
    GString *out = (GString *) arg;
    int len;

    if (line == NULL || out == NULL)
        return -EINVAL;

    len = strnlen(line, size);
    /* terminate the string */
    if (len >= size)
        line[len - 1] = '\0';

    /* remove '\n' */
    if ((len > 0) && (line[len - 1] == '\n'))
        line[len - 1] = '\0';

    switch (stream) {
    case STDOUT_FILENO:
        if (out->len == 0)
            g_string_append(out, line);
        break;
    case STDERR_FILENO:
        DisplayLog(LVL_EVENT, "cmd_stderr", "%s", line);
        break;
    }

    return 0;
}

/**
 * Run a shell command to perform an action.
 * @param [in,out] out  Initialized GString to collect command stdout
 *                      (NULL for no output).
 */
static int run_command(const char *name, char **cmd_in,
                       const entry_id_t *p_id,
                       const attr_set_t *p_attrs,
                       const action_params_t *params,
                       struct sm_instance *smi, GString *out)
{
    gchar **cmd;
    char *log_cmd;
    int rc = 0;

    /** @TODO set additional params */
    rc = subst_shell_params(cmd_in, "command", p_id, p_attrs, params, NULL,
                            smi, true, &cmd);
    if (rc)
        return rc;

    /* call custom purge command instead of unlink() */
    if (log_config.debug_level >= LVL_DEBUG) {
        log_cmd = concat_cmd(cmd);
        DisplayLog(LVL_DEBUG, __func__, DFID ": %s action: cmd(%s)",
                   PFID(p_id), name, log_cmd);
        free(log_cmd);
    }

    if (out == NULL)
        /* do not collect output, just redirect command stderr to the log */
        rc = execute_shell_command(cmd, cb_stderr_to_log, (void *)LVL_DEBUG);
    else
        /* collect stdout in out Gstring */
        rc = execute_shell_command(cmd, cb_collect_stdout, (void *)out);

    g_strfreev(cmd);

    return rc;
}

int action_helper(const policy_action_t *action, const char *name,
                  const entry_id_t *p_id, attr_set_t *p_attrs,
                  const action_params_t *params, struct sm_instance *smi,
                  GString *out, post_action_e *after,
                  db_cb_func_t db_cb_fn, void *db_cb_arg)
{
    int rc;

    switch (action->type) {
    case ACTION_COMMAND:
        rc = run_command(name, action->action_u.command, p_id, p_attrs,
                         params, smi, out);
        break;

    case ACTION_FUNCTION:
        DisplayLog(LVL_DEBUG, __func__, DFID ": %s action: %s", PFID(p_id),
                   name, action->action_u.func.name);
        rc = action->action_u.func.call(p_id, p_attrs, params, after,
                                        db_cb_fn, db_cb_arg);
        break;

    case ACTION_NONE:
        DisplayLog(LVL_DEBUG, __func__, "%s(" DFID "): noop", name, PFID(p_id));
        rc = 0;
        break;

    case ACTION_UNSET:
        DisplayLog(LVL_EVENT, __func__, "%s(" DFID "): no action specified",
                   name, PFID(p_id));
        rc = 0;
        break;

    default:
        RBH_BUG("action->type is invalid");
    }
    return rc;
}
