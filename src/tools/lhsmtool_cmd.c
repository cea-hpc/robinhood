/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this program; If not, see
 * http://www.gnu.org/licenses/gpl-2.0.htm
 *
 * GPL HEADER END
 */
/*
 * (C) Copyright 2016 Commissariat a l'Energie Atomique et aux Energies
 *     Alternatives
 */

/*
 * HSM copytool program for user-defined external commands.
 * Receives orders from coordinator and execute subprocesses accordingly.
 * Pass lustre file descriptor and fid as command arguments.
 *
 * Example configuration file:
 * #
 * # Each command should include the following variables related to the file
 * # to archive/restore:
 * # - {fd} will be the file descriptor number (seekable)
 * # - {fid} will be the Lustre FID
 * #
 * # Note that for restore, the file descriptor is a volatile file and thus
 * # is NOT set to the original file size.
 * #
 * # For a very basic posix copytool:
 * [commands]
 * archive = dd if=/proc/self/{fd} of=/tmp/arch/{fid}
 * restore = dd if=/tmp/arch/{fid} of=/proc/self/{fd}
 *
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <dirent.h>
#include <assert.h>
#include <unistd.h>
#include <getopt.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>
#include <time.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/xattr.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <lustre/lustreapi.h>
#include <lustre/lustre_user.h>

#include <glib.h>


/* Progress reporting period */
#define REPORT_INTERVAL_DEFAULT 30

/* Default configuration file path */
#define CONFIG_FILE_DEFAULT	"/etc/lhsm_cmd.conf"

/* .ini group label under which to define the commands format strings */
#define CFG_GROUP_COMMANDS	"commands"

/* Default max number of commands to execute in parallel */
#define FANOUT_DEFAULT		8

/* GLib spawn flags to execute subprocesses */
#define CMD_EXEC_FLAGS	(G_SPAWN_SEARCH_PATH |		\
			 G_SPAWN_DO_NOT_REAP_CHILD |	\
			 G_SPAWN_LEAVE_DESCRIPTORS_OPEN)

#ifndef LL_HSM_MAX_ARCHIVE
#define LL_HSM_MAX_ARCHIVE (sizeof(uint32_t) * 8)
#endif

/* long long presentation macro used to represent FIDs */
#ifndef LPX64
#define LPX64 "%#llx"
#endif


/** Move HAIs along with a copy of the HAL flags */
struct hai_desc {
	unsigned long		 hd_flags;
	size_t			 hd_datalen;
	char			 hd_data[0];
};

struct options {
	int			 o_daemonize;
	int			 o_dry_run;
	int			 o_abort_on_error;
	int			 o_verbose;
	int			 o_fanout;
	int			 o_report_int;
	int			 o_archive_cnt;
	int			 o_archive_id[LL_HSM_MAX_ARCHIVE];
	unsigned long long	 o_bandwidth;
	size_t			 o_chunk_size;
	char			*o_config;
	char			*o_event_fifo;
	char			*o_mnt;
	int			 o_mnt_fd;
};

/* Everything else is zeroed */
static struct options opt = {
	.o_verbose 	= LLAPI_MSG_INFO,
	.o_fanout	= FANOUT_DEFAULT,
	.o_report_int	= REPORT_INTERVAL_DEFAULT,
	.o_config	= CONFIG_FILE_DEFAULT,
};

/** Commands to execute on incoming HSM action orders */
static char *ct_commands[] = {
	[HSMA_ARCHIVE]	= NULL,
	[HSMA_RESTORE]	= NULL,
	[HSMA_CANCEL]	= NULL,
	[HSMA_REMOVE]	= NULL,
};


static int err_major;

static char cmd_name[PATH_MAX];
static char fs_name[MAX_OBD_NAME + 1];

static struct hsm_copytool_private *ctdata;

static GAsyncQueue	*mqueue;
static bool		 stop;
static GRegex		*fd_regex;
static GRegex		*fid_regex;


static inline double ct_now(void)
{
	struct timeval tv;

	gettimeofday(&tv, NULL);
	return tv.tv_sec + 0.000001 * tv.tv_usec;
}

static inline pid_t gettid(void)
{
	return syscall(SYS_gettid);
}

#define LOG_ERROR(_rc, _format, ...)					\
	llapi_error(LLAPI_MSG_ERROR, _rc,				\
		    "%f %s[%d]: "_format,				\
		    ct_now(), cmd_name, gettid(), ## __VA_ARGS__)

#define LOG_DEBUG(_format, ...)						\
	llapi_error(LLAPI_MSG_DEBUG | LLAPI_MSG_NO_ERRNO, 0,		\
		    "%f %s[%d]: "_format,				\
		    ct_now(), cmd_name, gettid(), ## __VA_ARGS__)

static void usage(const char *name, int rc)
{
	fprintf(stdout,
	" Usage: %s [options] <lustre_mount_point>\n"
	"   --daemon		  Daemon mode, run in background\n"
	"   --abort-on-error	  Abort operation on major error\n"
	"   -A, --archive <#>	  Archive number (repeatable)\n"
	"   --dry-run		  Don't run, just show what would be done\n"
	"   -f, --event-fifo <path>   Write events stream to fifo\n"
	"   -F, --fanout <n>	  Max parallel commands (number of threads)\n"
	"   -q, --quiet		  Produce less verbose output\n"
	"   -u, --update-interval <s> Interval between progress reports sent\n"
	"			     to Coordinator\n"
	"   -v, --verbose	  Produce more verbose output\n", cmd_name);
	exit(rc);
}

static int ct_parseopts(int argc, char * const *argv)
{
	struct option long_opts[] = {
		{"abort-on-error", no_argument,	      &opt.o_abort_on_error, 1},
		{"abort_on_error", no_argument,	      &opt.o_abort_on_error, 1},
		{"archive",	   required_argument, NULL,		   'A'},
		{"daemon",	   no_argument,	      &opt.o_daemonize,	     1},
		{"config",	   required_argument, NULL,		   'c'},
		{"event-fifo",	   required_argument, NULL,		   'f'},
		{"event_fifo",	   required_argument, NULL,		   'f'},
		{"dry-run",	   no_argument,	      &opt.o_dry_run,	     1},
		{"fanout",	   required_argument, NULL,		   'F'},
		{"help",	   no_argument,	      NULL,		   'h'},
		{"quiet",	   no_argument,	      NULL,		   'q'},
		{"update-interval", required_argument,	NULL,		   'u'},
		{"update_interval", required_argument,	NULL,		   'u'},
		{"verbose",	   no_argument,	      NULL,		   'v'},
		{0, 0, 0, 0}
	};
	int			 c;
	int			 rc;

	optind = 0;
	while ((c = getopt_long(argc, argv, "A:c:f:hqu:v",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'A':
			if ((opt.o_archive_cnt >= LL_HSM_MAX_ARCHIVE) ||
			    (atoi(optarg) >= LL_HSM_MAX_ARCHIVE)) {
				rc = -E2BIG;
				LOG_ERROR(rc, "archive number must be less"
					  "than %zu", LL_HSM_MAX_ARCHIVE);
				return rc;
			}
			opt.o_archive_id[opt.o_archive_cnt] = atoi(optarg);
			opt.o_archive_cnt++;
			break;
		case 'c':
			opt.o_config = optarg;
			break;
		case 'f':
			opt.o_event_fifo = optarg;
			break;
		case 'F':
			opt.o_fanout = atoi(optarg);
			if (opt.o_fanout < 1) {
				rc = -EINVAL;
				LOG_ERROR(rc, "bad value for -%c '%s'", c,
					  optarg);
				return rc;
			}
			break;
		case 'h':
			usage(argv[0], 0);
		case 'q':
			opt.o_verbose--;
			break;
		case 'u':
			opt.o_report_int = atoi(optarg);
			if (opt.o_report_int < 0) {
				rc = -EINVAL;
				LOG_ERROR(rc, "bad value for -%c '%s'", c,
					  optarg);
				return rc;
			}
			break;
		case 'v':
			opt.o_verbose++;
			break;
		case 0:
			break;
		default:
			return -EINVAL;
		}
	}

	if (argc != optind + 1) {
		rc = -EINVAL;
		LOG_ERROR(rc, "no mount point specified");
		return rc;
	}

	opt.o_mnt = argv[optind];
	opt.o_mnt_fd = -1;

	return 0;
}

static int ct_path_lustre(char *buf, int sz, const char *mnt,
			  const lustre_fid *fid)
{
	return snprintf(buf, sz, "%s/%s/fid/"DFID_NOBRACE, mnt,
			dot_lustre_name, PFID(fid));
}

static int ct_begin_restore(struct hsm_copyaction_private **phcp,
			    const struct hsm_action_item *hai,
			    int mdt_index, int open_flags)
{
	char	 src[PATH_MAX];
	int	 rc;

	rc = llapi_hsm_action_begin(phcp, ctdata, hai, mdt_index, open_flags,
				    false);
	if (rc < 0) {
		ct_path_lustre(src, sizeof(src), opt.o_mnt, &hai->hai_fid);
		LOG_ERROR(rc, "llapi_hsm_action_begin() on '%s' failed", src);
	}

	return rc;
}

static int ct_begin(struct hsm_copyaction_private **phcp,
		    const struct hsm_action_item *hai)
{
	/* Restore takes specific parameters. Call the same function w/ default
	 * values for all other operations. */
	return ct_begin_restore(phcp, hai, -1, 0);
}

static int ct_fini(struct hsm_copyaction_private **phcp,
		   const struct hsm_action_item *hai, int hp_flags, int ct_rc)
{
	struct hsm_copyaction_private	*hcp;
	char				 lstr[PATH_MAX];
	int				 rc;

	LOG_DEBUG("Action completed, notifying coordinator "
		  "cookie="LPX64", FID="DFID", hp_flags=%d err=%d",
		  hai->hai_cookie, PFID(&hai->hai_fid), hp_flags, -ct_rc);

	ct_path_lustre(lstr, sizeof(lstr), opt.o_mnt, &hai->hai_fid);

	if (phcp == NULL || *phcp == NULL) {
		rc = llapi_hsm_action_begin(&hcp, ctdata, hai, -1, 0, true);
		if (rc < 0) {
			LOG_ERROR(rc, "llapi_hsm_action_begin() on '%s' failed",
				  lstr);
			return rc;
		}
		phcp = &hcp;
	}

	rc = llapi_hsm_action_end(phcp, &hai->hai_extent, hp_flags, abs(ct_rc));
	if (rc == -ECANCELED)
		LOG_ERROR(rc, "completed action on '%s' has been canceled: "
			  "cookie="LPX64", FID="DFID, lstr, hai->hai_cookie,
			 PFID(&hai->hai_fid));
	else if (rc < 0)
		LOG_ERROR(rc, "llapi_hsm_action_end on '%s' failed", lstr);
	else
		LOG_DEBUG("llapi_hsm_action_end on '%s' ok (rc=%d)", lstr, rc);

	return rc;
}

static int ct_build_cmd(const enum hsm_copytool_action hsma, char *cmd,
			size_t cmdlen, const struct hsm_action_item *hai,
			int fd)
{
	const char	*cmd_format = ct_commands[hsma];
	gchar		*res_cmd_fd;
	gchar		*res_cmd_fid;
	gchar		*fdstr;
	char		 fidstr[128];

	if (cmd_format == NULL)
		return -ENOSYS;

	/* replace all {fd} placeholders by fd number */
	fdstr = g_strdup_printf("%i", fd);
	res_cmd_fd = g_regex_replace_literal(fd_regex, cmd_format, -1, 0, fdstr,
					     0, NULL);

	/* replace all {fid} placeholders by lustre fid */
	snprintf(fidstr, sizeof(fidstr), DFID, PFID(&hai->hai_dfid));
	res_cmd_fid = g_regex_replace_literal(fid_regex, res_cmd_fd, -1, 0,
					      fidstr,
					      0, NULL);

	strncpy(cmd, res_cmd_fid, cmdlen - 1);
	cmd[cmdlen - 1] = '\0';

	g_free(res_cmd_fd);
	g_free(res_cmd_fid);
	g_free(fdstr);
	return 0;
}


struct cmd_cb_args {
	struct hsm_copyaction_private	*hcp;
	const struct hsm_action_item	*hai;
	struct hsm_extent		 he;
	int				 fd;
	off_t				 last_pos;
	int				 retcode;
	GMainLoop			*loop;
};

/**
 * Report progress to the coordinator.
 * Sneak into fd, shared with child cmd, to get current position.
 * We only report progress bytes since last report (relative value).
 */
static gboolean cmd_progress_timer_cb(gpointer ud)
{
	struct cmd_cb_args	*args = ud;
	struct hsm_extent	*phe = &args->he;
	off_t			 pos;
	int			 rc;

	pos = lseek(args->fd, 0, SEEK_CUR);
	if (pos < 0) {
		rc = -errno;
		LOG_ERROR(rc, "cmd_progress_timer_cb: lseek failed for "DFID,
			  PFID(&args->hai->hai_fid));
		return FALSE;			/* stop progress report */
	}
	if (pos > args->last_pos) {
		phe->length = pos - phe->offset;
		args->last_pos = pos;
	}

	rc = llapi_hsm_action_progress(args->hcp, phe, phe->length, 0);
	if (rc) {
		LOG_ERROR(rc, "llapi_hsm_action_progress failed for "DFID,
			  PFID(&args->hai->hai_fid));
		return FALSE;			/* stop progress report */
	}
	phe->offset = pos;

	return TRUE;
}

static void cmd_termination_cb(GPid pid, gint status, gpointer ud)
{
	struct cmd_cb_args	*args = ud;

	if (WIFEXITED(status)) {
		if (WEXITSTATUS(status) == 0) {
			args->retcode = 0;
		} else {
			LOG_DEBUG("command failed with exit status %d",
				  WEXITSTATUS(status));
			args->retcode = -ECHILD;
		}
	} else {
		LOG_DEBUG("command failed w/o exit status)");
		args->retcode = -ECHILD;
	}

	g_spawn_close_pid(pid);

	/* Note that sources that have already been dispatched when
	 * g_main_loop_quit() is called will still be executed. */
	g_main_loop_quit(args->loop);
}

/**
 * Register a periodic timer callback to the thread-local context.
 */
static GSource *timer_subscribe(GMainLoop *loop, GSourceFunc func,
				gpointer udata)
{
	GSource	*gsrc;

	gsrc = g_timeout_source_new_seconds(opt.o_report_int);
	g_source_set_callback(gsrc, func, udata, NULL);
	g_source_attach(gsrc, g_main_loop_get_context(loop));
	g_source_unref(gsrc);
	return gsrc;
}

/**
 * Register a subprocess termination callback to the thread-local context.
 */
static GSource *term_subscribe(GMainLoop *loop, GPid pid, GChildWatchFunc func,
			       gpointer udata)
{
	GSource	*gsrc;

	gsrc = g_child_watch_source_new(pid);
	g_source_set_callback(gsrc, (GSourceFunc)func, udata, NULL);
	g_source_attach(gsrc, g_main_loop_get_context(loop));
	g_source_unref(gsrc);
	return gsrc;
}


/**
 * Start a new HSM copytool I/O command: archive or restore.
 */
static int ct_hsm_io_cmd(const enum hsm_copytool_action hsma, GMainLoop *loop,
			 const struct hsm_action_item *hai, const long hal_flags)
{
	struct cmd_cb_args	 *cb_args;
	GError			 *err;
	GPid			  pid;
	GSource			 *timer_gsrc;
	GSource			 *term_gsrc;
	gint			  ac;
	gchar			**av = NULL;
	const char		 *hsma_name = hsm_copytool_action2name(hsma);
	bool			  ok;
	char			  cmd[PATH_MAX] = "(undef)";
	int			  mdt_idx = -1;
	int			  rc;

	cb_args = calloc(1, sizeof(*cb_args));
	if (cb_args == NULL) {
		rc = -ENOMEM;
		LOG_ERROR(rc, "cannot allocate context to archive "DFID,
			  PFID(&hai->hai_fid));
		err_major++;
		goto out;
	}
	cb_args->retcode	= -1;	/* for debugging */
	cb_args->fd		= -1;

	if (hsma == HSMA_ARCHIVE) {

		rc = ct_begin(&cb_args->hcp, hai);
		if (rc < 0) {
			LOG_ERROR(rc, "ct_begin failed for "DFID, PFID(&hai->hai_fid));
			err_major++;
			goto out;
		}

	} else if (hsma == HSMA_RESTORE) {

#if HAVE_LLAPI_GET_MDT_INDEX_BY_FID
		rc = llapi_get_mdt_index_by_fid(opt.o_mnt_fd, &hai->hai_fid, &mdt_idx);
		if (rc < 0) {
			LOG_ERROR(rc, "cannot get MDT index for "DFID,
				  PFID(&hai->hai_fid));
			err_major++;
			goto out;
		}
#endif
		rc = ct_begin_restore(&cb_args->hcp, hai, mdt_idx, 0);
		if (rc < 0) {
			LOG_ERROR(rc, "cannot start restore operation for "DFID,
				  PFID(&hai->hai_fid));
			err_major++;
			goto out;
		}
	}

	cb_args->hai = hai;
	cb_args->fd = llapi_hsm_action_get_fd(cb_args->hcp);

	rc = ct_build_cmd(hsma, cmd, sizeof(cmd), hai, cb_args->fd);
	LOG_DEBUG("Running %s command: '%s'", hsma_name, cmd);
	if (opt.o_dry_run || rc == -ENOSYS) {
		err_major++;
		goto out;
	}

	ok = g_shell_parse_argv(cmd, &ac, &av, &err);
	if (!ok) {
		LOG_ERROR(EINVAL, "Invalid cmd '%s': %s", cmd, err->message);
		g_error_free(err);
		err_major++;
		goto out;
	}

	ok = g_spawn_async(NULL,                /* working directory */
			   av,			/* parsed command line */
			   NULL,		/* environment vars */
			   CMD_EXEC_FLAGS,	/* execution flags */
			   NULL,		/* child setup function */
			   NULL,		/* user data pointer */
			   &pid,		/* child pid address */
			   &err);		/* error marker */

	if (!ok) {
		LOG_ERROR(ECHILD, "Cannot spawn subprocess: %s", err->message);
		g_error_free(err);
		err_major++;
		goto out;
	}

	/* register a periodic timer callback for progress report */
	timer_gsrc = timer_subscribe(loop, cmd_progress_timer_cb, cb_args);

	/* register a subprocess termination callback */
	term_gsrc = term_subscribe(loop, pid, cmd_termination_cb, cb_args);

	cb_args->he.offset = hai->hai_extent.offset;
	cb_args->loop      = loop;

	g_main_loop_run(loop);

	/* This loop will run again, we need to explicitly destroy sources */
	g_source_destroy(term_gsrc);
	g_source_destroy(timer_gsrc);

out:
	g_strfreev(av);

	/* Obscure voodoo forces are summoned in this function in the
	 * restore case. Do not close the volatile before! */
	rc = ct_fini(&cb_args->hcp, hai, 0,
		     cb_args ? cb_args->retcode : -ENOMEM);

	close(cb_args->fd);

	free(cb_args);

	return rc;
}

static void handler(int signal)
{
	stop = true;
	psignal(signal, "exiting");
	/* If we don't clean up upon interrupt, umount thinks there's a ref
	 * and doesn't remove us from mtab (EINPROGRESS). The lustre client
	 * does successfully unmount and the mount is actually gone, but the
	 * mtab entry remains. So this just makes mtab happier. */
	llapi_hsm_copytool_unregister(&ctdata);

	/* Also remove fifo upon signal as during normal/error exit */
	if (opt.o_event_fifo != NULL)
		llapi_hsm_unregister_event_fifo(opt.o_event_fifo);
	_exit(1);
}

/* Daemon waits for messages from the kernel; run it in the background. */
static int ct_run(void)
{
	struct sigaction	act;
	int			rc;

	if (opt.o_daemonize) {
		rc = daemon(1, 1);
		if (rc < 0) {
			rc = -errno;
			LOG_ERROR(rc, "cannot daemonize");
			return rc;
		}
	}

	if (opt.o_event_fifo != NULL) {
		rc = llapi_hsm_register_event_fifo(opt.o_event_fifo);
		if (rc < 0) {
			LOG_ERROR(rc, "failed to register event fifo");
			return rc;
		}
		llapi_error_callback_set(llapi_hsm_log_error);
	}

	rc = llapi_hsm_copytool_register(&ctdata, opt.o_mnt,
					 opt.o_archive_cnt,
					 opt.o_archive_id, 0);
	if (rc < 0) {
		LOG_ERROR(rc, "cannot start copytool interface");
		return rc;
	}

	memset (&act, 0, sizeof(act));
	act.sa_handler = &handler;
	if (sigaction(SIGINT, &act, NULL) < 0
	    || sigaction(SIGTERM, &act, NULL) < 0) {
		rc = -errno;
		LOG_ERROR(rc, "cannot set signal handler for SIGINT/SIGTERM");
		return rc;
	}
	act.sa_handler = SIG_IGN;
	if (sigaction(SIGPIPE, &act, NULL) < 0) {
		rc = -errno;
		LOG_ERROR(rc, "cannot ignore signal SIGPIPE");
		return rc;
	}

	while (1) {
		struct hsm_action_list	*hal;
		struct hsm_action_item	*hai;
		int			 msgsize;
		int			 i = 0;

		LOG_DEBUG("waiting for message from kernel");

		rc = llapi_hsm_copytool_recv(ctdata, &hal, &msgsize);
		if (rc == -ESHUTDOWN) {
			LOG_DEBUG("shutting down");
			break;
		} else if (rc < 0) {
			fprintf(stderr, "cannot receive action list: %s\n",
				strerror(-rc));
			err_major++;
			if (opt.o_abort_on_error)
				break;
			else
				continue;
		}

		LOG_DEBUG("copytool fs=%s archive#=%d item_count=%d",
			  hal->hal_fsname, hal->hal_archive_id, hal->hal_count);

		if (strcmp(hal->hal_fsname, fs_name) != 0) {
			rc = -EINVAL;
			LOG_ERROR(rc, "'%s' invalid fs name, expecting: %s",
				 hal->hal_fsname, fs_name);
			err_major++;
			if (opt.o_abort_on_error)
				break;
			else
				continue;
		}

		hai = hai_first(hal);
		while (++i <= hal->hal_count) {
			struct hai_desc *hd;

			if ((char *)hai - (char *)hal > msgsize) {
				rc = -EPROTO;
				LOG_ERROR(rc,
					  "'%s' item %d past end of message!",
					  opt.o_mnt, i);
				err_major++;
				break;
			}

			hd = malloc(sizeof(*hd) + hai->hai_len);
			if (hd == NULL) {
				rc = -ENOMEM;
				LOG_ERROR(rc, "'%s' item cannot be processed",
					  opt.o_mnt);
				err_major++;
				break;
			}
			hd->hd_flags   = hal->hal_flags;
			hd->hd_datalen = hai->hai_len;
			memcpy(hd->hd_data, hai, hai->hai_len);

			g_async_queue_push(mqueue, hd);
			hai = hai_next(hai);
		}

		if (opt.o_abort_on_error && err_major) {
			LOG_DEBUG("copytool aborting on error");
			break;
		}
	}

	stop = true;
	llapi_hsm_copytool_unregister(&ctdata);
	if (opt.o_event_fifo != NULL)
		llapi_hsm_unregister_event_fifo(opt.o_event_fifo);

	return rc;
}

static gpointer subproc_mgr_main(gpointer data)
{
	GMainContext	*mctx;
	GMainLoop	*loop;

	mctx = g_main_context_new();
	g_main_context_push_thread_default(mctx);

	loop = g_main_loop_new(mctx, false);

	g_async_queue_ref(mqueue);

	while (!stop) {
		struct hsm_action_item	*hai;
		struct hai_desc		*hd;

		hd = g_async_queue_pop(mqueue);
		if (hd->hd_datalen < sizeof(*hai)) {
			LOG_ERROR(EPROTO, "Invalid record (ignoring)");
			continue;
		}

		hai = (struct hsm_action_item *)hd->hd_data;
		switch (hai->hai_action) {
		case HSMA_ARCHIVE:
		case HSMA_RESTORE:
			ct_hsm_io_cmd(hai->hai_action, loop, hai, hd->hd_flags);
			break;
		case HSMA_REMOVE:
		case HSMA_CANCEL:
			LOG_ERROR(ENOTSUP, "Operation not implemented");
			break;
		}
		fflush(stderr);
		free(hd);
	}

	g_async_queue_unref(mqueue);
	g_main_loop_unref(loop);
	g_main_context_unref(mctx);
	return NULL;
}

/**
 * Setup the producer thread, ie the one that reads records from coordinator
 * and publish them to the working threads.
 *
 * It keeps an fd open on the lustre filesystem root to prevent it from being
 * unmounter accidentally.
 */
static int ct_producer_setup(void)
{
	int	rc;

	rc = llapi_search_fsname(opt.o_mnt, fs_name);
	if (rc < 0) {
		LOG_ERROR(rc, "cannot find a Lustre filesystem mounted at '%s'",
			 opt.o_mnt);
		return rc;
	}

	opt.o_mnt_fd = open(opt.o_mnt, O_RDONLY);
	if (opt.o_mnt_fd < 0) {
		rc = -errno;
		LOG_ERROR(rc, "cannot open mount point at '%s'",
			  opt.o_mnt);
		return rc;
	}

	return 0;
}

/**
 * Load configuration file.
 * This is a GLib KeyFile, similar to .ini files.
 * The command are grouped under a same group (CFG_GROUP_COMMANDS) and expressed
 * as format strings associated to the HSM operation they correspond to.
 *
 * See:
 * https://developer.gnome.org/glib/unstable/glib-Key-value-file-parser.html
 */
static int ct_load_cfg_file(void)
{
	GKeyFile	*keys = g_key_file_new();
	GError		*err  = NULL;
	int		 rc = 0;

	if (!g_key_file_load_from_file(keys, opt.o_config,
				       G_KEY_FILE_NONE, &err)) {
		rc = -EINVAL;
		LOG_ERROR(rc, "cannot load configuration at '%s': %s",
			  opt.o_config, err->message);
		g_error_free(err);
		goto out;
	}

	/* Although the functions below will ensure that the group exists, this
	 * allows us to exit early with a clear error message and ignore errors
	 * due to missing keys (no command is mandatory). */
	if (!g_key_file_has_group(keys, CFG_GROUP_COMMANDS)) {
		rc = -EINVAL;
		LOG_ERROR(rc, "Missing group '%s'", CFG_GROUP_COMMANDS);
		goto out;
	}

	/* commands are to be freed using g_free() and can be NULL */
	ct_commands[HSMA_ARCHIVE] = g_key_file_get_string(keys,
							  CFG_GROUP_COMMANDS,
							  "archive", NULL);
	ct_commands[HSMA_RESTORE] = g_key_file_get_string(keys,
							  CFG_GROUP_COMMANDS,
							  "restore", NULL);
	ct_commands[HSMA_REMOVE] = g_key_file_get_string(keys,
							 CFG_GROUP_COMMANDS,
							 "remove", NULL);
	ct_commands[HSMA_CANCEL] = g_key_file_get_string(keys,
							 CFG_GROUP_COMMANDS,
							 "cancel", NULL);
out:
	g_key_file_free(keys);
	return rc;
}

/**
 * Fill in structures and spawn working threads.
 */
static int ct_setup(void)
{
	int i;
	int rc;

	/* Initialize regular expression patterns for argument substitution */
	fd_regex  = g_regex_new("{fd}", G_REGEX_OPTIMIZE, 0, NULL);
	fid_regex = g_regex_new("{fid}", G_REGEX_OPTIMIZE, 0, NULL);

	g_thread_init(NULL);

	llapi_msg_set_level(opt.o_verbose);
	rc = ct_load_cfg_file();
	if (rc)
		return rc;

	rc = ct_producer_setup();
	if (rc)
		return rc;

	/* Start working threads and communication channel */
	mqueue = g_async_queue_new();

	for (i = 0; i < opt.o_fanout; i++)
		g_thread_create(subproc_mgr_main, NULL, false, NULL);

	return 0;
}

/**
 * Clear everything before exit() for the sake of sane valgrind sessions.
 */
static int ct_cleanup(void)
{
	if (mqueue != NULL)
		g_async_queue_unref(mqueue);

	g_regex_unref(fd_regex);
	g_regex_unref(fid_regex);

	g_free(ct_commands[HSMA_ARCHIVE]);
	g_free(ct_commands[HSMA_RESTORE]);
	g_free(ct_commands[HSMA_REMOVE]);
	g_free(ct_commands[HSMA_CANCEL]);
	return 0;
}

int main(int argc, char **argv)
{
	int	rc;

	strncpy(cmd_name, basename(argv[0]), sizeof(cmd_name) - 1);
	rc = ct_parseopts(argc, argv);
	if (rc < 0) {
		fprintf(stderr, "try '%s --help' for more information\n",
			cmd_name);
		return -rc;
	}

	rc = ct_setup();
	if (rc < 0)
		goto error_cleanup;

	rc = ct_run();

error_cleanup:
	ct_cleanup();

	return -rc;
}
