AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libfsscan.la

libfsscan_la_SOURCES= fs_scan.c  fs_scan_main.c task_stack_mngmt.c task_tree_mngmt.c \
		      fs_scan.h  fs_scan_types.h  task_stack_mngmt.h  task_tree_mngmt.h

indent:
	$(top_srcdir)/scripts/indent.sh
