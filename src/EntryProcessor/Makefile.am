AM_CFLAGS= -Wall $(DB_CFLAGS) $(PURPOSE_CFLAGS)

COMMON_SRC=entry_proc_impl.c entry_proc_tools.c entry_proc_tools.h


noinst_LTLIBRARIES=libentryproc.la

if TMP_FS_MGR
PIPELINE_SRC=tmp_fs_mgr_pipeline.c
endif

if LUSTRE_HSM
PIPELINE_SRC=lustre_hsm_pipeline.c
endif

if SHERPA
PIPELINE_SRC=sherpa_pipeline.c
endif

libentryproc_la_SOURCES=entry_proc_impl.c entry_proc_tools.c entry_proc_tools.h $(PIPELINE_SRC)

indent:
	$(top_srcdir)/scripts/indent.sh
