
SUFFIXES = .def

TYPEGEN=$(top_srcdir)/scripts/type_gen.pl

.def.h:
	$(TYPEGEN) $< $*.h

noinst_HEADERS=db_types.h  list_mgr.h RW_Lock.h \
		uidgidcache.h Memory.h rbh_const.h RobinhoodLogs.h \
		RobinhoodConfig.h RobinhoodMisc.h config_parsing.h \
		global_config.h entry_processor.h      \
		fs_scan_main.h chglog_reader.h resource_monitor.h \
		xplatform_print.h lustre_extended_types.h \
		policies.h rmdir.h queue.h StringAlloc.h hsm_rm.h migration.h \
		entry_proc_hash.h list.h var_str.h \
        lustre/lustre_errno.h

if LUSTRE_HSM
noinst_HEADERS+=lustre_hsm_types.h lustre_hsm_types.def pipeline_types.h
lustre_hsm_types.h: lustre_hsm_types.def $(TYPEGEN)
all: lustre_hsm_types.h
endif
if TMP_FS_MGR
noinst_HEADERS+=tmp_fs_mgr_types.def pipeline_types.h
tmp_fs_mgr_types.h: tmp_fs_mgr_types.def $(TYPEGEN)
all: tmp_fs_mgr_types.h
endif
if HSM_LITE
noinst_HEADERS+=hsmlite_types.h hsmlite_types.def backend_ext.h backend_mgr.h  pipeline_types.h
hsmlite_types.h: hsmlite_types.def $(TYPEGEN)
all: hsmlite_types.h
endif

CLEANFILES = lustre_hsm_types.h tmp_fs_mgr_types.h hsmlite_types.h

indent:
	$(top_srcdir)/scripts/indent.sh
