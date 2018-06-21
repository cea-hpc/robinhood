
SUFFIXES = .def

TYPEGEN=$(top_srcdir)/scripts/type_gen.pl

.def.h:
	$(TYPEGEN) $< $*.h

noinst_HEADERS=db_types.h  list_mgr.h RW_Lock.h \
		uidgidcache.h Memory.h rbh_const.h rbh_logs.h \
		rbh_cfg.h rbh_misc.h config_parsing.h \
		global_config.h entry_processor.h status_manager.h\
		fs_scan_main.h chglog_reader.h policy_run.h\
		xplatform_print.h lustre_extended_types.h \
		policy_rules.h queue.h  \
		entry_proc_hash.h list.h \
        lustre/lustre_errno.h update_params.h \
        db_schema.h db_schema.def pipeline_types.h \
        rbh_params.h rbh_types.h rbh_boolexpr.h rbh_cfg_helpers.h \
        rbh_modules.h rbh_basename.h chglog_postproc.h

db_schema.h: db_schema.def $(TYPEGEN)
all: db_schema.h

CLEANFILES = db_schema.h

indent:
	$(top_srcdir)/scripts/indent.sh
