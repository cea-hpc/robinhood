SUBDIRS=include logs common list_mgr entry_processor fs_scan cfg_parsing policy_matching policy_modules tools

if CHANGELOGS
SUBDIRS+=chglog_reader
endif

if HSM_LITE
SUBDIRS+=backend_ext
endif

SUBDIRS += robinhood tests

indent:
	for d in $(SUBDIRS); do 	\
		make -C $$d indent ; 	\
	done
