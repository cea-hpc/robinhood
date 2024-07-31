SUBDIRS=include common list_mgr entry_processor fs_scan cfg_parsing modules policies

if CHANGELOGS
SUBDIRS+=chglog_reader
endif

# tests cannot build success on 5.10.0-202.0.0.115.oe2203sp3.aarch64, so disable it
# SUBDIRS += robinhood tools tests
SUBDIRS += robinhood tools

indent:
	for d in $(SUBDIRS); do 	\
		make -C $$d indent ; 	\
	done
