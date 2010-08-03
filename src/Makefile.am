SUBDIRS=include Logs Common ListMgr EntryProcessor InfoCollectors ConfigParsing Policies ResourceMonitor

if TMP_FS_MGR
SUBDIRS += Rmdir
endif
if LUSTRE_HSM
SUBDIRS += Migration Remove
endif
if SHERPA
SUBDIRS += Migration Rmdir
endif

SUBDIRS += Robinhood tests

indent:
	for d in $(SUBDIRS); do 	\
		make -C $$d indent ; 	\
	done
