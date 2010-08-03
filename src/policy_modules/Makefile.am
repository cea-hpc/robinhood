SUBDIRS=purge

if TMP_FS_MGR
SUBDIRS += rmdir
endif
if LUSTRE_HSM
SUBDIRS += migration hsm_remove
endif
if SHERPA
SUBDIRS += migration rmdir
endif

indent:
	for d in $(SUBDIRS); do 	\
		make -C $$d indent ; 	\
	done
