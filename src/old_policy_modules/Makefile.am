if TMP_FS_MGR
SUBDIRS = purge rmdir
endif
if LUSTRE_HSM
SUBDIRS = purge migration hsm_remove
endif
if HSM_LITE
if SHOOK
SUBDIRS = purge migration hsm_remove
else
SUBDIRS = migration hsm_remove
endif
endif

indent:
	for d in $(SUBDIRS); do 	\
		make -C $$d indent ; 	\
	done
