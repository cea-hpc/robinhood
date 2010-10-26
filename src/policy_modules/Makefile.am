if TMP_FS_MGR
SUBDIRS = purge rmdir
endif
if LUSTRE_HSM
SUBDIRS = purge migration hsm_remove
endif
if SHERPA
SUBDIRS = purge migration rmdir
endif
if BACKUP_FS
SUBDIRS = migration hsm_remove
endif

indent:
	for d in $(SUBDIRS); do 	\
		make -C $$d indent ; 	\
	done
