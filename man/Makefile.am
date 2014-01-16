if TMP_FS_MGR
dist_man_MANS=@PURPOSE_BIN@.1 rbh-report.1  rbh-find.1  rbh-du.1

@PURPOSE_BIN@.1: mans
rbh-report.1: mans
rbh-find.1: mans
rbh-du.1: mans

else
dist_man_MANS=@PURPOSE_BIN@.1 rbh-@PURPOSE_EXT@-report.1  rbh-@PURPOSE_EXT@-find.1  rbh-@PURPOSE_EXT@-du.1

@PURPOSE_BIN@.1: mans
rbh-@PURPOSE_EXT@-report.1: mans
rbh-@PURPOSE_EXT@-find.1: mans
rbh-@PURPOSE_EXT@-du.1: mans

endif

mans:
	chmod +x ../scripts/make_mans.sh && ../scripts/make_mans.sh
