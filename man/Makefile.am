if TMP_FS_MGR
dist_man_MANS=@PURPOSE_BIN@.1 rbh-report.1  rbh-find.1  rbh-du.1
else
dist_man_MANS=@PURPOSE_BIN@.1 rbh-@PURPOSE_EXT@-report.1  rbh-@PURPOSE_EXT@-find.1  rbh-@PURPOSE_EXT@-du.1
endif

# Manually generate man pages from each executable --help
manpages:
	chmod +x ../scripts/make_mans.sh && ../scripts/make_mans.sh
