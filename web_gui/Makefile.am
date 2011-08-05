EXTRA_DIST=acct README.install

distname=robinhood-webgui-$(VERSION)

dist: dist-webgui

dist-webgui: dist-dir
	tar zcvf $(distname).tar.gz $(distname)
	rm -rf $(distname)

dist-dir:
	rm -rf $(distname)
	cp -r acct $(distname)
