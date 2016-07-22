EXTRA_DIST=gui_v3 gui_v3/README.txt robinhood.conf

distname=robinhood-webgui-$(VERSION)

dist: dist-webgui

dist-webgui: dist-dir
	tar zcvf $(distname).tar.gz $(distname)
	rm -rf $(distname)

dist-dir:
	rm -rf $(distname)
	cp -r gui_v3 $(distname)
