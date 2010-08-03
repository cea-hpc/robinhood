AM_YFLAGS		    = --defines
AM_LFLAGS		    = -i
AM_CFLAGS= -Wall $(DB_CFLAGS) $(PURPOSE_CFLAGS) -DYY_NO_UNPUT
 

# yacc relative depedencies
conf_lex.l: conf_yacc.h
conf_yacc.h: conf_yacc.c

# cleaning FLEX/BISON generated files
#CLEANFILES = conf_yacc.h conf_yacc.c conf_lex.c

noinst_LTLIBRARIES          = libConfigParsing.la
libConfigParsing_la_SOURCES = conf_yacc.y conf_yacc.h conf_lex.l analyze.c config_parsing.c analyze.h \
			      RobinhoodConfig.c conf_yacc.c conf_lex.c

check_PROGRAMS		    = verif_syntax test_parsing

verif_syntax_SOURCES	    = verif_syntax.c 
verif_syntax_LDADD         = libConfigParsing.la

test_parsing_SOURCES	    = test_parse.c
test_parsing_LDADD         =  libConfigParsing.la

indent:
	$(top_srcdir)/scripts/indent.sh
