%{

/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2004-2009 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


#include "config.h"
#include "analyze.h"

#include <stdio.h>

#if HAVE_STRING_H
#   include <string.h>
#endif

    int yylex(void);
    void yyerror(char*);
    extern int yylineno;
    extern char * yytext;

    list_items * program_result=NULL;
    
	/* stock le message d'erreur donne par le lexer */
    char local_errormsg[1024]="";
	
    /* stock le message d'erreur complet */
    char extern_errormsg[1024]="";
    
#ifdef _DEBUG_PARSING
#define DEBUG_YACC   rh_config_print_list
#else
/* do nothing */
static void DEBUG_YACC( FILE * output, list_items * list ) { return ;}
#endif

    
%}

%error-verbose

%union {
    char         str_val[MAXSTRLEN];
    list_items              *  list;
    generic_item            *  item;
    arg_list_t	            *  arg_list;
};

%token _ERROR_
%token BEGIN_BLOCK
%token END_BLOCK
%token END_AFFECT
%token BEGIN_SUB_BLOCK
%token END_SUB_BLOCK
%token BEGIN_PARENTHESIS
%token END_PARENTHESIS
%token VALUE_SEPARATOR
%token AFFECT
%token EQUAL
%token DIFF
%token GT
%token GT_EQ
%token LT
%token LT_EQ
%token AND
%token OR
%token NOT
%token UNION
%token INTER
%token <str_val> IDENTIFIER
%token <str_val> NON_IDENTIFIER_VALUE 
%token <str_val> ENV_VAR

%type <str_val> value
%type <list> listblock
%type <list> listitems
%type <item> block
%type <item> definition
%type <item> expression 
%type <item> subblock
%type <item> key_value 
%type <item> extended_key_value 
%type <item> affect
%type <item> extended_affect
%type <item> set
%type <arg_list> arglist


%%

program: listblock {DEBUG_YACC(stderr,$1);program_result=$1;}
    ;

listblock:
    block listblock {rh_config_AddItem($2,$1);$$=$2;}
    | {$$=rh_config_CreateItemsList();}
    ;

block:
    IDENTIFIER BEGIN_BLOCK listitems END_BLOCK {$$=rh_config_CreateBlock($1,NULL,$3);}
    ;

listitems:
    definition listitems   {rh_config_AddItem($2,$1);$$=$2;}
    |                      {$$=rh_config_CreateItemsList();}
    ;

definition:
    extended_affect
    | subblock
    ;

value:
	IDENTIFIER	{strcpy($$,$1);}
	| NON_IDENTIFIER_VALUE {strcpy($$,$1);}
    | ENV_VAR {rh_config_resov_var($$,$1);}
	;

affect:
    IDENTIFIER AFFECT value {$$=rh_config_CreateAffect($1, $3);}

extended_affect:
    affect BEGIN_PARENTHESIS arglist END_PARENTHESIS END_AFFECT {rh_config_SetArglist( $1, $3 ); $$=$1;}
    | affect END_AFFECT {$$=$1;}
    ;

key_value:
    IDENTIFIER EQUAL value {$$=rh_config_CreateKeyValueExpr($1,OP_EQUAL, $3);}
    | IDENTIFIER DIFF value {$$=rh_config_CreateKeyValueExpr($1,OP_DIFF, $3);}
    | IDENTIFIER GT value {$$=rh_config_CreateKeyValueExpr($1,OP_GT, $3);}
    | IDENTIFIER GT_EQ value {$$=rh_config_CreateKeyValueExpr($1,OP_GT_EQ, $3);}
    | IDENTIFIER LT value {$$=rh_config_CreateKeyValueExpr($1,OP_LT, $3);}
    | IDENTIFIER LT_EQ value {$$=rh_config_CreateKeyValueExpr($1,OP_LT_EQ, $3);}
    ;

arglist:
	arglist VALUE_SEPARATOR value {rh_config_AddArg( $1, $3 ); $$=$1;}
	| value {$$=rh_config_CreateArgList(); rh_config_AddArg($$,$1);}
	;

extended_key_value:
    key_value BEGIN_PARENTHESIS arglist END_PARENTHESIS {rh_config_SetArglist( $1, $3 ); $$=$1;}
    | key_value {$$=$1;}
    | /*function definition*/ IDENTIFIER BEGIN_PARENTHESIS value END_PARENTHESIS { $$=rh_config_CreateKeyValueExpr($1,OP_CMD, $3);  }
    ;

expression:
    extended_key_value {$$=$1;}
    | NOT extended_key_value { $$=rh_config_CreateBoolExpr_Unary( BOOL_OP_NOT, $2 ); }
    | NOT BEGIN_PARENTHESIS expression END_PARENTHESIS { $$=rh_config_CreateBoolExpr_Unary( BOOL_OP_NOT, $3 ); }
    | BEGIN_PARENTHESIS expression END_PARENTHESIS { $$=$2; }
    | expression AND expression { $$=rh_config_CreateBoolExpr_Binary( BOOL_OP_AND, $1, $3 ); }
    | expression OR expression { $$=rh_config_CreateBoolExpr_Binary( BOOL_OP_OR, $1, $3 ); }
    ;

set:
    BEGIN_PARENTHESIS set END_PARENTHESIS { $$=$2; }
    | NOT set       { $$=rh_config_CreateSet_Unary( SET_OP_NOT, $2 ); }
    | set UNION set { $$=rh_config_CreateSet_Binary( SET_OP_UNION, $1, $3 ); }
    | set INTER set { $$=rh_config_CreateSet_Binary( SET_OP_INTER, $1, $3 ); }
    | IDENTIFIER    { $$=rh_config_CreateSet_Singleton( $1 ); }
    ;

subblock:
    IDENTIFIER IDENTIFIER BEGIN_SUB_BLOCK listitems END_SUB_BLOCK {$$=rh_config_CreateBlock($1,$2,$4);}
    | IDENTIFIER IDENTIFIER BEGIN_SUB_BLOCK expression END_SUB_BLOCK {$$=rh_config_CreateBoolExpr($1,$2,$4);}
    | IDENTIFIER BEGIN_SUB_BLOCK listitems END_SUB_BLOCK {$$=rh_config_CreateBlock($1,NULL,$3);}
    | IDENTIFIER BEGIN_SUB_BLOCK expression END_SUB_BLOCK {$$=rh_config_CreateBoolExpr($1,NULL,$3);}
    | IDENTIFIER BEGIN_SUB_BLOCK set END_SUB_BLOCK {$$=rh_config_CreateSet($1,NULL,$3);}
    ;


%%

    void yyerror(char *s){
    		if ( local_errormsg[0] && s[0] )
			snprintf(extern_errormsg,1024,"%s (%s) at '%s' line %d in '%s'",local_errormsg,s, (yytext?yytext:"???"), yylineno, current_file);
		else if (local_errormsg[0])
			snprintf(extern_errormsg,1024,"%s at '%s' line %d in '%s'",local_errormsg, (yytext?yytext:"???"), yylineno, current_file);
		else if (s[0])
			snprintf(extern_errormsg,1024,"%s at '%s' line %d in '%s'",s, (yytext?yytext:"???"), yylineno, current_file);
		else
			snprintf(extern_errormsg,1024,"Syntax error at '%s' line %d in '%s'",(yytext?yytext:"???"), yylineno, current_file);
    }
    

    void set_error(char * s){
        rh_strncpy(local_errormsg, s, 1024);
    }
