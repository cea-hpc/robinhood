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
#include "conf_yacc.h"

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <libgen.h>

#if HAVE_STRING_H
#   include <string.h>
#endif

/* current line number */
int yylineno;

/* levels of brackets nbr */
int accolades;

/* level of parenthesis nbr */
int parenthesis;

/* Traitement des messages d'erreur */
void set_error(char * s);

#define ERRLEN 1024
char err_str[ERRLEN]="";

/* Stockage des chaines 
*/
char YY_PARSED_STRING[MAXSTRLEN];

void YY_BUFFER_APPEND(char * s){
    strncat(YY_PARSED_STRING,s, MAXSTRLEN-1);
}

void YY_BUFFER_RESET(void){
    int i;
    for (i=0;i<MAXSTRLEN;i++){
        YY_PARSED_STRING[i]='\0';
    }
}

/* includes management */
#define FILE_LEN 1024
char current_file[FILE_LEN] = "";

#define MAX_INCLUDE_DEPTH  10
YY_BUFFER_STATE include_stack[MAX_INCLUDE_DEPTH];

/* keep track of filenames and line numbers */
unsigned int    lines_stack[MAX_INCLUDE_DEPTH];
char            files_stack[MAX_INCLUDE_DEPTH][FILE_LEN];

int include_stack_index = 0;


/* initialisation du parser */

#define YY_USER_INIT {          \
    unsigned int i;             \
    yylineno = 1;               \
    accolades = 0;              \
    parenthesis = 0;            \
    include_stack_index = 0;    \
    for ( i=0; i<MAX_INCLUDE_DEPTH; i++) {\
        lines_stack[i]=0;       \
        files_stack[i][0]='\0'; \
    }\
    BEGIN YY_INIT;\
}

#ifdef _DEBUG_PARSING
#define DEBUG_LEX   printf
#else
/* do nothing */
static void DEBUG_LEX( char * format, ... ) { return ; }
#endif



%}


SPACE        [ \t\r\f]
NL           [\n]
VAL_CHAR     [^ \t\r\n\f"'#(),;=<>!{}]
COMMENT	 #.*$
COMMENT2 \/\/.*$
/* lettre posant probleme dans une chaine */
STRING_CHAR       [^\n]
/* comment est compose un identifiant */
LETTER          [a-zA-Z_.]
IDENTIFIER_CHAR    [a-zA-Z0-9_.\-]

/* INCLUDE state is used for picking the name of the include file */
%START  YY_INIT BLOC INBLOC STRING1 STRING2 ESC1 INCLUDE INCL_STRING INCL_ESC

%%

<YY_INIT>"%include" {/* debut de lecture d'un fichier d'include */
                        DEBUG_LEX("INCLUDE\n");
                        BEGIN INCLUDE;
                        /* not a token, return nothing */
                     }

<INCLUDE>"\""       { /* start include file name */
                      BEGIN INCL_STRING;
                      DEBUG_LEX("file:<");
                      YY_BUFFER_RESET();
                    }

<INCL_STRING>\\     {BEGIN INCL_ESC;}

<INCL_STRING>"\""   { /* include file read */
                        unsigned int i;
                        char new_file_path[FILE_LEN];
                        DEBUG_LEX(">");
                        
                        if ( include_stack_index >= MAX_INCLUDE_DEPTH )
                        {
                           /* error */
                           snprintf(err_str,ERRLEN,"in \"%s\", line %d: includes nested too deeply",current_file, yylineno);
                           set_error(err_str);
                           return _ERROR_;                        
                        }
                        
                        include_stack[include_stack_index] = YY_CURRENT_BUFFER;
                        lines_stack[include_stack_index] = yylineno;                        
                        rh_strncpy(files_stack[include_stack_index], current_file, FILE_LEN);
                        
                        /* relative path management */
                        
                        /* 1) if the new path is absolute, nothing to do
                         * 2) if there was no '/' in previous dir, the new path
                         *  is relative to the current dir.
                         */
                        if ( ( YY_PARSED_STRING[0] == '/' )
                            || ( strchr( current_file, '/') == NULL ) )
                        {
                            rh_strncpy(new_file_path, YY_PARSED_STRING, FILE_LEN);
                        }
                        else
                        {
                            /* in any other case, path is relative to the current config file
                             * directory */
                            
                            char tmp_buf[FILE_LEN];
                            char * path;
                            
                            rh_strncpy(tmp_buf, current_file, FILE_LEN);
                            
                            path = dirname( tmp_buf );

                            snprintf( new_file_path, FILE_LEN, "%s/%s", path, YY_PARSED_STRING );
                        }                        
                                               
                        /* loop detection */
                        
                        for ( i = 0; i <= include_stack_index; i++ )
                        {
                            if (!strncmp(files_stack[i], new_file_path, FILE_LEN))
                            {
                               snprintf(err_str,ERRLEN,"in \"%s\", line %d: include loop detected: \"%s\" already parsed",current_file, yylineno, new_file_path );
                               set_error(err_str);
                               return _ERROR_;                                
                            }
                        }
                        
                        include_stack_index ++;
                        
                        yyin = fopen( new_file_path, "r" );
                        
                        if ( yyin == NULL )
                        {
                           /* error */
                           snprintf(err_str,ERRLEN,"in \"%s\", line %d: error %d opening file \"%s\": %s",current_file,yylineno,
                                    errno, new_file_path, strerror(errno) );
                           set_error(err_str);
                           return _ERROR_;                    
                        }
                        
                        yylineno = 1;
                        rh_strncpy(current_file, new_file_path, FILE_LEN);
                        
                        /* change current buffer */
                        yy_switch_to_buffer( yy_create_buffer( yyin, YY_BUF_SIZE ) );
                                                
                        /* start reading file from scratch */
                        BEGIN YY_INIT;
                        
                    }
                    
                    
<INCL_STRING>\n      {
                            snprintf(err_str,ERRLEN,"in \"%s\", line %d: missing closing quote.",current_file, yylineno);
                            set_error(err_str);
                            yylineno++;
                            return _ERROR_;
                     }
                     
<INCL_STRING>.      {YY_BUFFER_APPEND(yytext); DEBUG_LEX("%c",*yytext);/* caractere du fichier */}

<INCL_ESC>\n        {BEGIN INCL_STRING; yylineno++;}/* ignore un saut de ligne echappe*/
<INCL_ESC>.         {DEBUG_LEX("%c",*yytext);YY_BUFFER_APPEND(yytext);BEGIN INCL_STRING;/* caractere du fichier */}


<<EOF>> { /* end of included file */
            DEBUG_LEX("<EOF>\n");
            
            include_stack_index --;
            
            if ( include_stack_index < 0 )
            {
                /* eof of all streams */                
                yyterminate();
            }
            else
            {
                fclose(yyin);
                /*go down into stack */
                yy_delete_buffer( YY_CURRENT_BUFFER );
                
                yylineno = lines_stack[include_stack_index];
                rh_strncpy(current_file, files_stack[include_stack_index], FILE_LEN);
                
                yy_switch_to_buffer( include_stack[include_stack_index] );
            }
        }



<YY_INIT>{LETTER}({IDENTIFIER_CHAR})* {
                    /* identifier */
                    DEBUG_LEX("[bloc:%s]\n",yytext);
                    rh_strncpy(yylval.str_val,yytext,MAXSTRLEN);
                    BEGIN BLOC;
                    return IDENTIFIER;
                 }


<BLOC>"{"        {/* debut de bloc */
                        DEBUG_LEX("BEGIN_BLOCK\n");
                        BEGIN INBLOC;
                        accolades++;
                        return BEGIN_BLOCK;
                 }

<INBLOC>"("	 {
			DEBUG_LEX("(");
                        parenthesis++;
			return BEGIN_PARENTHESIS;
		 }

<INBLOC>","     {DEBUG_LEX(",  "); return VALUE_SEPARATOR;}
<INBLOC>")"     {BEGIN INBLOC;  DEBUG_LEX(")\n");
                if ( parenthesis <= 0 )
                    {
                       /* error */
                       snprintf(err_str,ERRLEN,"in \"%s\", line %d: '%c' too much closing parenthesis",current_file,yylineno,*yytext);
                       set_error(err_str);
                       return _ERROR_;
                    }
                    else
                        parenthesis --;

		return END_PARENTHESIS;
		}

<INBLOC>"not"  { DEBUG_LEX(" NOT "); return NOT; }
<INBLOC>"and"  { DEBUG_LEX(" AND "); return AND; }
<INBLOC>"or"  { DEBUG_LEX(" OR "); return OR; }

<INBLOC>"union"  { DEBUG_LEX(" UNION "); return UNION; }
<INBLOC>"inter"  { DEBUG_LEX(" INTER "); return INTER; }

<INBLOC>"$"{LETTER}({IDENTIFIER_CHAR})* {
                    /* environment variable */
                    DEBUG_LEX("[VAR:%s]",yytext);
                    rh_strncpy(yylval.str_val,yytext,MAXSTRLEN);
                    return ENV_VAR;
                }

<INBLOC>{LETTER}({IDENTIFIER_CHAR})* {
                    /* identifier */
                    DEBUG_LEX("[%s]",yytext);
                    rh_strncpy(yylval.str_val,yytext,MAXSTRLEN);
                    return IDENTIFIER;
                }


<INBLOC>"}"     {   /* end of block */
                    if ( accolades <= 0 )
                    {
                       /* error */
                       snprintf(err_str,ERRLEN,"in \"%s\", line %d: '%c' closing bracket outside a block",current_file,yylineno,*yytext);
                       set_error(err_str);
                       return _ERROR_;
                    }
                    else
                        accolades --;
                    
                    if ( accolades == 0 )
                    {
                    	DEBUG_LEX("END_BLOCK\n");
                        BEGIN YY_INIT;
                        return END_BLOCK;
                    }
                    else
                    {
                    	DEBUG_LEX("END_SUB_BLOCK\n");
                        BEGIN INBLOC;
                        return END_SUB_BLOCK;
                    }
                     
                }

<INBLOC>"=="  { DEBUG_LEX(" EQUAL "); return EQUAL; }
<INBLOC>">"  { DEBUG_LEX(" SUP "); return GT; }
<INBLOC>">="  { DEBUG_LEX(" SUP_OR_EQUAL "); return GT_EQ; }
<INBLOC>"<"  { DEBUG_LEX(" INF  "); return LT; }
<INBLOC>"<="  { DEBUG_LEX(" INF_OR_EQUAL "); return LT_EQ; }
<INBLOC>"<>"  { DEBUG_LEX(" DIFF "); return DIFF; }
<INBLOC>"!="  { DEBUG_LEX(" DIFF "); return DIFF; }
<INBLOC>"="  { DEBUG_LEX(" AFFECT "); return AFFECT; }
                
<INBLOC>"{"    {
                                /* sub-block */
                                DEBUG_LEX("\nBEGIN_SUB_BLOCK\n");
                                BEGIN INBLOC;
                                accolades++;
                                return BEGIN_SUB_BLOCK;
                            }


<INBLOC>"\""           {BEGIN STRING1;DEBUG_LEX("value:<");YY_BUFFER_RESET();} /* ouverture string 1 */
<INBLOC>"'"            {BEGIN STRING2;DEBUG_LEX("value:<");YY_BUFFER_RESET();} /* ouverture string 2 */

<INBLOC>({VAL_CHAR})+  {/* valeur */DEBUG_LEX("[value:%s]\n",yytext);rh_strncpy(yylval.str_val,yytext,MAXSTRLEN); return NON_IDENTIFIER_VALUE;}

<INBLOC>";"     {DEBUG_LEX(" end_AFFECT "); return END_AFFECT; }



<STRING1>\\     {BEGIN ESC1;}
<STRING1>"\""   {DEBUG_LEX(">");rh_strncpy(yylval.str_val,YY_PARSED_STRING,MAXSTRLEN);BEGIN INBLOC;/* chaine finie */ return NON_IDENTIFIER_VALUE; }
<STRING1>\n      {snprintf(err_str,ERRLEN,"in \"%s\", line %d: missing closing quote.",current_file,yylineno); set_error(err_str);yylineno++;return _ERROR_;}
<STRING1>.      {YY_BUFFER_APPEND(yytext); DEBUG_LEX("%c",*yytext);/* caractere de la chaine */}

<ESC1>\n        {BEGIN STRING1;yylineno++;}/* ignore un saut de ligne echappe*/
<ESC1>.         {DEBUG_LEX("%c",*yytext);YY_BUFFER_APPEND(yytext);BEGIN STRING1;/* caractere de la chaine */}

<STRING2>"'"    {DEBUG_LEX(">");rh_strncpy(yylval.str_val,YY_PARSED_STRING,MAXSTRLEN);BEGIN INBLOC ;/* chaine finie */ return NON_IDENTIFIER_VALUE;}
<STRING2>\n     {snprintf(err_str,ERRLEN,"in \"%s\", line %d: closing quote missing.",current_file,yylineno); set_error(err_str);yylineno++;return _ERROR_;}
<STRING2>.      {YY_BUFFER_APPEND(yytext);DEBUG_LEX("%c",*yytext);/* caractere de la chaine */}

{COMMENT}  ;/* ignore */
{COMMENT2} ;/* ignore */
{SPACE}        ;/* ignore */
{NL}           yylineno++;/* ignore */

. { snprintf(err_str,ERRLEN,"in \"%s\", line %d: '%c' unexpected",current_file,yylineno,*yytext); set_error(err_str);return _ERROR_;}

%%

int yywrap(void){
    return 1;
}

void yyreset(void){
    YY_FLUSH_BUFFER;
    YY_USER_INIT;
}

void yy_set_current_file( char * file )
{
    rh_strncpy(current_file, file, FILE_LEN);
}
