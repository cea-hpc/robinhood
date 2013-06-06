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
/**
 *
 * \file    verif_syntaxe.c
 * \author  $Author: leibovic $
 * \date    $Date: 2008/07/04 08:15:29 $
 * \version	$Revision: 1.2 $
 * \brief   Build syntax tree
 *
 * Test config file syntax
 *
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "config_parsing.h"
#include <errno.h>

int main( int argc, char **argv )
{
#ifdef _DEBUG_PARSING
    extern int yydebug;
    yydebug=1;
#endif


    char          *errtxt;
    char          *fichier;

    config_file_t  config;

    if ( ( argc > 1 ) && ( argv[1] ) )
    {
        fichier = argv[1];
    }
    else
    {
        fprintf( stderr, "Usage %s <config_file>\n", argv[0] );
        exit( EINVAL );
    }

    /* test syntax of configuration file */
    config = rh_config_ParseFile( fichier );
    if ( config == NULL )
    {
        errtxt = rh_config_GetErrorMsg(  );
        fprintf( stderr, "Error parsing %s: %s\n", argv[1], errtxt );
        exit( EINVAL );
    }
    else
    {
        fprintf( stderr, "Syntax of file %s is correct!\n", argv[1] );
        rh_config_Free( config );
    }


    return 0;                   /* avoid compiler warning */

}
