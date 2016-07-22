<?php
/*
 * Copyright (C) 2016 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


/*****************************
*        Database            *
*****************************/

$DB_HOST     = "";
$DB_NAME     = "";
$DB_USER     = "";
$DB_PWD      = "";

$db = mysql_connect($DB_HOST, $DB_USER, $DB_PWD );
mysql_select_db($DB_NAME, $db); 


/*****************************
*        Access              *
*****************************/

$ACCESS_LIST = array();
$ACCESS_LIST['webgui'] = array();
$ACCESS_LIST['api'] = array();

#Web GUI with graphs and lists
$ACCESS_LIST['webgui'][] = '*';
#Read Only API, required for webgui
$ACCESS_LIST['api-ro'][] = '*';
#Datatables
$ACCESS_LIST['datatables'][] = '*';
#Graphs
$ACCESS_LIST['graphs'][] = '*';

/*****************************
*        General parameters  *
*****************************/
//Max row per result
$MAX_ROWS = 10000;

/*****************************
*        Dynamic Fields      *
*****************************/
$FIELD_LIST = array();
$FIELD_LIST['sz0'] = 'Sizes';
$FIELD_LIST['sz1'] = 'Sizes';
$FIELD_LIST['sz32'] = 'Sizes';
$FIELD_LIST['sz1K'] = 'Sizes';
$FIELD_LIST['sz32K'] = 'Sizes';
$FIELD_LIST['sz1M'] = 'Sizes';
$FIELD_LIST['sz32M'] = 'Sizes';
$FIELD_LIST['sz1G'] = 'Sizes';
$FIELD_LIST['sz32G'] = 'Sizes';
$FIELD_LIST['sz1T'] = 'Sizes';
$FIELD_LIST['type'] = null;
$FIELD_LIST['size'] = null;
$FIELD_LIST['blocks'] = null;
$FIELD_LIST['count'] = null;

?>



