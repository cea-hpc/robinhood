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

//Support at least mysql/pgsql/sqlite
$DB_TYPE     = "mysql";
$DB_HOST     = "localhost";
$DB_NAME     = "";
$DB_USER     = "";
$DB_PASSWD   = "";


/*****************************
*        Access              *
*****************************/

//Web GUI with graphs and lists
//$ACCESS_LIST['webgui'][] = '*';
//Read Only API, required for webgui
//$ACCESS_LIST['api-ro'][] = '*';
//Datatables
//$ACCESS_LIST['datatables'][] = '*';
//Graphs
//$ACCESS_LIST['graphs'][] = '*';
//Native (raw data)
//$ACCESS_LIST['native_vars'][] = '*';
//$ACCESS_LIST['native_accts'][] = '*';
/*****************************
*        General parameters  *
*****************************/
//Max row per result
//$MAX_ROWS = 1000;

/*****************************
*           ChartJS          *
*****************************/
//$CHARTJS = array();
//$CHARTJS['Chart.defaults.global.title.display'] = "true";
//$CHARTJS['Chart.defaults.global.defaultFontSize'] = 13;
//$CHARTJS['responsiveChart'] = "true";
//$CHARTJS['animationChart'] = "false";
//CHARTJS['showAllTooltipsChart'] = "true";


?>
