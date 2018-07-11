<?php
/*
 * Copyright (C) 2016-2017 CEA/DAM
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

$ACCESS_LIST = array();
$ACCESS_LIST['webgui'] = array();
$ACCESS_LIST['api-ro'] = array();
$ACCESS_LIST['datatables'] = array();
$ACCESS_LIST['graphs'] = array();
$ACCESS_LIST['native_vars'] = array();
$ACCESS_LIST['native_acct'] = array();
$ACCESS_LIST['native_files'] = array();
$ACCESS_LIST['native_entries'] = array();
$ACCESS_LIST['native_names'] = array();

/* Beware, by default everyone can access to everything */

//Web GUI with graphs and lists
$ACCESS_LIST['webgui'][] = '*';
//Read Only API, required for webgui
$ACCESS_LIST['api-ro'][] = '*';
//Datatables
$ACCESS_LIST['datatables'][] = '*';
//Graphs
$ACCESS_LIST['graphs'][] = '*';
//Native (raw data)
$ACCESS_LIST['native_vars'][] = '*';
$ACCESS_LIST['native_acct'][] = '*';
$ACCESS_LIST['native_files'][] = '*';
$ACCESS_LIST['native_entries'][] = '*';
$ACCESS_LIST['native_names'][] = '*';
/*****************************
*        General parameters  *
*****************************/
//Max row per result
$MAX_ROWS = 1000;

$JSON_OPTIONS = null;
if (version_compare(phpversion(), '5.4.0', '>='))
    $JSON_OPTIONS |= JSON_PRETTY_PRINT;

$SHOW_FILES = True;

/*****************************
*       ChartJS/dataTable    *
*****************************/
$CHARTJS = array();
$CHARTJS['Chart.defaults.global.title.display'] = "true";
$CHARTJS['Chart.defaults.global.defaultFontSize'] = 13;
$CHARTJS['responsiveChart'] = "true";
$CHARTJS['animationChart'] = "false";
$CHARTJS['showAllTooltipsChart'] = "true";
$CHARTJS['maxdisplayedrows'] = 50000;

/*****************************
*        Customization       *
*****************************/
$CUSTOM['vendor_logo'] = 'images/logoCEA20.jpg';
$CUSTOM['vendor_url'] = 'http://www-hpc.cea.fr/index-en.htm';

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

/*****************************
 *        Language            *
 *****************************/
if (array_key_exists('HTTP_ACCEPT_LANGUAGE', $_SERVER))
    $lang = substr($_SERVER['HTTP_ACCEPT_LANGUAGE'], 0, 2);
else
    $lang = "en";

switch ($lang) {
case "fr":
        $lang_file = 'fr.php';
        break;
default:
        $lang_file = 'en.php';
}

include_once 'lang/sys.php';
include_once 'lang/'.$lang_file;


/*****************************
 *        Plugins            *
 *****************************/

$PLUGINS_REG = array();
$PLUGINS_INST = array();

$PLUGINS_REG[] = "stackgraph";
$PLUGINS_REG[] = "colorgraph";
$PLUGINS_REG[] = "plugdisplay";
$PLUGINS_REG[] = "internalstats";
$PLUGINS_REG[] = "browser";
$PLUGINS_REG[] = "console";
$PLUGINS_REG[] = "output";
//This plugin requires a valid ldap conf.
//$PLUGINS_REG[] = "ldapauth";

/*****************************
 *        Local config        *
 *****************************/
//Allow to override config with local file
if (!@include "config_local.php") {
        $err = error_get_last();
        if ($err["type"] == 2){
                //Clear the last error if the file is not found
                if (version_compare(phpversion(), '7.0.0', '>='))
                    error_clear_last();
        } else {
                //something get wrong in the file, notify !
                print_r(get_last_error());
        }
}

/****************************
*        DB Connection       *
*****************************/
$DB_LASTERROR = "";
try {
    $db = new PDO("$DB_TYPE:host=$DB_HOST;dbname=$DB_NAME", $DB_USER, $DB_PASSWD);
    $db->exec("USE $DB_NAME;");
} catch(Exception $e) {
    $DB_LASTERROR = $e->getMessage();
}


?>
