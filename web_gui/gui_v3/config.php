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

$DB_LASTERROR = "";
try {
    $db = new PDO("$DB_TYPE:$DB_HOST;dbname=$DB_NAME", $DB_USER, $DB_PASSWD);
    $db->exec("USE $DB_NAME;");
} catch(Exception $e) {
    $DB_LASTERROR = $e->getMessage();
}

/*****************************
*        Access              *
*****************************/

$ACCESS_LIST = array();
$ACCESS_LIST['webgui'] = array();
$ACCESS_LIST['api-ro'] = array();
$ACCESS_LIST['datatables'] = array();
$ACCESS_LIST['graphs'] = array();

#Web GUI with graphs and lists
$ACCESS_LIST['webgui'][] = '*';
#Read Only API, required for webgui
$ACCESS_LIST['api-ro'][] = '*';
#Datatables
$ACCESS_LIST['datatables'][] = '*';
#Graphs
$ACCESS_LIST['graphs'][] = '*';
#Native (raw data)
$ACCESS_LIST['native'][] = '*';
/*****************************
*        General parameters  *
*****************************/
//Max row per result
$MAX_ROWS = 1000;

$JSON_OPTIONS = null;
if (version_compare(phpversion(), '5.4.0', '>='))
    $JSON_OPTIONS |= JSON_PRETTY_PRINT;

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

?>
