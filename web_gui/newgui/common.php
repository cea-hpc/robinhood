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


/**
 * 
 * Just the "standard" startsWith
 *
 * @param string $haystack haystack.
 * @param string $needle needle.
 * @return bool "startsWith" or false is needle not a string 
 */
function startsWith($haystack, $needle) {
    // search backwards starting from haystack length characters from the end
    return $needle === "" || strrpos($haystack, $needle, -strlen($haystack)) !== false;
}

/**
 * 
 * Just the "standard" endsWith
 *
 * @param string $haystack haystack.
 * @param string $needle needle.
 * @return bool "endsWith" or false is needle not a string 
 */
function endsWith($haystack, $needle) {
    // search forward starting from end minus needle length characters
    return $needle === "" || (($temp = strlen($haystack) - strlen($needle)) >= 0 && strpos($haystack, $needle, $temp) !== false);
}

/**
 * 
 * Convert size to human readable format
 *
 * @param int $number number to convert
 * @param int optional $precision number of digit
 * @return string human readable size
 */
function formatSizeNumber( $number, $precision=2 )
{
    $base = log($number, 1024);
    $suffixes = array('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZT', 'YT');

    return round(pow(1024, $base - floor($base)), $precision) .' '. $suffixes[floor($base)];
}


/**
 * 
 * Get Columns from ACCT_STAT tables and clean/convert them
 *
 * @return array array of columns names as string
 */
function get_acct_columns() {
    global $FIELD_LIST;
    global $db;
    $final = array();
    //$result = $db->query("SHOW COLUMNS FROM ACCT_STAT;");
    $result = $db->query("select column_name from information_schema.columns where table_name = 'ACCT_STAT';");
    if ($result->rowCount() <1) {
        echo 'Something goes wrong with db schema: ' . mysql_error();
        exit;
    }
    if ($result->rowCount() > 0) {
       while ($row = $result->fetch()) {
        if (array_key_exists($row[0], $FIELD_LIST)) {
            if (!in_array($FIELD_LIST[$row[0]], $final) and $FIELD_LIST[$row[0]]) {
                $final[] = $FIELD_LIST[$row[0]];
            }
        } else {
            $final[] = $row[0];
        }
       }
    }
    return $final;
}

/**
 * 
 * check user access
 *
 * @param string $part Name of the access to be check
 * @return bool 
 */
function check_access($part)
{
    GLOBAl $ACCESS_LIST;
    $user='';
    if (isset($_SERVER['PHP_AUTH_USER'])) {
        $user = $_SERVER['PHP_AUTH_USER'];
    } else {
        $user='$NOAUTH';
    }
    if (in_array('*', $ACCESS_LIST[$part]))
        return $user;
    if (in_array('$AUTH', $ACCESS_LIST[$part]))
        return $user;
    if (in_array($user, $ACCESS_LIST[$part]))
        return $user;
    return False;
}

/**
 * 
 * generate HEX color
 *
 * @return string "#RRGGBB" 
 */
function rand_color() {
    return sprintf('#%06X', mt_rand(0, 0xFFFFFF));
}

/**
 * 
 * generate HEX color frim string
 *
 * @param string $str
 * @return string "#RRGGBB" 
 */

function string_color($str){
    return '#'.substr(md5($str), 0, 6);
}

function get_filter_from_list($datalist, $term)
{
    $i = array_search($term, $datalist);
    if ($i==-1)
        return false;
    if ($i+1<count($datalist)) {
        if ($datalist[$i+1]=='')
            return False;
        else
            return $datalist[$i+1];
    }
    return false;

}


function build_filter($args, $filter) {
    $sqlfilter = "";
    $values = array();
    foreach ($filter as $k => $v) {
            if(get_filter_from_list($args,$k)) {
                $val = get_filter_from_list($args,$k);
                if ($sqlfilter!="")
                        $sqlfilter = $sqlfilter." AND ";
                $sqlfilter = $sqlfilter."$v LIKE :k_$v ";
                $values["k_$v"] = $val;
            }
    }
        if ($sqlfilter != "")
            $sqlfilter = " WHERE ".$sqlfilter;
    return array($sqlfilter, $values);
}


function l($text)
{
    global $lang;
    if (array_key_exists($text, $lang))
        return $lang[$text];
    return $text;
}
