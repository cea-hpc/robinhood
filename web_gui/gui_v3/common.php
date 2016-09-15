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
function get_acct_columns($all=false) {
        global $FIELD_LIST;
        global $DB_LASTERROR;
        global $DB_NAME;
        global $db;
        $final = array();
        if (!$db)
                return $final;
        $result = $db->query("select column_name from information_schema.columns where table_name = 'ACCT_STAT' AND TABLE_SCHEMA = '$DB_NAME';");
        if ($result->rowCount() <1) {
                $DB_LASTERROR = 'Something goes wrong with db schema: ACCT_STAT doesn\'t exist';
                return $final;
        }
        if ($result->rowCount() > 0) {
                while ($row = $result->fetch()) {
                        if (array_key_exists($row[0], $FIELD_LIST) and !$all) {
                                if (!in_array($FIELD_LIST[$row[0]], $final) and $FIELD_LIST[$row[0]]) {
                                        $final[] = $FIELD_LIST[$row[0]];
                                }
                        } else {
                                $final[] = $row[0];
                        }
                }
        }
        return array_unique($final);
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

/**
 *
 * Build SQLRequest from REST args
 *
 * @param array $args REST args (=>README.txt)
 * @return array String,Array with sql request and array of filter
 */
function build_advanced_filter($args) {
        global $db;
        global $DB_LASTERROR;
        global $DB_NAME;
        $shortcuts = array();
        $fields = array();
        $select = array();
        $filter = array();
        $group = array();
        $values = array();

        $shortcuts['GROUP_CONCAT'] = "_set";
        $shortcuts['COUNT'] = "_count";
        $shortcuts['MAX'] = "_max";
        $shortcuts['MIN'] = "_min";
        $shortcuts['AVG'] = "_avg";

        $sqlrequest = "SELECT ";
        $result = $db->query("select column_name,column_type from information_schema.columns where table_name = 'ACCT_STAT' AND TABLE_SCHEMA = '$DB_NAME';");
        if ($result->rowCount() <1) {
                $DB_LASTERROR = 'Something goes wrong with db schema: ACCT_STAT doesn\'t exist';
                exit;
        }
        if ($result->rowCount() > 0) {
                while ($row = $result->fetch()) {
                        $fields[$row[0]] = $row[1];
                        $grouptype = false;
                        if (strstr($row[1],"int")!=false)
                                $grouptype="SUM";
                        elseif (strstr($row[1],"var")!=false)
                                $grouptype="GROUP_CONCAT";
                        elseif (strstr($row[1],"enum")!=false)
                                $grouptype="GROUP_CONCAT";
                        $select[$row[0]] = $grouptype;
                }
                $i=0;
                foreach ($args as $arg) {
                        if (strstr($arg,".")!=false) {
                                $prop = explode(".",$arg);
                                $field = $prop[0];
                                unset($prop[0]);
                                if (array_key_exists($field, $fields)) {
                                        if (in_array("group", $prop)) {
                                                $group[]=$field;
                                                unset($select[$field]);
                                        }
                                        if (in_array("count", $prop)) {
                                                $select[$field]="COUNT";
                                        }
                                        if (in_array("max", $prop)) {
                                                $select[$field]="MAX";
                                        }
                                        if (in_array("min", $prop)) {
                                                $select[$field]="MIN";
                                        }
                                        if (in_array("avg", $prop)) {
                                                $select[$field]="AVG";
                                        }
                                        if (in_array("remove", $prop)) {
                                                unset($select[$field]);
                                        }
                                        if (in_array("filter", $prop)) {
                                                $filter[$field] = $args[$i+1];
                                        }
                                }
                        }
                        $i++;
                }

                //build select
                if (sizeof($group)!=0)
                        $sqlrequest = $sqlrequest."".implode(", ",$group);
                $first = true;
                foreach ($select as $k => $v) {
                        if($v && sizeof($group)!=0) {
                                $attr= "";
                                if ($v=="GROUP_CONCAT")
                                        $attr="DISTINCT ";
                                $ext= "";
                                if (array_key_exists($v, $shortcuts))
                                        $ext = $shortcuts[$v];
                                $sqlrequest = $sqlrequest.", $v($attr$k) AS $k$ext";
                        } elseif (sizeof($group)==0) {
                                if ($first)
                                        $sqlrequest = $sqlrequest."$k";
                                else
                                        $sqlrequest = $sqlrequest.", $k";
                                $first = false;
                        }
                }
                $sqlrequest = $sqlrequest." FROM ACCT_STAT ";
                //build where
                if (sizeof($filter)!=0)
                        $sqlrequest = $sqlrequest." WHERE ";
                $first = true;
                foreach ($filter as $k => $v) {
                        if (!$first)
                                $sqlrequest = $sqlrequest." AND ";
                        $sqlrequest = $sqlrequest."$k LIKE :k_$k ";
                        $values["k_$k"] = str_replace('*', '%', $v);
                        $first=false;
                }

                //build group by
                if (sizeof($group)!=0)
                        $sqlrequest = $sqlrequest." GROUP BY ".implode(",",$group);

        }

        return array($sqlrequest, $values);
}

/**
 *
 * Translate string
 *
 * @param string $str word to translate
 * @return string Translation
 */
function l($text)
{
        global $lang;
        if (array_key_exists($text, $lang)) {
                return $lang[$text];
        } elseif (endsWith($text,"_status")) {
                return ucfirst(str_replace("_", " ", $text));
        }
        return $text;
}

/**
 *
 * Check file permissions
 *
 * @param string $file path to file
 * @return string File permissions in "xxx" format
 */
function getFilePermission($file) {
        $length = strlen(decoct(fileperms($file)))-3;
        return substr(decoct(fileperms($file)),$length);
}
