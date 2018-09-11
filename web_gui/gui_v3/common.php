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

function is_assoc($var)
{
        return is_array($var) && array_diff_key($var,array_keys(array_keys($var)));
}

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
    if ($number === 0)
	return '0';

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
    global $DBA;
    global $CURRENT_DB;
    global $db;
    $final = array();
    if (!$db[$CURRENT_DB])
        return $final;
    $result = $db[$CURRENT_DB]->query("select column_name from information_schema.columns where table_name = 'ACCT_STAT' AND TABLE_SCHEMA = '".$DBA[$CURRENT_DB]["DB_NAME"]."';");
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
 * get user
 *
 * @return string user
 */
function get_user()
{
    $user = False;
    if (isset($_SERVER['PHP_AUTH_USER'])) {
        $user = $_SERVER['PHP_AUTH_USER'];
    } else {
            $user = '$NOAUTH';
    }
    $user = plugins_call("get_user", $user);
    return $user;
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
    $user = '';
    if (get_user()) {
        $user = get_user();
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
 * build sql filter from privileges
 *
 * @param array list of uid, uidnumber and groups
 * @return string SQL filter
 */
function build_sql_access($part)
{
    $sql_where = "(";
    $sql_where.= "uid IN (".$part['uids'].implode(",").") OR gid IN (";
    $sql_where.= $part['groups'].implode(",").")";
    return $sql_where;
}


/**
 *
 * check user self access
 *
 * Allow user to access to his own data
 *
 * @param string $part Name of the access to be check
 * @return bool
 */
function check_self_access($part)
{
    GLOBAl $ACCESS_LIST;
    $user='';
    if (get_user()) {
        $user = get_user();
    } else {
        return False;
    }
    if (in_array('$SELF', $ACCESS_LIST[$part]))
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
    if ($i == false) {
        return false;
    }
    if ($i+1 < count($datalist)) {
        if ($datalist[$i+1] == '')
            return False;
        else
            return $datalist[$i+1];
    }
    return false;
}

/**
 *
 * Build SQLRequest/Table from REST Args
 *
 * @param array $args REST args as key/val/key/val/... list
 * @param self Filter to show only user data (for self service)
 * @return array String,Array with sql request and array of filter
 */
function build_filter($args, $filter, $self='$SELF') {
    $sqlfilter = "";
    $havingfilter = "";
    $values = array();

    //Ensure uid if present for self usage
    if ($self!='$SELF')
        $filter['uid'] = 'uid';

    foreach ($filter as $k => $v) {
        $op="LIKE";
        if (startsWith($k, "min"))
            $op=">";
        if (startsWith($k, "max"))
            $op="<";
        if(get_filter_from_list($args, $k)) {
            $val = get_filter_from_list($args, $k);
            if ($v != 'offset') {
                if (strstr($v, "(") != false){
                    if ($havingfilter != "")
                        $havingfilter = $havingfilter." AND ";
                    $havingfilter = $havingfilter."$v $op :k_$k ";
                }else{
                    if ($sqlfilter != "")
                        $sqlfilter = $sqlfilter." AND ";
                    $sqlfilter = $sqlfilter."$v $op :k_$k ";
                }
            }
            $values["k_$k"] = $val;
        } elseif ($self != '$SELF' && $k == 'uid') {
            if ($v!='offset') {
                if (strstr($v, "(") != false){
                    if ($havingfilter != "")
                        $havingfilter = $havingfilter." AND ";
                    $havingfilter = $havingfilter."$v $op :k_$k ";
                }else{
                    if ($sqlfilter != "")
                        $sqlfilter = $sqlfilter." AND ";
                    $sqlfilter = $sqlfilter."$v $op :k_$k ";
                }
            }
            $values["k_$k"] = $self;
        }
    }

    if ($sqlfilter != "")
        $sqlfilter = " WHERE ".$sqlfilter;

    if ($havingfilter != "")
        $havingfilter = " HAVING ".$havingfilter;

    return array($sqlfilter, $values, $havingfilter);
}

/**
 *
 * Build SQLRequest from REST args
 *
 * @param array $args REST args (=>README.txt)
 * @param array $access User identity
 * @param string $table mysql table
 * @param string $join mysql table to join
 * @return array String,Array with sql request and array of filter
 */
function build_advanced_filter($args, $access = '$SELF', $table, $join = false) {
    global $db;
    global $DB_LASTERROR;
    global $DBA;
    global $CURRENT_DB;

    $shortcuts = array();
    $fields = array();
    $select = array();
    $filter = array();
    $operator = array();
    $group = array();
    $group_select = array();
    $order_by = array();
    $select_cache = array();
    $values = array();
    $whitelist=false;
    $limit=false;

    $shortcuts['GROUP_CONCAT'] = "_set";
    $shortcuts['COUNT'] = "_count";
    $shortcuts['MAX'] = "_max";
    $shortcuts['MIN'] = "_min";
    $shortcuts['AVG'] = "_avg";
    $shortcuts['*'] = "_all";

    $sqlrequest = "SELECT ";
    $ttable = "table_name ='$table'";

    if (in_array("whitelist", $args))
        $whitelist=true;

    $i = array_search("limit", $args);
    if ($i)
        $limit=intval($args[$i+1]);

    if ($join)
        $ttable = $ttable." OR table_name='$join'";

    $result = $db[$CURRENT_DB]->query("SELECT column_name,column_type,table_name FROM information_schema.columns WHERE ($ttable) AND TABLE_SCHEMA = '".$DBA[$CURRENT_DB]["DB_NAME"]."';");
    if ($result->rowCount() <1) {
        $DB_LASTERROR = 'Something goes wrong with db schema: $TABLE doesn\'t exist';
        exit;
    }
    if ($result->rowCount() > 0) {
        while ($row = $result->fetch()) {
            $fields[$row[0]] = $row[1];
            $grouptype = false;
            if (strstr($row[1], "int")!=false)
                $grouptype="SUM";
            elseif (strstr($row[1], "var")!=false)
                $grouptype="GROUP_CONCAT";
            elseif (strstr($row[1], "enum")!=false)
                $grouptype="GROUP_CONCAT";
            if (!$whitelist) {
                if ($join)
                    $select[$row[2].'.'.$row[0]] = $grouptype;
                else
                    $select[$row[0]] = $grouptype;
            }
        }
        $i=0;
        foreach ($args as $arg) {
            if (strstr($arg, ".")!=false) {
                $prop = explode(".", $arg);
                $field = $prop[0];
                if ($join && $field=="id")
                    $field = $table.".".$field;
                unset($prop[0]);
                if (array_key_exists($field, $fields) OR $field=="*") {
                    if (in_array("group", $prop)) {
                        $group[] = $field;
                        $group_select[] = $field;
                        unset($select[$field]);
                    }
                    if (in_array("groupbytime", $prop)) {
                        $interval = "86400";
                        if (in_array("hour", $prop))
                            $interval = "3600";
                        if (in_array("day", $prop))
                            $interval = "86400";
                        if (in_array("week", $prop))
                            $interval = "604800";
                        if (in_array("month", $prop))
                            $interval = "26280030";
                        if (in_array("year", $prop))
                            $interval = "315360365";

                        $group[] = "FLOOR(".$field."/".$interval.")*".$interval;
                        $group_select[] = "FLOOR(".$field."/".$interval.")*".$interval." AS ".$field."_by";
                        unset($select[$field]);
                    }
                    if (in_array("groupbylog2", $prop)) {
                        $div = "";
                        if (in_array("unit", $prop))
                                $div = "/10";
                        if (in_array("hunit", $prop))
                            $div = "/5";
                        $group[] = "FLOOR(LOG2(".$field.")$div)";
                        $group_select[] = "FLOOR(LOG2(".$field.")$div)  AS ".$field."_by";
                        unset($select[$field]);
                    }
                    if (in_array("count", $prop)) {
                        $select[$field] = "COUNT";
                    }
                    if (in_array("max", $prop)) {
                        $select[$field] = "MAX";
                    }
                    if (in_array("min", $prop)) {
                        $select[$field] = "MIN";
                    }
                    if (in_array("avg", $prop)) {
                        $select[$field] = "AVG";
                    }
                    if (in_array("sum", $prop)) {
                        $select[$field] = "SUM";
                    }
                    if (in_array("concat", $prop)) {
                        $select[$field] = "GROUP_CONCAT";
                    }
                    if (in_array("remove", $prop)) {
                        unset($select[$field]);
                    }
                    if (in_array("filter", $prop)) {
                        $filter[$field] = $args[$i+1];
                        $operator[$field] = "LIKE";
                    }
                    if (in_array("nfilter", $prop)) {
                        $filter[$field] = $args[$i+1];
                        $operator[$field] = "NOT LIKE";
                    }
                    if (in_array("equal", $prop)) {
                        $filter[$field] = $args[$i+1];
                        $operator[$field] = "=";
                    }
                    if (in_array("less", $prop)) {
                        $filter[$field] = $args[$i+1];
                        $operator[$field] = "<";
                    }
                    if (in_array("bigger", $prop)) {
                        $filter[$field] = $args[$i+1];
                        $operator[$field] = ">";
                    }
                    if (in_array("soundslike", $prop)) {
                        $filter[$field] = $args[$i+1];
                        $operator[$field] = "SOUNDS LIKE";
                    }
                    if (in_array("asc", $prop)) {
                        $order_by[$field] = "ASC";
                    }
                    if (in_array("desc", $prop)) {
                        $order_by[$field] = "DESC";
                    }
                }
            }
            $i++;
        }

        //build select
        if (sizeof($group)!=0)
            $sqlrequest = $sqlrequest."".implode(", ", $group_select);
        $first = true;
        foreach ($select as $k => $v) {
            if($v && sizeof($group)!=0) {
                $attr= "";
                if ($v == "GROUP_CONCAT")
                    $attr="DISTINCT ";
                $ext= "";
                if (array_key_exists($v, $shortcuts))
                        $ext = $shortcuts[$v];
                $kk = str_replace("*", "_all", $k);
                $sqlrequest = $sqlrequest.", $v($attr$k) AS $kk$ext";
                $select_cache[$k] = $kk.$ext;
            } elseif (sizeof($group) == 0) {
                if ($first)
                    $sqlrequest = $sqlrequest."$k";
                else
                    $sqlrequest = $sqlrequest.", $k";
                $select_cache[$k] = $k;
                $first = false;
            }
        }
        $sqlrequest = $sqlrequest." FROM $table ";
        if ($join)
            $sqlrequest = $sqlrequest." LEFT JOIN $join ON $table.id = $join.id ";
        //build where
        if (sizeof($filter) !=0 )
            $sqlrequest = $sqlrequest." WHERE ";
        $first = true;
        foreach ($filter as $k => $v) {
            if (!$first)
                $sqlrequest = $sqlrequest." AND ";
            $sqlrequest = $sqlrequest."$k ".$operator[$k]." :k_$k ";
            $values["k_$k"] = str_replace('*', '%', $v);
            $first=false;
        }
        if ($access != '$SELF') {
                if (sizeof($filter) != 0)
                        $sqlrequest.= " AND ";
                else
                        $sqlrequest.= " WHERE ";

             $values["k_uid"] = $access;
             $data = plugins_call("access_sql_filter",[" uid LIKE :k_uid ", $table, $values]);
             $sqlrequest = $sqlrequest.$data[0];
             $values = $data[2];
        }
        //build group by
        if (sizeof($group)!=0)
                $sqlrequest = $sqlrequest." GROUP BY ".implode(", ", $group);

        //order by
        if (sizeof($order_by) != 0) {
                $sqlrequest = $sqlrequest." ORDER BY ";
                $first = true;
                foreach ($order_by as $k => $v) {
                    if (!$first)
                        $sqlrequest = $sqlrequest.", ";
                    $sqlrequest = $sqlrequest.$select_cache[$k]." ".$v;
                    $first = false;
                }
        }
        if ($limit) {
            $sqlrequest = $sqlrequest." LIMIT $limit";
        }

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
    } elseif (endsWith($text, "_status")) {
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
    return substr(decoct(fileperms($file)), $length);
}


/**
 *
 * Set form values from url
 *
 * @return string javascript which set the form value
 */
function setFormValues() {

    $js = "<script>\n";
    foreach ($_GET as $k => $v) {
        if (startsWith($k, "form")) {
            $js.="document.getElementById('$k').value='$v';\n";
        }
    }
    $js.= "</script>\n";
    return $js;
}


/**
 *
 * Call a specific graph from URL parameter
 *
 * @return string javascript which set the graph call
 */
function callGraph() {

    $js = "<script>\n";
    foreach ($_GET as $k => $v) {
        if ($k=="callGraph") {
            $js.="GetGraph('$v');\n";
        }
    }
    $js.= "</script>\n";
    return $js;
}


/**
 *
 * Return DBs from type
 *
 * @param string filter
 * @return list of db name
 */
function getDB($filter)
{
    global $DBA;
    $result = array();
    foreach ($DBA as $k=>$v) {
	if (in_array($filter,$v["DB_USAGE"])) {
		array_push($result,$k);
	}
    }
    return $result;
}
