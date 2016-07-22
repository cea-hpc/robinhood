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

function startsWith($haystack, $needle) {
    // search backwards starting from haystack length characters from the end
    return $needle === "" || strrpos($haystack, $needle, -strlen($haystack)) !== false;
}

function endsWith($haystack, $needle) {
    // search forward starting from end minus needle length characters
    return $needle === "" || (($temp = strlen($haystack) - strlen($needle)) >= 0 && strpos($haystack, $needle, $temp) !== false);
}

function formatSizeNumber( $number, $precision=2 )
{
    $base = log($number, 1024);
    $suffixes = array('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZT', 'YT');

    return round(pow(1024, $base - floor($base)), $precision) .' '. $suffixes[floor($base)];
}

//Dynamicaly list columns from db table acct_stat
function get_acct_columns() {
    global $FIELD_LIST;
    $final = array();
    $result = mysql_query("SHOW COLUMNS FROM ACCT_STAT;");
    if (!$result) {
        echo 'Something goes wrong with db schema: ' . mysql_error();
        exit;
    }
    if (mysql_num_rows($result) > 0) {
       while ($row = mysql_fetch_assoc($result)) {
        if (array_key_exists($row['Field'], $FIELD_LIST)) {
            if (!in_array($FIELD_LIST[$row['Field']], $final) and $FIELD_LIST[$row['Field']]) {
                $final[] = $FIELD_LIST[$row['Field']];
            } 
        } else {
            $final[] = $row['Field'];
        }
       }
    }
    return $final;
}

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

function rand_color() {
    return sprintf('#%06X', mt_rand(0, 0xFFFFFF));
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
	foreach ($filter as $k => $v) {
            if(get_filter_from_list($args,$k)) {
                $val = get_filter_from_list($args,$k);
                if ($sqlfilter!="")
                        $sqlfilter = $sqlfilter." AND ";
                $sqlfilter = $sqlfilter." $v LIKE \"$val\" ";
            }
	}
        if ($sqlfilter != "")
	        $sqlfilter = " WHERE ".$sqlfilter;
	return $sqlfilter;
}
