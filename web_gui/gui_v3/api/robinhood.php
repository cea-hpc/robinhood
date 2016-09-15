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


require_once "../config.php";
require_once "../common.php";
require_once 'rest.class.php';

class MyAPI extends API
{
        protected $User;
        /***************************************
         * Test function to check the api works
         **************************************/
        protected function robinhood() {
                if ($this->method == 'GET') {
                        return "\"Fear not, my friends. This will be my greatest performance.\"";
                } else {
                        return "\"Faint hearts never won fair ladies.\"";
                }
        }

        /*****************************************
         * Test function that returns args as json
         ****************************************/
        protected function test() {
                if ($this->method == 'GET') {
                        $this->args;
                } else {
                        return "\"Faint hearts never won fair ladies.\"";
                }
        }

        /***************************************
         * return your current AUTH
         **************************************/
        protected function current_auth() {
                if ($this->method == 'GET') {
                        return check_access('api-ro');
                } else {
                        return "\"Faint hearts never won fair ladies.\"";
                }
        }


        /***************************************
         * return differents kinds of graph
         * JSON output with graphjs format
         **************************************/
        protected function native() {
                global $db;

                if ($this->method == 'GET') {

                        if (!check_access("native"))
                                return "Permission denied";

                        $content_requested = $this->verb;

                        switch ($content_requested) {
                        case 'vars':
                                $req = $db->prepare("SELECT * from VARS;");
                                $req->execute();
                                $data = array();
                                while($sqldata = $req->fetch(PDO::FETCH_ASSOC)){
                                        $data[$sqldata['varname']] = $sqldata['value'];
                                }
                                break;
                        case 'acct':
                                $fullfilter = build_advanced_filter($this->args);
                                $req = $db->prepare($fullfilter[0]);
                                $req->execute($fullfilter[1]);
                                $data = $req->fetchall(PDO::FETCH_ASSOC);
                                break;
                        }
                        return $data;

                } else {
                        return "\"Faint hearts never won fair ladies.\"";
                }


        }
        /***************************************
         * return differents kinds of graph
         * JSON output with graphjs format
         **************************************/
        protected function graph() {
                global $db;

                if ($this->method == 'GET') {

                        if (!check_access("graphs"))
                                return "Permission denied";

                        $content_requested = $this->verb;
                        $data = array();
                        $labels = array();
                        $size = array();
                        $count = array();
                        $color = array();

                        switch ($content_requested) {
                        case 'uid':
                        case 'gid':
                                $fullfilter = build_filter($this->args, array('uid'=>'uid', 'gid'=>'gid'));
                                $sqlfilter=$fullfilter[0];
                                $req = $db->prepare("SELECT $content_requested, SUM(size) AS ssize, SUM(count) AS scount FROM ACCT_STAT $sqlfilter GROUP BY $content_requested");
                                $req->execute($fullfilter[1]);
                                while($sqldata = $req->fetch(PDO::FETCH_ASSOC)) {
                                        $labels[] = $sqldata[$content_requested];
                                        $size[] = $sqldata['ssize'];
                                        $count[] = $sqldata['scount'];
                                        $color[] = string_color($sqldata[$content_requested]);
                                }

                                $data = array(
                                        'labels' => $labels,
                                        'default_graph' => 'doughnut',
                                        'filter' => array(),
                                        'datasets' => array()
                                );
                                $data['datasets'][] = array('data'=>$size, 'backgroundColor'=>$color, 'label'=>'size', 'unit'=>'size');
                                $data['datasets'][] = array('data'=>$count, 'backgroundColor'=>$color, 'label'=>'count', 'unit'=>'count');
                                break;

                        case 'Sizes':
                                $fullfilter = build_filter($this->args, array('uid'=>'uid', 'gid'=>'gid'));
                                $sqlfilter=$fullfilter[0];
                                $ssize = array("sz0","sz1","sz32","sz1K","sz32K","sz1M","sz32M","sz1G","sz32G","sz1T");
                                $select_str = "SUM(sz0) AS ssz0";
                                foreach ($ssize as $ssz)
                                        $select_str = $select_str.", SUM($ssz) AS s$ssz";
                                $req = $db->prepare("SELECT $select_str FROM ACCT_STAT $sqlfilter;");
                                $req->execute($fullfilter[1]);
                                while($sqldata = $req->fetch(PDO::FETCH_ASSOC)) {
                                        foreach ($ssize as $ssz) {
                                                $labels[] = l($ssz);
                                                $count[] = $sqldata['s'.$ssz];
                                                $color[] = "#DDDDFF";
                                        }
                                }

                                $data = array(
                                        'labels' => $labels,
                                        'default_graph' => 'bar',
                                        'filter' => array(),
                                        'datasets' => array()
                                );
                                $data['datasets'][] = array('data'=>$count, 'backgroundColor'=>$color, 'label'=>'Number of files', 'unit'=>'count');

                                break;

                        case 'Files':
                                global $MAX_ROWS;
                                $fullfilter = build_filter($this->args, array('filename'=>'name', 'uid'=>'uid', 'gid'=>'gid'));
                                $sqlfilter=$fullfilter[0];
                                $req = $db->prepare("SELECT uid, gid, size, blocks, name, creation_time, last_access, last_mod FROM NAMES INNER JOIN ENTRIES ON ENTRIES.id = NAMES.id $sqlfilter LIMIT $MAX_ROWS");
                                $req->execute($fullfilter[1]);
                                $count = $req->rowCount();
                                while($sqldata = $req->fetch(PDO::FETCH_ASSOC)) {
                                        $labels[] = $sqldata['uid'];
                                        $size[] = array('x' => $sqldata['last_access'], 'y'=> $sqldata['size'], 'r'=>'2');
                                        $color[] = string_color($sqldata['gid']);
                                }

                                $data = array(
                                        'labels' => $labels,
                                        'limited' => ($count == $MAX_ROWS) ? $MAX_ROWS : false,
                                        'default_graph' => 'bubble',
                                        'filter' => array(),
                                        'datasets' => array()
                                );
                                $data['datasets'][] = array('data'=>$size, 'backgroundColor'=>$color, 'label'=>'Last Access VS Size');

                                break;



                        default:
                                $fullfilter = build_filter($this->args, array('filename'=>'name', 'uid'=>'uid', 'gid'=>'gid'));
                                $sqlfilter=$fullfilter[0];

                                if (endsWith($content_requested,"_status")) {
                                        $req = $db->prepare("SELECT $content_requested AS sstatus, SUM(size) AS ssize, SUM(count) AS scount FROM ACCT_STAT $sqlfilter GROUP BY $content_requested;");
                                        $req->execute($fullfilter[1]);
                                        while($sqldata = $req->fetch(PDO::FETCH_ASSOC)) {
                                                $labels[] = ($sqldata['sstatus'] == '') ? 'None': $sqldata['sstatus'];
                                                $size[] = $sqldata['ssize'];
                                                $count[] = $sqldata['scount'];
                                                $color[] = string_color($sqldata['sstatus']);
                                        }

                                        $data = array(
                                                'labels' => $labels,
                                                'default_graph' => 'doughnut',
                                                'filter' => array(),
                                                'datasets' => array()
                                        );
                                        $data['datasets'][] = array('data'=>$size, 'backgroundColor'=>$color, 'label'=>'size', 'unit'=>'size');
                                        $data['datasets'][] = array('data'=>$count, 'backgroundColor'=>$color, 'label'=>'count', 'unit'=>'count');

                                } else {
                                        //provide sample data
                                        $data = array(
                                                'labels' => array("un", "deux", "trois"),
                                                'datasets' => array()
                                        );
                                        $data['datasets'][] = array('data'=>array("300","50","100"));
                                }
                                break;
                        }
                        return $data;

                } else {
                        return "\"Faint hearts never won fair ladies.\"";
                }
        }

        /****************************************************
         * return your data as json in "datatables.js" format
         ***************************************************/
        protected function data() {
                if ($this->method == 'GET') {
                        global $db;

                        if (!check_access("datatables"))
                                return "Permission denied";

                        $content_requested = $this->verb;
                        $data = array();
                        $columns = array();
                        $columnsDefs = array();
                        $datasets = array();
                        switch ($content_requested) {
                        case 'uid':
                        case 'gid':
                                $fullfilter = build_filter($this->args, array('uid'=>'uid', 'gid'=>'gid'));
                                $sqlfilter=$fullfilter[0];

                                $columns[] = array('title' => $content_requested);
                                $columns[] = array('title' => 'Size');
                                $columns[] = array('title' => 'File Count');
                                $columnsDefs[] = array('type' => 'file-size', 'targets' => 1);
                                $req = $db->prepare("SELECT $content_requested, SUM(size) AS ssize, SUM(count) AS scount FROM ACCT_STAT $sqlfilter GROUP BY $content_requested;");
                                $req->execute($fullfilter[1]);
                                while($sqldata = $req->fetch(PDO::FETCH_ASSOC)) {
                                        $datasets[] = array( $sqldata[$content_requested],formatSizeNumber($sqldata['ssize']),$sqldata['scount']);
                                }
                                break;

                        case 'Sizes':
                                $fullfilter = build_filter($this->args, array('uid'=>'uid', 'gid'=>'gid'));
                                $sqlfilter=$fullfilter[0];

                                $columns[] = array('title' => 'Owner');
                                $ssize = array("sz0","sz1","sz32","sz1K","sz32K","sz1M","sz32M","sz1G","sz32G","sz1T");
                                $select_str = "SUM(sz0) AS ssz0";
                                foreach ($ssize as $ssz) {
                                        $select_str = $select_str.", SUM($ssz) AS s$ssz";
                                        $columns[] = array('title' => l($ssz));
                                }
                                $req = $db->prepare("SELECT uid, $select_str FROM ACCT_STAT $sqlfilter GROUP BY uid;");
                                $req->execute($fullfilter[1]);
                                while($sqldata = $req->fetch(PDO::FETCH_ASSOC)) {
                                        $list = array();
                                        $list[]=$sqldata["uid"];
                                        foreach ($ssize as $ssz) {
                                                $list[] = $sqldata["s".$ssz];
                                        }
                                        $datasets[] = $list;
                                }
                                break;

                        case 'Files':
                                global $MAX_ROWS;
                                $fullfilter = build_filter($this->args, array('filename'=>'name', 'uid'=>'uid', 'gid'=>'gid'));
                                $sqlfilter=$fullfilter[0];
                                $req = $db->prepare("SELECT uid, gid, size, blocks, name, from_unixtime(creation_time) AS creation_time".
                                                    ", from_unixtime(last_access) AS last_access, from_unixtime(last_mod) AS last_mod".
                                                    " FROM NAMES INNER JOIN ENTRIES ON ENTRIES.id = NAMES.id $sqlfilter LIMIT $MAX_ROWS");
                                $req->execute($fullfilter[1]);

                                //we should autorize the user to see his own files
                                $columns[] = array('title' => 'uid');
                                $columns[] = array('title' => 'gid');
                                $columns[] = array('title' => 'size');
                                $columns[] = array('title' => 'blocks');
                                $columns[] = array('title' => 'File Name');
                                $columns[] = array('title' => 'creation_time');
                                $columns[] = array('title' => 'last_access');
                                $columns[] = array('title' => 'last_mod');
                                $columnsDefs[] = array('type' => 'file-size', 'targets' => 3);
                                while($sqldata = $req->fetch(PDO::FETCH_ASSOC)) {

                                        $datasets[] = array_values($sqldata);
                                }
                                break;


                        default:
                                if (endsWith($content_requested,"_status")) {
                                        $fullfilter = build_filter($this->args, array('filename'=>'name', 'uid'=>'uid', 'gid'=>'gid'));
                                        $sqlfilter=$fullfilter[0];

                                        $columns[] = array('title' => 'Status');
                                        $columns[] = array('title' => 'Size');
                                        $columns[] = array('title' => 'File Count');
                                        $columnsDefs[] = array('type' => 'file-size', 'targets' => 1);
                                        $req = $db->prepare("SELECT $content_requested AS sstatus, SUM(size) AS ssize, SUM(count) AS scount FROM ACCT_STAT $sqlfilter GROUP BY $content_requested;");
                                        $req->execute($fullfilter[1]);

                                        while($sqldata = $req->fetch()) {
                                                $datasets[] = array( ($sqldata['sstatus'] == '') ? 'None': $sqldata['sstatus'],formatSizeNumber($sqldata['ssize']),$sqldata['scount']);
                                        }
                                }
                                break;
                        }
                        $data['columns'] = $columns;
                        $data['datasets'] = $datasets;
                        $data['columnsDefs'] = $columnsDefs;
                        return $data;

                } else {
                        return "\"Faint hearts never won fair ladies.\"";
                }
        }


}

?>
