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
     * Test function to see if the api works
     **************************************/
     protected function robinhood() {
        if ($this->method == 'GET') {
            return "Fear not, my friends. This will be my greatest performance.";
        } else {
            return "Faint hearts never won fair ladies.";
        }
     }

    /*****************************************
     * Test function that returns args as json
     ****************************************/
     protected function test() {
        if ($this->method == 'GET') {
		return json_encode($this->args);
        } else {
            return "Faint hearts never won fair ladies.";
        }
     }

    /***************************************
     * return your current AUTH
     **************************************/
     protected function current_auth() {
        if ($this->method == 'GET') {
        return json_encode(check_access('api-ro'));
        } else {
            return "Faint hearts never won fair ladies.";
        }
     }

    /***************************************
     * return differents kinds of graph
     * JSON output with graphjs format
     **************************************/
     protected function graph() {
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
        case 'owner':
	case 'uid':
        case 'gid':
        case 'gr_name':
            $req = mysql_query("SELECT $content_requested, SUM(size) AS ssize, SUM(count) AS scount FROM ACCT_STAT GROUP BY $content_requested;");
            while($sqldata = mysql_fetch_assoc($req)) {
                $labels[] = $sqldata[$content_requested];
                $size[] = $sqldata['ssize'];
                $count[] = $sqldata['scount'];
                $color[] = rand_color();
            }
            
            $data = array(
                'labels' => $labels,
                'default_graph' => 'pie',
                'filter' => array(),
                'datasets' => array()
                );
            $data['datasets'][] = array('data'=>$size, 'backgroundColor'=>$color, 'label'=>'size');
            $data['datasets'][] = array('data'=>$count, 'backgroundColor'=>$color, 'label'=>'count');
            break;

        case 'Sizes':
            $ssize = array("sz0","sz1","sz32","sz1K","sz32K","sz1M","sz32M","sz1G","sz32G","sz1T"); 
            $select_str = "SUM(sz0) AS ssz0";
            foreach ($ssize as $ssz)
                    $select_str = $select_str.", SUM($ssz) AS s$ssz";
            $req = mysql_query("select $select_str FROM ACCT_STAT;");
            while($sqldata = mysql_fetch_assoc($req)) {
                    foreach ($ssize as $ssz) {
                            $labels[] = $ssz;
                            $count[] = $sqldata['s'.$ssz];
                            $color[] = rand_color();
                    }
            }

            $data = array(
                            'labels' => $labels,
                            'default_graph' => 'bar',
                            'filter' => array(),
                            'datasets' => array()
                         );
            $data['datasets'][] = array('data'=>$count, 'backgroundColor'=>$color, 'label'=>'count');

            break;

        case 'Files':
            global $MAX_ROWS;
            $sqlfilter = build_filter($this->args, array('filename'=>'name', 'uid'=>'owner', 'gid'=>'gr_name'));
            $req = mysql_query("select owner, gr_name, size, blocks, name, creation_time, last_access, last_mod FROM NAMES INNER JOIN ENTRIES ON ENTRIES.id = NAMES.id $sqlfilter");
            if (mysql_num_rows($req)>$MAX_ROWS)
                    return "Too many rows";

            while($sqldata = mysql_fetch_assoc($req)) {
                    $labels[] = $sqldata['gr_name'];
                    $size[] = array('x' => $sqldata['last_access'], 'y'=> $sqldata['size'], 'r'=>'2');
                    $color[] = rand_color();
            }

            $data = array(
                            'labels' => $labels,
                            'default_graph' => 'bubble',
                            'filter' => array(),
                            'datasets' => array()
                         );
            $data['datasets'][] = array('data'=>$size, 'backgroundColor'=>$color, 'label'=>'Last Access VS Size');

            break;



        default:
            if (endsWith($content_requested,"_status")) {
                    $req = mysql_query("SELECT $content_requested AS sstatus, SUM(size) AS ssize, SUM(count) AS scount FROM ACCT_STAT GROUP BY $content_requested;");
                    while($sqldata = mysql_fetch_assoc($req)) {
                            $labels[] = ($sqldata['sstatus'] == '') ? 'None': $sqldata['sstatus'];
                            $size[] = $sqldata['ssize'];
                            $count[] = $sqldata['scount'];
                            $color[] = rand_color();
                    }

                    $data = array(
                                    'labels' => $labels,
                                    'default_graph' => 'pie',
                                    'filter' => array(),
                                    'datasets' => array()
                                 );
                    $data['datasets'][] = array('data'=>$size, 'backgroundColor'=>$color, 'label'=>'size');
                    $data['datasets'][] = array('data'=>$count, 'backgroundColor'=>$color, 'label'=>'count');

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
        return json_encode($data);

        } else {
                return "Faint hearts never won fair ladies.";
        }
     }

     /****************************************************
      * return your data as json in "datatables.js" format 
      ***************************************************/
     protected function data() {
             if ($this->method == 'GET') {

                     if (!check_access("datatables"))
                             return "Permission denied";

                     $content_requested = $this->verb;
                     $data = array();
                     $columns = array();
                     $datasets = array();

                     switch ($content_requested) {
                             case 'owner':
                             case 'gr_name':
                             case 'uid':
                             case 'gid':
                                     $columns[] = array('title' => $content_requested);
                                     $columns[] = array('title' => 'Size');
                                     $columns[] = array('title' => 'File Count');
                                     $req = mysql_query("SELECT $content_requested, SUM(size) AS ssize, SUM(count) AS scount FROM ACCT_STAT GROUP BY $content_requested;");
                                     while($sqldata = mysql_fetch_assoc($req)) {
                                             $datasets[] = array( $sqldata[$content_requested],formatSizeNumber($sqldata['ssize']),$sqldata['scount']);
                                     } 
                                     $data['columns'] = $columns;
                                     $data['datasets'] = $datasets;
                                     break;

                             case 'Sizes':
                                     $columns[] = array('title' => 'Owner');
                                     $ssize = array("sz0","sz1","sz32","sz1K","sz32K","sz1M","sz32M","sz1G","sz32G","sz1T");
                                     $select_str = "SUM(sz0) AS ssz0";
                                     foreach ($ssize as $ssz) {
                                             $select_str = $select_str.", SUM($ssz) AS s$ssz";
                                             $columns[] = array('title' => $ssz);
                                     }
                                     $req = mysql_query("SELECT owner, $select_str FROM ACCT_STAT GROUP BY owner;");
                                     while($sqldata = mysql_fetch_assoc($req)) {
                                             $datasets[] = array_values($sqldata);
                                     }
                                     $data['columns'] = $columns;
                                     $data['datasets'] = $datasets;
                                     break;

                             case 'Files':
                                     global $MAX_ROWS;

                                     $sqlfilter = build_filter($this->args, array('filename'=>'name', 'uid'=>'owner', 'gid'=>'gr_name'));
                                     //we should autorize the user to see his own files
                                     $columns[] = array('title' => 'owner');
                                     $columns[] = array('title' => 'gr_name');
                                     $columns[] = array('title' => 'size');
                                     $columns[] = array('title' => 'blocks');
                                     $columns[] = array('title' => 'File Name');
                                     $columns[] = array('title' => 'creation_time');
                                     $columns[] = array('title' => 'last_access');
                                     $columns[] = array('title' => 'last_mod');
                                     $req = mysql_query("select owner, gr_name, size, blocks, name, creation_time, last_access, last_mod FROM NAMES INNER JOIN ENTRIES ON ENTRIES.id = NAMES.id $sqlfilter");
                                     if (mysql_num_rows($req)>$MAX_ROWS)
                                             return "Too many rows";
                                     while($sqldata = mysql_fetch_assoc($req)) {
                                             $datasets[] = array_values($sqldata);
                                     }
                                     $data['columns'] = $columns;
                                     $data['datasets'] = $datasets;
                                     break;


                             default:
                                     if (endsWith($content_requested,"_status")) {
                                             $columns[] = array('title' => 'Status');
                                             $columns[] = array('title' => 'Size');
                                             $columns[] = array('title' => 'File Count');
                                             $req = mysql_query("SELECT $content_requested AS sstatus, SUM(size) AS ssize, SUM(count) AS scount FROM ACCT_STAT GROUP BY $content_requested;");
                                             while($sqldata = mysql_fetch_assoc($req)) {
                                                     $datasets[] = array( ($sqldata['sstatus'] == '') ? 'None': $sqldata['sstatus'],formatSizeNumber($sqldata['ssize']),$sqldata['scount']);
                                             }
                                             $data['columns'] = $columns;
                                             $data['datasets'] = $datasets;

                                     }
                                     break;
                     }
                     return json_encode($data);

             } else {
                     return "Faint hearts never won fair ladies.";
             }
     }


}

?>
