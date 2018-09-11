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


/*
 * Custom Graph V0.1
 * Create Custom Graphs
 *
 */

class customgraph extends Plugin {
    public $Name = "Custom Graph";
    public $Description = "Manage Custom Graph";
    public $Version = "V0.1";


    public $Req_table = array(
            "CUSTOMGRAPH" => "(Id INT NOT NULL AUTO_INCREMENT, Name VARCHAR(255), ChartType VARCHAR(255), Request VARCHAR(255), X VARCHAR(1024), Y VARCHAR(255), Series VARCHAR(255), Mapping VARCHAR(512), PRIMARY KEY(Id))",
            );

    function init() {
    }

    /* Called from UI menu */
    function ui_header($param) {
        $newparam = '<script src="plugins/customgraph/script.js"></script>'."\n";
        $param = $param.$newparam;
        return $param;
    }

    /* Called from UI menu */
    function ui_menu_top($param) {
        global $db;
        if (!check_access("customgraph")) {
            return $param;
        }
        $newparam = "<li><a href='#' onclick='customgraph_GetInfo()'>Custom Graph</a></li>\n";
        $param = $param.$newparam;
        $confdb = getDB("config")[0];
        $req = $db[$confdb]->prepare("SELECT * FROM CUSTOMGRAPH;");
        $req->execute();
        $data = $req->fetchall(PDO::FETCH_ASSOC);
        foreach ($data as $v) {
            $newparam = "<li><a href='#' onclick='customgraph_GetGraph(\"".$v['Id']."\")'>*".$v['Name']."</a></li>\n";
            $param = $param.$newparam;
        }
        return $param;
    }

    /* Called from api just processing data */
    function api_native($param) {
        global $db;
        global $CURRENT_DB;
        if (!check_access("customgraph")) {
            return "Permission denied";
        }
        if ($param[0] == "customgraph" && $param[1][0]== "get") {
            $confdb = getDB("config")[0];
            $req = $db[$confdb]->prepare("SELECT * FROM CUSTOMGRAPH;");
            $req->execute();
            $data = $req->fetchall(PDO::FETCH_ASSOC);
            return $data;
        }
        if ($param[0] == "customgraph" && $param[1][0]== "add") {
            $confdb = getDB("config")[0];

            $keys = Array();
            $vals = Array();
            $flip = True;
            $last_item="";
            foreach (array_slice($param[1],2) as $item) {
                if ($flip == True) {
                    $keys[]=":".$item;
                    $last_item=$item;
                    $flip=False;
                } else {
                    $vals[":".$last_item]=str_replace("---","/",$item);
                    $flip=True;
                }
            }

            $request = "INSERT INTO CUSTOMGRAPH ( Id,Name, ChartType, Request, X, Y, Series, Mapping) VALUES ('', ".join(",",$keys).");";
            $req = $db[$confdb]->prepare($request);
            $req->execute($vals);
        }
        if ($param[0] == "customgraph" && $param[1][0]== "del") {
            $confdb = getDB("config")[0];
            $request = "DELETE FROM CUSTOMGRAPH WHERE Id = :Id";
            $vals = Array();
            $vals[":Id"]=$param[1][1];
            $req = $db[$confdb]->prepare($request);
            $req->execute($vals);
        }



    }

}
