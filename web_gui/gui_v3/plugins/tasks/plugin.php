<?php
/*
 * Copyright (C) 2018 CEA/DAM
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */


/*
 * Tasks V0.1
 * Run tasks
 *
 */

class tasks extends Plugin {
    public $Name = "Tasks";
    public $Description = "Manage and run tasks";
    public $Version = "V0.1";


    public $Req_table = array(
            "TASKS" => "(Id INT NOT NULL AUTO_INCREMENT, Name VARCHAR(255), Trig VARCHAR(255), Request VARCHAR(1024), Action VARCHAR(255), TargetDB VARCHAR(255), LastRun TIMESTAMP, ExecTime INT, PRIMARY KEY(Id))",
            );

    function init() {
    }

    /* Called from UI menu */
    function ui_header($param) {
        $newparam = '<script src="plugins/tasks/script.js"></script>'."\n";
        $param = $param.$newparam;
        return $param;
    }

    /* Called from UI menu */
    function ui_menu_top($param) {
        $newparam = "<li><a href='#' onclick='tasks_GetInfo()'>Tasks</a></li>\n";
        $param = $param.$newparam;
        return $param;
    }

    /* Called from api just processing data */
    function api_native($param) {
        global $db;
        global $CURRENT_DB;
        if (!check_access("tasks")) {
            return "Permission denied";
        }
        if ($param[0] == "tasks" && $param[1][0]== "get") {
            $confdb = getDB("config")[0];
            $req = $db[$confdb]->prepare("SELECT * FROM TASKS;");
            $req->execute();
            $data = $req->fetchall(PDO::FETCH_ASSOC);
            return $data;
        }
        if ($param[0] == "tasks" && $param[1][0]== "add") {
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

            $request = "INSERT INTO TASKS ( Id,ExecTime, LastRun, Name, Trig, Request, Action, TargetDB) VALUES ('',0,0, ".join(",",$keys).");";
            $req = $db[$confdb]->prepare($request);
            $req->execute($vals);
        }
        if ($param[0] == "tasks" && $param[1][0]== "del") {
            $confdb = getDB("config")[0];
            $request = "DELETE FROM TASKS WHERE Id = :Id";
            $vals = Array();
            $vals[":Id"]=$param[1][1];
            $req = $db[$confdb]->prepare($request);
            $req->execute($vals);
        }


        if ($param[0] == "task") {
            $self = '$SELF';
            if (!check_access("tasks")) {
                $self = check_self_access("tasks");
                if (!$self)
                    return "Permission denied";
            }
            $confdb = getDB("config")[0];
            $request = "SELECT * FROM TASKS WHERE Name = :Name";
            $vals = Array();
            $vals[":Name"]=$param[1][0];
            $req = $db[$confdb]->prepare($request);
            $req->execute($vals);
            $data = $req->fetchall(PDO::FETCH_ASSOC);
            $confdb = $data[0]['TargetDB'];
            $CURRENT_DB=$confdb;
            $fullfilter = build_advanced_filter($param[1], $self, $param[1][0]);

            $req = $db[$confdb]->prepare($fullfilter[0]);
            $req->execute($fullfilter[1]);
            $data = $req->fetchall(PDO::FETCH_ASSOC);
            return $data;
        }

    }

    function cron() {
        global $db;
        global $DBA;
        echo "Starting CRON from Console\n";
        $confdb = getDB("config")[0];
        $req = $db[$confdb]->prepare("SELECT *, TIMESTAMPDIFF(MINUTE,`LastRun`,NOW()) AS Delta FROM TASKS WHERE Trig != 'Never' ORDER BY LastRun DESC;");
        $req->execute();
        $data = $req->fetchall(PDO::FETCH_ASSOC);
        foreach ($data as $task) {
            if ( $task['Delta'] == ""
                 || $task['Delta'] == "NULL"
                 || ($task['Delta']>=60 && $task['Trig']=="hourly")
                 || ($task['Delta']>60*24 && $task['Trig']=="daily")) {

                echo "Task ".$task['Name']." is starting ".$task['Delta']." M. Late\n";
                $start = microtime(true);
                $req = $db[$confdb]->prepare("UPDATE TASKS SET LastRun=NOW() WHERE Id=".$task['Id'].";");
                $req->execute();

                /* Run request / Might be changed to internal request */
                echo "Run query: http://127.0.0.1/robinhood/api/index.php?request=".$task['Request']."\n";
                $ctx = stream_context_create(array('http'=>
                            array(
                                'timeout' => 1200,
                                )
                            ));

                $data = json_decode(file_get_contents("http://127.0.0.1/robinhood/api/index.php?request=".$task['Request'], false, $ctx), true);

                /* Build fields array for table create and insert */
                $fields = Array();
                $build = Array();

                $insert_fields = Array();
                $value_fields = Array();



                foreach ($data[0] as $k=>$v) {
                    if (is_numeric($v)) {
                        $fields[$k]="INT";

                        foreach ($data as $dk=>$dv) {
                            if (!is_numeric($dv[$k]) && $dv[$k]!="") {
                                $fields[$k]="VARCHAR(255)";
                            }
                        }
                    } else {
                        $fields[$k]="VARCHAR(255)";
                    }

                    array_push($build,$k." ".$fields[$k]);
                    array_push($insert_fields,$k);
                    array_push($value_fields,":".$k);
                }
                if ($task['Action']=='history') {
                    echo "Task: History action\n";
                    $bfields="( CronDate TIMESTAMP, ".join(", ",$build).")";
                    $ifields="( CronDate, ".join(", ",$insert_fields).")";
                    $vfields="( NOW(), ".join(", ",$value_fields).")";
                } else {
                    echo "Task: Copy action\n";
                    $bfields="(".join(", ",$build).")";
                    $ifields="(".join(", ",$insert_fields).")";
                    $vfields="(".join(", ",$value_fields).")";
                }


                $table = $task['Name'];
                /* Check target DB & Table */
                $result = $db[$task['TargetDB']]->query("SELECT * FROM information_schema.columns WHERE (table_name = '$table') AND TABLE_SCHEMA = '".$DBA[$task['TargetDB']]["DB_NAME"]."';");
                if ($result->rowCount()<1) {
                    echo "Task: Create new table: CREATE TABLE IF NOT EXISTS $table $bfields";
                    $db[$task['TargetDB']]->query("CREATE TABLE IF NOT EXISTS $table $bfields");
                }

                if ($task['Action']=='History') {
                    $db[$task['TargetDB']]->query("TRUNCATE TABLE $table");
                }
                foreach ($data as $line) {
                    $dict = Array();
                    foreach ($insert_fields as $f) {
                        $val =  $line[$f];
                        $dict[":".$f] = $line[$f];
                    }
                    $req = $db[$task['TargetDB']]->prepare("INSERT INTO $table $ifields VALUES $vfields");
                    $req->execute($dict);
                }


                $req = $db[$confdb]->prepare("UPDATE TASKS SET ExecTime=".(microtime(true)-$start)." WHERE Id=".$task['Id'].";");
                $req->execute();
                echo "Task Done \n";

            }
        }
        return $data;
    }

}
