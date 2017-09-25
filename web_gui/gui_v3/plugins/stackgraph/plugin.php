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
 * StackGraph V0.1
 * Just keep the n in uid/gid charts and stack the others
 *
 */

class stackgraph extends Plugin {
    public $Name="Stack Graph";
    public $Description="Stack users";
    public $Version="V0.1";

    /* sort key, ssize, scount or off  */
    public $uid_sortby='ssize';
    /* max_items to display, stack the others */
    public $max_items=10;
    /* strings for others */
    public $other_string="Others";

    /* Called from api just processing data */
    function api_process($param) {
        if ($param[0]=="graph")
        {
            $p_count=count($param[1]);
            for ($i=0; $i<$p_count-1;$i++) {
                if ($param[1][$i]="stackgraph") {
                    switch ($param[1][$i+1]) {
                    case "size":
                        $this->uid_sortby="ssize";
                        break;
                    case "count":
                        $this->uid_sortby="scount";
                        break;
                    case "off":
                        $this->uid_sortby="off";
                        break;
                    }
                }
            }
        }
    }

    public function graph_presql_uid($param) {
        if ($this->uid_sortby=="off")
            return NULL;

        $param = $param." ORDER BY ".$this->uid_sortby." DESC";
        return $param;
    }

    public function graph_postdata_uid($param) {
        if ($this->uid_sortby=="off")
            return NULL;

        $c_count=count($param['labels']);
        $c_trunk = $this->max_items;
        if ($c_count > $this->max_items) {
            $param['labels'][$this->max_items]=$this->other_string;
            for($i = $c_trunk; $i<$c_count-1;$i++) {
                $param['datasets'][0]['data'][$c_trunk]+=$param['datasets'][0]['data'][$i+1];
                $param['datasets'][1]['data'][$c_trunk]+=$param['datasets'][1]['data'][$i+1];
                unset($param['datasets'][0]['data'][$i+1]);
                unset($param['datasets'][1]['data'][$i+1]);
                unset($param['datasets'][0]['backgroundColor'][$i+1]);
                unset($param['datasets'][1]['backgroundColor'][$i+1]);
                unset($param['labels'][$i+1]);
            }
        }
        return $param;
    }

    /* Called from UI in form filter */
    function ui_form_filter($param) {
        $newparam= <<<EOT
            <fieldset class="form-group">
            <label>StackGraph</label>
            <select class="form-control" id="stackgraph" name="formstackgraph">
                <option value="off">Off</option>
                <option selected value="size">Size</option>
                <option value="count">Count</option>
            </select>
            </fieldset>
EOT;
        $param=$param.$newparam;
        return $param;
    }
}

