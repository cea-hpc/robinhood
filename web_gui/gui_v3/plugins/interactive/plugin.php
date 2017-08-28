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
 * Interactive V0.1
 * Use the API to export data
 *
 */

class interactive extends Plugin {
    public $Name="Interactive";
    public $Description="Use API to Export Data";
    public $Version="V0.1";

    private $output = false;
    /*
     * Plugin options
     */

    public function init() {
    }

    /* Called from api just processing data */
    function api_process($param) {
        if ($param[0]=="native")
        {
            $p_count=count($param[1]);
            for ($i=0; $i<$p_count-1;$i++) {
                if ($param[1][$i]="output") {
                    /* @TODO switch */
                    if ($param[1][$i+1]=="csv")
                        $this->output="csv";
                }
            }
        }
    }

    /* Change the HTTP Header type */
    function api_header_type($param) {

            if ($this->output=="csv") {
                    $param="Content-Type: text";
            }
            return $param;
    }

    /* Called from api just before sending data */
    function api_response($param) {
            if ($this->output=="csv") {
                    $out="";
                    if ( count($param)==0) {
                            return null;
                    }
                    foreach ($param[0] as $key => $val) {
                            $out.=$key.";";
                    }
                    $out.="\n";
                    foreach ($param as $item) {
                            foreach ($item as $key => $val) {
                                    $out.=$val.";";
                            }
                            $out.="\n";
                    }
                    return $out;
            }

    }

    /* Called from UI menu */
    function ui_header($param) {
            $newparam="";
            $newparam.='<script src="plugins/interactive/script.js"></script>\n';
            $param=$param.$newparam;
            return $param;
    }

    /* Called from UI menu */
    function ui_menu_top($param) {
            $newparam="";
            $newparam.="<li><a href='#' onclick='interactive_GetInfo()'>Interactive</a></li>";
            $param=$param.$newparam;
            return $param;
    }

}

