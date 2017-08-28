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
 * Browser V0.1
 * Just browse your file system
 *
 */

class browser extends Plugin {
    public $Name="Browser";
    public $Description="Browser";
    public $Version="V0.1";

    /*
     * Plugin options
     */

    public function init() {
    }

    /* Called from UI menu */
    function ui_header($param) {
            $newparam="";
            $newparam.='<link rel="stylesheet" href="plugins/browser/css/bootstrap-treeview.css">\n';
            $newparam.='<script src="plugins/browser/js/bootstrap-treeview.js"></script>\n';
            $newparam.='<script src="plugins/browser/script.js"></script>\n';
            $param=$param.$newparam;
            return $param;
    }

    /* Called from UI menu */
    function ui_menu_top($param) {
            $newparam="";
            $newparam.="<li><a href='#' onclick='browser_GetInfo()'>Browser</a></li>";
            $param=$param.$newparam;
            return $param;
    }

}

