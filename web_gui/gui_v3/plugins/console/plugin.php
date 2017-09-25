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
 * Console V0.1
 * Use the API to export data
 *
 */

class console extends Plugin {
    public $Name="Console";
    public $Description="Use API to Export Data";
    public $Version="V0.1";

    private $output = false;

    /* Called from UI menu */
    function ui_header($param) {
            $newparam='<script src="plugins/console/script.js"></script>'."\n";
            $param=$param.$newparam;
            return $param;
    }

    /* Called from UI menu */
    function ui_menu_top($param) {
            $newparam="<li><a href='#' onclick='console_GetInfo()'>Console</a></li>\n";
            $param=$param.$newparam;
            return $param;
    }

}

