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
 * InternalStats V0.1
 * Just display informations from var table
 *
 */

class internalstats extends Plugin {
    public $Name = "Internal Stats";
    public $Description = "Add page with FS Information";
    public $Version = "V0.1";

    /* Called from UI menu */
    function ui_header($param) {
            $newparam = '<script src="plugins/internalstats/script.js"></script>'."\n";
            $param = $param.$newparam;
            return $param;
    }

    /* Called from UI menu */
    function ui_menu_top($param) {
            $newparam = "<li><a href='#' onclick='internalstats_GetInfo()'>Internal Stats</a></li>\n";
            $param = $param.$newparam;
            return $param;
    }

}

