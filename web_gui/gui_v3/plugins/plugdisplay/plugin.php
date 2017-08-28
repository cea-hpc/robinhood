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
 * plugdisplay V0.1
 * Just keep the n in uid/gid charts and stack the others
 *
 */

class plugdisplay extends Plugin {
    public $Name="Plugins Display";
    public $Description="Display Information about plugins";
    public $Version="V0.1";

    /* Called from UI menu */
    function ui_menu_bottom($param) {
            global $PLUGINS_INST;
            $newparam="<div>";
            $newparam.="<br><label>Plugins</label>";
            $newparam.='<div class="list-group">';
            foreach ($PLUGINS_INST as $p) {
                    $newparam.=' <a href="#" class="list-group-item">'.$p->Name.' - '.$p->Version.'</a>';
            }
            $newparam.="</div>";
            $newparam.="</div>";
            $param=$param.$newparam;
            return $param;
    }

}

