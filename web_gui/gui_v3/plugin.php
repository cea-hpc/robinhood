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


/* Plugin template */

class Plugin {
    public $Name="Generic";
    public $Description="Something about your plugin";
    public $Version="0";

    /* Called from UI and api */
    function init() {
            /* Plugin init */
    }

    /* Called from jscript customlib */
    function jscript($param) {
            /* JS Required by the plugin */
    }

    /* Called from api before processing data */
    function api_preprocess() {
            /* Just before the API args parsing */
    }

    /* Called from api just before processing data */
    function api_process($param) {
           /* Just after API args parsing */
           /* $param = parsed args */

    }
    /* Called from API before sending json data of uid/gid for graphs*/
    function graph_postdata_uid($param) {
            /* Just before sending uid graph data */
            /* Whole json as args, for compatibility */
    }

    /* Called from API before sending json data of sizes for graphs */
    function graph_postdata_sizes($param) {
            /* Just before sending sizes graph data */
            /* Whole json as args, for compatibility */
    }

    /* Called from API before doing initial SQL request for uid/gid graphs */
    function graph_presql_uid($param) {
            /* The sql request as string */
    }

    /* Called from API before doing initial SQL request for sizes graphs */
    function graph_presql_sizes($param) {
            /* The sql request as string */
    }

    /* Called from UI in form filter */
    function ui_form_filter($param) {
            /* HTML in the filter form */
    }

    /* Called from UI in menu */
    function ui_menu_bottom($param) {
            /* HTML */
    }

    /* Called from UI in menu */
    function ui_menu_top($param) {
            /* HTML */
    }

    /* Called from UI in menu */
    function ui_header($param) {
            /* HTML inside <header></header> */
    }
}

/**
 *
 * Load all the plugins
 *
 * @return nothing
 */
function plugins_load() {
        global $PLUGINS_INST;
        global $PLUGINS_REG;

    foreach ($PLUGINS_REG as $p) {
        require_once "plugins/$p/plugin.php";
        $PLUGINS_INST[]=new $p();
    }
}

/**
 *
 * Call specific function from plugin
 *
 * @return nothing
 */
function plugins_call($function, $param=NULL) {
   global $PLUGINS_INST;
   foreach ($PLUGINS_INST as $p) {
           if ($param==NULL) {
                $p->$function();

        } else {
                $ret = $p->$function($param);
                if ($ret)
                    $param=$ret;
        }
   }
   return $param;
}

plugins_load();
plugins_call("init");
