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
 * ColorGraph V0.1
 * Just keep the n in uid/gid charts and stack the others
 *
 */

class colorgraph extends Plugin {
    public $Name="Color Graph";
    public $Description="Clean Graph colors";
    public $Version="V0.1";

    /*
     * Plugin options
     */

    public function init() {
    }

    public function graph_postdata_uid($param) {

        $c_count=count($param['labels']);
        for($i = 0; $i<$c_count;$i++) {
            $alt = $i%2;
            $r=round(($i+1)*220/$c_count)+16;
            $g=(240-$r)+16;
            $b=230*$alt+16;
            $color = "#".dechex($r).dechex($g).dechex($b);
            $param['datasets'][0]['backgroundColor'][$i]=$color;
            $param['datasets'][1]['backgroundColor'][$i]=$color;
            }
        return $param;
    }

}

