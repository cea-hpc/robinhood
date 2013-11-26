<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
    $graph = new ezcGraphBarChart();
    $graph->palette = new ezcGraphPaletteEzBlue();
    $graph->legend = false; 
    $graph->data['File size profile'] = new ezcGraphArrayDataSet($sz_ranges);

    $graph->xAxis->label = 'Size range';
    $graph->yAxis->label = 'File count';

    $graph->driver = new ezcGraphGdDriver();
    $graph->options->font = 'app/img/KhmerOSclassic.ttf';

    $graph->driver->options->imageFormat = IMG_PNG; 
    $graph->render( 532, 300, 'app/img/graph/sizeCountBarGraph.png' );

    echo '<h2>File size profile</h2>';

?>

<img src="app/img/graph/sizeCountBarGraph.png"/>
<table class="simple">
     <thead>
        <tr>
            <th rowspan="2">User</th>
            <th rowspan="2">Total volume</th>
            <th rowspan="2">File count</th>
            <th rowspan="2">Avg file size</th>
            <th colspan="4">file size ratio</th>
        </tr>
        <tr>
            <th>empty</th>
            <th>&lt;1K</th>
            <th>&lt;1M</th>
            <th>&lt;1G</th>
        </tr>
    </thead>
    <tbody>
        <?php
            global $sz_range_fields;
            $i=0;
            foreach ($users as $u => $arr)
            {
                $i++;
                $nbz = $users[$u][$sz_range_fields[0]];
                $nb1K = $nbz + $users[$u][$sz_range_fields[1]] + $users[$u][$sz_range_fields[2]];
                $nb1M = $nb1K + $users[$u][$sz_range_fields[3]] + $users[$u][$sz_range_fields[4]];
                $nb1G = $nb1M + $users[$u][$sz_range_fields[5]] + $users[$u][$sz_range_fields[6]];
                if ($users[$u][COUNT] == 0)
                    $rz = $r1K = $r1M = $r1G = 0.0;
                else
                {
                    $rz = 1.0 * $nbz / $users[$u][COUNT];
                    $r1K = 1.0 * $nb1K / $users[$u][COUNT];
                    $r1M = 1.0 * $nb1M / $users[$u][COUNT];
                    $r1G = 1.0 * $nb1G / $users[$u][COUNT];
                }
               
                echo "<tr><td><a href='".str_replace( " ", "%20", $u). "_size_popup.php' rel='#count'>".$u."</a></td>";
                echo "<td>".formatSizeNumber($users[$u][SIZE])."</td>";
                echo "<td>".formatNumber($users[$u][COUNT])."</td>";
                echo "<td>".formatSizeNumber(round($users[$u][SIZE]/$users[$u][COUNT],0))."</td>";
                echo "<td>". sprintf("%.2f%%", 100.0*$rz)."</td>";
                echo "<td>". sprintf("%.2f%%", 100.0*$r1K)."</td>";
                echo "<td>". sprintf("%.2f%%", 100.0*$r1M)."</td>";
                echo "<td>". sprintf("%.2f%%", 100.0*$r1G)."</td></tr>\n";

                if ($i>LIMIT)
                    break;
            }
        ?>
    </tbody>
</table>



<!-- POPUP -->
<div class="apple_overlay" style="width:800px;" id="count">
    <!-- the status detailed stat is loaded inside this tag -->
    <div class="contentWrap" style="height:800px;"></div>
</div>

