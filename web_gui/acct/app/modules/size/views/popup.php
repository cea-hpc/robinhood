<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

$user=$_GET['user'];

echo "<h1>User ".$user."</h1>";
echo "<hr/>";

    global $sz_range_fields;
    global $sz_range_name;

    // summary table

    $tab = array( array() );
    $i = 0;
    foreach( $user_info as $g => $vals )
    {
        if ($vals[COUNT] > 0)
            $avg = round($vals[SIZE] / $vals[COUNT], 0);
        else
            $avg = 0;

        $tab[$i][] = "$user/$g";
        $tab[$i][] = formatSizeNumber($vals[SIZE]);
        $tab[$i][] = formatNumber($vals[COUNT]);
        $tab[$i][] = formatSizeNumber($avg);

        $i++;
    }

    $header = "<thead> <tr>";
    $header = $header."<th>User/Group</th>";
    $header = $header."<th>Volume</th>";
    $header = $header."<th>File count</th>";
    $header = $header."<th>Avg Size</th>";
    $header = $header."</tr> </thead>";

    generateMergedTable( $tab, $header );

    // profile table
    $sz_ranges = array();
    $tab = array( array() );

    $header = "<thead> <tr>";
    foreach ($sz_range_name as $range)
        $header = $header . '<th>'.$range.'</th>';
    $header = $header."</tr> </thead>";

    $i=0;
    foreach( $user_info as $g => $vals )
    {
        for ($j=0; $j < count($sz_range_fields); $j++)
        {
            $tab[$i][] = $vals[$sz_range_fields[$j]];
            if (isset($sz_ranges[$sz_range_name[$j]]))
                $sz_ranges[$sz_range_name[$j]] += $vals[$sz_range_fields[$j]];
            else
                $sz_ranges[$sz_range_name[$j]] = $vals[$sz_range_fields[$j]];
        }
        $i++;
    }
    echo '<h2 style="line-height:1;">File count per size range</h2>';
    generateMergedTable( $tab, $header );

    // build profile chart

    $graph = new ezcGraphBarChart();
    $graph->palette = new ezcGraphPaletteEzBlue();
    $graph->legend = false; 
    $graph->data['File size profile'] = new ezcGraphArrayDataSet($sz_ranges);

    $graph->xAxis->label = 'Size range';
    $graph->yAxis->label = 'File count';

    $graph->driver = new ezcGraphGdDriver();
    $graph->options->font = 'app/img/KhmerOSclassic.ttf';

    $graph->driver->options->imageFormat = IMG_PNG; 
    $graph->render( 532, 300, 'app/img/graph/sizeCountBarGraph-'.$user.'.png' );

    echo '<br><img src="app/img/graph/sizeCountBarGraph-'.$user.'.png" />';
    echo '<br><i>Size repartition for user '.$user.'</i>';

?>

