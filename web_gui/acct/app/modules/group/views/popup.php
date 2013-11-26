<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
* vim:expandtab:shiftwidth=4:tabstop=4:
*/

    $group = $_GET['group'];
echo "<h1>Group ".$group."</h1>";
echo "<hr/>";

    $tab = array( array() );
    $i = 0;
    foreach( $result as $line )
    {
        if( array_key_exists( OWNER, $acct_schema ) ) 
            $tab[$i][] = $line[OWNER];
        if( array_key_exists( TYPE, $acct_schema ) )
            $tab[$i][] = $line[TYPE];
        if( array_key_exists( STATUS, $acct_schema ) )
            $tab[$i][] = $line[STATUS];
        if( array_key_exists( BLOCKS, $acct_schema ) )
            $tab[$i][] = $line[BLOCKS];
        if( array_key_exists( SIZE, $acct_schema ) )
            $tab[$i][] = $line[SIZE];
        if( array_key_exists( COUNT, $acct_schema ) )
            $tab[$i][] = $line[COUNT];
        $i++;
    }
    $header = "<thead> <tr>";
    if( array_key_exists( OWNER, $acct_schema ) )
        $header = $header."<th>User</th>";
    if( array_key_exists( TYPE, $acct_schema ) )
        $header = $header."<th>Type</th>";
    if( array_key_exists( STATUS, $acct_schema ) )
        $header = $header."<th>Status</th>";
    $header = $header."<th>Blocks</th>";
    $header = $header."<th>Size</th>";
    $header = $header."<th>Count</th>";
    $header = $header."</tr> </thead>";
    // display the table
    generateMergedTable( $tab, $header );

    if (isset($group_status))
    {
        // build status chart
        /*TODO Generalize pie chart generation*/
        $graph = new ezcGraphPieChart();
        $graph->palette = new ezcGraphPaletteEzBlue();
        $graph->legend = false; 
        $title = 'Status repartition for group '.$group.' (count)';
        $graph->data[$title] = new ezcGraphArrayDataSet( $group_status );

        $graph->renderer = new ezcGraphRenderer3d();
        $graph->renderer->options->moveOut = .2;
        $graph->renderer->options->pieChartOffset = 63;
        $graph->renderer->options->pieChartGleam = .3;
        $graph->renderer->options->pieChartGleamColor = '#FFFFFF';
        $graph->renderer->options->pieChartGleamBorder = 2; 
        $graph->renderer->options->pieChartShadowSize = 5;
        $graph->renderer->options->pieChartShadowColor = '#BABDB6';
        $graph->renderer->options->pieChartHeight = 5;
        $graph->renderer->options->pieChartRotation = .8;
        $graph->driver = new ezcGraphGdDriver();
        $graph->options->font = 'app/img/KhmerOSclassic.ttf';
        $graph->driver->options->imageFormat = IMG_PNG; 
        // FIXME change png name depending on group
        $graph->render( 532, 195, 'app/img/graph/groupStatusPieGraph-'.$group.'.png' );

        echo '<h2>'.$title.'</h2>';
        echo '<img src="app/img/graph/groupStatusPieGraph-'.$group.'.png"/>';
    }
?>



