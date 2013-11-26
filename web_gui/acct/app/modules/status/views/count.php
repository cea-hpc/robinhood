<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
    $graph = new ezcGraphPieChart();
    $graph->palette = new ezcGraphPaletteEzBlue();
    $graph->legend = false; 
    $graph->data['File number per status'] = new ezcGraphArrayDataSet( $top_count );

    $graph->data['File number per status']->highlight['Others'] = true;

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
    $graph->render( 532, 195, 'app/img/graph/statusCountPieGraph.png' );

    echo '<h2>File count per status</h2>';

?>

<img src="app/img/graph/statusCountPieGraph.png"/>
<table class="simple">
     <thead>
        <tr>
            <th>Status</th>
            <th>Space used</th>
            <th>Count</th>
        </tr>
    </thead>
    <tbody>
        <?php
        reset( $top_count );
        $blks = $statistics->getBlocks();
        for ($i = 0; $i < sizeof($top_count)-1; $i++)
        {
            $status = key($top_count);
            ?>
            <tr>
                <td>
                    <?php echo "<a href='".str_replace( " ", "%20", $status).
                        "_status_popup.php'rel='#count'>".$status."</a>"; ?>
                </td>
                <td><?php echo formatSizeNumber( $blks[$status] *DEV_BSIZE); ?></td>
                <td><?php echo formatNumber( $top_count[$status] ); ?></td>
            </tr>
            <?php
            next($top_count);
        }
    ?>
    </tbody>
</table>



<!-- POPUP -->
<div class="apple_overlay" id="count">
    <!-- the status detailed stat is loaded inside this tag -->
    <div class="contentWrap"></div>
</div>

