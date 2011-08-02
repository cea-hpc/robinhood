<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

    $graph = new ezcGraphPieChart();
    $graph->palette = new ezcGraphPaletteEzBlue();
    $graph->legend = false; 
    $graph->data['Volume per '.$index] = new ezcGraphArrayDataSet( $size );

    $graph->data['Volume per '.$index]->highlight['Others'] = true;

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
    $graph->options->font = 'app/img/arial.ttf';

    $graph->driver->options->imageFormat = IMG_PNG; 
    $graph->render( 532, 195, 'app/img/graph/volumePieGraph.png' );

    echo '<h2>Volume per '.$index.'  </h2>';
    
?>

<img src="app/img/graph/volumePieGraph.png"/>
<table class="simple">
     <thead>
        <tr>
            <th><?php echo ucfirst($index); ?></th>
            <th>Space used (bytes)</th>
            <th>Count</th>
        </tr>
    </thead>
    <tbody>
        <?php
        reset( $size );
        $whole_count = $statistics->getCount();
        for ($i = 0; $i < sizeof($size)-1; $i++)
        {
            $key = key($size);
            ?>
            <tr>
                <td><?php echo $key; ?></td>
                <td><?php echo formatSizeNumber($size[$key]); ?></td>
                <td><?php echo formatNumber($whole_count[$key]); ?></td>
            </tr>
            <?php
            next($size);
        }
    ?>
    </tbody>
</table>
