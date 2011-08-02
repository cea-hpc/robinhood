<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
    $graph = new ezcGraphPieChart();
    $graph->palette = new ezcGraphPaletteEzBlue();
    $graph->legend = false; 
    $graph->data['File number per '.$index] = new ezcGraphArrayDataSet( $count );

    $graph->data['File number per '.$index]->highlight['Others'] = true;

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
    $graph->render( 532, 195, 'app/img/graph/countPieGraph.png' );

    echo '<h2>File number per '.$index.'</h2>';

?>

<img src="app/img/graph/countPieGraph.png"/>
<table class="simple">
     <thead>
        <tr>
            <th><?php echo ucfirst( $index ); ?></th>
            <th>Space used</th>
            <th>Count</th>
        </tr>
    </thead>
    <tbody>
        <?php
        reset( $count );
        $whole_size = $statistics->getSize();
        for ($i = 0; $i < sizeof($count)-1; $i++)
        {
            $key = key($count);
            ?>
            <tr>
                <td><?php echo $key; ?></td>
                <td><?php echo formatSizeNumber($whole_size[$key]); ?></td>
                <td><?php echo formatNumber($count[$key]); ?></td>
            </tr>
            <?php
            next($count);
        }
    ?>
    </tbody>
</table>
