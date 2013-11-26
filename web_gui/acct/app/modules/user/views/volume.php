<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
    /*TODO Generalize pie chart generation*/
    $graph = new ezcGraphPieChart();
    $graph->palette = new ezcGraphPaletteEzBlue();
    $graph->legend = false; 
    $graph->data['Volume per user'] = new ezcGraphArrayDataSet( $top_size );

    $graph->data['Volume per user']->highlight['Others'] = true;

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
    $graph->render( 532, 195, 'app/img/graph/userVolumePieGraph.png' );

    echo '<h2>Space used per user</h2>';
    
?>

<img src="app/img/graph/userVolumePieGraph.png"/>
<table class="simple">
     <thead>
        <tr>
            <th>User</th>
            <th>Space used</th>
            <th>Count</th>
        </tr>
    </thead>
    <tbody>
        <?php

        reset( $top_size );
        $count = $statistics->getCount();
        for ($i = 0; $i < sizeof($top_size)-1; $i++)
        {
            $user = key($top_size);
            ?>
            <tr>
                <td>
                     <?php echo "<a href='".str_replace( " ", "%20", $user).
                        "_user_popup.php'rel='#volume'>".$user."</a>"; ?>
                </td>
                <td><?php echo formatSizeNumber( $top_size[$user] ); ?></td>
                <td><?php echo formatNumber( $count[$user] ); ?></td>
            </tr>
            <?php
            next($top_size);
        }
    ?>
    </tbody>
</table>

<!-- POPUP -->
<div class="apple_overlay" id="volume">
    <!-- the user detailed stat is loaded inside this tag -->
    <div class="contentWrap"></div>
</div>

