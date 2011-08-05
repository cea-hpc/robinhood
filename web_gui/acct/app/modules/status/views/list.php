<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
?>


<table cellpadding="0" cellspacing="0" border="0" class="display" id="jQueryTable">
    <thead>
        <tr>
            <th>Status</th>
            <th>Space used</th>
            <th>Count</th>
        </tr>
    </thead>
    <tbody>
        <?php
        $blks = $statistics->getBlocks();
        $count = $statistics->getCount();
        foreach( $blks as $status => $size_value )
        {
        ?>
            <tr class="gradeB">
                <?php
                echo "<td><a href='".str_replace( " ", "%20", $status)."_status_popup.php'
                        rel='#overlay'>".$status."</a></td>";
                $spc = $blks[$status] * DEV_BSIZE;
                ?>
                <td align="right"><?php echo $spc; ?></td>
                <td align="right"><?php echo $count[$status]; ?></td>
            </tr>
        <?php
        }
        ?>
   </tbody>

</table>



<!-- POPUP -->
<div class="apple_overlay" id="overlay">
    <!-- the status detailed stat is loaded inside this tag -->
    <div class="contentWrap"></div>
</div>


