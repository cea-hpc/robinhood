<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
?>


<table cellpadding="0" cellspacing="0" border="0" class="display" id="jQueryTable">
    <thead>
        <tr>
            <th><?php echo ucfirst( $index ); ?></th>
            <th>Size</th>
            <th>Count</th>
        </tr>
    </thead>
    <tbody>
        <?php
        $size = $statistics->getSize();
        $count = $statistics->getCount();
        foreach( $size as $owner => $size_value )
        {
        ?>
            <tr class="gradeB">
                <?php
                echo "<td><a href='".str_replace( " ", "%20", $owner)."_".$index."_popup.php'
                        rel='#overlay'>".$owner."</a></td>";
                ?>
                <td><?php echo $size[$owner]; ?></td>
                <td><?php echo $count[$owner]; ?></td>
            </tr>
        <?php
        }
        ?>
   </tbody>

</table>



<!-- POPUP -->
<div class="apple_overlay" id="overlay">
    <!-- the user detailed stat is loaded inside this tag -->
    <div class="contentWrap"></div>
</div>


