<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
?>


<table cellpadding="0" cellspacing="0" border="0" class="display" id="jQueryTable">
    <thead>
        <tr>
            <th>Group</th>
            <th>Space used</th>
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
                echo "<td><a href='".str_replace( " ", "%20", $owner)."_group_popup.php'
                        rel='#overlay'>".$owner."</a></td>";
                ?>
                <td align="right"><?php echo $size[$owner]; ?></td>
                <td align="right"><?php echo $count[$owner]; ?></td>
            </tr>
        <?php
        }
        ?>
   </tbody>

</table>



<!-- POPUP -->
<div class="apple_overlay" id="overlay">
    <!-- the group detailed stat is loaded inside this tag -->
    <div class="contentWrap"></div>
</div>


