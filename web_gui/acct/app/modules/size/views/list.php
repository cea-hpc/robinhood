<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
?>


<table cellpadding="0" cellspacing="0" border="0" class="display" id="jQueryTable">
    <thead>
        <tr style="font-size:small;">
            <th>User</th>
            <th>Total volume</th>
            <th>File count</th>
            <th>Avg file size</th>
            <?php
                global $sz_range_name;
                foreach ($sz_range_name as $range)
                    echo '<th>'.$range.'</th>';
            ?>
        </tr>
        <tr>
        </tr>

    </thead>
    <tbody>
        <?php
            global $sz_range_fields;
            foreach ($users as $u => $arr)
            {
                echo "<tr><td><a href='".str_replace( " ", "%20", $u)."_size_popup.php' rel='#overlay'>".$u."</a></td>";
                echo "<td>".$users[$u][SIZE]."</td>" ;
                echo "<td>".$users[$u][COUNT]."</td>" ;
                echo "<td>".round($users[$u][SIZE]/$users[$u][COUNT],0) ."</td>" ;
                for ($i=0; $i < count($sz_range_fields); $i++)
                {
                    echo '<td>'.$users[$u][$sz_range_fields[$i]].'</td>';
                }
                echo "</tr>\n";
            }
        ?>
   </tbody>

</table>



<!-- POPUP -->
<div class="apple_overlay" style="width:800px;" id="overlay">
    <!-- the status detailed stat is loaded inside this tag -->
    <div class="contentWrap" style="height:800px;"></div>
</div>



