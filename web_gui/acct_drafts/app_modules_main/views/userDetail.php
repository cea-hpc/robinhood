<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

echo "<h1>".$_GET['user']."</h1>";

//Array with group as key and rowspan as value
$group_rowspan = array();

/*If there is type info, */
if( array_key_exists( TYPE, $acct_schema ) && array_key_exists( GROUP, $acct_schema ) )
{
    $current_group = "";
    $current_rowspan = 0;
    foreach( $result as $line )
    {
        if( $line[GROUP] != $current_group )
        {
            $current_rowspan = 1;
            $group_rowspan[$line[GROUP]] = $current_rowspan;
            $current_group = $line[GROUP];
        }
        else
        {
            $group_rowspan[$line[GROUP]] = $current_rowspan+1;
        }
    }
}

?>

<hr/>

<table class="simple">
     <thead>
        <tr>
            <?php if( array_key_exists( GROUP, $acct_schema ) ) echo "<th>Group</th>"; ?>
            <?php if( array_key_exists( TYPE, $acct_schema ) ) echo "<th>Type</th>"; ?>
            <th>Blocks</th>
            <th>Size</th>
            <th>Count</th>
        </tr>
    </thead>
    <tbody>
        <?php
        $current_group = "";
        foreach( $result as $line )
        {
            echo "<tr>";
            if( array_key_exists( GROUP, $acct_schema ) && $line[GROUP] != $current_group )
            {
                $current_group = $line[GROUP];
                echo "<th rowspan='".$group_rowspan[$line[GROUP]]."'>".$line[GROUP]."</th>";
                if( array_key_exists( TYPE, $acct_schema ) )
                {
                    echo "<td>".$line[TYPE]."</td>";
                }
                echo "<td>".$line[BLOCKS]."</td>";
                echo "<td>".$line[SIZE]."</td>";
                echo "<td>".$line[COUNT]."</td>";
            }
            else
            {
                if( array_key_exists( TYPE, $acct_schema ) )
                {
                    echo "<td>".$line[TYPE]."</td>";
                }
                echo "<td>".$line[BLOCKS]."</td>";
                echo "<td>".$line[SIZE]."</td>";
                echo "<td>".$line[COUNT]."</td>";
                echo "</tr>";
            }
        }?>
    </tbody>
</table>

