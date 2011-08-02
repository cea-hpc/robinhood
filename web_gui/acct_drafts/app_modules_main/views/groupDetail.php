<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

echo "<h1>".$_GET['group']."</h1>";

//Array with user as key and rowspan as value
$user_rowspan = array();

/*If there is type info, */
if( array_key_exists( TYPE, $acct_schema ) && array_key_exists( OWNER, $acct_schema ) )
{
    $current_user = "";
    $current_rowspan = 0;
    foreach( $result as $line )
    {
        if( $line[OWNER] != $current_user )
        {
            $current_rowspan = 1;
            $user_rowspan[$line[OWNER]] = $current_rowspan;
            $current_user = $line[OWNER];
        }
        else
        {
            $user_rowspan[$line[OWNER]] = $current_rowspan+1;
        }
    }
}

?>

<hr/>

<table class="simple">
     <thead>
        <tr>
            <?php if( array_key_exists( OWNER, $acct_schema ) ) echo "<th>User</th>"; ?>
            <?php if( array_key_exists( TYPE, $acct_schema ) ) echo "<th>Type</th>"; ?>
            <th>Blocks</th>
            <th>Size</th>
            <th>Count</th>
        </tr>
    </thead>
    <tbody>
        <?php
        $current_user = "";
        foreach( $result as $line )
        {
            echo "<tr>";
            if( array_key_exists( OWNER, $acct_schema ) && $line[OWNER] != $current_user )
            {
                $current_user = $line[OWNER];
                echo "<th rowspan='".$user_rowspan[$line[OWNER]]."'>".$line[OWNER]."</th>";
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

