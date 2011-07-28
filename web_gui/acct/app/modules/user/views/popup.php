<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

echo "<h1>".$_GET['user']."</h1>";
echo "<hr/>";

    $tab = array( array() );
    $i = 0;
    foreach( $result as $line )
    {
        if( array_key_exists( GROUP, $acct_schema ) ) 
            $tab[$i][] = $line[GROUP];
        if( array_key_exists( TYPE, $acct_schema ) )
            $tab[$i][] = $line[TYPE];
        if( array_key_exists( STATUS, $acct_schema ) )
            $tab[$i][] = $line[STATUS];
        if( array_key_exists( BLOCKS, $acct_schema ) )
            $tab[$i][] = $line[BLOCKS];
        if( array_key_exists( SIZE, $acct_schema ) )
            $tab[$i][] = $line[SIZE];
        if( array_key_exists( COUNT, $acct_schema ) )
            $tab[$i][] = $line[COUNT];
        $i++;
    }
    $header = "<thead> <tr>";
    if( array_key_exists( GROUP, $acct_schema ) )
        $header = $header."<th>Group</th>";
    if( array_key_exists( TYPE, $acct_schema ) )
        $header = $header."<th>Type</th>";
    if( array_key_exists( STATUS, $acct_schema ) )
        $header = $header."<th>Status</th>";
    $header = $header."<th>Blocks</th>";
    $header = $header."<th>Size</th>";
    $header = $header."<th>Count</th>";
    $header = $header."</tr> </thead>";
    generateMergedTable( $tab, $header );
?>



