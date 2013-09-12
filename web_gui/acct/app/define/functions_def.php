<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

/**
 * This method returns the number with space between every group of thousands
 * @param number
 * @return number formatted
 */
function formatNumber( $number )
{
    return number_format( $number, 0 , '.', ' ' );
}

/**
 * This method returns the number in byte unit
 * @param number
 * @return number formatted
 */
function formatSizeNumber( $number )
{
    if ( $number < 1024)
        return $number.' B';
    else if ( $number >= 1024 && $number < 1048576 )
        return ( round( $number/1024 , 2 ) ).' KB';
    else if ( $number >= 1048576 && $number < 1073741824 )
        return ( round ( $number/1048576 , 2 ) ).' MB';
    else if ( $number >= 1073741824 && $number < 1099511627776 )
        return ( round( $number/1073741824 , 2 ) ).' GB';
    else if ( $number >= 1099511627776 && $number < 1125899906842624 )
        return ( round( $number/1099511627776 , 2 ) ).' TB';
    else
        return ( round( $number/1125899906842624 , 2 )  ).' PB';
}

function generateMergedTable( $tab, $header )
{
    echo "<table class='simple'>";
    echo "<thead>";
    echo $header;
    echo "</thead>";
    echo "<tbody>";
    $tabResult = "";
    $check_fusion = array(); //fifo array

    for( $i = 0; $i < sizeof( $tab ); $i++ )
    {
        $tabResult = "<tr>\n";
        for( $j = 0; $j < sizeof( $tab[$i] ); $j++ )
        {
            /*TODO merge algorithm to be continued*/
            /*$rowspan = 1;
            if( ($i>0) && ( $tab[$i][$j] == $tab[$i-1][$j]) )
            {
                if( array_shift( $check_fusion ) == FALSE )
                {
                    echo "<td>".$tab[$i][$j]."</td>";
                }
            }
            else
            {
                for( $k = $i+1; $k < sizeof( $tab ); $k++ )
                {
                    if( $tab[$i][$j] == $tab[$k][$j] )
                    {
                        if( $j == 0 )
                        {
                                $rowspan = ($k - $i) + 1;
                                $check_fusion[] = TRUE;
                        }
                        else
                        {
                            if( $tab[$i][$j-1] == $tab[$i+1][$j-1] )
                            {
                                    $rowspan = ($k - $i) + 1;
                                    $check_fusion[] = TRUE;
                            }
                            else
                            {
                                    $check_fusion[] = FALSE;
                                    break;
                            }
                        }
                    }
                    else break;
                }

                $tabResult = $tabResult."<td rowspan='".$rowspan."'>".$tab[$i][$j]."</td>\n";*/
                $tabResult = $tabResult."<td>".$tab[$i][$j]."</td>\n";
                echo  $tabResult;
                $tabResult = "";
            //}
        }
        echo "</tr>\n";
    }
    echo "</tbody>";
    echo "</table>";
}

# convert a db path regexp to an array of mathing expressions
function db_path_match( $path )
{
	$to_match = array();
	# if the path ends with '/' => this is a directory
	# => find it or its sub entries
	if (substr($path, -1) == '/') {
		array_push( $to_match, $path.'%' );
		if ( strlen($path > 1) )
			array_push( $to_match, substr($path, 0, -1) );
	} else {
		 # else, find the exact match or 'entry/%'
		array_push( $to_match, $path );
		array_push( $to_match, $path.'/%' );
	}
	return $to_match;
}
