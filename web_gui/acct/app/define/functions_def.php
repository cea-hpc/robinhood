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
    else if ( $number > 1024 && $number < 1048576 )
        return ( round( $number/1024 , 2 ) ).' KB';
    else if ( $number > 1048576 && $number < 1073741824 )
        return ( round ( $number/1048576 , 2 ) ).' MB';
    else if ( $number > 1073741824 && $number < 1099511627776 )
        return ( round( $number/1073741824 , 2 ) ).' GB';
    else if ( $number > 1099511627776 && $number < 1125899906842624 )
        return ( round( $number/1099511627776 , 2 ) ).' TB';
    else
        return ( round( $number/1125899906842624 , 2 )  ).' PB';
}
