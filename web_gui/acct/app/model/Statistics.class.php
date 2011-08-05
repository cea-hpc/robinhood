<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */

class Statistics
{
    private $size = array();
    private $blocks = array();
    private $count = array();


    public function setSize( $size )
    {
        if( in_array( false, array_map( 'is_numeric', $size ) ) )
            throw new InvalidArgumentException("The size attribute must be a number (".$size." is not a number)");
        else
            $this->size = $size;
    }

    public function setBlocks( $blocks )
    {
        if( in_array( false, array_map( 'is_numeric', $blocks ) ) )
            throw new InvalidArgumentException('The blocks attribute must be a number');
        else
            $this->blocks = $blocks;
    }

    public function setCount( $count )
    {
        if( in_array( false, array_map( 'is_numeric', $count ) ) )
            throw new InvalidArgumentException('The count attribute must be a number');
        else
            $this->count = $count;
    }

    public function getSize()
    {
        return $this->size;
    }

    public function getBlocks()
    {
        return $this->blocks;
    }
    
    public function getCount()
    {
        return $this->count;
    }

}

?>
