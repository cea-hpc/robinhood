<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
?>
<ul class="tabs">
    <li><a href="#volume">Volume</a></li>
    <li><a href="#count">Count</a></li>
    <li><a href="#list">List</a></li>
</ul>

<div class="panes">
    <div>
        <center>
            <?php include 'volume.php'; ?>
            <br/><a href="#list" class="classic_link">View all groups</a>
        </center>
    </div>
    <div>
        <center>
            <?php include 'count.php'; ?>
            <br/><a href="#list" class="classic_link">View all groups</a>
        </center>
    </div>
    <div><?php include 'list.php'; ?></div>
</div>

