<?php
/* -*- mode: php; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
?>
<ul class="tabs">
    <li><a href="#count">Global</a></li>
    <li><a href="#list">Users</a></li>
</ul>

<div class="panes">
    <div>
        <center>
            <?php include 'count.php'; ?>
            <br/><a href="#list" class="classic_link">View all user's profiles</a>
        </center>
    </div>
    <div><?php include 'list.php'; ?></div>
</div>

