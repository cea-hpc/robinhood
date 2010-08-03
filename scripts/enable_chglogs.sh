#!/bin/sh


#lctl set_param mdd.*.changelog on
lctl set_param mdd.*.changelog_mask "CREAT UNLNK OPEN CLOSE TRUNC TIME HSM"
lctl --device lustre-MDT0000 changelog_register
