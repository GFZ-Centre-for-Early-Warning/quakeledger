#!/bin/bash

#
# This script sets the year of all the peru events
# that are stochastic and doesn't have a year to a
# specific value in order to make them useful.
#

# use this script only if oyu have a
# version of add_csv_data_to_events.py
# that does not handle the year properly.

VALUE=3000

cat <<EOF | sqlite3 ../sqlite3.db
update events
set year = $VALUE
where eventid like 'peru%'
and type = 'stochastic'
and year is null;
EOF
