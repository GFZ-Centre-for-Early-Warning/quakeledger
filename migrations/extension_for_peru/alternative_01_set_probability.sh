#!/bin/bash

#
# This script sets the probability of all the peru events
# that are stoastic and doesn't have a probability to a
# specific value in order to make them useful.
#

# use this script only if you used
# a version of add_csv_data_to_events.py
# that doesn't handled the default value for
# the probability

VALUE=0.1

cat <<EOF | sqlite3 ../sqlite3.db
update events
set probability = $VALUE
where eventid like 'peru%'
and type = 'stochastic'
and probability is null;
EOF
