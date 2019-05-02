#!/bin/bash

rm -vf "test.xml"

set -x
python3 eventquery.py 288 292 -70 -10 6.6 8.5 5 140 0.1 deaggregation -71.5730623712764 -33.1299174879672
set +x

if ! [ -f "test.xml" ]; then
    echo "Output file was not created."
    exit 1
fi

echo "Output file 'test.xml' was created"

if [ -x "$(command -v xmllint)" ]; then
    xmllint --noout --schema "QuakeML-BED-1.2.xsd" "test.xml"
fi
