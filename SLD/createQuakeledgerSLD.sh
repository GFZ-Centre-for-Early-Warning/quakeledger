#!/bin/bash

MAG[0]="0 2"
MAG[1]="2 3"
MAG[2]="3 4"
MAG[3]="4 4.5"
MAG[4]="4.5 5"
MAG[5]="5 5.5"
MAG[6]="5.5 6"
MAG[7]="6 6.5"
MAG[8]="6.5 7"
MAG[9]="7 7.5"
MAG[10]="7.5 8"
MAG[11]="8 8.5"
MAG[12]="8.5 9"
MAG[13]="9 X"

# above magnitude 4 following formula is used:
# INT(x * x / 1.5)
RADIUS[0]="2"
RADIUS[1]="4"
RADIUS[2]="8"
RADIUS[3]="12"
RADIUS[4]="15"
RADIUS[5]="18"
RADIUS[6]="22"
RADIUS[7]="26"
RADIUS[8]="30"
RADIUS[9]="35"
RADIUS[10]="40"
RADIUS[11]="45"
RADIUS[12]="51"
RADIUS[13]="60"

DEPTH[0]="0 20"
DEPTH[1]="20 50"
DEPTH[2]="50 100"
DEPTH[3]="100 250"
DEPTH[4]="250 500"
DEPTH[5]="500 800"
DEPTH[6]="800 X"

COLOR[0]="#ff0000"
COLOR[1]="#ff7f00"
COLOR[2]="#ffff00"
COLOR[3]="#00ff00"
COLOR[4]="#0000ff"
COLOR[5]="#7f00ff"
COLOR[6]="#000000"

cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/StyledLayerDescriptor.xsd" xmlns:se="http://www.opengis.net/se">
  <NamedLayer>
    <se:Name>selected_rows</se:Name>
    <UserStyle>
      <se:Name>selected_rows</se:Name>
      <se:FeatureTypeStyle>
EOF

for i in $(seq 0 13); do
    SNIPPETFILE="snippet.xml"
    MAG_FT=( ${MAG[$i]} )

    for j in $(seq 0 6); do
        DEP_FT=( ${DEPTH[$j]} )
        NAME="MAGNITUDE FROM: ${MAG_FT[0]} TO: ${MAG_FT[1]} RADIUS: ${RADIUS[$i]} ; DEPTH FROM: ${DEP_FT[0]} TO: ${DEP_FT[1]} COLOR: ${COLOR[$j]}"

        if [ "${MAG_FT[1]}" == "X" ]; then
            SNIPPETFILE="snippet_magX.xml"
        fi

        if [ "${DEP_FT[1]}" == "X" ]; then
            SNIPPETFILE="snippet_depX.xml"
        fi

        if [ "${MAG_FT[1]}" == "X" -a "${DEP_FT[1]}" == "X" ]; then
            SNIPPETFILE="snippet_magXdepX.xml"
        fi

        sed -e "s@__NAME__@${NAME}@" \
            -e "s@__MAG_FROM__@${MAG_FT[0]}@" \
            -e "s@__MAG_TO__@${MAG_FT[1]}@" \
            -e "s@__RADIUS__@${RADIUS[$i]}@" \
            -e "s@__DEPTH_FROM__@${DEP_FT[0]}@" \
            -e "s@__DEPTH_TO__@${DEP_FT[1]}@" \
            -e "s@__COLOR__@${COLOR[$j]}@" \
            "${SNIPPETFILE}"
    done
done

cat <<EOF
      </se:FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
EOF
