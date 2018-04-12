#!/bin/bash

FILE="results.dat"
echo "# Query timing(ms)" > $FILE

# while read -r; do
#   line="$REPLY"
#   echo "$REPLY"
# done

# for FILENAME in queries/*.rq;
# do
#   echo -ne $FILENAME
#   echo -ne "\t"
#   node client.js $FILENAME
#   echo ""
# done

node client.js queries/query_1017.rq >> $FILE
