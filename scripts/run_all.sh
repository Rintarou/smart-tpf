#!/bin/bash

FILE=$1
echo "Query Number, LDF Time (ms), ZZ Time (ms), Sound & Complete, ZZ Server Calls" > $FILE

# while read -r; do
#   line="$REPLY"
#   echo "$REPLY"
# done

for FILENAME in queries/*.rq;
do
  node clientLDF.js $FILENAME
  node client.js $FILENAME >> $FILE
done

# node client.js queries/query_1017.rq >> $FILE
