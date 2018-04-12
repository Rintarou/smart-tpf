#!/bin/bash

FILE=$1+".csv"
echo "QueryNumber, LDFtime(ms), ZZtime(ms), soundness, ldftimeout, zzTimeout" > $FILE

# while read -r; do
#   line="$REPLY"
#   echo "$REPLY"
# done

for FILENAME in queries/*.rq;
do
  node client.js $FILENAME >> $FILE
done

# node client.js queries/query_1017.rq >> $FILE
