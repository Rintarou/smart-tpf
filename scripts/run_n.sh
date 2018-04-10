#!/bin/bash

for FILENAME in queries/*.rq;
do
  echo -ne $FILENAME
  echo -ne "\t"
  node client.js $FILENAME
  echo ""
done
