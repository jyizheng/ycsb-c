#!/bin/bash

source ~/.bashrc

rm -rf rocksdb.db
rm -rf splinterdb.db


./ycsbc -db splinterdb \
        -threads 4 -L workloads/load.spec \
        -w fieldlength 1024 \
        -w fieldcount  1 \
        -w recordcount 2000000 \
        -W workloads/workloadc.spec \
        -w operationcount 1000000

