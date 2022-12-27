#/bin/bash

rm splinterdb.db
rm splinterdb_cache.log
rm /mnt/splinterdb.db
rm /mnt/splinterdb_cache.log

rm -rf /mnt/rocksdb.db/

iostat -p /dev/nvme0n1

#/home/betrfs/linux-5.9.15/tools/perf/perf record -ag -o perf.log  -- \

start=$(date +%s%N)


#gdb --args \
SCONE_VERSION=1 SCONE_HEAP=94371840 /home/betrfs/tweezer/Docker/ycsb-c/ycsbc -db rocksdb \
        -threads 1 \
        -L workloads/load.spec -w fieldlength 1024 -w fieldcount 1 -w recordcount 2000000 \
        -W workloads/workloada.spec -w operationcount 1000

end=$(date +%s%N)

echo "Total Time is (ns):"
echo "print $end - $start" | python

iostat -p /dev/nvme0n1


