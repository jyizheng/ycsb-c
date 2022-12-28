#/bin/bash

rm splinterdb.db
rm splinterdb_cache.log
rm /mnt/splinterdb.db
rm /mnt/splinterdb_cache.log

rm -rf /mnt/rocksdb.db/

io_start=`iostat -p  | grep nvme`

#/home/betrfs/linux-5.9.15/tools/perf/perf record -ag -o perf.log  -- \

ls -lash /app/untrusted_memory 

start=$(date +%s%N)

#gdb --args \
SCONE_VERSION=1 SCONE_HEAP=98566144 /home/betrfs/tweezer/Docker/ycsb-c/ycsbc -db rocksdb \
        -threads 1 \
        -L workloads/load.spec -w fieldlength 1024 -w fieldcount 1 -w recordcount 2000000 \
        -W workloads/workloada.spec -w operationcount 1000000

end=$(date +%s%N)

echo "Total Time is (ns):"
echo "print $end - $start" | python

iostat -p  | grep Device
io_end=`iostat -p  | grep nvme`
echo ${io_start}
echo ${io_end}
