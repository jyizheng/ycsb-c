#gdb --args  
#./ycsbc -db splinterdb -threads 1 -L workloads/load.spec \
#        -w fieldlength 256 \
#        -w fieldcount  1 \
#        -w recordcount 1400000 \
#        -W workloads/workloada.spec \
#        -w operationcount 1000000 \
#        -p splinterdb.disk_size_gb 256


#./ycsbc -db splinterdb -threads 1 -L workloads/load.spec \
#        -w fieldlength 256 \
#        -w fieldcount  1 \
#        -w recordcount 5400000 \
#        -W workloads/workloadc.spec \
#        -w operationcount 2000000 \
#        -p splinterdb.disk_size_gb 256


source ~/.bashrc

#./ycsbc-scone \
#        -db splinterdb -threads 1 -L workloads/load.spec \
#        -w fieldlength 1024 \
#        -w fieldcount  1 \
#        -w recordcount 2000000 \
#        -W workloads/workloadf.spec \
#        -w operationcount 2000000 \
#        -p splinterdb.disk_size_gb 10

./ycsbc \
        -db splinterdb -threads 1 -L workloads/load.spec \
        -w fieldlength 1024 \
        -w fieldcount  1 \
        -w recordcount 1000000 \
        -W workloads/workloadf.spec \
        -w operationcount 1000000

