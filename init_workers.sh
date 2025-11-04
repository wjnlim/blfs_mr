#! /bin/bash

workerlist="workers"
workers_n_splits="workers_n_splits"

if [ ! -f "$workerlist" ]; then
    echo "Error: File '$workerlist' not found."
    exit 1
fi

# create worker_n_splits file in current dir
if [ ! -f "$workers_n_splits" ]; then
    for worker in $(cat $workerlist | awk -F: '{print $1}');
    do
        echo "${worker}:0" >> $workers_n_splits
    done
fi

# create dirs in each worker
for line in $(cat $workerlist);
do
    worker="$(echo $line | awk -F: '{print $1}')"
    ip="$(echo $line | awk -F: '{print $2}')"
    # echo $ip
    ssh "$ip" 'mkdir -p $MRWSI_HOME/data/inputs; mkdir $MRWSI_HOME/data/outputs;\
    mkdir $MRWSI_HOME/mnt'
    for l in $(cat $workerlist);
    do
        w="$(echo $l | awk -F: '{print $1}')"
        ssh "$ip" "mkdir \$MRWSI_HOME/mnt/$w"
    done
done