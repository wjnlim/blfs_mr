#! /bin/bash

if [ $# -lt 1 ];then
    echo "Usage: $0 <output_metainfo_file> -s"
    echo "Option:"
    echo " -s) print the output sorted"
    exit 1
fi

opts=$(getopt -o=s --name "$0" -- "$@") || exit 1
eval set -- "$opts"

workerlist="workers"
workers_n_splits="workers_n_splits"
worker_arr=()
metadata_file=""
# parse options
while true; do
    case "$1" in
        -s)
            sort="true"
            shift
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Unexpected option: $1"
            exit 1
            ;;
    esac
done

metainfo_file="$1"
workerlist="workers"
# workers_n_splits="workers_n_splits"
#Check file
if [[ "$metainfo_file" != *".meta" ]]; then
    echo "Error: File '$metainfo_file' is not metadata file."
    exit 1
fi
if [ ! -f "$metainfo_file" ]; then
    echo "Error: File '$metainfo_file' not found."
    exit 1
fi

output=$(
    for line in $(cat "$metainfo_file");
    do
        path="$(echo $line | awk -F: '{print $1}')"
        worker="$(echo $line | awk -F: '{print $2}')"

        ip="$(awk -F: "/^$worker/ {print \$2}" $workerlist)"

        ssh $ip "cat $path"
    done
)

if [[ "$sort" == "true" ]]; then
    output=$(echo "$output" | LC_COLLATE=C sort -k1)
fi

echo "$output"