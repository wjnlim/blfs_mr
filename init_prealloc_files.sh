#! /bin/bash

if [ $# -ne 1 ]; then
	echo "Usage: $0 <shared_dir>"
	exit 1
fi

shared_dir="$1"
if [[ $shared_dir != */ ]]; then
	shared_dir="$shared_dir/"
fi
shfile_list="sharedfiles"

for entry in $(cat $shfile_list)
do
    shf_name="$(echo $entry | cut -d':' -f1)"
	size=$(echo $entry | cut -d':' -f2-)

	dd if=/dev/zero of="$shared_dir$shf_name" bs="$size" count=1
done

echo "syncing..."
sync

