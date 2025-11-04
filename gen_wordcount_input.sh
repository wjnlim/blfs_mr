#! /bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <filename> <size ex)32M>"
    exit 1
fi

filename="$1"

if [ -f "$filename" ]; then
  echo "Error: File '$filename' is already exist."
  exit 1
fi

target_size="$(numfmt --from=auto $2)"

pool_size=2000
words_pool=$(shuf -n $pool_size /usr/share/dict/words)

chunk_size=50000
filesize=0
while [ $filesize -lt $target_size ]
do  
    {
        echo "$words_pool" | shuf -r -n $chunk_size |
        awk 'BEGIN {i=1}{printf "%s%s", $0, i % 10? " ": "\n"; ++i}END{if(i % 10)print ""}'
    } >> "$filename"

    filesize=$(stat -c%s "$filename")
done

tmp_file="${filename}_tmp"
# echo "$tmp_file"

# strip last partial word
head -c $target_size $filename | sed '$s/[^[:space:]]*$/\n/' > $tmp_file
mv $tmp_file $filename

echo "generated word count input $filename with size $(stat -c%s "$filename") bytes"
