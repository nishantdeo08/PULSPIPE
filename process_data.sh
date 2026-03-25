#!/bin/bash

# Usage: ./process_data.sh <file_name> <lower_threshold> <upper_threshold> <col_index> <order_flag>
INPUT_FILE=$1
LOWER_TH=$2
UPPER_TH=$3
COL_INDEX=$(( $4 + 1 )) # 1-based indexing for AWK/Sort
ORDER_FLAG=$5

if [ "$#" -ne 5 ]; then
    echo "Usage: $0 <file_name> <lower_th> <upper_th> <col_index> <order_flag (0:asc, 1:desc)>"
    exit 1
fi

# 1. Detect delimiter
DELIM=","
[[ "$INPUT_FILE" == *.tsv ]] || grep -q $'\t' "$INPUT_FILE" && DELIM=$'\t'

# 2. Generate Output Filename
BASE="${INPUT_FILE%.*}"
EXT="${INPUT_FILE##*.}"
ORDER_STR="increasing"
[[ "$ORDER_FLAG" == "1" ]] && ORDER_STR="decreasing"
OUTPUT_FILE="${BASE}_new.${EXT}"

# 3. Process the file
# awk handles the range: ($1 >= lower && $1 <= upper)
HEADER=$(head -n 1 "$INPUT_FILE")

(
  echo "$HEADER"; 
  tail -n +2 "$INPUT_FILE" | \
  awk -F"$DELIM" -v low="$LOWER_TH" -v high="$UPPER_TH" '$1 >= low && $1 <= high {print $0}' | \
  sort -t"$DELIM" -k"${COL_INDEX},${COL_INDEX}" -n $( [[ "$ORDER_FLAG" == "1" ]] && echo "-r" )
) > "$OUTPUT_FILE"

echo "Filtering ($LOWER_TH to $UPPER_TH) complete. Saved to: $OUTPUT_FILE"
