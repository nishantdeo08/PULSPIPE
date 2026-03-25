#!/bin/bash
# This script reads the global_pulscan_candidates_new.csv file and then creates files based on individual dm values

# 1. Check if an argument was actually provided
if [ -z "$1" ]; then
    echo "Usage: $0 <input_file.csv>"
    exit 1
fi

INPUT_FILE="$1"

# 2. Check if the file actually exists before processing
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: File '$INPUT_FILE' not found."
    exit 1
fi

# Get directory of the input file
INPUT_DIR="$(dirname "$INPUT_FILE")"

# Go one level up (to /output)
BASE_DIR="$(dirname "$INPUT_DIR")"

# Create new directory inside /output
OUTPUT_DIR="$BASE_DIR/dm_split_results"
mkdir -p "$OUTPUT_DIR"

echo "Processing $INPUT_FILE..."

# 4. Use awk to split the file
# -v passes the bash variable OUTPUT_DIR into awk
awk -F, -v outdir="$OUTPUT_DIR" '
NR==1 { 
    header = $0; 
    next 
} 
{
    # $8 is the DM column
    outfile = outdir "/DM_" $8 ".csv";
    
    if (!(outfile in seen)) {
        print header > outfile;
        seen[outfile] = 1;
    }
    print $0 >> outfile;
}' "$INPUT_FILE"

echo "Success! Files are located in the '$OUTPUT_DIR' directory."
