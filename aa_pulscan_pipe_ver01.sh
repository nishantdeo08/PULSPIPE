#!/bin/bash

# --- 1. USER CONFIGURATION (Thresholds & Arguments) ---
# Usage: ./aa_pulscan_pipe.sh [data_path] [low_th] [high_th] [sort_col] [order_flag]
DATA_PATH=${1:-"/default/path/to/data.fil"}
LOW_TH=${2:-5}
HIGH_TH=${3:-100}
SORT_COL=${4:-2}
ORDER_FLAG=${5:-1}

# --- 2. PIPELINE PARAMETERS ---
# Harmonic Filtering
H_TOLERANCE=0.001
H_THREADS=8
H_LIST="0.1 0.111 0.125 0.142 0.166 0.2 0.25 0.333 0.5 1 2 3 4 5 6 7 8 9 10"

# RFI Mitigation
FREQ_TOL=0.003
DM_PERSIST=0.1
PEAK_RATIO=1.2

# --- 3. PATH SETTINGS ---
TEMPLATE_FILE="../../input_files/aa_test_input_file.txt"
WORKING_FILE="../../input_files/aa_test_input_file_copy.txt"
BASE_DIR="/lustre_archive/spotlight/Nishant/AA_PULSCAN_TEST/DATA"
OUTPUT_DIR="$BASE_DIR/output/aa_test_input_file_copy"

# --- 4. PREPARATION ---
# Initialize Timing Log
TIMING_LOG="$BASE_DIR/output/pipeline_timing.log"
echo "Step,Duration_Seconds,Status" > "$TIMING_LOG"

rm -f "$WORKING_FILE"
head -n -1 "$TEMPLATE_FILE" > "$WORKING_FILE"
printf "file %s\n" "$DATA_PATH" >> "$WORKING_FILE"

source /lustre_archive/apps/tdsoft/env.sh
source ../environment.sh

# --- 5. EXECUTION ---

echo "Step 1: Running Astro-Accelerate..."
START=$SECONDS
../astro-accelerate.sh "$WORKING_FILE" "$BASE_DIR"
echo "Step_1_AA,$((SECONDS - START)),$?" >> "$TIMING_LOG"

echo "Step 2: Candidate Sifting (Thresholds: $LOW_TH to $HIGH_TH)..."
START=$SECONDS
./process_data.sh "$OUTPUT_DIR/global_pulscan_candidates.csv" "$LOW_TH" "$HIGH_TH" "$SORT_COL" "$ORDER_FLAG"
echo "Step_2_Sifting,$((SECONDS - START)),$?" >> "$TIMING_LOG"

echo "Step 3: Splitting by DM..."
START=$SECONDS
./split_by_dm.sh "$OUTPUT_DIR/global_pulscan_candidates_new.csv"
echo "Step_3_DM_Split,$((SECONDS - START)),$?" >> "$TIMING_LOG"

echo "Step 4: Parallel Harmonic Filtering (Intra-DM)..."
START=$SECONDS
python3 parallel_harmonic_filter.py \
    --input_dir "$BASE_DIR/output/dm_split_results" \
    --tolerance "$H_TOLERANCE" \
    --harmonics $H_LIST \
    --threads "$H_THREADS" \
    --debug  # <--- Added debug flag here

status=$?
echo "Step_4_IntraDM_Harmonic,$((SECONDS - START)),$status" >> "$TIMING_LOG"

if [ $status -eq 0 ]; then
    echo "Debug files created in: $BASE_DIR/output/dm_filtered_results/debug_harmonics"
fi

# --- NEW DIRECTORY SETUP ---
SYNC_DIR="$BASE_DIR/output/dm_synchronized_results"

echo "Step 5: Harmonic Synchronization (Cross-DM)..."
START=$SECONDS
python3 process_harmonics.py \
    --input_dir "$BASE_DIR/output/dm_filtered_results/" \
    --output_dir "$SYNC_DIR" \
    --tol "$H_TOLERANCE" \
    --harmonics $H_LIST
    # Debug is ON by default
    
echo "Step_5_CrossDM_Harmonic,$((SECONDS - START)),$?" >> "$TIMING_LOG"

# --- IMPORTANT: UPDATE STEP 6 INPUT ---
echo "Step 6: Final RFI Mitigation..."
START=$SECONDS
python3 rfi_dm_curve_filter.py \
    --input_dir "$SYNC_DIR" \  # Points to synchronized output now
    --output_file "$BASE_DIR/output/final_pulsar_candidates.csv" \
    --freq_tol "$FREQ_TOL" \
    --dm_persistence "$DM_PERSIST" \
    --peak_ratio "$PEAK_RATIO"

echo "Step_6_RFI_Mitigation,$((SECONDS - START)),$?" >> "$TIMING_LOG"

echo "Step 7: Running Parallel Accelsearch..."
START=$SECONDS
# Note: Adjust --cores to match your available hardware
python3 run_accelsearch_new.py \
    --input "$BASE_DIR/output/final_pulsar_candidates.csv" \
    --fil "$DATA_PATH" \
    --cores 32 \
    --output "$BASE_DIR/output/accel_search_results"
echo "Step_7_Accelsearch,$((SECONDS - START)),$?" >> "$TIMING_LOG"

echo "Step 8: Validating Candidates..."
START=$SECONDS
# f_tol=0.01Hz to match PRESTO results to AA candidates
python3 candidate_validator.py \
    --input "$BASE_DIR/output/final_pulsar_candidates.csv" \
    --results "$BASE_DIR/output/accel_search_results/accel_results" \
    --f_tol 0.05 \
    --output_csv "$BASE_DIR/output/verified_pulsar_candidates.csv"
echo "Step_8_Validation,$((SECONDS - START)),$?" >> "$TIMING_LOG"

echo "------------------------------------------------"
echo "Pipeline Complete."
echo "Verified Results: $BASE_DIR/output/verified_pulsar_candidates.csv"
echo "Timing Log: $TIMING_LOG"
