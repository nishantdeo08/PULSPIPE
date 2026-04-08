#!/bin/bash

# --- Configuration ---
# Define your list of phases here (e.g., 001 002 004 010)
PHASES=("000" "001" "002" "003" "004" "005" "006" "007" "008" "009" "010" "011" "012" "013" "014" "015" "016" "017" "018" "019" "020") 

BASE_DIR="/lustre_archive/spotlight/Nishant/AA_PULSCAN_OUTPUT/DATA"
PIPE_CMD="./aa_pulscan_pipe_ver01.sh"

# --- Execution ---
# Using a counter (i) to track the run number for directory renaming
i=1

for PHASE in "${PHASES[@]}"; do
    FILE_PATH="${BASE_DIR}/Sim_1_26_T0_phase_${PHASE}.fil"
    
    echo "------------------------------------------------"
    echo "Starting Run $i: Phase $PHASE"
    echo "Processing $FILE_PATH"
    
    # 1. Execute the command
    # The script waits here until this command finishes
    $PIPE_CMD "$FILE_PATH"
    
    # 2. Check if the previous command succeeded before moving files
    if [ $? -eq 0 ]; then
        echo "Command successful. Moving output directory..."
        
        # 3. Change directory and rename
        cd "$BASE_DIR" || { echo "Failed to cd to $BASE_DIR"; exit 1; }
        
        # Format the counter with leading zeros if you prefer (e.g., output_test01)
        TARGET_DIR=$(printf "output_test%02d" $i)
        
        if [ -d "output" ]; then
            mv output "$TARGET_DIR"
            echo "Renamed 'output' to '$TARGET_DIR'"
        else
            echo "Warning: 'output' directory not found for Run $i"
        fi
        
        # Move back to original script location if necessary (optional)
        cd - > /dev/null
    else
        echo "Error: Command failed for Phase $PHASE. Skipping rename."
    fi
    
    # Increment counter
    ((i++))
done

echo "------------------------------------------------"
echo "All runs completed."
