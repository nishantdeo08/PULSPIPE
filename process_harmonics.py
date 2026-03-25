import glob
import os
import pandas as pd
import numpy as np
import argparse
import sys

def process_harmonics():
    parser = argparse.ArgumentParser(description="Harmonic frequency synchronizer for pulsar candidates.")
    
    # Path Arguments
    parser.add_argument("--input_dir", type=str, required=True, help="Directory containing candidate CSV files")
    
    # Physics/Threshold Arguments
    parser.add_argument("--tol", type=float, default=0.0001, help="Relative tolerance for harmonic matching (default: 0.0001)")
    parser.add_argument("--harmonics", type=float, nargs='+', 
                        default=[0.1, 0.111, 0.125, 0.142, 0.166, 0.2, 0.25, 0.333, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        help="Space-separated list of harmonic multiples to check (e.g. 0.5 1 2)")
    
    # Column Name Arguments (In case your CSV headers change)
    parser.add_argument("--freq_col", type=str, default="frequency_hz", help="Name of the frequency column")
    parser.add_argument("--sigma_col", type=str, default="sigma", help="Name of the SNR/Sigma column")

    args = parser.parse_args()

    # 1. Load all data
    pattern = os.path.join(args.input_dir, "*.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"No CSV files found in {args.input_dir}")
        return

    all_candidates = []
    for f in files:
        try:
            df = pd.read_csv(f)
            if not df.empty:
                df['source_file'] = f  # Track origin for step 4
                all_candidates.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")

    if not all_candidates:
        print("No valid data found in CSVs.")
        return

    master_df = pd.concat(all_candidates, ignore_index=True)
    master_df['group_id'] = -1
    group_counter = 0

    # 2. Identify Harmonic Groups
    print(f"Processing {len(master_df)} rows for harmonic grouping...")
    
    for i in range(len(master_df)):
        if master_df.loc[i, 'group_id'] != -1:
            continue

        master_df.loc[i, 'group_id'] = group_counter
        base_freq = master_df.loc[i, args.freq_col]

        # Compare against all other ungrouped rows
        for j in range(i + 1, len(master_df)):
            if master_df.loc[j, 'group_id'] != -1:
                continue

            test_freq = master_df.loc[j, args.freq_col]
            is_harmonic = False

            for n in args.harmonics:
                target = base_freq * n
                if target != 0 and abs(test_freq - target) / target < args.tol:
                    is_harmonic = True
                    break

            if is_harmonic:
                master_df.loc[j, 'group_id'] = group_counter

        group_counter += 1

    # 3. Synchronize frequencies to the 'best' (max sigma) candidate in the group
    print(f"Found {group_counter} harmonic groups. Synchronizing...")
    for gid in range(group_counter):
        group_indices = master_df[master_df['group_id'] == gid].index
        if len(group_indices) > 0:
            max_sigma_idx = master_df.loc[group_indices, args.sigma_col].idxmax()
            best_freq = master_df.loc[max_sigma_idx, args.freq_col]
            master_df.loc[group_indices, args.freq_col] = best_freq

    # 4. Save results back to original files
    for f in files:
        original_file_data = master_df[master_df['source_file'] == f].copy()
        # Clean up helper columns
        original_file_data = original_file_data.drop(columns=['source_file', 'group_id'])
        original_file_data.to_csv(f, index=False)
        print(f"Updated: {os.path.basename(f)}")

if __name__ == "__main__":
    process_harmonics()
