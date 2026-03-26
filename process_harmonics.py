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
    parser.add_argument("--output_dir", type=str, required=True, help="Directory to save synchronized CSV files")
    
    # Debug Arguments
    parser.add_argument("--debug", action="store_false", dest="no_debug", help="Turn off debug logging (default is ON)")
    parser.set_defaults(no_debug=True)

    # Physics/Threshold Arguments
    # CHANGE: Tolerance is now interpreted as a percentage (e.g., 0.1 for 0.1%)
    parser.add_argument("--f_tol_pct", type=float, default=0.1, help="Percentage tolerance for harmonic matching (e.g. 0.1 for 0.1%)")
    parser.add_argument("--harmonics", type=float, nargs='+',
                        default=[0.1, 0.111, 0.125, 0.142, 0.166, 0.2, 0.25, 0.333, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        help="Space-separated list of harmonic multiples")

    parser.add_argument("--freq_col", type=str, default="frequency_hz", help="Name of frequency column")
    parser.add_argument("--sigma_col", type=str, default="sigma", help="Name of SNR/Sigma column")

    args = parser.parse_args()

    # Convert percentage to a decimal ratio (e.g., 0.1% -> 0.001)
    rel_tol = args.f_tol_pct / 100.0

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

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
                df['source_file'] = os.path.basename(f)
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
    print(f"Processing {len(master_df)} rows for harmonic grouping (Tol: {args.f_tol_pct}%)...")
    
    # Sort by sigma descending so we find strongest leads first
    master_df = master_df.sort_values(by=args.sigma_col, ascending=False).reset_index(drop=True)

    for i in range(len(master_df)):
        if master_df.loc[i, 'group_id'] != -1:
            continue

        master_df.loc[i, 'group_id'] = group_counter
        base_freq = master_df.loc[i, args.freq_col]

        for j in range(i + 1, len(master_df)):
            if master_df.loc[j, 'group_id'] != -1:
                continue

            test_freq = master_df.loc[j, args.freq_col]
            is_harmonic = False

            for n in args.harmonics:
                target = base_freq * n
                # Logic: Percentage difference between found freq and target harmonic
                if target != 0 and abs(test_freq - target) / target <= rel_tol:
                    is_harmonic = True
                    break

            if is_harmonic:
                master_df.loc[j, 'group_id'] = group_counter

        group_counter += 1

    # 3. Synchronize frequencies and Log Debug info
    debug_content = []
    debug_content.append(f"Harmonic Grouping Report - Found {group_counter} unique groups\n" + "="*60)

    for gid in range(group_counter):
        group_mask = master_df['group_id'] == gid
        group_df = master_df[group_mask]
        
        if len(group_df) > 0:
            # Strongest candidate is the lead
            lead_idx = group_df.index[0]
            best_freq = master_df.loc[lead_idx, args.freq_col]
            lead_sigma = master_df.loc[lead_idx, args.sigma_col]
            lead_file = master_df.loc[lead_idx, 'source_file']

            if args.no_debug:
                debug_content.append(f"\nGroup {gid}: Lead Freq {best_freq:.6f} Hz (Sigma: {lead_sigma:.2f}) in {lead_file}")
                for idx, row in group_df.iloc[1:].iterrows():
                    debug_content.append(f"  -> Match: {row[args.freq_col]:.6f} Hz (Sigma: {row[args.sigma_col]:.2f}) in {row['source_file']}")

            # Update frequencies in master list to match the lead frequency
            master_df.loc[group_mask, args.freq_col] = best_freq

    # 4. Save results
    if args.no_debug:
        debug_path = os.path.join(args.output_dir, "harmonic_groups_debug.txt")
        with open(debug_path, 'w') as f:
            f.write("\n".join(debug_content))
        print(f"Debug log saved to: {debug_path}")

    for f_name in master_df['source_file'].unique():
        file_out_data = master_df[master_df['source_file'] == f_name].copy()
        file_out_data = file_out_data.drop(columns=['source_file', 'group_id'])
        
        out_path = os.path.join(args.output_dir, f_name)
        file_out_data.to_csv(out_path, index=False)
        print(f"Saved Synchronized File: {out_path}")

if __name__ == "__main__":
    process_harmonics()
