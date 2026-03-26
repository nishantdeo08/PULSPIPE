import pandas as pd
import numpy as np
import glob
import os
import sys
import argparse

def mitigate_rfi():
    parser = argparse.ArgumentParser(description="RFI Mitigation for Pulsar Candidates")
    
    # Path Arguments
    parser.add_argument("--input_dir", type=str, required=True, help="Directory containing input CSV files")
    parser.add_argument("--output_file", type=str, required=True, help="Path to save the final CSV results")
    
    # Threshold Arguments
    parser.add_argument("--freq_tol", type=float, default=0.003, help="Rounding precision for frequency grouping (default: 0.003)")
    parser.add_argument("--dm_persistence", type=float, default=0.1, help="Persistence threshold (0.0 to 1.0) (default: 0.1)")
    parser.add_argument("--peak_ratio", type=float, default=1.2, help="Peak SNR must be X times the average SNR (default: 1.2)")
    
    args = parser.parse_args()

    all_files = glob.glob(os.path.join(args.input_dir, "*.csv"))
    if not all_files:
        print(f"No files found in {args.input_dir}")
        return

    # 1. Load all candidates
    data_list = []
    for f in all_files:
        try:
            temp_df = pd.read_csv(f)
            if not temp_df.empty:
                data_list.append(temp_df)
        except Exception as e:
            print(f"Skipping {f} due to error: {e}")

    if not data_list:
        print("All found files were empty.")
        return

    master_df = pd.concat(data_list, ignore_index=True)

    # Column Mapping (Sigma=0, Freq=6, DM=7)
    sig_col = master_df.columns[0]
    freq_col = master_df.columns[6]
    dm_col = master_df.columns[7]

    # 2. Group by Frequency
    master_df['group_freq'] = master_df[freq_col].round(3) if args.freq_tol == 0 else master_df[freq_col].apply(lambda x: round(x / args.freq_tol) * args.freq_tol)

    final_keepers = []
    total_dms = len(all_files)

    print(f"Analyzing {master_df['group_freq'].nunique()} unique frequency groups...")

    for freq, group in master_df.groupby('group_freq'):
        num_appearances = len(group)
        persistence = num_appearances / total_dms

        max_snr = group[sig_col].max()
        avg_snr = group[sig_col].mean()
        min_snr = group[sig_col].min()

        # Test 1: Low persistence - likely a weak candidate, keep best instance
        if persistence < args.dm_persistence:
            final_keepers.append(group.loc[group[sig_col].idxmax()])
            continue

        # Test 2: Peaked distribution check
        # is_peaked = max_snr > (avg_snr * args.peak_ratio)
        is_peaked = max_snr > (min_snr * args.peak_ratio)
        
        # RFI Mitigation Logic
        if persistence > args.dm_persistence and not is_peaked:
            # Likely broadband RFI appearing across many DMs without a clear peak
            continue
        else:
            final_keepers.append(group.loc[group[sig_col].idxmax()])

    # 3. Save final results
    if final_keepers:
        result_df = pd.DataFrame(final_keepers)
        # Remove the helper column before saving
        if 'group_freq' in result_df.columns:
            result_df = result_df.drop(columns=['group_freq'])
            
        result_df.to_csv(args.output_file, index=False)
        print(f"Success! Saved {len(result_df)} candidates to {args.output_file}")
    else:
        print("No candidates survived RFI mitigation.")

if __name__ == "__main__":
    mitigate_rfi()
