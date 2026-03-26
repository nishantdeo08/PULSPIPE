import pandas as pd
import numpy as np
import os
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="RFI Mitigation: Harmonic-Aware Strict Gradient Filter")
    parser.add_argument("--input_dir", type=str, required=True, help="Directory containing synchronized CSVs")
    parser.add_argument("--output_file", type=str, required=True, help="Path for final candidates.csv")
    parser.add_argument("--f_tol_pct", type=float, default=0.1, help="Frequency matching tolerance in PERCENT")
    parser.add_argument("--neighbor_n", type=int, default=3, help="Number of DM steps to check on each side")
    # This matches the --harmonics flag you use in other scripts
    parser.add_argument("--harmonics", type=float, nargs='+', 
                        default=[0.1, 0.111, 0.125, 0.142, 0.166, 0.2, 0.25, 0.333, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                        help="List of harmonic factors to consider for grouping")
    return parser.parse_args()

def is_valid_pulsar_gradient(group, n):
    """
    Validates the 'Peakiness' of the candidate in DM space.
    Requires a strict monotonic increase to the peak and decrease after.
    """
    # Group must be sorted by DM for the gradient check to make sense
    group = group.sort_values('dm').reset_index(drop=True)
    sigmas = group['sigma'].values
    
    if len(sigmas) < 3:
        return False

    max_idx = sigmas.argmax()
    
    # Adaptive n: Check as many neighbors as possible up to 'n'
    max_possible_n = min(max_idx, len(sigmas) - 1 - max_idx)
    check_n = min(n, max_possible_n)

    if check_n == 0:
        return False

    # Check Gradient: Must be strictly increasing THEN strictly decreasing
    for i in range(max_idx - check_n, max_idx):
        if not (sigmas[i] < sigmas[i+1]): return False
            
    for i in range(max_idx, max_idx + check_n):
        if not (sigmas[i] > sigmas[i+1]): return False

    return True

def main():
    args = parse_args()
    
    # 1. Load Data
    all_files = [os.path.join(args.input_dir, f) for f in os.listdir(args.input_dir) if f.endswith('.csv')]
    if not all_files:
        print("No CSV files found.")
        return
    
    df = pd.concat([pd.read_csv(f) for f in all_files], ignore_index=True)
    
    # Sort by frequency to make grouping more efficient
    df = df.sort_values('frequency_hz').reset_index(drop=True)
    processed_indices = set()
    final_candidates = []

    # 2. Grouping by Harmonic Families
    rel_tol = args.f_tol_pct / 100.0

    for i, row in df.iterrows():
        if i in processed_indices:
            continue

        f_base = row['frequency_hz']
        group_indices = []

        # Check this frequency against all others using the harmonics list
        # This handles cases where we found the 2nd harmonic but not the fundamental, etc.
        for factor in args.harmonics:
            target_f = f_base * factor
            # Vectorized percentage match
            matches = df[((df['frequency_hz'] - target_f).abs() / target_f) <= rel_tol].index
            group_indices.extend(matches.tolist())

        # Consolidate the group
        group_indices = list(set(group_indices))
        group = df.loc[group_indices]
        
        # 3. Apply the Gradient Filter
        if is_valid_pulsar_gradient(group, args.neighbor_n):
            # If valid, pick the single highest SNR point from the whole harmonic family
            peak_row = group.loc[group['sigma'].idxmax()]
            final_candidates.append(peak_row)

        # Mark all these as processed so we don't duplicate the pulsar
        processed_indices.update(group_indices)

    # 4. Save Output
    if final_candidates:
        out_df = pd.DataFrame(final_candidates)
        out_df.to_csv(args.output_file, index=False)
        print(f"Success: {len(out_df)} candidates passed harmonic-gradient filtering.")
    else:
        print("All candidates were filtered out as RFI.")

if __name__ == "__main__":
    main()