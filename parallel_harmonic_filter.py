import pandas as pd
import numpy as np
import glob
import os
import argparse
from multiprocessing import Pool

def get_harmonics(freq, factors):
    """Generates harmonic frequencies based on provided factors"""
    return [freq * f for f in factors]

def process_single_dm(args_tuple):
    """Filter a single DM file"""
    # tolerance here is now passed as the decimal ratio (e.g., 0.001 for 0.1%)
    file_path, output_dir, rel_tol, factors, freq_idx, debug = args_tuple
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            return None

        # Sort by sigma descending (strongest first)
        # Using the first column for sorting as per your previous logic
        df = df.sort_values(by=df.columns[0], ascending=False).reset_index(drop=True)

        keep_indices = []
        discarded_indices = set()
        freq_col = df.columns[freq_idx]
        
        # Debug storage
        debug_log = []

        for i, row in df.iterrows():
            if i in discarded_indices:
                continue

            keep_indices.append(i)
            current_freq = row[freq_col]
            harmonics = get_harmonics(current_freq, factors)

            for j in range(i + 1, len(df)):
                if j in discarded_indices:
                    continue

                check_freq = df.loc[j, freq_col]
                for idx, h in enumerate(harmonics):
                    # --- PERCENTAGE MATCH CHECK ---
                    if h != 0 and abs(check_freq - h) <= (h * rel_tol):
                        discarded_indices.add(j)
                        
                        if debug:
                            factor = factors[idx]
                            debug_log.append(
                                f"MATCH: Main_Freq {current_freq:.6f} (Idx {i}) "
                                f"matched Harmonic_Freq {check_freq:.6f} (Idx {j}) "
                                f"via Factor {factor} (Target: {h:.6f})"
                            )
                        break

        # Write filtered results
        filtered_df = df.loc[keep_indices]
        out_name = os.path.join(output_dir, os.path.basename(file_path))
        filtered_df.to_csv(out_name, index=False, float_format='%.6f')

        # Write debug info if requested
        if debug and debug_log:
            debug_dir = os.path.join(output_dir, "debug_harmonics")
            os.makedirs(debug_dir, exist_ok=True)
            debug_path = os.path.join(debug_dir, os.path.basename(file_path) + ".debug")
            with open(debug_path, 'w') as f:
                f.write("\n".join(debug_log))

        return f"Processed {os.path.basename(file_path)}: {len(df)} -> {len(filtered_df)}"

    except Exception as e:
        return f"Error processing {os.path.basename(file_path)}: {e}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parallel harmonic filtering of DM files (Percentage Based)")

    parser.add_argument("--input_dir", type=str, required=True, help="Directory containing DM-split CSV files")
    parser.add_argument("--output_dir", type=str, help="Custom output directory")
    # Changed from --tolerance to --f_tol_pct
    parser.add_argument("--f_tol_pct", type=float, default=0.1, help="Harmonic matching tolerance in PERCENT (default: 0.1)")
    parser.add_argument("--harmonics", type=float, nargs='+',
                        default=[0.1, 0.111, 0.125, 0.142, 0.166, 0.2, 0.25, 0.333, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        help="List of harmonic multiples")
    parser.add_argument("--freq_idx", type=int, default=6, help="0-based index of frequency column")
    parser.add_argument("--threads", type=int, default=None, help="Number of parallel processes")
    parser.add_argument("--debug", action="store_true", help="Write debug files")

    args = parser.parse_args()

    # Convert percentage to decimal ratio once here
    rel_tol = args.f_tol_pct / 100.0

    input_path = args.input_dir.rstrip("/")
    if not os.path.isdir(input_path):
        print(f"Error: {input_path} is not a valid directory")
        exit(1)

    output_path = args.output_dir if args.output_dir else os.path.join(os.path.dirname(input_path), "dm_filtered_results")
    os.makedirs(output_path, exist_ok=True)

    file_list = glob.glob(os.path.join(input_path, "*.csv"))
    if not file_list:
        print("No CSV files found.")
        exit(1)

    print(f"Starting parallel filtering on {len(file_list)} files using {args.f_tol_pct}% tolerance...")

    # Pass rel_tol (the decimal ratio) into the pool
    pool_args = [(f, output_path, rel_tol, args.harmonics, args.freq_idx, args.debug) for f in file_list]

    with Pool(processes=args.threads) as pool:
        results = pool.map(process_single_dm, pool_args)

    for res in results:
        if res:
            print(res)