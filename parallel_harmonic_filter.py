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
    file_path, output_dir, tolerance, factors, freq_idx, debug = args_tuple
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            return None

        # Sort by sigma descending (strongest first)
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
                    if h != 0 and abs(check_freq - h) < (h * tolerance):
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
    parser = argparse.ArgumentParser(description="Parallel harmonic filtering of DM files")

    parser.add_argument("--input_dir", type=str, required=True, help="Directory containing DM-split CSV files")
    parser.add_argument("--output_dir", type=str, help="Custom output directory")
    parser.add_argument("--tolerance", type=float, default=0.001, help="Harmonic matching tolerance")
    parser.add_argument("--harmonics", type=float, nargs='+',
                        default=[0.1, 0.111, 0.125, 0.142, 0.166, 0.2, 0.25, 0.333, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        help="List of harmonic multiples")
    parser.add_argument("--freq_idx", type=int, default=6, help="0-based index of frequency column")
    parser.add_argument("--threads", type=int, default=None, help="Number of parallel processes")
    
    # New Debug Flag
    parser.add_argument("--debug", action="store_true", help="Write debug files listing harmonic matches for each DM")

    args = parser.parse_args()

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

    print(f"Starting parallel filtering on {len(file_list)} files...")

    # Updated pool_args to include args.debug
    pool_args = [(f, output_path, args.tolerance, args.harmonics, args.freq_idx, args.debug) for f in file_list]

    with Pool(processes=args.threads) as pool:
        results = pool.map(process_single_dm, pool_args)

    for res in results:
        if res:
            print(res)
