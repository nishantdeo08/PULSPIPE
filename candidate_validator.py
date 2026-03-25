import pandas as pd
import os
import glob
import re
import argparse

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Validate ACCEL results with Harmonic Matching.")
parser.add_argument("--input", type=str, required=True, help="Original candidates.csv")
parser.add_argument("--results", type=str, required=True, help="Path to 'accel_results' directory")
parser.add_argument("--f_tol", type=float, default=0.05, help="Frequency matching tolerance (Hz)")
parser.add_argument("--output_csv", type=str, default="verified_candidates.csv", help="Name of the new filtered CSV")
# Added harmonics argument to match your pipeline's logic
parser.add_argument("--harmonics", type=float, nargs='+', 
                    default=[0.1, 0.111, 0.125, 0.142, 0.166, 0.2, 0.25, 0.333, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                    help="Harmonic factors to check for matches")
args = parser.parse_args()

def parse_accel_table(file_path):
    candidates = []
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()

        start_parsing = False
        for line in lines:
            if "Summed" in line and "Coherent" in line:
                start_parsing = True
                continue

            if start_parsing:
                if "Power /" in line or line.strip() == "" or "---" in line:
                    if candidates: break
                    else: continue

                parts = line.split()
                if len(parts) >= 11 and parts[0].isdigit():
                    try:
                        raw_f = re.sub(r'\(.*\)', '', parts[6])
                        raw_s = parts[1]
                        raw_a = re.sub(r'\(.*\)', '', parts[10])

                        candidates.append({
                            'accel_freq': float(raw_f),
                            'accel_sigma': float(raw_s),
                            'accel_val': float(raw_a)
                        })
                    except (ValueError, IndexError):
                        continue
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return candidates

def main():
    df = pd.read_csv(args.input)
    verified_rows = []

    print(f"Validating {len(df)} candidates using Harmonic Matching (Tol: {args.f_tol} Hz)...")

    for idx, row in df.iterrows():
        target_f = row['frequency_hz']
        dm = row['dm']
        r_bin = row['r']

        cand_label = f"cand_{idx}_dm{dm:.2f}_r{r_bin:.1f}"
        cand_dir = os.path.join(args.results, cand_label)

        if not os.path.exists(cand_dir):
            continue

        accel_files = glob.glob(os.path.join(cand_dir, "*_ACCEL_*"))
        best_match = None

        for accel in accel_files:
            if accel.endswith('.cand'): continue 
            
            detected_hits = parse_accel_table(accel)

            for hit in detected_hits:
                hit_f = hit['accel_freq']
                
                # Check match against fundamental AND harmonics
                is_match = False
                matched_factor = 1.0
                
                for factor in args.harmonics:
                    expected_f = target_f * factor
                    if abs(hit_f - expected_f) <= args.f_tol:
                        is_match = True
                        matched_factor = factor
                        break
                
                if is_match:
                    # Keep the hit with the highest Sigma overall
                    if best_match is None or hit['accel_sigma'] > best_match['accel_sigma']:
                        best_match = hit.copy()
                        best_match['matched_factor'] = matched_factor

        if best_match:
            new_row = row.to_dict()
            new_row.update(best_match) 
            verified_rows.append(new_row)

    if verified_rows:
        out_df = pd.DataFrame(verified_rows)
        out_df.to_csv(args.output_csv, index=False)
        print(f"Success! {len(out_df)} verified. Results: {args.output_csv}")
    else:
        print("No candidates matched, even considering harmonics.")

if __name__ == "__main__":
    main()
