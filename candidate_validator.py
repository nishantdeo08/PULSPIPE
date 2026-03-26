import pandas as pd
import os
import glob
import re
import argparse

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Validate ACCEL results with Harmonic Percentage Matching.")
parser.add_argument("--input", type=str, required=True, help="Original candidates.csv")
parser.add_argument("--results", type=str, required=True, help="Path to 'accel_results' directory")
# Changed from absolute Hz to Percentage
parser.add_argument("--f_tol_pct", type=float, default=0.1, help="Frequency matching tolerance in PERCENT (default: 0.1%)")
parser.add_argument("--output_csv", type=str, default="verified_candidates.csv", help="Name of the new filtered CSV")
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

    # Convert percentage to decimal for calculation (e.g., 0.1% -> 0.001)
    rel_tol = args.f_tol_pct / 100.0

    print(f"Validating {len(df)} candidates using Harmonic Matching (Rel Tol: {args.f_tol_pct}%)...")

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
                
                is_match = False
                matched_factor = 1.0
                
                for factor in args.harmonics:
                    expected_f = target_f * factor
                    
                    # --- PERCENTAGE TOLERANCE CHECK ---
                    # Calculate relative difference
                    diff = abs(hit_f - expected_f) / expected_f
                    
                    if diff <= rel_tol:
                        is_match = True
                        matched_factor = factor
                        break
                
                if is_match:
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
        print(f"Success! {len(out_df)} verified. Results saved to {args.output_csv}")
    else:
        print("No candidates matched within the specified percentage tolerance.")

if __name__ == "__main__":
    main()