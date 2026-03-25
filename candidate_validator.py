import pandas as pd
import os
import glob
import re
import argparse

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Validate ACCEL results and update candidates list.")
parser.add_argument("--input", type=str, required=True, help="Original candidates.csv")
parser.add_argument("--results", type=str, required=True, help="Path to 'accel_results' directory")
parser.add_argument("--f_tol", type=float, default=0.05, help="Frequency matching tolerance (Hz)")
parser.add_argument("--output_csv", type=str, default="verified_candidates.csv", help="Name of the new filtered CSV")
args = parser.parse_args()

def parse_accel_table(file_path):
    """
    Parses the first table of a PRESTO ACCEL file.
    Returns a list of dicts: {'freq': float, 'sigma': float, 'accel': float}
    """
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
                # Stop if we hit the second table or empty space
                if "Power /" in line or line.strip() == "" or "---" in line:
                    if candidates: break 
                    else: continue
                
                parts = line.split()
                # Table column index mapping:
                # 1: Sigma, 5: Period(ms), 6: Freq(Hz), 10: Accel(m/s^2)
                if len(parts) >= 11 and parts[0].isdigit():
                    try:
                        # Clean PRESTO uncertainty notation: 10.0534(1) -> 10.0534
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
    # 1. Load Original Data
    df = pd.read_csv(args.input)
    verified_rows = []

    print(f"Validating {len(df)} candidates...")

    # 2. Iterate through each row in the CSV
    for idx, row in df.iterrows():
        target_f = row['frequency_hz']
        dm = row['dm']
        r_bin = row['r']
        
        # Locate the specific folder for this candidate
        # Folder name logic from previous step: cand_{idx}_dm{dm}_r{r}
        cand_label = f"cand_{idx}_dm{dm:.2f}_r{r_bin:.1f}"
        cand_dir = os.path.join(args.results, cand_label)
        
        if not os.path.exists(cand_dir):
            continue

        accel_files = glob.glob(os.path.join(cand_dir, "*_ACCEL_*"))
        match_found = False
        best_match = None

        for accel in accel_files:
            if accel.endswith('.cand'): continue # Skip binary files
            
            detected = parse_accel_table(accel)
            
            for hit in detected:
                # Frequency Match Check
                if abs(hit['accel_freq'] - target_f) <= args.f_tol:
                    match_found = True
                    # If multiple hits match, we keep the one with the highest Sigma
                    if best_match is None or hit['accel_sigma'] > best_match['accel_sigma']:
                        best_match = hit

        # 3. If matched, update the row with ACCEL-refined values and save
        if match_found:
            new_row = row.to_dict()
            new_row.update(best_match) # Adds accel_freq, accel_sigma, accel_val
            verified_rows.append(new_row)

    # 4. Save to new CSV
    if verified_rows:
        out_df = pd.DataFrame(verified_rows)
        out_df.to_csv(args.output_csv, index=False)
        print(f"Success! {len(out_df)} candidates verified and saved to {args.output_csv}")
    else:
        print("No candidates matched the frequency criteria.")

if __name__ == "__main__":
    main()
