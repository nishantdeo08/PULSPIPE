import pandas as pd
import os
import glob
import re
import argparse
import subprocess
import shutil

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Validate ACCEL results and fold candidates.")
parser.add_argument("--input", type=str, required=True, help="Original final_pulsar_candidates.csv")
parser.add_argument("--results", type=str, required=True, help="Path to 'accel_results' directory")
parser.add_argument("--fil", type=str, required=True, help="Path to the original .fil file")
parser.add_argument("--dat_dir", type=str, required=True, help="Directory containing .dat/.inf files")
parser.add_argument("--fold_type", type=str, choices=['dat', 'fil'], default='fil',
                    help="Choose 'dat' for fast folding or 'fil' for full diagnostic folding (default: fil)")
parser.add_argument("--f_tol_pct", type=float, default=0.1, help="Frequency matching tolerance in PERCENT")
parser.add_argument("--output_csv", type=str, default="verified_candidates.csv", help="Filtered CSV output")
parser.add_argument("--harmonics", type=float, nargs='+',
                    default=[0.1, 0.1111111111111111, 0.125, 0.14285714285714285, 0.16666666666666666, 0.2, 0.25, 0.3333333333333333, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
args = parser.parse_args()

PROFILES_DIR = os.path.join(args.results, "folded_profiles")

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
                        candidates.append({
                            'accel_idx': int(parts[0]),
                            'accel_freq': float(raw_f),
                            'accel_sigma': float(parts[1]),
                        })
                    except (ValueError, IndexError): continue
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return candidates

def main():
    os.makedirs(PROFILES_DIR, exist_ok=True)
    df = pd.read_csv(args.input)
    verified_rows = []
    rel_tol = args.f_tol_pct / 100.0

    print(f"--- Mode: Folding from {args.fold_type.upper()} ---")

    for idx, row in df.iterrows():
        target_f = row['frequency_hz']
        dm, r_bin = row['dm'], row['r']
        cand_label = f"cand_{idx}_dm{dm:.2f}_r{r_bin:.1f}"
        cand_dir = os.path.join(args.results, cand_label)

        if not os.path.exists(cand_dir): continue

        accel_files = [f for f in glob.glob(os.path.join(cand_dir, "*_ACCEL_*"))
                       if not any(f.endswith(ext) for ext in ['.cand', '.inf', '.txt', '.pfd', '.ps'])]

        best_match = None
        for accel_txt in accel_files:
            detected_hits = parse_accel_table(accel_txt)
            for hit in detected_hits:
                hit_f = hit['accel_freq']
                for factor in args.harmonics:
                    if abs(hit_f - (target_f * factor)) / (target_f * factor) <= rel_tol:
                        if best_match is None or hit['accel_sigma'] > best_match['accel_sigma']:
                            best_match = hit.copy()
                            dm_str = f"{round(dm, 2):.2f}"
                            best_match['dat_name'] = f"dm_DM{dm_str}.dat"
                            best_match['inf_name'] = f"dm_DM{dm_str}.inf"
                            best_match['accel_file_path'] = os.path.abspath(accel_txt + ".cand")
                            best_match['cand_dir'] = os.path.abspath(cand_dir)
                        break

        if best_match:
            src_inf = os.path.join(args.dat_dir, best_match['inf_name'])
            loc_inf = os.path.join(best_match['cand_dir'], best_match['inf_name'])
            src_dat = os.path.join(args.dat_dir, best_match['dat_name'])
            loc_dat = os.path.join(best_match['cand_dir'], best_match['dat_name'])

            try:
                if not os.path.exists(loc_inf): os.symlink(src_inf, loc_inf)

                fold_cmd = ["prepfold", "-accelcand", str(best_match['accel_idx']),
                            "-accelfile", best_match['accel_file_path'], "-noxwin", "-ncpus", "8"]

                if args.fold_type == 'dat':
                    if not os.path.exists(loc_dat): os.symlink(src_dat, loc_dat)
                    fold_cmd.append(best_match['dat_name'])
                else:
                    # UPDATED: Added -nodmsearch and -dm for filterbank folding
                    fold_cmd.extend(["-nodmsearch", "-dm", f"{dm:.2f}", os.path.abspath(args.fil)])

                print(f"Folding Candidate {idx} (DM {dm:.2f})...")
                subprocess.run(fold_cmd, cwd=best_match['cand_dir'], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

                if os.path.exists(loc_inf): os.remove(loc_inf)
                if os.path.exists(loc_dat): os.remove(loc_dat)

                for ext in ["*.ps", "*.pfd", "*.bestprof"]:
                    for f in glob.glob(os.path.join(best_match['cand_dir'], ext)):
                        new_name = f"cand_{idx}_dm{dm:.2f}_type_{args.fold_type}_" + os.path.basename(f)
                        shutil.move(f, os.path.join(PROFILES_DIR, new_name))

                verified_rows.append({**row.to_dict(), **best_match})
            except Exception as e:
                print(f"Failed to fold {cand_label}: {e}")

    if verified_rows:
        pd.DataFrame(verified_rows).to_csv(args.output_csv, index=False)
        print(f"\nCompleted! Files moved to: {PROFILES_DIR}")

if __name__ == "__main__":
    main()
