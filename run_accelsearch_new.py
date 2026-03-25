import pandas as pd
import subprocess
import glob
import os
import argparse
import shutil
from concurrent.futures import ProcessPoolExecutor

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Pulsar Search: Parallel Prepdata + Parallel Accelsearch")
parser.add_argument("--cores", type=int, default=32, help="Number of CPU cores")
parser.add_argument("--tol", type=float, default=1, help="Tolerance in PERCENTAGE")
parser.add_argument("--input", type=str, required=True, help="Input CSV file (candidates.csv)")
parser.add_argument("--fil", type=str, required=True, help="Path to the .fil source file")
parser.add_argument("--output", type=str, help="Optional: Path to output directory")
args = parser.parse_args()

# --- Dynamic Path Logic ---
if args.output:
    BASE_OUT = args.output
else:
    input_dir = os.path.dirname(os.path.abspath(args.input))
    BASE_OUT = input_dir

DAT_DIR = os.path.join(BASE_OUT, "dedispersed_timeseries")
RESULTS_DIR = os.path.join(BASE_OUT, "accel_results")

def setup_directories():
    for folder in [BASE_OUT, DAT_DIR, RESULTS_DIR]:
        os.makedirs(folder, exist_ok=True)
    print(f"Working Directory: {BASE_OUT}")

def run_single_prepdata(dm):
    """Worker function to process one DM."""
    out_prefix = os.path.join(DAT_DIR, f"dm_{dm:.2f}")
    if glob.glob(f"{out_prefix}*.dat"):
        return f"Skipped: DM {dm:.2f}"

    cmd = ["prepdata", "-nobary", "-dm", str(dm), args.fil, "-o", out_prefix]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        return f"Finished: DM {dm:.2f}"
    except subprocess.CalledProcessError:
        return f"Failed: DM {dm:.2f}"

def process_candidate(row_tuple):
    """Run Accelsearch and move results safely with race-condition handling."""
    idx, row = row_tuple
    dm = row['dm']
    r_bin = row['r']

    offset = r_bin * (args.tol / 100.0)
    r_lo = r_bin - offset
    r_hi = r_bin + offset

    cand_label = f"cand_{idx}_dm{dm:.2f}_r{r_bin:.1f}"
    cand_dir = os.path.join(RESULTS_DIR, cand_label)
    os.makedirs(cand_dir, exist_ok=True)

    dat_pattern = os.path.join(DAT_DIR, f"dm_{dm:.2f}*.dat")
    dat_files = glob.glob(dat_pattern)

    found_anything = False
    for dat in dat_files:
        accel_cmd = f"accelsearch -zmax 200 -rlo {r_lo:.2f} -rhi {r_hi:.2f} {dat}"
        try:
            # Running accelsearch (note: PRESTO overwrites same-named ACCEL files in DAT_DIR)
            subprocess.run(accel_cmd, shell=True, check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

            base_name = os.path.basename(dat).replace(".dat", "")
            generated_files = glob.glob(os.path.join(DAT_DIR, f"{base_name}*ACCEL*"))

            if not generated_files:
                continue

            found_anything = True
            for f in generated_files:
                # --- THE FIX: RACE CONDITION GUARD ---
                if os.path.exists(f):
                    try:
                        # Use copy2 then remove, or just move with error catching
                        shutil.move(f, os.path.join(cand_dir, os.path.basename(f)))
                    except FileNotFoundError:
                        # Another process moved it already. This is okay.
                        pass
                else:
                    # File already gone, ignore and move to next
                    continue

        except subprocess.CalledProcessError:
            pass
    
    # Cleanup empty folders
    if not found_anything:
        try:
            os.rmdir(cand_dir)
        except OSError:
            pass
            
    return f"Done: {cand_label}"

def main():
    setup_directories()
    try:
        df = pd.read_csv(args.input)
    except Exception as e:
        print(f"Error reading input file: {e}")
        return

    unique_dms = df['dm'].unique()

    print(f"--- Starting Parallel Prepdata: {len(unique_dms)} DMs on {args.cores} cores ---")
    with ProcessPoolExecutor(max_workers=args.cores) as executor:
        list(executor.map(run_single_prepdata, unique_dms))

    print(f"\n--- Parallel Accelsearch: {len(df)} candidates on {args.cores} cores ---")
    tasks = list(df.iterrows())
    with ProcessPoolExecutor(max_workers=args.cores) as executor:
        list(executor.map(process_candidate, tasks))

    print(f"\nPipeline Finished.")
    print(f"Results located in: {RESULTS_DIR}")

if __name__ == "__main__":
    main()
