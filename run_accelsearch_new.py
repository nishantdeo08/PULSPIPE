import pandas as pd
import subprocess
import glob
import os
import argparse
import shutil
from concurrent.futures import ProcessPoolExecutor

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Pulsar Search: Parallel Prepdata + Isolated Accelsearch")
parser.add_argument("--cores", type=int, default=32, help="Number of CPU cores")
parser.add_argument("--tol", type=float, default=1, help="Tolerance in PERCENTAGE")
parser.add_argument("--input", type=str, required=True, help="Input CSV file (candidates.csv)")
parser.add_argument("--fil", type=str, required=True, help="Path to the .fil source file")
parser.add_argument("--output", type=str, help="Output directory")
args = parser.parse_args()

# --- Path Setup ---
BASE_OUT = args.output if args.output else os.path.dirname(os.path.abspath(args.input))
DAT_DIR = os.path.join(BASE_OUT, "dedispersed_timeseries")
RESULTS_DIR = os.path.join(BASE_OUT, "accel_results")

def setup_directories():
    os.makedirs(DAT_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)

# --- STEP 1: PARALLEL PREPDATA ---
def run_single_prepdata(dm):
    """Generates one .dat and .inf file per unique DM."""
    out_prefix = os.path.join(DAT_DIR, f"dm_{dm:.2f}")
    if os.path.exists(f"{out_prefix}.dat"):
        return f"Skipped: DM {dm:.2f} (Already exists)"

    cmd = ["prepdata", "-nobary", "-dm", str(dm), args.fil, "-o", out_prefix]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        return f"Finished: DM {dm:.2f}"
    except subprocess.CalledProcessError:
        return f"Failed: DM {dm:.2f}"

# --- STEP 2: ISOLATED ACCELSEARCH ---
def process_candidate(row_tuple):
    """Run Accelsearch in a sandbox for a specific candidate."""
    idx, row = row_tuple
    dm = row['dm']
    r_bin = row['r']

    cand_label = f"cand_{idx}_dm{dm:.2f}_r{r_bin:.1f}"
    cand_dir = os.path.join(RESULTS_DIR, cand_label)
    os.makedirs(cand_dir, exist_ok=True)

    target_dat = os.path.join(DAT_DIR, f"dm_{dm:.2f}.dat")
    target_inf = os.path.join(DAT_DIR, f"dm_{dm:.2f}.inf")

    if not os.path.exists(target_dat):
        return f"Error: Missing {target_dat}"

    offset = r_bin * (args.tol / 100.0)
    r_lo, r_hi = r_bin - offset, r_bin + offset

    # Symlink setup to prevent race conditions in DAT_DIR
    local_dat = os.path.join(cand_dir, os.path.basename(target_dat))
    local_inf = os.path.join(cand_dir, os.path.basename(target_inf))

    try:
        if not os.path.exists(local_dat): os.symlink(target_dat, local_dat)
        if not os.path.exists(local_inf): os.symlink(target_inf, local_inf)

        # Execute accelsearch inside the sandbox
        cmd = f"accelsearch -zmax 200 -rlo {r_lo:.2f} -rhi {r_hi:.2f} {os.path.basename(local_dat)}"
        subprocess.run(cmd, shell=True, check=True, cwd=cand_dir,
                       stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

        # Cleanup sandbox links
        os.remove(local_dat)
        os.remove(local_inf)

        # Delete folder if no actual candidates found
        if not glob.glob(os.path.join(cand_dir, "*ACCEL*")):
            os.rmdir(cand_dir)
            return f"No Hits: {cand_label}"

        return f"Success: {cand_label}"

    except Exception as e:
        return f"Failed: {cand_label} - {str(e)}"

def main():
    setup_directories()
    df = pd.read_csv(args.input)

    # Parallel Prepdata
    unique_dms = df['dm'].unique()
    print(f"--- Launching Parallel Prepdata ({len(unique_dms)} unique DMs) ---")
    with ProcessPoolExecutor(max_workers=args.cores) as executor:
        list(executor.map(run_single_prepdata, unique_dms))

    # Parallel Accelsearch
    print(f"\n--- Launching Parallel Accelsearch ({len(df)} candidates) ---")
    tasks = list(df.iterrows())
    with ProcessPoolExecutor(max_workers=args.cores) as executor:
        results = list(executor.map(process_candidate, tasks))

    success_count = sum(1 for r in results if "Success" in r)
    print(f"\nFinal Statistics: {success_count} successful detections from {len(df)} candidates.")

if __name__ == "__main__":
    main()
