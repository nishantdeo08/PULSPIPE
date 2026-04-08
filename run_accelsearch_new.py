import pandas as pd
import subprocess
import glob
import os
import argparse
import shutil
import numpy as np
from concurrent.futures import ProcessPoolExecutor

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Pulsar Search: prepsubband + Optimized Accelsearch")
parser.add_argument("--cores", type=int, default=32, help="Total CPU cores available")
parser.add_argument("--tol", type=float, default=1, help="Tolerance in PERCENTAGE for r-bin range")
parser.add_argument("--input", type=str, required=True, help="Input CSV file (candidates.csv)")
parser.add_argument("--fil", type=str, required=True, help="Path to the .fil source file")
parser.add_argument("--output", type=str, help="Output directory")
parser.add_argument("--dm_step", type=float, default=1, help="DM step for prepsubband grid")
args = parser.parse_args()

# --- Path Setup ---
BASE_OUT = args.output if args.output else os.path.dirname(os.path.abspath(args.input))
DAT_DIR = os.path.join(BASE_OUT, "dedispersed_timeseries")
RESULTS_DIR = os.path.join(BASE_OUT, "accel_results")

def setup_directories():
    os.makedirs(DAT_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)

# --- STEP 1: PREPSUBBAND (Internal Multi-threading) ---
def run_prepsubband_logic(df):
    unique_dms = np.sort(df['dm'].unique())
    if len(unique_dms) == 0:
        return "Error: No DMs found in input CSV."

    lo_dm = unique_dms[0]
    hi_dm = unique_dms[-1]
    dm_range = hi_dm - lo_dm
    num_dms = int(dm_range / args.dm_step) + 1
    
    out_prefix = os.path.join(DAT_DIR, "dm")

    cmd = [
        "prepsubband",
        "-lodm", f"{lo_dm:.2f}",
        "-dmstep", f"{args.dm_step:.4f}",
        "-numdms", str(num_dms),
        "-ncpus", str(args.cores),
        "-nobary",
        "-o", out_prefix,
        args.fil
    ]

    print(f"--- Launching prepsubband (Internal Parallelism) ---")
    print(f"Range: {lo_dm:.2f} to {hi_dm:.2f} | Step: {args.dm_step} | Total DMs: {num_dms}")

    try:
        subprocess.run(cmd, check=True)
        return "Success: prepsubband finished."
    except subprocess.CalledProcessError as e:
        return f"Error: prepsubband failed with exit code {e.returncode}"

# --- STEP 2: ISOLATED ACCELSEARCH (External Process Pooling) ---
def build_dm_map(dat_dir):
    """Scans the directory once to create a mapping of DM values to absolute file paths."""
    files = glob.glob(os.path.join(dat_dir, "dm_DM*.dat"))
    dm_map = {}
    for f in files:
        try:
            # Extracts '70.10' from 'dm_DM70.10.dat'
            dm_str = os.path.basename(f).split('DM')[1].replace('.dat', '')
            dm_map[float(dm_str)] = os.path.abspath(f)
        except (IndexError, ValueError):
            continue
    return dm_map

def process_candidate(row_info):
    """Worker function to run accelsearch in a sandbox."""
    idx, row, dm_map = row_info
    target_dm = row['dm']
    r_bin = row['r']

    # --- Find Nearest DM Logic ---
    available_dms = np.array(list(dm_map.keys()))
    if len(available_dms) == 0:
        return f"Error: No .dat files found in {DAT_DIR}"
    
    nearest_val = available_dms[np.abs(available_dms - target_dm).argmin()]
    target_dat = dm_map[nearest_val]
    target_inf = target_dat.replace(".dat", ".inf")

    cand_label = f"cand_{idx}_dm{target_dm:.2f}_r{r_bin:.1f}"
    cand_dir = os.path.join(RESULTS_DIR, cand_label)
    os.makedirs(cand_dir, exist_ok=True)

    # Search Range calculation
    offset = r_bin * (args.tol / 100.0)
    r_lo, r_hi = r_bin - offset, r_bin + offset

    # Sandbox symlinks
    local_dat = os.path.join(cand_dir, os.path.basename(target_dat))
    local_inf = os.path.join(cand_dir, os.path.basename(target_inf))

    try:
        if not os.path.exists(local_dat): os.symlink(target_dat, local_dat)
        if not os.path.exists(local_inf): os.symlink(target_inf, local_inf)

        # Execute
        cmd = f"accelsearch -zmax 200 -rlo {r_lo:.2f} -rhi {r_hi:.2f} {os.path.basename(local_dat)}"
        subprocess.run(cmd, shell=True, check=True, cwd=cand_dir,
                        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

        # Cleanup Links
        os.remove(local_dat)
        os.remove(local_inf)

        # Results Check
        if not glob.glob(os.path.join(cand_dir, "*ACCEL*")):
            shutil.rmtree(cand_dir)
            return f"No Hits: {cand_label} (Checked DM {nearest_val:.2f})"

        return f"Success: {cand_label}"

    except Exception as e:
        return f"Failed: {cand_label} - {str(e)}"

def main():
    setup_directories()
    df = pd.read_csv(args.input)

    # --- Step 1: Run prepsubband ---
    status = run_prepsubband_logic(df)
    print(status)
    if "Error" in status:
        return

    # --- Step 2: Build DM Map and Run Accelsearch ---
    print("\n--- Building DM-to-File Map ---")
    dm_map = build_dm_map(DAT_DIR)
    
    print(f"--- Launching Parallel Accelsearch ({len(df)} candidates) ---")
    
    # Prepare tasks (passing the dm_map to every worker)
    tasks = [(idx, row, dm_map) for idx, row in df.iterrows()]
    
    with ProcessPoolExecutor(max_workers=args.cores) as executor:
        results = list(executor.map(process_candidate, tasks))

    success_count = sum(1 for r in results if "Success" in r)
    print(f"\nFinal Statistics: {success_count} successful detections from {len(df)} candidates.")

if __name__ == "__main__":
    main()
