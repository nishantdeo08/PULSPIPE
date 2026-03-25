import pandas as pd
import subprocess
import glob
import os
import argparse
import shutil
from concurrent.futures import ProcessPoolExecutor

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Pulsar Search: Prepsubband + Parallel Accelsearch")
parser.add_argument("--cores", type=int, default=32, help="Number of CPU cores")
parser.add_argument("--tol", type=float, default=1, help="Tolerance in PERCENTAGE (e.g., 5 for 5%)")
parser.add_argument("--input", type=str, required=True, help="Input CSV file (candidates.csv)")
parser.add_argument("--fil", type=str, required=True, help="Path to the .fil source file")
parser.add_argument("--output", type=str, help="Optional: Path to output directory (Defaults to input CSV directory)")
args = parser.parse_args()

# --- Dynamic Path Logic ---
# If no output is provided, use the directory where the input file lives
if args.output:
    BASE_OUT = args.output
else:
    # Get absolute path of input file and its directory
    input_dir = os.path.dirname(os.path.abspath(args.input))
    BASE_OUT = input_dir

DAT_DIR = os.path.join(BASE_OUT, "dedispersed_timeseries")
RESULTS_DIR = os.path.join(BASE_OUT, "accel_results")

def setup_directories():
    """Create the directory tree."""
    for folder in [BASE_OUT, DAT_DIR, RESULTS_DIR]:
        os.makedirs(folder, exist_ok=True)
    print(f"Working Directory: {BASE_OUT}")

def run_prepsubband(unique_dms):
    """Step 1: Generate all required .dat files."""
    print(f"--- Starting Prepsubband for {len(unique_dms)} unique DMs ---")
    for dm in unique_dms:
        # Use a consistent prefix for globbing later
        out_prefix = os.path.join(DAT_DIR, f"dm_{dm:.2f}")
        
        if glob.glob(f"{out_prefix}*.dat"):
            continue

        #cmd = [
        #    "prepsubband", "-nobary", 
        #    "-lodm", str(dm), 
        #    "-dmstep", "0.5", 
        #    "-numdms", "4", 
        #    args.fil, 
        #    "-o", out_prefix
        #]

        cmd = [
            "prepdata", "-nobary", 
            "-dm", str(dm),
            args.fil,
            "-o", out_prefix
        ]
        
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            print(f"Prepsubband failed for DM {dm}: {e}")

def process_candidate(row_tuple):
    """Step 2: Run Accelsearch and move results to unique folders."""
    idx, row = row_tuple
    dm = row['dm']
    r_bin = row['r']
    
    # Percentage-based tolerance math
    offset = r_bin * (args.tol / 100.0)
    r_lo = r_bin - offset
    r_hi = r_bin + offset
    
    # Candidate specific subdirectory
    cand_label = f"cand_{idx}_dm{dm:.2f}_r{r_bin:.1f}"
    cand_dir = os.path.join(RESULTS_DIR, cand_label)
    os.makedirs(cand_dir, exist_ok=True)

    # Find the specific .dat files for this DM
    dat_pattern = os.path.join(DAT_DIR, f"dm_{dm:.2f}*.dat")
    dat_files = glob.glob(dat_pattern)

    for dat in dat_files:
        # Run Accelsearch (outputs go to DAT_DIR initially)
        accel_cmd = f"accelsearch -zmax 200 -rlo {r_lo:.2f} -rhi {r_hi:.2f} {dat}"
        
        try:
            subprocess.run(accel_cmd, shell=True, check=True, 
                           stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            
            # Move the specific ACCEL files generated for this .dat file
            base_name = os.path.basename(dat).replace(".dat", "")
            generated_files = glob.glob(os.path.join(DAT_DIR, f"{base_name}*ACCEL*"))
            
            for f in generated_files:
                shutil.move(f, os.path.join(cand_dir, os.path.basename(f)))
        except subprocess.CalledProcessError:
            pass

    return f"Done: {cand_label}"

def main():
    setup_directories()
    
    try:
        df = pd.read_csv(args.input)
    except Exception as e:
        print(f"Error reading input file: {e}")
        return

    # 1. Prepsubband (Sequential)
    run_prepsubband(df['dm'].unique())

    # 2. Accelsearch (Parallel)
    print(f"\n--- Parallel Accelsearch: {len(df)} candidates on {args.cores} cores ---")
    tasks = list(df.iterrows())
    
    with ProcessPoolExecutor(max_workers=args.cores) as executor:
        list(executor.map(process_candidate, tasks))

    print(f"\nPipeline Finished.")
    print(f"Results located in: {RESULTS_DIR}")

if __name__ == "__main__":
    main()
