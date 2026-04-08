import subprocess
import time
import os
import glob
import numpy as np
import shutil
from concurrent.futures import ProcessPoolExecutor

# -------- USER CONFIG --------
# Expanded glob at the start to ensure subprocess can read the files
FIL_PATTERN = "/lustre_archive/spotlight/Nishant/AA_PULSCAN_TEST/DATA_Test02/*.fil"
INPUT_FILES = sorted(glob.glob(FIL_PATTERN))

OUT_DIR = "benchmark_output_prepsubband"
CORE_LIST = [1, 4, 8, 16]

# 100 evenly spaced DMs
DM_LIST = np.linspace(10, 200, 100)
# ----------------------------

if not INPUT_FILES:
    raise FileNotFoundError(f"No files found matching pattern: {FIL_PATTERN}")

os.makedirs(OUT_DIR, exist_ok=True)


def chunk_dms(dm_list, n_chunks):
    """Splits DM list into roughly equal chunks."""
    return np.array_split(dm_list, n_chunks)


def run_internal(ncpus):
    """Method 1: PRESTO's internal OpenMP parallelism."""
    start = time.time()

    lodm = float(DM_LIST[0])
    # Calculate step; default to 1.0 if only one DM provided
    dmstep = float(DM_LIST[1] - DM_LIST[0]) if len(DM_LIST) > 1 else 1.0
    numdms = len(DM_LIST)

    out_prefix = os.path.join(OUT_DIR, f"internal_c{ncpus}")

    # Build command list
    cmd = [
        "prepsubband",
        "-nobary",
        "-ncpus", str(ncpus),
        "-lodm", f"{lodm:.4f}",
        "-dmstep", f"{dmstep:.4f}",
        "-numdms", str(numdms),
        "-o", out_prefix
    ] + INPUT_FILES

    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error in Internal (Cores {ncpus}): {e.stderr.decode()}")
        return None

    return time.time() - start


def run_chunk_worker(args):
    """Worker function for Method 2."""
    dm_chunk, tag, idx = args
    
    lodm = float(dm_chunk[0])
    dmstep = float(dm_chunk[1] - dm_chunk[0]) if len(dm_chunk) > 1 else 1.0
    numdms = len(dm_chunk)
    out_prefix = os.path.join(OUT_DIR, f"external_{tag}_chunk{idx}")

    cmd = [
        "prepsubband",
        "-nobary",
        "-lodm", f"{lodm:.4f}",
        "-dmstep", f"{dmstep:.4f}",
        "-numdms", str(numdms),
        "-o", out_prefix
    ] + INPUT_FILES

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)


def run_external(ncores):
    """Method 2: Python ProcessPoolExecutor parallelism."""
    start = time.time()
    dm_chunks = chunk_dms(DM_LIST, ncores)

    # Preparing arguments for the pool
    tasks = [(dm_chunks[i], f"c{ncores}", i) for i in range(len(dm_chunks))]

    try:
        with ProcessPoolExecutor(max_workers=ncores) as executor:
            executor.map(run_chunk_worker, tasks)
    except Exception as e:
        print(f"Error in External (Cores {ncores}): {e}")
        return None

    return time.time() - start


def main():
    results = []

    print(f"\n{'='*50}")
    print("PREPSUBBAND Parallelization Benchmark")
    print(f"Files found: {len(INPUT_FILES)}")
    print(f"{'='*50}\n")

    for cores in CORE_LIST:
        print(f"--- Testing {cores} Cores ---")

        # Method 1
        t_internal = run_internal(cores)
        if t_internal:
            print(f"  Internal (-ncpus): {t_internal:10.2f}s")
        
        # Method 2
        t_external = run_external(cores)
        if t_external:
            print(f"  External (Pool):   {t_external:10.2f}s")

        if t_internal and t_external:
            results.append((cores, t_internal, t_external))

    # Final Summary Table
    print("\n" + "="*60)
    print(f"{'Cores':<8} | {'Internal(s)':<12} | {'External(s)':<12} | {'Efficiency'}")
    print("-" * 60)

    for cores, ti, te in results:
        winner = "Internal" if ti < te else "External"
        diff = abs(ti - te)
        print(f"{cores:<8} | {ti:<12.2f} | {te:<12.2f} | {winner} (+{diff:.1f}s)")
    print("="*60)

if __name__ == "__main__":
    main()
