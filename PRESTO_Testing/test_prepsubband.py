import subprocess
import time
import os
import numpy as np
from concurrent.futures import ProcessPoolExecutor

# -------- USER CONFIG --------
FIL_FILE = "/lustre_archive/spotlight/Nishant/AA_PULSCAN_TEST/DATA_Test02/*.fil"
OUT_DIR = "benchmark_output_prepsubband"
CORE_LIST = [8, 16, 32]

# 100 evenly spaced DMs
DM_LIST = np.linspace(10, 200, 100)
# ----------------------------

os.makedirs(OUT_DIR, exist_ok=True)


# ----------------------------
# Helper: Split DM list into chunks
# ----------------------------
def chunk_dms(dm_list, n_chunks):
    return np.array_split(dm_list, n_chunks)


# ----------------------------
# Method 1: Internal parallelism
# ----------------------------
def run_internal(ncpus):
    start = time.time()

    lodm = float(DM_LIST[0])
    dmstep = float(DM_LIST[1] - DM_LIST[0])
    numdms = len(DM_LIST)

    out_prefix = os.path.join(OUT_DIR, f"internal_c{ncpus}")

    cmd = [
        "prepsubband",
        "-nobary",
        "-ncpus", str(ncpus),
        "-lodm", str(lodm),
        "-dmstep", str(dmstep),
        "-numdms", str(numdms),
        FIL_FILE,
        "-o", out_prefix
    ]

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    return time.time() - start


# ----------------------------
# Worker for external parallelism
# ----------------------------
def run_chunk(args):
    dm_chunk, tag, idx = args

    lodm = float(dm_chunk[0])

    if len(dm_chunk) > 1:
        dmstep = float(dm_chunk[1] - dm_chunk[0])
    else:
        dmstep = 1.0

    numdms = len(dm_chunk)

    out_prefix = os.path.join(OUT_DIR, f"external_{tag}_chunk{idx}")

    cmd = [
        "prepsubband",
        "-nobary",
        "-lodm", str(lodm),
        "-dmstep", str(dmstep),
        "-numdms", str(numdms),
        FIL_FILE,
        "-o", out_prefix
    ]

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# ----------------------------
# Method 2: External parallelism
# ----------------------------
def run_external(ncores):
    start = time.time()

    dm_chunks = chunk_dms(DM_LIST, ncores)

    tasks = [(dm_chunks[i], f"c{ncores}", i) for i in range(len(dm_chunks))]

    with ProcessPoolExecutor(max_workers=ncores) as executor:
        list(executor.map(run_chunk, tasks))

    return time.time() - start


# ----------------------------
# Main Benchmark Loop
# ----------------------------
def main():
    results = []

    print("\n--- PREPSUBBAND Parallelization Benchmark ---\n")

    for cores in CORE_LIST:
        print(f"\nTesting with {cores} cores...")

        # Internal parallelism
        t_internal = run_internal(cores)
        print(f"Internal (-ncpus {cores}): {t_internal:.2f} sec")

        # External parallelism
        t_external = run_external(cores)
        print(f"External ({cores} processes): {t_external:.2f} sec")

        results.append((cores, t_internal, t_external))

    # ----------------------------
    # Final Summary
    # ----------------------------
    print("\n=== FINAL RESULTS ===")
    print("Cores | Internal(s) | External(s) | Faster")

    for cores, ti, te in results:
        faster = "Internal" if ti < te else "External"
        print(f"{cores:5d} | {ti:10.2f} | {te:10.2f} | {faster}")


if __name__ == "__main__":
    main()
