import subprocess
import time
import os
from concurrent.futures import ProcessPoolExecutor
import numpy as np

# -------- USER CONFIG --------
FIL_FILE = "/lustre_archive/spotlight/Nishant/AA_PULSCAN_TEST/DATA_Test02/*.fil"
DM_LIST = np.linspace(10, 200, 100)  # 100 DMs from 10 to 200
OUT_DIR = "benchmark_output"
CORE_LIST = [8, 16, 32]
# ----------------------------

os.makedirs(OUT_DIR, exist_ok=True)


# ----------------------------
# Method 1: Internal parallelism
# ----------------------------
def run_internal(ncpus):
    start = time.time()

    for dm in DM_LIST:
        out_prefix = os.path.join(OUT_DIR, f"internal_dm_{dm:.2f}_c{ncpus}")

        cmd = [
            "prepdata",
            "-nobary",
            "-ncpus", str(ncpus),
            "-dm", str(dm),
            FIL_FILE,
            "-o", out_prefix
        ]

        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    end = time.time()
    return end - start


# ----------------------------
# Method 2: External parallelism
# ----------------------------
def run_single_dm(args):
    dm, tag = args
    out_prefix = os.path.join(OUT_DIR, f"external_dm_{dm:.2f}_{tag}")

    cmd = [
        "prepdata",
        "-nobary",
        "-dm", str(dm),
        FIL_FILE,
        "-o", out_prefix
    ]

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def run_external(ncores):
    start = time.time()

    tasks = [(dm, f"c{ncores}") for dm in DM_LIST]

    with ProcessPoolExecutor(max_workers=ncores) as executor:
        executor.map(run_single_dm, tasks)

    end = time.time()
    return end - start


# ----------------------------
# Main Benchmark Loop
# ----------------------------
def main():
    results = []

    print("\n--- PRESTO Parallelization Benchmark ---\n")

    for cores in CORE_LIST:
        print(f"\nTesting with {cores} cores...")

        # Internal
        t_internal = run_internal(cores)
        print(f"Internal (-ncpus {cores}): {t_internal:.2f} sec")

        # External
        t_external = run_external(cores)
        print(f"External ({cores} processes): {t_external:.2f} sec")

        results.append((cores, t_internal, t_external))

    # ----------------------------
    # Summary
    # ----------------------------
    print("\n=== FINAL RESULTS ===")
    print("Cores | Internal(s) | External(s) | Faster")

    for cores, ti, te in results:
        faster = "Internal" if ti < te else "External"
        print(f"{cores:5d} | {ti:10.2f} | {te:10.2f} | {faster}")


if __name__ == "__main__":
    main()
