#!/usr/bin/env python3
import os
import subprocess
import argparse
from pathlib import Path
import multiprocessing

def check_tools():
    """Check if GTDB-Tk and pplacer are installed in gtdbtk_env, install if missing."""
    try:
        # Check if gtdbtk_env exists
        result = subprocess.run(
            ["conda", "env", "list"], capture_output=True, text=True, check=True
        )
        if "gtdbtk_env" not in result.stdout:
            print("Creating gtdbtk_env and installing GTDB-Tk 2.4.1 and pplacer 1.1.alpha19...")
            subprocess.run(
                ["conda", "create", "-n", "gtdbtk_env", "-c", "bioconda", "-c", "conda-forge",
                 "gtdbtk=2.4.1", "pplacer=1.1.alpha19"],
                check=True
            )
            print("gtdbtk_env created and tools installed.")
        # Verify GTDB-Tk version
        conda_cmd = "source /opt/conda/etc/profile.d/conda.sh && conda activate gtdbtk_env && gtdbtk --version"
        gtdbtk_version = subprocess.run(
            conda_cmd, shell=True, capture_output=True, text=True, check=True, executable="/bin/bash"
        ).stdout.strip()
        if "2.4.1" not in gtdbtk_version:
            raise ValueError("GTDB-Tk version 2.4.1 not found in gtdbtk_env")
    except (subprocess.CalledProcessError, FileNotFoundError, ValueError) as e:
        print(f"Error checking/installing tools: {e}")
        print("Attempting to recreate gtdbtk_env...")
        subprocess.run(["conda", "env", "remove", "-n", "gtdbtk_env"], check=True)
        subprocess.run(
            ["conda", "create", "-n", "gtdbtk_env", "-c", "bioconda", "-c", "conda-forge",
             "gtdbtk=2.4.1", "pplacer=1.1.alpha19"],
            check=True
        )
        print("gtdbtk_env recreated and tools installed.")

def run_gtdbtk(input_dir, gtdbtk_db, output_dir, cpus):
    """Run GTDB-Tk pipeline on .fa files in Refined_bins."""
    refined_bins = Path(input_dir) / "Refined_bins"
    if not refined_bins.is_dir():
        print(f"Error: {refined_bins} directory not found.")
        exit(1)

    fa_files = [f for f in refined_bins.glob("*.fa")]
    if not fa_files:
        print(f"Error: No .fa files found in {refined_bins}.")
        exit(1)
    print(f"Found {len(fa_files)} .fa files in {refined_bins}.")

    # Create output directories
    identify_dir = Path(output_dir) / "Refined_identify"
    align_dir = Path(output_dir) / "Refined_align"
    classify_dir = Path(output_dir) / "Refined_classify"
    for d in [identify_dir, align_dir, classify_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Conda activation command
    conda_cmd = "source /opt/conda/etc/profile.d/conda.sh && conda activate gtdbtk_env"

    # Run GTDB-Tk identify
    print("Running GTDB-Tk identify...")
    subprocess.run(
        f"{conda_cmd} && gtdbtk identify --genome_dir {refined_bins} "
        f"--out_dir {identify_dir} -x fa --cpus {cpus}",
        shell=True, check=True, executable="/bin/bash"
    )
    print("GTDB-Tk identify completed.")

    # Run GTDB-Tk align
    print("Running GTDB-Tk align...")
    subprocess.run(
        f"{conda_cmd} && gtdbtk align --identify_dir {identify_dir} "
        f"--out_dir {align_dir} --cpus {cpus}",
        shell=True, check=True, executable="/bin/bash"
    )
    print("GTDB-Tk align completed.")

    # Run GTDB-Tk classify
    print("Running GTDB-Tk classify...")
    subprocess.run(
        f"{conda_cmd} && gtdbtk classify --genome_dir {refined_bins} "
        f"--out_dir {classify_dir} --skip_ani_screen -x fa "
        f"--pplacer_cpus {cpus} --scratch_dir {classify_dir} --align_dir {align_dir}",
        shell=True, check=True, executable="/bin/bash"
    )
    print("GTDB-Tk classify completed.")

def main():
    parser = argparse.ArgumentParser(description="Run GTDB-Tk on Refined_bins FASTA files")
    parser.add_argument("--input_dir", required=True, help="Input directory containing Refined_bins")
    parser.add_argument("--output_dir", required=True, help="Output directory for GTDB-Tk results")
    parser.add_argument("--gtdbtk_db", required=True, help="Path to GTDB-Tk database")
    args = parser.parse_args()

    # Set GTDB-Tk database environment variable
    os.environ["GTDBTK_DATA_PATH"] = args.gtdbtk_db

    # Determine CPU count (use 16 if available, else 12)
    cpus = min(multiprocessing.cpu_count(), 16)
    if cpus < 16:
        cpus = min(cpus, 12)
    print(f"Using {cpus} CPUs.")

    # Check and install tools
    check_tools()

    # Run GTDB-Tk pipeline
    run_gtdbtk(args.input_dir, args.gtdbtk_db, args.output_dir, cpus)

if __name__ == "__main__":
    main()

#python run_gtdbtk.py --input_dir /results --output_dir /results/gtdbtk_out --gtdbtk_db /path/to/gtdbtk_db
