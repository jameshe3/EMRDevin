#!/usr/bin/env python3
import os
import shutil

def copy_failing_job_files():
    # Source directory containing the failing job files
    src_dir = "../spark_jobs"
    
    # Destination directory (latest diagnostics directory)
    diagnostics_dirs = [d for d in os.listdir('.') if d.startswith('diagnostics_')]
    if not diagnostics_dirs:
        print("No diagnostics directory found")
        return False
    
    # Sort by creation time (newest first)
    diagnostics_dirs.sort(reverse=True)
    dst_dir = diagnostics_dirs[0]
    
    # Create a failing_job directory in the diagnostics directory
    failing_job_dir = os.path.join(dst_dir, "failing_job")
    os.makedirs(failing_job_dir, exist_ok=True)
    
    # Files to copy
    files_to_copy = [
        "failing_job.py",
        "failing_job_output.txt",
        "failing_job_error.txt",
        "failing_job_result.txt"
    ]
    
    # Copy the files
    for file in files_to_copy:
        src_file = os.path.join(src_dir, file)
        dst_file = os.path.join(failing_job_dir, file)
        if os.path.exists(src_file):
            shutil.copy2(src_file, dst_file)
            print(f"Copied {file} to {failing_job_dir}")
        else:
            print(f"Warning: {file} not found in {src_dir}")
    
    return True

if __name__ == "__main__":
    copy_failing_job_files()
