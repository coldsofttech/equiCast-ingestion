import fnmatch
import os
import sys
from pathlib import Path

RATE_PER_GB = 0.023  # USD
THRESHOLD = 1.0  # USD for coloring

if len(sys.argv) < 4:
    print("Usage: python calculate_s3_cost.py <directory_path> <file_pattern> <custom_message>")
    print("Example: python calculate_s3_cost.py ./data '*.json' 'FX Pairs'")
    exit(1)

dir_path = Path(sys.argv[1])
file_pattern = sys.argv[2]
custom_message = sys.argv[3]

if not dir_path.exists() or not dir_path.is_dir():
    print(f"Error: Path '{dir_path}' does not exist or is not a directory.")
    exit(1)

all_files = [f for f in dir_path.rglob("*") if f.is_file()]
artifacts = [f for f in all_files if fnmatch.fnmatch(f.name, file_pattern)]

if not artifacts:
    print(f"No files found in {dir_path}/ matching pattern: {file_pattern}")
    exit(1)

total_bytes = sum(f.stat().st_size for f in artifacts)
total_gb = total_bytes / (1024 ** 3)
cost = total_gb * RATE_PER_GB
color = "red" if cost > THRESHOLD else "green"

print(f"Total Directory Size (GB) for pattern '{file_pattern}': {total_gb:.6f}")
print(f"Estimated S3 Storage Cost: ${cost:.6f} ({color})")

summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
if summary_path:
    with open(summary_path, "a") as f:
        f.write(f"### 💰 {custom_message}\n")
        f.write("| Directory | Pattern | Storage Size (GB) | Estimated Monthly Cost |\n")
        f.write("|-----------|---------|-------------------|------------------------|\n")
        f.write(
            f"| `{dir_path}` | `{file_pattern}` | `{total_gb:.6f}` | "
            f"<span style='color: {color}'>${cost:.6f}</span> |\n"
        )

print(f"::set-output name=directory_size_gb::{total_gb:.6f}")
print(f"::set-output name=estimated_cost::{cost:.6f}")
