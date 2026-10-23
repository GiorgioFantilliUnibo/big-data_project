import os
import shutil
import zipfile


SOURCE_DIR = r"C:\Users\giorg\IdeaProjects\big-data_project\datasets"
TX_ZIP_PATH = os.path.join(SOURCE_DIR, "transactions.csv.zip")
BLOCKS_SRC_PATH = os.path.join(SOURCE_DIR, "blocks.csv")

PROJECT_DIR = r"C:\Users\giorg\IdeaProjects\big-data_project"
BIG_DIR = os.path.join(PROJECT_DIR, "datasets", "big")
SAMPLE_DIR = os.path.join(PROJECT_DIR, "datasets", "sample")

os.makedirs(BIG_DIR, exist_ok=True)
os.makedirs(SAMPLE_DIR, exist_ok=True)

TX_FULL_OUT = os.path.join(BIG_DIR, "transactions.csv")
BLOCKS_FULL_OUT = os.path.join(BIG_DIR, "blocks.csv")
TX_SAMPLE_OUT = os.path.join(SAMPLE_DIR, "transactions-sample.csv")
BLOCKS_SAMPLE_OUT = os.path.join(SAMPLE_DIR, "blocks-sample.csv")

FULL_TX_LIMIT_BYTES = int(5.2 * (1024**3))
SAMPLE_TX_LIMIT_BYTES = int(6.5 * (1024**2))
SAMPLE_BLOCKS_LIMIT_BYTES = int(2.5 * (1024**2))

sample_block_numbers = set()

print("1. Analyzing and extracting transactions.csv.zip...")
with zipfile.ZipFile(TX_ZIP_PATH, "r") as z:
  csv_names = [name for name in z.namelist() if name.endswith(".csv")]
  tx_csv_name = csv_names[0]
  info = z.getinfo(tx_csv_name)
  print(
      f"   Found file in zip: {tx_csv_name} (Uncompressed file size:"
      f" {info.file_size / (1024**3):.2f} GB)"
  )

  with (
      z.open(tx_csv_name, "r") as src,
      open(TX_FULL_OUT, "wb") as f_full,
      open(TX_SAMPLE_OUT, "wb") as f_sample,
  ):

    header_line = src.readline()
    header_cols = header_line.decode("utf-8", errors="ignore").strip().split(",")
    print(f"   Columns in transactions.csv ({len(header_cols)}): {header_cols}")

    block_num_idx = header_cols.index("block_number")

    f_full.write(header_line)
    f_sample.write(header_line)

    full_bytes = len(header_line)
    sample_bytes = len(header_line)
    last_print_bytes = 0

    for line in src:
      f_full.write(line)
      full_bytes += len(line)

      if sample_bytes < SAMPLE_TX_LIMIT_BYTES:
        f_sample.write(line)
        sample_bytes += len(line)
        parts = line.decode("utf-8", errors="ignore").split(",")
        if len(parts) > block_num_idx:
          sample_block_numbers.add(parts[block_num_idx].strip())

      if full_bytes - last_print_bytes >= 250 * 1024 * 1024:
        last_print_bytes = full_bytes
        print(
            f"   Extracted {full_bytes / (1024**3):.2f} GB /"
            f" {FULL_TX_LIMIT_BYTES / (1024**3):.2f} GB...",
            end="\r",
            flush=True,
        )

      if full_bytes >= FULL_TX_LIMIT_BYTES:
        break

print(
    f"\n   Completed transactions.csv! Distinct sample blocks collected:"
    f" {len(sample_block_numbers)}"
)

print(
    "\n2. Creating consistent blocks-sample.csv and copying full blocks.csv..."
)
with (
    open(BLOCKS_SRC_PATH, "rb") as b_src,
    open(BLOCKS_SAMPLE_OUT, "wb") as b_sample,
):

  b_header = b_src.readline()
  b_cols = b_header.decode("utf-8", errors="ignore").strip().split(",")
  print(f"   Columns in blocks.csv ({len(b_cols)}): {b_cols}")

  num_idx = b_cols.index("number")
  b_sample.write(b_header)
  b_sample_bytes = len(b_header)
  matched_blocks = 0

  for line in b_src:
    parts = line.decode("utf-8", errors="ignore").split(",")
    if len(parts) > num_idx and parts[num_idx].strip() in sample_block_numbers:
      b_sample.write(line)
      b_sample_bytes += len(line)
      matched_blocks += 1
      if b_sample_bytes >= SAMPLE_BLOCKS_LIMIT_BYTES:
        break

if os.path.abspath(BLOCKS_SRC_PATH) != os.path.abspath(BLOCKS_FULL_OUT):
  print(f"   Copying {os.path.basename(BLOCKS_SRC_PATH)} to {BIG_DIR}...")
  shutil.copy2(BLOCKS_SRC_PATH, BLOCKS_FULL_OUT)

std_tx_sample = os.path.join(SAMPLE_DIR, "transactions.csv")
std_blocks_sample = os.path.join(SAMPLE_DIR, "blocks.csv")
if os.path.abspath(TX_SAMPLE_OUT) != os.path.abspath(std_tx_sample):
  shutil.copy2(TX_SAMPLE_OUT, std_tx_sample)
if os.path.abspath(BLOCKS_SAMPLE_OUT) != os.path.abspath(std_blocks_sample):
  shutil.copy2(BLOCKS_SAMPLE_OUT, std_blocks_sample)

total_sample_mb = (
    os.path.getsize(TX_SAMPLE_OUT) + os.path.getsize(BLOCKS_SAMPLE_OUT)
) / (1024**2)
total_full_gb = (
    os.path.getsize(TX_FULL_OUT) + os.path.getsize(BLOCKS_FULL_OUT)
) / (1024**3)

print(f"\n=== FINAL SUMMARY ===")
print(f"- Matching blocks found in sample: {matched_blocks}")
print(
    f"- Total SAMPLE size (datasets/sample/): {total_sample_mb:.2f} MB"
    " (Limit <= 10 MB)"
)
print(
    f"- Total FULL size   (datasets/big/):    {total_full_gb:.2f} GB"
    " (Minimum >= 5 GB)"
)