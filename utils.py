import hashlib
import glob
import csv

def human_size(num):
    """Convert bytes to a human‑readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num < 1024:
            return f"{num:.2f}{unit}"
        num /= 1024
    return f"{num:.2f}PB"

def calculate_md5(file_path, block_size=65536):
    """Calculate md5 checksum from file path"""
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for block in iter(lambda: f.read(block_size), b''):
            md5.update(block)
    return md5.hexdigest()

def glob_assets(dir):
    return sorted(glob.glob(f"{dir}/*/assets"))

def glob_fm(dir):
    return sorted(glob.glob(f"{dir}/*/file_manifest.csv"))

def gen_csv_rows(csv_file):
    with open(csv_file, newline='', encoding='utf-8') as f:
        dict_reader = csv.DictReader(f)
        for dict in dict_reader:
            yield dict