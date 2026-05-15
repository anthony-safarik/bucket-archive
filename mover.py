import csv
import glob
import os
import shutil
from pathlib import Path
from utils import calculate_md5, human_size

def get_csv_rows(csv_file):
    rows = []
    with open(csv_file, newline='', encoding='utf-8') as f:
        dict_reader = csv.DictReader(f)
        for dict in dict_reader:
            rows.append(dict)
    return rows

def preflight(src):
    total_bytes = 0
    all_paths = set()
    csv_files = sorted(glob.glob(f"{src}/*.csv"))
    for csv_file in csv_files:
        for row in get_csv_rows(csv_file):
            total_bytes += int(row['Bytes'])
            csv_origin_path = Path (row['Origin']) / row['File Path']
            if not os.path.exists(csv_origin_path):
                print(f'CSV origin path missing:\n{csv_origin_path}\nexiting...')
                exit()
            if row['File Path'] in all_paths:
                print(f'Duplicate paths found in {csv_file}')
                return -1
            else:
                all_paths.add(row['File Path'])
    return total_bytes


def main(src):
    total_bytes = preflight(src)
    total_size_human = human_size(total_bytes)

    #exit out if there are duplicate paths
    if total_bytes == -1:
        print("Skipping move")
    else:
        copied_bytes = 0
        csv_files = sorted(glob.glob(f"{src}/*.csv"))
        for csv_file in csv_files:

            csv_path = Path(csv_file)
            csv_assets_path = csv_path.parent / csv_path.stem / 'assets'
            csv_manifest_path = csv_path.parent / csv_path.stem / 'file_manifest.csv'

            for row in get_csv_rows(csv_file):
                csv_bytes = int(row['Bytes'])
                csv_origin_path = Path (row['Origin']) / row['File Path']
                csv_target_path = csv_assets_path / row['File Path']
                csv_target_dir = csv_target_path.parent

                os.makedirs(csv_target_dir, exist_ok=True)

                percent_completed = copied_bytes/total_bytes * 100

                print(f"{int(percent_completed)}% of {total_size_human} moved {csv_target_path}")

                try:
                    os.rename(csv_origin_path, csv_target_path)
                except OSError:
                   print(f'unable to move {csv_origin_path}')

                if os.path.exists(csv_target_path): copied_bytes += csv_bytes
                


            if os.path.exists(csv_assets_path): os.rename(csv_path, csv_manifest_path)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python mover.py <input directory> (dir containing csv files)")
    else:
        main(*sys.argv[1:])