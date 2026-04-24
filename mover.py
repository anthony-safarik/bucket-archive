import csv
import glob
import os
from pathlib import Path

def get_csv_rows(csv_file):
    rows = []
    with open(csv_file, newline='', encoding='utf-8') as f:
        dict_reader = csv.DictReader(f)
        for dict in dict_reader:
            rows.append(dict)
    return rows

def main(src):
    csv_files = sorted(glob.glob(f"{src}/*.csv"))
    for csv_file in csv_files:

        csv_path = Path(csv_file)
        csv_assets_path = csv_path.parent / csv_path.stem / 'assets'
        csv_manifest_path = csv_path.parent / csv_path.stem / 'file_manifest.csv'

        for row in get_csv_rows(csv_file):
            csv_origin_path = Path (row['Origin']) / row['File Path']
            csv_target_path = csv_assets_path / row['File Path']
            csv_target_dir = csv_target_path.parent

            print(f"----\nmake dir {csv_target_dir}\nrename file {csv_origin_path}\nto... {csv_target_path}\n")
            os.makedirs(csv_target_dir, exist_ok=True)
            if os.path.exists(csv_origin_path):
                os.rename(csv_origin_path, csv_target_path)
        if os.path.exists(csv_assets_path): os.rename(csv_path, csv_manifest_path)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python chunker.py <input directory> (subfolders must contain file_manifest.csv)")
    else:
        main(*sys.argv[1:])
    main()