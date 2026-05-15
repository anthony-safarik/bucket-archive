import glob
import sys
import os
from pathlib import Path
from utils import gen_csv_rows

def main(src):
    print(f'-----undo-move-----\n{src}\n--------------')
    csv_files = sorted(glob.glob(f"{src}/*/file_manifest.csv"))

    for csv_file in csv_files:
        csv_assets = Path(csv_file.replace('file_manifest.csv', 'assets'))
        null = input('PAUSE')

        for row in gen_csv_rows(csv_file):
            csv_origin_path = Path (row['Origin']) / row['File Path']
            chunked_file_path = csv_assets / row['File Path']

            print(f'{chunked_file_path} --> {csv_origin_path}')

            os.makedirs(csv_origin_path.parent, exist_ok=True)

            try:
                os.rename(chunked_file_path, csv_origin_path)
            except OSError:
                print(f'unable to move {csv_origin_path}')

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python undover.py <input directory> (parent of chunk folders)>")
    else:
        main(*sys.argv[1:])