import csv
import sys
from pathlib import Path


import csv

def write_manifest_csv(source_csv_file_path, target_csv_file_path):
    """
    Writes a CSV file filtered to standard file_manifest.csv columns.
    """

    columns = ["File Path", "Bytes", "MD5", "Timestamp"]

    filtered_rows = dict_filter(
        gen_csv_rows(source_csv_file_path),
        *columns
    )

    with open(target_csv_file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(filtered_rows)

def gen_csv_rows(csv_file):
    """
    reads a csv file and generates rows
    """

    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            yield row

def dict_filter(iterable_of_dicts, *keys):
    for d in iterable_of_dicts:
        yield dict((k, d[k]) for k in keys)

def main():
    if len(sys.argv) < 2:
        print("Usage: python manifilter.py <input csv>")
        sys.exit(1)

    csv_file = Path(sys.argv[1])
    manifest_folder_path = Path(csv_file.parent / csv_file.stem)
    manifest_folder_path.mkdir(parents=True, exist_ok=True)
    write_manifest_csv(csv_file, manifest_folder_path / 'file_manifest.csv')

if __name__ == "__main__":
    main()