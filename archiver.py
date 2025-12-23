# -*- coding: utf-8 -*-
import os
import csv
import pickle
import hashlib
from datetime import datetime
from pathlib import Path

def calculate_md5(file_path, block_size=65536):
    """Calculate md5 checksum from file path"""
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for block in iter(lambda: f.read(block_size), b''):
            md5.update(block)
    return md5.hexdigest()

def get_file_info(file_path, root):
    """returns ['File Path', 'Bytes', 'MD5', 'Timestamp']"""
    file_size = os.path.getsize(file_path)
    file_md5 = calculate_md5(file_path)
    file_timestamp = os.path.getmtime(file_path)
    timestamp_str = datetime.fromtimestamp(file_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    relative_path = os.path.relpath(file_path, root)
    return relative_path, file_size, file_md5, timestamp_str

def write_csv(csv_file_path, list_of_rows):
    """
    Writes a csv file
    
    :param csv_file_path: sting, file path of csv file to write
    :param list_of_rows: list containing csv.DictReader rows
    """


    def dict_filter(iterable_of_dicts, *keys):
        for d in iterable_of_dicts:
            yield dict((k, d[k]) for k in keys)

    with open(csv_file_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["File Path", "Bytes", "MD5", "Timestamp"])
        writer.writeheader()
        writer.writerows(dict_filter(list_of_rows, "File Path", "Bytes", "MD5", "Timestamp"))

def write_data(list_of_rows, asset_path):
    """
    Writes data
    
    :param list_of_rows: list containing csv.DictReader rows
    """
    total_bytes = 0
    for row in list_of_rows:
        filepath = row["File Path"]
        origin = row["Origin"]
        total_bytes += int(row["Bytes"])
        old_filepath = os.path.join(origin,filepath)
        new_filepath = os.path.join(asset_path,filepath)
        #move or copy logic goes here
        # print(f"{old_filepath}\n-->{new_filepath}")
    print(f"Total GB = {round(total_bytes / 1000 ** 3,2)}")

def generate_file_manifest(folder_path):
    parent_directory = os.path.dirname(folder_path)
    output_csv = os.path.join(parent_directory, 'file_manifest.csv')
    
    with open(output_csv, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['File Path', 'Bytes', 'MD5', 'Timestamp'])

        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                if not filename.startswith('.'):
                    file_path = os.path.join(dirpath, filename)
                    file_info = get_file_info(file_path, folder_path)
                    csv_writer.writerow(file_info)

    print(f"File manifest created: {output_csv}")

def verify_file_manifest(csv_file, expected_header = ['File Path', 'Bytes', 'MD5', 'Timestamp']):
    """
    Params: path to file_manifest.csv
    Returns True if the manifest is valid
    """
    asset_folder = os.path.join(os.path.dirname(csv_file), 'assets')
    if not os.path.isdir(asset_folder):
        print("No asset folder found.")
        return False
    

    with open(csv_file, mode='r', newline='') as csv_file:
        csv_reader = csv.reader(csv_file)
        header = next(csv_reader)  # Skip the header row, checking first
        if expected_header and expected_header != header:
            print("Header mismatch found.")
            return False

        for row in csv_reader:
            csv_asset_file_path, _, csv_md5, _ = row # TODO: change this to accept csvs with any number of fields
            file_path = os.path.join(asset_folder, csv_asset_file_path)
            if not os.path.exists(file_path):
                print(f'"File missing: "{file_path}')
                return False
            current_md5 = calculate_md5(file_path)
            if current_md5 != csv_md5:
                print(f'"MD5 mismatch: "{file_path}')
                return False
                
    return True

def group_files(csv_files, chunk_size = 50 * 1000**3, avoid_duplicates = True, seen_md5 = set()):
    """
    Processes a csv or a list of csv files. Returns the csvs in chunks based on size.
    Option to avoid duplicates and accept a set of known md5 to check against
    
    :param csv_files: list of csv files to process
    :param chunk_size: integer, size of each chunk in bytes (default to 50GB)
    :param avoid_duplicates: True/False, filter out duplicate files (default True)
    :param seen_md5: set of existing md5 to mark as duplicates (duplicates within the csv_files list will be added)
    """
    duplicates = []
    chunks = []
    current_chunk = []
    current_size = 0

    for csv_file in csv_files:
        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                file_path = row["File Path"]
                size = int(row["Bytes"])
                md5 = row["MD5"]
                timestamp = row["Timestamp"]
                # Add a new key-value pair
                row["Origin"] = csv_file.replace("file_manifest.csv","assets")

                # Check for duplicates
                if avoid_duplicates and md5 in seen_md5:
                    duplicates.append(row)
                    continue
                seen_md5.add(md5)

                # If adding this file exceeds required chunk size, start a new chunk
                if current_size + size > chunk_size:
                    chunks.append(current_chunk)
                    current_chunk = []
                    current_size = 0

                current_chunk.append(row)
                current_size += size

    # Add the last chunk if not empty
    if current_chunk:
        chunks.append(current_chunk)

    return chunks, duplicates

def load_pkl(filepath):
    with open(filepath, "rb") as f:
        loaded_data = pickle.load(f)
    return loaded_data

def write_chunks(chunks, output_dir, chunk_prefix = "CHK-", start_chunk = 1, mode = "move"):
    """
    converts a list of lists that contain dicts to chunked csvs
    
    :param chunks: list of lists containing file dicts for writing to csv
    :param output_dir: string, dir to write chunked csv files to
    :param chunk_prefix: string, prefix for the chunk buckets
    :param start_chunk: integer, number for the first chunk
    """
    if mode == "csv":
        # Make output dir
        os.makedirs(output_dir, exist_ok=True)
        # Write chunks to separate CSV files
        for i, chunk in enumerate(chunks, start_chunk):
            filename = f"{output_dir}/{chunk_prefix}{str(i).zfill(4)}.csv"
            write_csv(filename, chunk)
            print(f"Written {len(chunk)} files to {filename}")

    if mode == "move":
        # Make output dir
        os.makedirs(output_dir, exist_ok=True)
        # Write chunks to separate CSV files
        for i, chunk in enumerate(chunks, start_chunk):
            asset_folder_path = f"{output_dir}/{chunk_prefix}{str(i).zfill(4)}/assets"
            os.makedirs(asset_folder_path, exist_ok=True)
            filename = f"{output_dir}/{chunk_prefix}{str(i).zfill(4)}/file_manifest.csv"
            write_csv(filename, chunk)
            write_data(chunk, asset_folder_path)
            print(f"Written {len(chunk)} files to {filename}")





class Archiver:
    def __init__(self, source, output_dir="output", bucket_size= 50 * 1000 ** 3, dedupe=False, prefix="BDL-", truth=None):
        # self.source = Path(source)
        self.source = source
        self.bucket_size = bucket_size
        self.dedupe = dedupe
        self.prefix = prefix
        self.truth = truth
        self.output_dir = Path(output_dir)

    def run(self):
        print(f"Archiving from {self.source}")
        print(f"Bucket size: {self.bucket_size} bytes")
        print(f"Dedupe: {self.dedupe}")
        print(f"Prefix: {self.prefix}")
        print(f"Truth table: {self.truth}")
        print(f"Output directory: {self.output_dir}")

        # Create output directory if needed
        # self.output_dir.mkdir(parents=True, exist_ok=True)

        # Walk files
        # files = list(self.source.rglob("*"))
        # files = [f for f in files if f.is_file()]

        # Placeholder: your real logic goes here
        # current_bucket = []
        # current_size = 0
        # bucket_index = 1

        # for file in files:
        #     size = file.stat().st_size

        #     if current_size + size > self.bucket_size:
        #         # self._write_bucket(bucket_index, current_bucket)
        #         bucket_index += 1
        #         current_bucket = []
        #         current_size = 0

        #     current_bucket.append(file)
        #     current_size += size

        # if current_bucket:
        #     print(bucket_index, current_size)
            # self._write_bucket(bucket_index, current_bucket)

    # def _write_bucket(self, index, files):
    #     bucket_path = self.output_dir / f"bucket_{index}"
    #     bucket_path.mkdir(exist_ok=True)

    #     print(f"Writing bucket {index} with {len(files)} files")

    #     for file in files:
    #         dest = bucket_path / file.name
    #         dest.write_bytes(file.read_bytes())

if __name__ == "__main__":
    # Import vars
    from config import *

    # Create the archiver object
    archiver = Archiver(SOURCE, OUTPUT_DIR, BUCKET_SIZE, DEDUPE, PREFIX, TRUTH_PICKLE)
    archiver.run()
    # exit()


    # import glob
    # csv_files = sorted(glob.glob(f"{INPUT_DIR}/*/file_manifest.csv"))


    truth_table = load_pkl(TRUTH_PICKLE)
    groups, dupes = group_files(SOURCE, BUCKET_SIZE, DEDUPE, truth_table)

    write_chunks(groups, OUTPUT_DIR, PREFIX, start_chunk=1)
    write_chunks([dupes], OUTPUT_DIR, chunk_prefix = "DUP-", start_chunk = 1, mode = "csv")
