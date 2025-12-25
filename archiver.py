# -*- coding: utf-8 -*-
# TODO need to generalize write chunks and write data. right now this is only testing ok with groups, need to address dupes and oversize
import os
import csv
import pickle
import hashlib
from datetime import datetime
from pathlib import Path

class Manifest:
    def __init__(self, source):
        self.source = source
        self.parent_directory = os.path.dirname(self.source)
        self.output_csv = os.path.join(self.parent_directory, 'file_manifest.csv')
        print(self.source)

    def generate_file_manifest(self):
        
        with open(self.output_csv, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['File Path', 'Bytes', 'MD5', 'Timestamp'])

            for dirpath, dirnames, filenames in os.walk(self.source):
                dirnames.sort()
                for filename in sorted(filenames):
                    if not filename.startswith('.'):
                        file_path = os.path.join(dirpath, filename)
                        file_info = self.get_file_info(file_path, self.source)
                        csv_writer.writerow(file_info)

    def calculate_md5(self, file_path, block_size=65536):
        """Calculate md5 checksum from file path"""
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for block in iter(lambda: f.read(block_size), b''):
                md5.update(block)
        return md5.hexdigest()

    def get_file_info(self, file_path, root):
        """returns ['File Path', 'Bytes', 'MD5', 'Timestamp']"""
        file_size = os.path.getsize(file_path)
        file_md5 = self.calculate_md5(file_path)
        file_timestamp = os.path.getmtime(file_path)
        timestamp_str = datetime.fromtimestamp(file_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        relative_path = os.path.relpath(file_path, root)
        return relative_path, file_size, file_md5, timestamp_str

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
        print(f"{old_filepath}\n-->{new_filepath}")
        os.makedirs(os.path.dirname(new_filepath), exist_ok=True)
        os.rename(old_filepath,new_filepath)
    print(f"Total GB = {round(total_bytes / 1000 ** 3,2)}")

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

def load_pkl(filepath):
    with open(filepath, "rb") as f:
        loaded_data = pickle.load(f)
    return loaded_data

class Archiver:
    def __init__(self, csv_files, output_dir="output", mode= "move", bucket_size= 50 * 1000 ** 3, start_num=1, dedupe=False, prefix="BDL-", seen_md5 = set()):
        self.csv_files = csv_files
        self.output_dir = Path(output_dir)
        self.mode = mode
        self.bucket_size = bucket_size
        self.start_num = start_num
        self.dedupe = dedupe
        self.prefix = prefix
        self.seen_md5 = seen_md5

    def run(self):
        print(f"Archiving from {self.csv_files}")
        print(f"Bucket size: {self.bucket_size} bytes")
        print(f"Dedupe: {self.dedupe}")
        print(f"Prefix: {self.prefix}")
        print(f"Seen MD5: {self.seen_md5}")
        print(f"Output directory: {self.output_dir}")

        self.groups, self.dupes, self.oversized = self.group_files()
        self.write_chunks()
        # for group in self.groups:
        #     write_data(group,self.output_dir/"assets")

    def write_csv(self, csv_file_path, list_of_rows):
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

    def group_files(self):
        """
        Processes a csv or a list of csv files. Returns the csvs in chunks based on size.
        Option to avoid duplicates and accept a set of known md5 to check against
        
        :param csv_files: list of csv files to process
        :param self.bucket_size: integer, size of each chunk in bytes (default to 50GB)
        :param self.dedupe: True/False, filter out duplicate files (default True)
        :param seen_md5: set of existing md5 to mark as duplicates (duplicates within the csv_files list will be added)
        """
        duplicates = []
        chunks = []
        oversized = []
        current_chunk = []
        current_size = 0

        for csv_file in self.csv_files:
            with open(csv_file, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    file_path = row["File Path"]
                    size = int(row["Bytes"])
                    md5 = row["MD5"]
                    timestamp = row["Timestamp"]
                    # Add a new key-value pair
                    row["Origin"] = csv_file.replace("file_manifest.csv","assets")

                    # Check for oversized
                    if size > self.bucket_size:
                        oversized.append(row)
                        continue

                    # Check for duplicates
                    if self.dedupe and md5 in self.seen_md5:
                        duplicates.append(row)
                        continue
                    self.seen_md5.add(md5)

                    # If adding this file exceeds required chunk size, start a new chunk
                    if current_size + size > self.bucket_size:
                        chunks.append(current_chunk)
                        current_chunk = []
                        current_size = 0

                    current_chunk.append(row)
                    current_size += size

        # Add the last chunk if not empty
        if current_chunk:
            chunks.append(current_chunk)

        return chunks, duplicates, oversized

    def write_chunks(self):
        # Make output dir
        os.makedirs(self.output_dir, exist_ok=True)
        # Write chunks to separate CSV files
        for i, chunk in enumerate(self.groups, self.start_num):
            asset_folder_path = f"{self.output_dir}/{self.prefix}{str(i).zfill(4)}/assets"
            os.makedirs(asset_folder_path, exist_ok=True)
            filename = f"{self.output_dir}/{self.prefix}{str(i).zfill(4)}/file_manifest.csv"
            self.write_csv(filename, chunk)
            if self.mode == "move":
                write_data(chunk, asset_folder_path)
                print(f"-----------------Written {len(chunk)} files to {filename}")




# if __name__ == "__main__":
#     # Import vars
#     from config import *

#     # Create the archiver object
#     archiver = Archiver(SOURCE, OUTPUT_DIR, BUCKET_SIZE, DEDUPE, PREFIX, TRUTH_PICKLE)
#     archiver.run()
#     # exit()


#     # import glob
#     # csv_files = sorted(glob.glob(f"{INPUT_DIR}/*/file_manifest.csv"))


#     truth_table = load_pkl(TRUTH_PICKLE)
#     groups, dupes = group_files(SOURCE, BUCKET_SIZE, DEDUPE, truth_table)

#     write_chunks(groups, OUTPUT_DIR, PREFIX, start_chunk=1)
#     write_chunks([dupes], OUTPUT_DIR, chunk_prefix = "DUP-", start_chunk = 1, mode = "csv")
