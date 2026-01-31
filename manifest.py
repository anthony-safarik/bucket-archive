# -*- coding: utf-8 -*-
#TODO improve this by defining source better. should it be assets or file_manifest.csv? add Origin to header?
import os
import csv
import hashlib
from datetime import datetime
import sys

class Manifest:
    def __init__(self, source):
        self.source = source
        self.parent_directory = os.path.dirname(self.source)
        self.output_csv = os.path.join(self.parent_directory, 'file_manifest.csv')
        self.header = ['File Path', 'Bytes', 'MD5', 'Timestamp']
        # print(self.source)

    def generate_file_manifest(self):
        
        with open(self.output_csv, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(self.header)

            for dirpath, dirnames, filenames in os.walk(self.source):
                dirnames.sort()
                for filename in sorted(filenames):
                    if not filename.startswith('.'):
                        file_path = os.path.join(dirpath, filename)
                        file_info = self.get_file_info(file_path, self.source)
                        csv_writer.writerow(file_info)

            return self.output_csv

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

    def verify_file_manifest(self, csv_file, expected_header = True):
        """
        Params: path to file_manifest.csv
        Returns True if the manifest is valid
        """
        if expected_header:
            expected_header = self.header

        asset_folder = os.path.join(os.path.dirname(csv_file), 'assets')
        if not os.path.isdir(asset_folder):
            print("No asset folder found.")
            return False
        

        with open(csv_file, mode='r', newline='') as temp_csv_file:
            csv_reader = csv.reader(temp_csv_file)
            header = next(csv_reader)  # Skip the header row, checking first
            if expected_header and expected_header != header:
                print("Header mismatch found.")
                return False

        # print(csv_file, type(csv_file))

        with open(csv_file, newline='', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)

            for row in csv_reader:
                csv_asset_file_path = row["File Path"]
                csv_md5 = row["MD5"]
                file_path = os.path.join(asset_folder, csv_asset_file_path)
                if not os.path.exists(file_path):
                    print(f'"File missing: "{file_path}')
                    return False
                current_md5 = self.calculate_md5(file_path)
                if current_md5 != csv_md5:
                    print(f'"MD5 mismatch: "{file_path}')
                    return False
                    
        return True
    
def main():
    if len(sys.argv) < 2:
        print("Usage: python manifest.py <asset folder or manifest>")
        sys.exit(1)
    else:
        for i in sys.argv:
            if os.path.isfile(i) and i.endswith('file_manifest.csv'):
                print(f"Verifying manifest: {i}")
                this_manifest = Manifest(i)
                result = this_manifest.verify_file_manifest(i, expected_header = False)
                print(f"Manifest valid: {result}")
            if os.path.isdir(i) and i.endswith('assets'):
                this_manifest = Manifest(i)
                this_manifest.generate_file_manifest()

if __name__ == "__main__":
    main()