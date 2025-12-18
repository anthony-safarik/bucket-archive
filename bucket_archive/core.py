# -*- coding: utf-8 -*-
import os
import csv
from . import helpers

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
                    file_info = helpers.get_file_info(file_path, folder_path)
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
        header = next(csv_reader)  # Skip the header row
        if expected_header and expected_header != header:
            print("Header mismatch found.")
            return False

        for row in csv_reader:
            csv_asset_file_path, _, csv_md5, _ = row
            file_path = os.path.join(asset_folder, csv_asset_file_path)
            if not os.path.exists(file_path):
                print(f'"File missing: "{file_path}')
                return False
            current_md5 = helpers.calculate_md5(file_path)
            if current_md5 != csv_md5:
                print(f'"MD5 mismatch: "{file_path}')
                return False
                
    return True
