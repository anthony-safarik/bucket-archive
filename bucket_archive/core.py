# -*- coding: utf-8 -*-
import os
import csv
from datetime import datetime
from . import helpers

def get_file_info(file_path, root):
    """returns ['File Path', 'Bytes', 'MD5', 'Timestamp']"""
    file_size = os.path.getsize(file_path)
    file_md5 = helpers.calculate_md5(file_path)
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
    with open(csv_file_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["File Path", "Bytes", "MD5", "Timestamp"])
        writer.writeheader()
        writer.writerows(list_of_rows)

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
        header = next(csv_reader)  # Skip the header row
        if expected_header and expected_header != header:
            print("Header mismatch found.")
            return False

        for row in csv_reader:
            csv_asset_file_path, _, csv_md5, _ = row # TODO: change this to accept csvs with any number of fields
            file_path = os.path.join(asset_folder, csv_asset_file_path)
            if not os.path.exists(file_path):
                print(f'"File missing: "{file_path}')
                return False
            current_md5 = helpers.calculate_md5(file_path)
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

def write_chunks(chunks, output_dir, chunk_prefix = "CHK-", start_chunk = 1):
    """
    converts a list of lists that contain dicts to chunked csvs
    
    :param chunks: list of lists containing file dicts for writing to csv
    :param output_dir: string, dir to write chunked csv files to
    :param chunk_prefix: string, prefix for the chunk buckets
    :param start_chunk: integer, number for the first chunk
    """
    # Make output dir
    os.makedirs(output_dir, exist_ok=True)
    # Write chunks to separate CSV files
    for i, chunk in enumerate(chunks, start_chunk):
        filename = f"{output_dir}/{chunk_prefix}{str(i).zfill(4)}.csv"
        write_csv(filename, chunk)
        print(f"Written {len(chunk)} files to {filename}")