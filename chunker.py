# -*- coding: utf-8 -*-

import csv
import glob
import time
import os
import sys
import pickle

class Chunker:
    def __init__(self, input_dir, output_dir, chunk_size_gb = 500, ignore_dupes = False):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.seen_md5 = set()
        self.ignore_dupes = ignore_dupes
        self.seen_md5_pkl = os.path.join(output_dir,"md5.pkl")
        if ignore_dupes == "y": 
            self.ignore_dupes = True
        self.init_time = time.strftime("%y%m%d%H%M%S")
        self.grouped_csvs = []

        try:
            self.chunk_size = float(chunk_size_gb) * 1000**3
            print(f"Using Chunk Size: {chunk_size_gb} GB")
        except:
            self.chunk_size = 500 * 1000**3
            print(f"Defaulting to Chunk Size: 500GB")

        def load_pkl(filepath):
            with open(filepath, "rb") as f:
                loaded_data = pickle.load(f)
            return loaded_data
        
        if os.path.exists(self.seen_md5_pkl):
            print(f"discovered md5 pickle file: {self.seen_md5_pkl}")
            self.seen_md5 = load_pkl(self.seen_md5_pkl)
        else:
            print(f"file not found: {self.seen_md5_pkl}")

    def run(self):

        print(f"self.input_dir: {self.input_dir}")
        print(f"self.output_dir: {self.output_dir}")
        print(f"self.ignore_dupes: {self.ignore_dupes}")
        print(f"self.seen_md5_pkl: {self.seen_md5_pkl}")
        
        csv_files = sorted(glob.glob(f"{self.input_dir}/*/file_manifest.csv"))

        chunks, duplicates = self.group_files(csv_files)

        prefix_chunks = f'chunk_{self.init_time}_'
        prefix_duplicates = f'duplicates_{self.init_time}_'

        self.write_csv_chunks(chunks,prefix_chunks)
        self.write_csv_chunks(duplicates,prefix_duplicates)
        self.dump_md5_pkl()

    def retire_groups(self):
        for csv_file in self.grouped_csvs:
            parent_dir = os.path.dirname(csv_file)
            parent_folder_name = os.path.basename(parent_dir)
            session_folder = os.path.join(self.input_dir,'_previously_chunked',f'session_{self.init_time}')
            os.makedirs(session_folder, exist_ok=True)
            previously_chunked_folder = os.path.join(session_folder, parent_folder_name)
            os.rename(parent_dir,previously_chunked_folder)
        self.grouped_csvs = []

    def dump_md5_pkl(self):
        filepath = f'{self.output_dir}/md5.pkl'
        data = self.seen_md5
        os.makedirs(self.output_dir, exist_ok=True)
        with open(filepath, "wb") as f:
            pickle.dump(data, f)

    def write_csv_chunks(self, chunks, chunk_prefix, start_num=1):
        chunk_list = []
        # Make output dir
        os.makedirs(self.output_dir, exist_ok=True)
        # Write chunks to separate CSV files
        for i, chunk in enumerate(chunks, start_num):
            filename = f"{self.output_dir}/{chunk_prefix}{str(i).zfill(4)}.csv"
            with open(filename, "w", newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=["File Path", "Bytes", "MD5", "Timestamp","Origin"])
                writer.writeheader()
                writer.writerows(chunk)
            chunk_list.append(filename)

            total_size = sum(int(file["Bytes"]) for file in chunk)

            print(f"Written {len(chunk)} files to {filename}, {total_size / (1000**3):.2f} GB")

        return chunk_list

    def group_files(self, csv_files):
        # seen_md5 = self.seen_md5
        duplicates = []
        chunks = []
        current_chunk = []
        current_size = 0

        for csv_file in csv_files:
            with open(csv_file, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    row["Origin"] = csv_file.replace("file_manifest.csv","assets")
                    file_path = row["File Path"]
                    size = int(row["Bytes"])
                    md5 = row["MD5"]
                    timestamp = row["Timestamp"]

                    # Check for duplicates
                    if not self.ignore_dupes and md5 in self.seen_md5:
                    # if md5 in self.seen_md5:
                        duplicates.append(row)
                        continue
                    self.seen_md5.add(md5)

                    # If adding this file exceeds chunk size, start a new chunk
                    if current_size + size > self.chunk_size:
                        chunks.append(current_chunk)
                        current_chunk = []
                        current_size = 0

                    current_chunk.append(row)
                    current_size += size

            self.grouped_csvs.append(csv_file)

        # Add the last chunk if not empty
        if current_chunk:
            chunks.append(current_chunk)

        return chunks, [duplicates]

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if len(sys.argv) < 3:
        print("Usage: python chunker.py <input directory> <output directory> <size in gigabytes> <ignore dupes (y/n)>")
    else:
        main_chunker = Chunker(*sys.argv[1:])
        main_chunker.run()

if __name__ == "__main__":
    main()
