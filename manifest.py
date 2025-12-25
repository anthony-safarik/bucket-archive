import os
import csv
import pickle
import hashlib
from datetime import datetime
from pathlib import Path

class Manifest:
    def __init__(self, source):
        self.source = source
        print(self.source)

    def generate_file_manifest(self):
        parent_directory = os.path.dirname(self.source)
        output_csv = os.path.join(parent_directory, 'file_manifest.csv')
        
        with open(output_csv, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['File Path', 'Bytes', 'MD5', 'Timestamp'])

            for dirpath, dirnames, filenames in os.walk(self.source):
                dirnames.sort()
                for filename in sorted(filenames):
                    if not filename.startswith('.'):
                        file_path = os.path.join(dirpath, filename)
                        file_info = self.get_file_info(file_path, self.source)
                        csv_writer.writerow(file_info)

        # Walk files
            # source_path = Path(self.source)
            # files = list(source_path.rglob("*"))
            # files = [f for f in files if f.is_file()]
            # for file_path in files:
            #     # if not file_path.startswith('.'):
            #         # file_path = os.path.join(dirpath, filename)
            #     file_info = self.get_file_info(file_path, self.source)
            #     csv_writer.writerow(file_info)

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

        # print(f"File manifest created: {output_csv}")