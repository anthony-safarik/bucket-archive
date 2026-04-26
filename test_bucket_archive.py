import unittest
import os
import shutil
import time
import csv

from manifest import Manifest
from chunker import Chunker

class TestBasic(unittest.TestCase):
    """Basic test cases."""

    def setUp(self):

        self.test_root = "trash_testing"
        self.init_time = time.strftime("%y%m%d%H%M%S")
        return super().setUp()
    
    def tearDown(self):
        shutil.rmtree(self.test_root)
        return super().tearDown()

    def test_manifest(self):
        print("---Testing Manifest---")

        test_manifest_load = f"{self.test_root}/manifest/loads/test-load-{self.init_time}"
        test_manifest_load_fail = f"{self.test_root}/manifest/loads/test-load-{self.init_time}_fail"

        file_sizes = list(range(1, 10))
        file_sizes_fail = list(range(1, 9))
        self.make_some_files(test_manifest_load,file_sizes,1)
        self.make_some_files(test_manifest_load_fail,file_sizes_fail,1)

        manifest = Manifest(f"{test_manifest_load}/assets")
        manifest_file = manifest.generate_file_manifest()

        assert os.path.isfile(manifest_file) == True

        verify_manifest = Manifest(manifest_file)
        result = verify_manifest.verify_file_manifest(manifest_file, expected_header = True)
        assert result == True


        # File missing case
        manifest_file_fail = os.path.join(test_manifest_load_fail,'file_manifest.csv')
        shutil.copy2(manifest_file,manifest_file_fail)
        assert os.path.isfile(manifest_file_fail) == True
        verify_manifest = Manifest(manifest_file_fail)
        result = verify_manifest.verify_file_manifest(manifest_file_fail, expected_header = True)
        assert result == False

        #File mismatch case
        failed_file_dir = f'{test_manifest_load_fail}/assets/TestFiles_09bytes'
        failed_file = f'{failed_file_dir}/TestFiles_09bytes_0.txt' #the missing file
        os.makedirs (failed_file_dir)

        with open(failed_file, "w") as f:
            f.write("foo") #File name matches but content does not

        result = verify_manifest.verify_file_manifest(manifest_file_fail, expected_header = True)
        assert result == False

    def test_chunker(self):
        print("---Testing Chunker---")
        chunker_load_parent = f"{self.test_root}/chunker/loads"
        chunker_chunks_dir = f"{self.test_root}/chunker/chunks"
        chunker_load_a = f"{self.test_root}/chunker/loads/test-files_{self.init_time}"


        file_sizes = list(range(1, 16))
        self.make_some_files(chunker_load_a,file_sizes,1)
        manifest = Manifest(f"{chunker_load_a}/assets")
        manifest_file = manifest.generate_file_manifest()
        bytes_to_chunk = 12
        bytes_to_gb = bytes_to_chunk / (1000**3)
        this_chunker = Chunker(chunker_load_parent,chunker_chunks_dir,bytes_to_gb,False)
        this_chunker.run()

        csv_files = sorted(os.listdir(chunker_chunks_dir))

        csv_counter = 0
        first_file_bytes = 0
        prev_csv_total_bytes = 0

        for csv_file in csv_files:
            if csv_file.startswith("chunk_"):
                csv_file = os.path.join(chunker_chunks_dir,csv_file)
                this_csv_total_bytes = 0
                row_counter = 0

                with open(csv_file, newline='', encoding='utf-8') as f:
                    csv_reader = csv.DictReader(f)
                    for row in csv_reader:
                        row_counter +=1
                        file_bytes = int(row["Bytes"])
                        if row_counter == 1: first_file_bytes = file_bytes
                        this_csv_total_bytes += file_bytes

                print(f'{csv_file},{this_csv_total_bytes} bytes total, {first_file_bytes} bytes for first file')

                # Check that the csv is not oversized
                assert this_csv_total_bytes <= bytes_to_chunk
                print (f'this_csv_total_bytes {this_csv_total_bytes} <= bytes_to_chunk {bytes_to_chunk}')

                csv_counter += 1

                # Check that the first file of the next chunk can not fit inside the previous
                if csv_counter > 1:
                    assert prev_csv_total_bytes + first_file_bytes > bytes_to_chunk
                    print(f'prev_csv_total_bytes {prev_csv_total_bytes} + first_file_bytes {first_file_bytes} > bytes_to_chunk {bytes_to_chunk}\n')
                prev_csv_total_bytes = this_csv_total_bytes




        null = input("PAUSED")

    @staticmethod
    def make_some_files(load_name, sizes, number_of_files =5):
        for size in sizes:
            subname= f"TestFiles_{str(size).zfill(2)}bytes"
            folder_path = os.path.join(load_name, "assets", subname)
            os.makedirs(folder_path, exist_ok=True)
            for i in range(number_of_files):
                test_file_name = f'{subname}_{i}.txt'
                with open(f'{folder_path}/{test_file_name}', 'wb') as f:
                    f.write(b'\0' * size)  # Write null bytes

if __name__ == '__main__':
    unittest.main()