import unittest
import os
import shutil
import time
import csv
import mover
import utils

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

    def test_mover_basic(self):
        print("---Testing Mover-Basic---")
        load_path = f'{self.test_root}/mover_basic/loads'
        load_a = f'{self.test_root}/mover_basic/loads/load-a'
        load_b = f'{self.test_root}/mover_basic/loads/load-b'
        chunks_path = f'{self.test_root}/mover_basic/chunks'
        gb = 3 / (1000**3)

        self.make_some_files(load_a, [1,2], number_of_files =3) # make some 1 and 2 byte files
        self.make_some_files(load_b, [5], number_of_files =1) # oversized 5 byte file

        assert os.path.exists(f'{load_a}/assets/TestFiles_01bytes/TestFiles_01bytes_0.txt')
        assert os.path.exists(f'{load_a}/assets/TestFiles_01bytes/TestFiles_01bytes_1.txt')
        assert os.path.exists(f'{load_a}/assets/TestFiles_01bytes/TestFiles_01bytes_2.txt')

        assert os.path.exists(f'{load_a}/assets/TestFiles_02bytes/TestFiles_02bytes_0.txt')
        assert os.path.exists(f'{load_a}/assets/TestFiles_02bytes/TestFiles_02bytes_1.txt')
        assert os.path.exists(f'{load_a}/assets/TestFiles_02bytes/TestFiles_02bytes_2.txt')


        # Manifest it
        this_manifest = Manifest(f"{load_a}/assets")
        this_manifest_file = this_manifest.generate_file_manifest()
        assert os.path.exists(f'{load_a}/file_manifest.csv')
        null = input("PAUSED")

        # Chunk it
        this_chunker = Chunker(load_path,chunks_path, gb, False)
        this_chunker.run()
        null = input("PAUSED")

        mover.main(chunks_path)
        assert os.path.exists(f'{chunks_path}/chunk_0001/file_manifest.csv')
        # assert test files exist
        # assert dupes exist
        null = input("PAUSED")


    def test_mover(self):
        print("---Testing Mover---")
        mover_load_parent = f"{self.test_root}/mover/loads"
        mover_chunks_dir = f"{self.test_root}/mover/chunks"
        mover_load_a = f"{mover_load_parent}/test-files_a_{self.init_time}"
        mover_load_b = f"{mover_load_parent}/test-files_b_{self.init_time}"
        bytes_to_chunk = 12
        bytes_to_gb = bytes_to_chunk / (1000**3)

        # Make some files in two different loads
        file_sizes = list(range(1, 16))

        self.make_some_files(mover_load_a,file_sizes,1)
        manifest_a = Manifest(f"{mover_load_a}/assets")
        manifest_file_a = manifest_a.generate_file_manifest()

        self.make_some_files(mover_load_b,file_sizes,1)
        manifest_b = Manifest(f"{mover_load_b}/assets")
        manifest_file_b = manifest_b.generate_file_manifest()

        # Chunk it
        this_chunker = Chunker(mover_load_parent,mover_chunks_dir,bytes_to_gb,False)
        this_chunker.run()

        # get the initial file summary for the loads
        initial_file_count, initial_total_size = self.get_summary(mover_load_parent)

        # the first run will just exit due to duplicate paths
        mover.main(mover_chunks_dir)
        second_file_count, second_total_size = self.get_summary(mover_load_parent)

        # remove oversized and dups csvs
        dup_csv = os.path.join(mover_chunks_dir,f'duplicates_{self.init_time}_0001.csv')
        over_csv = os.path.join(mover_chunks_dir,f'oversized_{self.init_time}_0001.csv')
        for i in (dup_csv, over_csv):
            if os.path.exists(i):
                os.remove(i)

        # run mover again
        mover.main(mover_chunks_dir)

        # final_file_count, final_total_size = self.get_summary(mover_load_parent)

        # print (initial_file_count, initial_total_size)
        # print (second_file_count, second_total_size)
        # print (final_file_count, final_total_size)

        assert initial_file_count == second_file_count # nothing is moved in the first run
        assert initial_total_size == second_total_size # nothing is moved in the first run

        fms = utils.glob_fm(mover_chunks_dir)
        for fm in fms:
            this_manifest = Manifest(fm)
            result = this_manifest.verify_file_manifest(fm, expected_header = False)
            assert result == True


        fms = utils.glob_fm(mover_load_parent)
        for i, fm in enumerate(fms, 1):

            result = this_manifest.verify_file_manifest(fm, expected_header = False)
            print(result)

            #first load is moved, verify fails
            if i == 1:
                assert result == False
            # second load is all duplicate so verify will pass
            if i == 2:
                assert result == True



        shutil.rmtree(f"{self.test_root}/mover")

    def test_chunker(self):
        print("---Testing Chunker---")
        chunker_load_parent = f"{self.test_root}/chunker/loads"
        chunker_chunks_dir = f"{self.test_root}/chunker/chunks"
        chunker_load_c = f"{self.test_root}/chunker/loads/test-files_a_{self.init_time}"
        chunker_load_b = f"{self.test_root}/chunker/loads/test-files_b_{self.init_time}"
        bytes_to_chunk = 12
        bytes_to_gb = bytes_to_chunk / (1000**3)

        # Make some files in two different loads
        file_sizes = list(range(1, 16))
        self.make_some_files(chunker_load_c,file_sizes,1)
        self.make_some_files(chunker_load_b,file_sizes,1)
        manifest_a = Manifest(f"{chunker_load_c}/assets")
        manifest_file_a = manifest_a.generate_file_manifest()
        manifest_b = Manifest(f"{chunker_load_b}/assets")
        manifest_file_b = manifest_b.generate_file_manifest()
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

                csv_counter += 1

                # Check that the first file of the next chunk can not fit inside the previous
                if csv_counter > 1:
                    assert prev_csv_total_bytes + first_file_bytes > bytes_to_chunk
                prev_csv_total_bytes = this_csv_total_bytes

            # Check for handling of oversized files, dupes and path collisions (right now it should just exit)
            elif csv_file.startswith("oversized_"):
                csv_file = os.path.join(chunker_chunks_dir,csv_file)
                with open(csv_file, newline='', encoding='utf-8') as f:
                    csv_reader = csv.DictReader(f)
                    for row in csv_reader:
                        file_bytes = int(row["Bytes"])
                        file_path = row["File Path"]
                        assert bytes_to_chunk < file_bytes

        shutil.rmtree(f"{self.test_root}/chunker")

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

    @staticmethod
    def get_summary(input_path):
        total_size = 0
        file_count = 0
        if os.path.isdir(input_path):
            for root, dirs, files in os.walk(input_path):
                dirs.sort()
                for file_name in sorted(files):
                    if not file_name.startswith("._"):
                        total_size += os.path.getsize(os.path.join(root, file_name))
                        file_count += 1
        return (file_count, total_size)


if __name__ == '__main__':
    unittest.main()