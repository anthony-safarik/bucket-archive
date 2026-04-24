import unittest
import os
import shutil
import time

from manifest import Manifest

class TestBasic(unittest.TestCase):
    """Basic test cases."""

    def setUp(self):

        self.test_root = "trash_testing"
        self.init_time = time.strftime("%y%m%d%H%M%S")
        self.test_load = f"{self.test_root}/test-load-{self.init_time}"
        self.test_load_fail = f"{self.test_root}/test-load-{self.init_time}_fail"

        def make_some_files(load_name, sizes, number_of_files =5):
            for size in sizes:
                subname= f"TestFiles_{str(size).zfill(2)}bytes"
                folder_path = os.path.join(load_name, "assets", subname)
                os.makedirs(folder_path, exist_ok=True)
                for i in range(number_of_files):
                    test_file_name = f'{subname}_{i}.txt'
                    with open(f'{folder_path}/{test_file_name}', 'wb') as f:
                        f.write(b'\0' * size)  # Write null bytes

        file_sizes = list(range(1, 10))
        file_sizes_fail = list(range(1, 9))
        make_some_files(self.test_load,file_sizes,1)
        make_some_files(self.test_load_fail,file_sizes_fail,1)
        return super().setUp()
    
    def tearDown(self):
        shutil.rmtree(self.test_root)
        return super().tearDown()

    def test_manifest(self):
        print("---Testing Manifest---")
        manifest = Manifest(f"{self.test_load}/assets")
        manifest_file = manifest.generate_file_manifest()

        assert os.path.isfile(manifest_file) == True

        verify_manifest = Manifest(manifest_file)
        result = verify_manifest.verify_file_manifest(manifest_file, expected_header = True)
        assert result == True


        # File missing case
        manifest_file_fail = os.path.join(self.test_load_fail,'file_manifest.csv')
        shutil.copy2(manifest_file,manifest_file_fail)
        assert os.path.isfile(manifest_file_fail) == True
        verify_manifest = Manifest(manifest_file_fail)
        result = verify_manifest.verify_file_manifest(manifest_file_fail, expected_header = True)
        assert result == False

        #File mismatch case
        failed_file_dir = f'{self.test_load_fail}/assets/TestFiles_09bytes'
        failed_file = f'{failed_file_dir}/TestFiles_09bytes_0.txt' #the missing file
        os.makedirs (failed_file_dir)

        with open(failed_file, "w") as f:
            f.write("foo") #File name matches but content does not

        result = verify_manifest.verify_file_manifest(manifest_file_fail, expected_header = True)
        assert result == False

        null = input("PAUSED")

if __name__ == '__main__':
    unittest.main()