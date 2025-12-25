import unittest
import os
import shutil
from archiver import Archiver
from archiver import calculate_md5
from manifest import Manifest

class TestArchiver(unittest.TestCase):
    """Basic test cases."""

    def setUp(self):
        with open('trashme.log', 'wb') as f:
            f.write(b'\0' * 1)  # Write null byte



        self.test_root = "trash_testing"
        self.test_disc_size = 50 #bytes
        self.test_full_disc_size = 45 #bytes

        # Create some subfolders and files for testing
        self.subfolders = [("TestFiles_10bytes", 10), ("TestFiles_15bytes", 15), ("TestFiles_55bytes", 55)]
        for folder in self.subfolders:
            folder_path = os.path.join(self.test_root, "To_Chunk", "TestFiles", "assets", folder[0])
            os.makedirs(folder_path, exist_ok=True)
            for i in range(5):

                test_file_name = f"{folder[0]}_{i}.txt"
                with open(f'{folder_path}/{test_file_name}', 'wb') as f:
                    f.write(b'\0' * folder[1])  # Write null bytes

        return super().setUp()
    
    def tearDown(self):
        os.remove('trashme.log')
        shutil.rmtree(self.test_root)
        return super().tearDown()
    
    def test_Manifest(self):
        self.manifest = Manifest(os.path.join(self.test_root, "To_Chunk", "TestFiles", "assets"))
        self.manifest.generate_file_manifest()

        null = input("PAUSED")

    def test_calculate_md5(self):
        trashme_md5 = calculate_md5('trashme.log')
        assert(trashme_md5 == '93b885adfe0da089cdf634904fd59f71')

if __name__ == '__main__':
    unittest.main()