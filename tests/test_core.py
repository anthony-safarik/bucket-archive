# -*- coding: utf-8 -*-

import bucket_archive

import unittest
import os
import shutil

class BasicTestSuite(unittest.TestCase):
    """Basic test cases."""

    testfile = 'trashme/assets/trashme.log'
    test_asset_dir = os.path.dirname(testfile)
    test_base_dir = os.path.dirname(test_asset_dir)

    def setUp(self):
        os.makedirs(self.test_asset_dir, exist_ok=True)
        with open(self.testfile, 'wb') as f:
            f.write(b'\0' * 1)  # Write null byte
        return super().setUp()
    
    def tearDown(self):
        shutil.rmtree('trashme')
    
    def test_generate_file_manifest(self):
        # Create and verify a manifest
        bucket_archive.generate_file_manifest(self.test_asset_dir)
        result = bucket_archive.verify_file_manifest(f'{self.test_base_dir}/file_manifest.csv')
        assert(result == True)

        # Alter test file and verify it fails
        with open(self.testfile, 'wb') as f:
            f.write(b'\0' * 2)  # Write 2 null bytes

        result = bucket_archive.verify_file_manifest(f'{self.test_base_dir}/file_manifest.csv')
        assert(result == False)

        # Remove file and verify fails
        os.remove(self.testfile)
        result = bucket_archive.verify_file_manifest(f'{self.test_base_dir}/file_manifest.csv')
        assert(result == False)

        # Temp Trash
        import glob
        INPUT_DIR = "/Volumes/X9Pro4TB/ARCHIVE/verified_buckets"
        csv_files = sorted(glob.glob(f"{INPUT_DIR}/*/file_manifest.csv"))
        groups, dupes = bucket_archive.group_files(csv_files)

        # for index, group in enumerate(groups):
        #     total_size = sum(int(file["Bytes"]) for file in group)
        #     print(f"group {index}, {total_size / (1000**3):.2f} GB: contains {len(group)} files")

        bucket_archive.write_chunks(groups, f'{INPUT_DIR}/test_chunks')
        bucket_archive.write_chunks([dupes], f'{INPUT_DIR}/test_chunks', chunk_prefix = "DUP-", start_chunk = 1)
if __name__ == '__main__':
    unittest.main()