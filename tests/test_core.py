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


if __name__ == '__main__':
    unittest.main()