# -*- coding: utf-8 -*-

import bucket_archive

import unittest
import os


class BasicTestSuite(unittest.TestCase):
    """Basic test cases."""

    def setUp(self):
        with open('trashme.log', 'wb') as f:
            f.write(b'\0' * 1)  # Write null byte
        return super().setUp()
    
    def tearDown(self):
        os.remove('trashme.log')
        return super().tearDown()
    
    def test_calculate_md5(self):
        trashme_md5 = bucket_archive.calculate_md5('trashme.log')
        assert(trashme_md5 == '93b885adfe0da089cdf634904fd59f71')

if __name__ == '__main__':
    unittest.main()