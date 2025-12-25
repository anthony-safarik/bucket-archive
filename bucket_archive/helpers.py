# -*- coding: utf-8 -*-
import hashlib

def calculate_md5(file_path, block_size=65536):
    """Calculate md5 checksum from file path"""
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for block in iter(lambda: f.read(block_size), b''):
            md5.update(block)
    return md5.hexdigest()