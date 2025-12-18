# -*- coding: utf-8 -*-
import hashlib
import os
from datetime import datetime

def calculate_md5(file_path, block_size=65536):
    """Calculate md5 checksum from file path"""
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for block in iter(lambda: f.read(block_size), b''):
            md5.update(block)
    return md5.hexdigest()

def get_file_info(file_path, root):
    """returns ['File Path', 'Bytes', 'MD5', 'Timestamp']"""
    file_size = os.path.getsize(file_path)
    file_md5 = calculate_md5(file_path)
    file_timestamp = os.path.getmtime(file_path)
    timestamp_str = datetime.fromtimestamp(file_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    relative_path = os.path.relpath(file_path, root)
    return relative_path, file_size, file_md5, timestamp_str
