# basic steps to making a bucket archive from a folder of verifiable assets
manifest.py - make an asset folder and create a manifest csv file
chunker.py - chunk the manifest into bite sized chunk csv files with origin field and make md5 pickle
mover.py - move the chunks into a new asset folder in their own chunk folders
manifilter.py - filter out the origin field to make new manifests inside the chunk
manifest.py - verify the manifest
clean up the old folders and csv files

ARCHIVE
/01_Landing
/02_Chunking
/03_Staging
/04_Burning
/05_Archive

# todo
fix manifest so the files go in order
create cleanup
it might be easier to just chunk and dedupe at the same time?

# bucket-archive
Puts files into numbered folders of equal size. Written in Python with an emphasis on backup and restoration. Option to filter out duplicates and verify bucket integrity.

# testing
run python -m unittest discover

# structure
helloworld/
│
├── helloworld/
│   ├── __init__.py
│   ├── helloworld.py
│   └── helpers.py
│
├── tests/
│   ├── helloworld_tests.py
│   └── helpers_tests.py
│
├── .gitignore
├── LICENSE
├── README.md
├── requirements.txt
└── setup.py

# resources
[structure adapted from realpython.com/python-application-layouts](https://realpython.com/python-application-layouts/)
