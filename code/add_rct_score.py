import gzip
import jsonlines
import os
import sys

# Arguments of script:
# Directory, ending in /, containing JSON lines files with rctScore missing
# Directory, ending in /, to place output JSON lines files with rctScore added

source_dir = sys.argv[1]
dest_dir = sys.argv[2]

os.makedirs(dest_dir)

for filename in os.listdir(source_dir):
    full_filename = os.path.join(source_dir, filename)
    # checking if it is a file
    if os.path.isfile(full_filename) and full_filename.endswith('.gz'):
        out_filename = "{}{}".format(dest_dir,filename[:-3])
        print("Reading {}, producing {}".format(full_filename, out_filename))
        with gzip.open(full_filename, "rb") as source_file:
            with jsonlines.open(out_filename, mode='w') as writer:
                reader = jsonlines.Reader(source_file)
                for row in reader:
                    row['rctScore'] = 0
                    writer.write(row)
