import gzip
import json
import jsonlines
import os
import requests
import sys

# Arguments of script:
# Directory, ending in /, containing JSON lines files with rctScore missing
# Directory, ending in /, to place output JSON lines files with rctScore added

# Prerequisites:
# Run the RCT score service on localhost:8080 by running in two separate terminal windows:
#   dockerd
#   docker run -p 8080:8080 public.ecr.aws/t9g4g7y2/rct-svm:latest

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
                    payload='{{"title_abstract_pairs": {}}}'.format(json.dumps([[row['title'], row['abstract']]]))
                    rct_score_result = requests.post(
                        url='http://localhost:8080/',
                        data=payload,
                        headers={
                            'accept': 'application/json',
                            'Content-Type': 'application/json',
                            'X-Api-Key': 'anything',
                        },
                    )
                    rct_score_result_parsed = rct_score_result.json()
                    if 'scores' not in rct_score_result_parsed:
                        print("Error getting rctScore:", rct_score_result_parsed)
                        row['rctScore'] = None
                    else:
                        row['rctScore'] = rct_score_result_parsed['scores'][0]
                    writer.write(row)

print ('Finished adding rctScore for all rows')
