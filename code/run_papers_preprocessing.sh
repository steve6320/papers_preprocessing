#!/bin/bash

# Prerequisites
# Run `aws configure` to set access credentials and AWS region.
# Install Python jsonlines library by running `pip install jsonlines`.
# Set environment variable AWS_ROOT to the AWS path where files will be stored under.
#   Example: s3://mybucket/my/path/
# Set environment variable RUN_ID to a unique string for this run.
# Set environment variable RESULTS_LOCAL_DIR to a local directory to place results into.
# Run the RCT score service on localhost:8080 by running in two separate terminal windows:
#   dockerd
#   docker run -p 8080:8080 public.ecr.aws/t9g4g7y2/rct-svm:latest
#
# Then run this script from the repository root directory:
# ./code/run_papers_preprocessing.sh

BASE_PATH=${AWS_ROOT}${RUN_ID}/

# Upload to S3
aws s3 cp --recursive sample_data/s2ag/ ${BASE_PATH}source_data/s2ag/

# Create Athena tables for S2AG data
WORKGROUP=workgroup_$RUN_ID
DATABASE=papers_preprocessing_$RUN_ID

aws athena create-work-group \
    --name $WORKGROUP \
    --configuration ResultConfiguration={OutputLocation=\"${BASE_PATH}query_results\"}

aws athena start-query-execution \
    --query-string "CREATE database $DATABASE" \
    --work-group $WORKGROUP

aws athena start-query-execution \
    --query-string \
"
CREATE EXTERNAL TABLE IF NOT EXISTS s2ag_abstracts (
  corpusid int,
  abstract string
  ) 
  ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  LOCATION '${BASE_PATH}source_data/s2ag/abstracts/'
" \
    --work-group $WORKGROUP \
    --query-execution-context Database=$DATABASE

aws athena start-query-execution \
    --query-string \
"
CREATE EXTERNAL TABLE IF NOT EXISTS s2ag_papers (
  corpusid int,
  externalids struct<DOI:string>,
  title string,
  authors array<struct<authorId:string, name:string>>,
  year int,
  citationcount int
  ) 
  ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  LOCATION '${BASE_PATH}source_data/s2ag/papers/'
" \
    --work-group $WORKGROUP \
    --query-execution-context Database=$DATABASE

# Create denormalized S2AG records, no rctScore ("Level 1" result)
aws athena start-query-execution \
    --query-string \
"
UNLOAD(
SELECT UUID() AS uniqueId,
       title,
       abstract,
       TRANSFORM(authors, s -> s.name) AS authorNames,
       year AS publicationYear,
       externalids.doi AS doi,
       citationcount as citedByCount
FROM
  s2ag_papers p INNER JOIN s2ag_abstracts a on p.corpusid = a.corpusid
)
TO '${BASE_PATH}results/s2ag/denormalized/'
WITH (format = 'JSON')
" \
    --work-group $WORKGROUP \
    --query-execution-context Database=$DATABASE

# TODO Use get-query-execution to wait for query to finish, instead of a dumb sleep
sleep 2

# Retrieve to local machine.
aws s3 cp --recursive ${BASE_PATH}results/s2ag/denormalized/ $RESULTS_LOCAL_DIR/s2ag/denormalized/

# Add rctScore ("Level 2" result)
python3 code/add_rct_score.py $RESULTS_LOCAL_DIR/s2ag/denormalized/ $RESULTS_LOCAL_DIR/s2ag/with_rct_score/

# Copy a sample of OpenAlex data to my S3 bucket
aws s3 cp s3://openalex/data/works/updated_date=2022-02-13/part_030.gz ${BASE_PATH}source_data/openalex/works/

# Create Athena table for OpenAlex data
aws athena start-query-execution \
    --query-string \
"
CREATE EXTERNAL TABLE IF NOT EXISTS works (
  doi string,
  title string,
  publication_year int,
  authorships array<struct<author:struct<display_name:string>>>,
  cited_by_count int,
  abstract_inverted_index map<string, array<int>>
  ) 
  ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  LOCATION '${BASE_PATH}source_data/openalex/works/'
" \
    --work-group $WORKGROUP \
    --query-execution-context Database=$DATABASE

# Create denormalized OpenAlex records, no rctScore
# TODO Determine if their are entries with non-empty abstract_inverted_index,
# and add clause `WHERE CARDINALITY(abstract_inverted_index) > 0`
aws athena start-query-execution \
    --query-string \
"
UNLOAD(
SELECT UUID() AS uniqueId,
       title,
       abstract_inverted_index,
       TRANSFORM(authorships, s -> s.author.display_name) AS authorNames,
       publication_year AS publicationYear,
       doi,
       cited_by_count as citedByCount
FROM works
)
TO '${BASE_PATH}results/openalex/denormalized/'
WITH (format = 'JSON')
" \
    --work-group $WORKGROUP \
    --query-execution-context Database=$DATABASE

# TODO Use get-query-execution to wait for query to finish, instead of a dumb sleep
sleep 90

# Retrieve to local machine.
aws s3 cp --recursive ${BASE_PATH}results/openalex/denormalized/ $RESULTS_LOCAL_DIR/openalex/denormalized/

# TODO Add rctScore using a modified version of code/add_rct_score.py,
# which is the same except that it must construct the abstract text from abstract_inverted_index.
