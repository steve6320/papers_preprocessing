#!/bin/bash

# Prerequisites
# Run `aws configure` to set access credentials and AWS region.
# Set environment variable AWS_ROOT to the AWS path where files will be stored under.
#   Example: s3://mybucket/my/path/
# Set environment variable RUN_ID to a unique string for this run.
# Set environment variable RESULTS_LOCAL_DIR to a local directory to place results into.

BASE_PATH=${AWS_ROOT}${RUN_ID}/

# Upload to S3
aws s3 cp --recursive sample_data/s2ag/ ${BASE_PATH}source_data/s2ag/

# Create Athena tables
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
TO '${BASE_PATH}results/s2ag/joined/'
WITH (format = 'JSON')
" \
    --work-group $WORKGROUP \
    --query-execution-context Database=$DATABASE

# TODO Use get-query-execution to wait for query to finish, instead of a dumb sleep
sleep 2

# Retrieve to local machine.
aws s3 cp --recursive ${BASE_PATH}results/s2ag/joined/ $RESULTS_LOCAL_DIR/s2ag/joined/
