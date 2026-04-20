#!/bin/bash

ENV=${1:-dev}

aws cloudformation deploy \
  --template-file infra/aws/cloudformation/main.json \
  --stack-name martech-$ENV \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides file://infra/aws/cloudformation/parameters/$ENV.parameters.json
