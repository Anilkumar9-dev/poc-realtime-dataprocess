# Realtime Loads CI/CD Setup

This repository now includes AWS CloudFormation templates and a CodePipeline-based CI/CD flow for:

- Producer Lambda -> Kinesis Data Stream
- Kinesis Data Stream -> Consumer Lambda
- Consumer Lambda -> S3 raw zone

## Files added

- infra/cloudformation/app-stack.yaml: Application infrastructure stack.
- infra/cloudformation/pipeline-stack.yaml: CI/CD infrastructure stack.
- buildspecs/deploy.yml: Build and deployment script run by CodeBuild.

## Prerequisites

- AWS account with permissions to create IAM, S3, Lambda, Kinesis, CodeBuild, CodePipeline, and CloudFormation resources.
- Source code pushed to GitHub repository.
- CodeStar Connection created between AWS and GitHub.
- AWS CLI configured locally for initial bootstrap.

## 1) Create CodeStar connection

In AWS Console:

1. Open Developer Tools -> Settings -> Connections.
2. Create connection for GitHub.
3. Authorize and save.
4. Copy the connection ARN.

## 2) Deploy pipeline stack (one-time bootstrap)

Run from the repository root:

```bash
aws cloudformation deploy \
  --template-file infra/cloudformation/pipeline-stack.yaml \
  --stack-name realtime-loads-cicd \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    RepositoryId=<github-owner>/<github-repo> \
    BranchName=main \
    ConnectionArn=<codestar-connection-arn> \
    LambdaArtifactBucketName=<globally-unique-lambda-artifacts-bucket> \
    AppStackName=realtime-loads-app
```

Optional parameter:

- RawBucketName=<existing-or-preferred-bucket-name>

If omitted, the app stack creates a bucket with generated name.

## 3) Trigger deployment

- Any commit to the configured branch triggers the pipeline.
- Build stage zips Lambda code and uploads to Lambda artifact bucket.
- Build stage then deploys infra/cloudformation/app-stack.yaml.

## 4) Validate deployment

After pipeline succeeds, check:

- CloudFormation stack: realtime-loads-app
- Lambda functions: realtime-loads-producer and realtime-loads-consumer
- Kinesis stream: realtime-order-events
- S3 raw bucket output from CloudFormation

## 5) Test end-to-end

- Invoke producer Lambda with producer/test_event.json payload.
- Verify records are written by consumer into:
  raw/year=YYYY/month=MM/day=DD/hour=HH/*.json

## Notes for Databricks bronze/silver

This CI/CD setup deploys ingestion infrastructure only (Lambda, Kinesis, S3).
Databricks bronze/silver jobs should be managed in your Databricks deployment pipeline (for example Databricks Asset Bundles, Terraform, or Databricks Jobs API) and can consume the S3 raw zone deployed here.
