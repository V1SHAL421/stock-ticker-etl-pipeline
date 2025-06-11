#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { S3BucketStack } from '../lib/s3-bucket-stack';
import { GlueWorkflowStack } from '../lib/glue-workflow-stack';

const app = new cdk.App();

const s3_bucket_stack = new S3BucketStack(app, 's3-bucket-stack', {
  env: { account: '187065639894', region: 'eu-west-2'},
  stackName: 'S3BucketStack',
  description: 'Creates the S3 Buckets'
});

const glue_workflow_stack = new GlueWorkflowStack(app, 'glue-workflow-stack', {
  env: { account: '187065639894', region: 'eu-west-2'},
  stackName: 'GlueWorkflowStack',
  description: 'Creates the Glue Workflow'
})