#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { S3BucketStack } from '../lib/s3-bucket-stack';

const app = new cdk.App();

const s3_bucket_stack = new S3BucketStack(app, 'CdkStack', {
  env: { account: '187065639894', region: 'eu-west-2'},
  stackName: 's3-bucket-stack',
  description: 'Creates the S3 Buckets'
});