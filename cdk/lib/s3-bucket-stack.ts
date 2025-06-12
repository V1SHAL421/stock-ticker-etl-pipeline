import { Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Bucket, BlockPublicAccess, BucketEncryption } from 'aws-cdk-lib/aws-s3';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class S3BucketStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Defining the Logs Bucket for Glue Jobs
    const glueLogBucket = new Bucket(
      this,
      "GlueLogBucket",
      {
        bucketName: "glue-job-logs-bucket",
        removalPolicy: RemovalPolicy.DESTROY,
        autoDeleteObjects: true
      }
    )

    // Defining the Main Raw S3 Bucket
    const main_raw_bucket = new Bucket(
      this, // Refers to the stack that the bucket will be deployed to
      "MainRawS3Bucket", // ID
      {
        bucketName: "main-raw-tick-data-bucket",
        encryption: BucketEncryption.S3_MANAGED,
        versioned: true,
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL, // No one can access bucket without permissions
        removalPolicy: RemovalPolicy.DESTROY // The bucket deletes when the stack is deleted
      }
    )

    // Defining the Test Raw S3 Bucket
    const test_raw_bucket = new Bucket(
      this, // Refers to the stack that the bucket will be deployed to
      "TestRawS3Bucket", // ID
      {
        bucketName: "test-raw-tick-data-bucket",
        encryption: BucketEncryption.S3_MANAGED,
        versioned: true,
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL, // No one can access bucket without permissions
        removalPolicy: RemovalPolicy.DESTROY // The bucket deletes when the stack is deleted
      }
    )

    const main_cleaned_bucket = new Bucket(
      this, // Refers to the stack that the bucket will be deployed to
      "MainCleanedS3Bucket", // ID
      {
        bucketName: "main-cleaned-tick-data-bucket",
        encryption: BucketEncryption.S3_MANAGED,
        versioned: true,
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL, // No one can access bucket without permissions
        removalPolicy: RemovalPolicy.DESTROY // The bucket deletes when the stack is deleted
      }
    )

    // Defining the Test Cleaned S3 Bucket
    const test_cleaned_bucket = new Bucket(
      this, // Refers to the stack that the bucket will be deployed to
      "TestCleanedS3Bucket", // ID
      {
        bucketName: "test-cleaned-tick-data-bucket",
        encryption: BucketEncryption.S3_MANAGED,
        versioned: true,
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL, // No one can access bucket without permissions
        removalPolicy: RemovalPolicy.DESTROY // The bucket deletes when the stack is deleted
      }
    ) 
  }
}
