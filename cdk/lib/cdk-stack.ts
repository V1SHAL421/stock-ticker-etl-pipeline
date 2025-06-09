import { Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Bucket, BlockPublicAccess, BucketEncryption } from 'aws-cdk-lib/aws-s3';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class CdkStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Defining the Main Raw S3 Bucket
    const main_raw_bucket = new Bucket(
      this, // Refers to the stack that the bucket will be deployed to
      "S3Bucket", // ID
      {
        bucketName: "raw-tick-data-bucket",
        encryption: BucketEncryption.S3_MANAGED,
        versioned: true,
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL, // No one can access bucket without permissions
        removalPolicy: RemovalPolicy.DESTROY // The bucket deletes when the stack is deleted
      }
    )

    // Defining the Test Raw S3 Bucket
    const test_raw_bucket = new Bucket(
      this, // Refers to the stack that the bucket will be deployed to
      "S3Bucket", // ID
      {
        bucketName: "test-raw-tick-data-bucket",
        encryption: BucketEncryption.S3_MANAGED,
        versioned: true,
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL, // No one can access bucket without permissions
        removalPolicy: RemovalPolicy.DESTROY // The bucket deletes when the stack is deleted
      }
    )

    // The code that defines your stack goes here

    // example resource
    // const queue = new sqs.Queue(this, 'CdkQueue', {
    //   visibilityTimeout: cdk.Duration.seconds(300)
    // });
  }
}
