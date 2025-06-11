import { Stack, StackProps } from "aws-cdk-lib";
import { ManagedPolicy, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";
import { CfnCrawler, CfnDatabase } from 'aws-cdk-lib/aws-glue';
// import * as glue from 'aws-cdk-lib/aws-glue';

const glue_managed_policy = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole";
const glue_service_url = "glue.amazonaws.com"
const raw_s3_bucket_file_path = "s3://main-raw-tick-data-bucket/"
// s3:// is for most AWS services, s3a:// is for Hadoop/Spark

export class GlueWorkflowStack extends Stack {

    public readonly glueRole: Role;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const glue_db = new CfnDatabase(this, 'glue-workflow-db', {
        catalogId: "glue-workflow-db",
        databaseInput: {
            name: "raw-tick-data",
            description: "Glue Database for storing raw tick data from the raw S3 bucket",
            parameters: {
                classification: "parquet"
            }
        }
    })

    const glue_crawler_role = new Role(this, "glue-crawler-role", {
        assumedBy: new ServicePrincipal(glue_service_url),
        roleName: "AWSGlueServiceRole-AccessS3Bucket",
        description: "Assigns the managed policy AWSGlueServiceRole to AWS Glue Crawler so it can crawl S3 buckets",
        managedPolicies: [
            ManagedPolicy.fromManagedPolicyArn(
                this,
                "glue-service-policy",
                glue_managed_policy
            )
        ]
    })
    this.glueRole = glue_crawler_role

    const glue_crawler_s3 = new CfnCrawler(this, "glue-crawler-s3", {
        name: "s3-parquet-crawler",
        role: glue_crawler_role.roleName,
        targets: {
            s3Targets: [
                {
                    path: raw_s3_bucket_file_path
                }
            ]
        },
        databaseName: glue_db.databaseName,
        schemaChangePolicy: {
            updateBehavior: "UPDATE_IN_DATABASE",
            deleteBehavior: "DEPRECATE_IN_DATABASE"
        }
    });

  }
}