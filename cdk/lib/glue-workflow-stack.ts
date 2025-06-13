import { Stack, StackProps } from "aws-cdk-lib";
import { ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";
import { CfnCrawler, CfnDatabase, CfnJob } from 'aws-cdk-lib/aws-glue';
import { Asset } from "aws-cdk-lib/aws-s3-assets";
import * as path from "path";
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
        catalogId: this.account,
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

    glue_crawler_role.addToPolicy(
        new PolicyStatement({
            actions: [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            resources: [
                "arn:aws:s3:::main-raw-tick-data-bucket",
                "arn:aws:s3:::main-raw-tick-data-bucket/*" 
            ]
        })
    )
    this.glueRole = glue_crawler_role

    const glue_crawler_s3 = new CfnCrawler(this, "glue-crawler-s3", {
        name: "s3-parquet-crawler",
        role: glue_crawler_role.roleArn,
        targets: {
            s3Targets: [
                {
                    path: raw_s3_bucket_file_path
                }
            ]
        },
        databaseName: glue_db.ref,
        schemaChangePolicy: {
            updateBehavior: "UPDATE_IN_DATABASE",
            deleteBehavior: "DEPRECATE_IN_DATABASE"
        }
    });

    const rawToCleanETLAsset = new Asset(this, "raw-to-clean-etl", {
        path: path.join(
            __dirname,
            "../../src/interfaces/glue/raw_to_cleaned_main.py"
        )
    })

    const glue_job_asset = new CfnJob(this, "glue-job-asset", {
        name: "glue-raw-to-clean-asset-job",
        description: "Clean the raw tick data and output to cleaned S3 bucket",
        role: glue_crawler_role.roleArn,
        executionProperty: { maxConcurrentRuns: 1},
        glueVersion: "4.0",
        defaultArguments: {
            "--TempDir": raw_s3_bucket_file_path + "temp/",
            "--enable-job-insights": "false",
            "--job-language": "python",
            "--enable-continuous-logging": "true",
            "--enable-metrics": "true"
        },
        command: {
            name: "glueetl",
            pythonVersion: "3",
            scriptLocation: rawToCleanETLAsset.s3ObjectUrl
        },
        logUri: "s3://glue-job-logs-bucket",
        maxRetries: 3,
        timeout: 60,
        workerType: "G.1X",
        numberOfWorkers: 10
    })

  }
}