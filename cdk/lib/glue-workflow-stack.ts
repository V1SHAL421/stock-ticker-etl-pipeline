import { Stack, StackProps } from "aws-cdk-lib";
import { ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";
import { CfnCrawler, CfnDatabase, CfnJob } from 'aws-cdk-lib/aws-glue';
import { Asset } from "aws-cdk-lib/aws-s3-assets";
import * as path from "path";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { Cluster, NodeType, Table, TableAction, TableSortStyle, User } from '@aws-cdk/aws-redshift-alpha';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
// import * as glue from 'aws-cdk-lib/aws-glue';

const glue_managed_policy = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole";
const glue_service_url = "glue.amazonaws.com"
const raw_s3_bucket_file_path = "s3://main-raw-tick-data-bucket/"
const cleaned_s3_bucket_file_path = "s3://main-cleaned-tick-data-bucket/cleaned-data/"
// s3:// is for most AWS services, s3a:// is for Hadoop/Spark

export class GlueWorkflowStack extends Stack {

    public readonly glueRole: Role;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'Vpc');
            const cluster = new Cluster(this, 'Redshift', {
                masterUser: {
                    masterUsername: 'admin'
                },
                vpc,
                nodeType: NodeType.RA3_LARGE,
                availabilityZoneRelocation: true,
                defaultDatabaseName: 'redshift_database'
            })
    
            const user = new User(this, 'User', {
                username: 'adminuser',
                cluster: cluster,
                databaseName: 'redshift_database'
            })
    
            const table = new Table(this, 'MarketData', {
                tableName: 'MarketData',
                tableColumns: [
                    {name: 'datetime', dataType: 'timestamp', sortKey: true},
                    {name: 'open_price', dataType: 'float'},
                    {name: 'high_price', dataType: 'float'},
                    {name: 'low_price', dataType: 'float'},
                    {name: 'close_price', dataType: 'float'},
                    {name: 'volume', dataType: 'integer'},
                    {name: 'dividends', dataType: 'float'},
                    {name: 'stock_splits', dataType: 'float'},
                    {name: 'partition_0', dataType: 'varchar(10)'},
                    {name: 'date_col', dataType: 'varchar(10)'},
                ],
                cluster: cluster,
                databaseName: 'redshift_database',
                sortStyle: TableSortStyle.COMPOUND
            });
    
            table.grant(user, TableAction.ALL);
    
            cluster.addToParameterGroup('enable_user_activity_logging', 'true');
    

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
                "s3:DeleteObject",
                "redshift-data:ExecuteStatement",
                "redshift-data:DescribeStatement"
            ],
            resources: [
                "arn:aws:s3:::main-raw-tick-data-bucket",
                "arn:aws:s3:::main-raw-tick-data-bucket/*",
                "arn:aws:s3:::main-cleaned-tick-data-bucket",
                "arn:aws:s3:::main-cleaned-tick-data-bucket/cleaned_data",
                "arn:aws:s3:::main-cleaned-tick-data-bucket/cleaned_data/*",
                `arn:aws:redshift:${this.region}:${this.account}:cluster:Redshift`

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

    const cleanedToRedshiftETLAsset = new Asset(this, "cleaned-to-redshift-etl", {
        path: path.join(
            __dirname,
            "../../src/interfaces/glue/cleaned_to_redshift_main.py"
        )
    })

    const glue_job_asset_raw_to_cleaned = new CfnJob(this, "glue-job-asset", {
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

    const glue_job_copy_asset = new CfnJob(this, 'glue-job-copy', {
        name: "glue-cleaned-to-redshift-asset-job",
        description: "Copy the transformed tick data into a Redshift database",
        role: glue_crawler_role.roleArn,
        executionProperty: { maxConcurrentRuns: 1},
        glueVersion: "4.0",
        defaultArguments: {
            "--TempDir": cleaned_s3_bucket_file_path + "temp/",
            "--enable-job-insights": "false",
            "--job-language": "python",
            "--enable-continuous-logging": "true",
            "--enable-metrics": "true",
            "--clusterName": cluster.clusterName,
            "--database": "redshift_database",
            "--dbUser": user.username
        },
        command: {
            name: "glueetl",
            pythonVersion: "3",
            scriptLocation: cleanedToRedshiftETLAsset.s3ObjectUrl
        },
        logUri: "s3://glue-job-logs-bucket",
        maxRetries: 3,
        timeout: 60,
        workerType: "G.1X",
        numberOfWorkers: 10
    })

    const cdk_assets_bucket = Bucket.fromBucketName(
        this,
        'CDKAssetsBucket',
        `cdk-hnb659fds-assets-${this.account}-${this.region}`
    );

    cdk_assets_bucket.grantRead(glue_crawler_role);

  }
}