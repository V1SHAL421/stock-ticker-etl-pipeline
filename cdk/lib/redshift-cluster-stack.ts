import { Cluster, NodeType, Table, TableAction, TableSortStyle, User } from '@aws-cdk/aws-redshift-alpha';
import { Stack, StackProps } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Construct } from 'constructs';

export class RedshiftClusterStack extends Stack {

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
    }
}
