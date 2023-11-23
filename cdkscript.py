import * as cdk from 'aws-cdk-lib';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';

class DataPipelineStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    # Amazon RDS MySQL Database
    const db = new rds.DatabaseInstance(this, 'MyDatabase', {
      engine: rds.DatabaseInstanceEngine.mysql({ version: rds.MysqlEngineVersion.VER_8_0 }),
      instanceClass: new rds.InstanceTypePair(rds.InstanceClass.BURSTABLE2, rds.InstanceSize.MICRO),
      masterUsername: 'admin',
      masterUserPassword: cdk.SecretValue.plainText('your-db-password'),
      databaseName: 'your-database-name',
    });

    # S3 Buckets
    const sourceBucket = new s3.Bucket(this, 'SourceBucket');
    const rawBucket = new s3.Bucket(this, 'RawDataBucket');
    const processedBucket = new s3.Bucket(this, 'ProcessedDataBucket');

    # Lambda for Data Ingestion
    const dataIngestionLambda = new lambda.Function(this, 'DataIngestionLambda', {
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('path/to/your/lambda/code'),
      environment: {
        RDS_ENDPOINT: db.dbInstanceEndpointAddress,
        RDS_USERNAME: 'admin',
        RDS_PASSWORD: 'your-db-password',
        DATABASE_NAME: 'your-database-name',
      },
    });

    #Grant necessary permissions for Lambda to access RDS
    db.grantConnect(dataIngestionLambda);
    
    # EventBridge Rule for Triggering Lambda every hour
    const rule = new events.Rule(this, 'IngestionRule', {
      schedule: events.Schedule.expression('rate(1 hour)'),
    });
    rule.addTarget(new targets.LambdaFunction(dataIngestionLambda));

    # Glue Crawler
    const crawler = new glue.CfnCrawler(this, 'MyCrawler', {
      name: 'MyCrawler',
      role: 'glue-service-role', // Replace with your Glue service role name
      databaseName: 'your-database-name',
      targets: {
        s3Targets: [{
          path: `s3://${rawBucket.bucketName}/`,
        }],
      },
    });

    #Glue Job
    const job = new glue.CfnJob(this, 'MyGlueJob', {
      name: 'MyGlueJob',
      role: 'glue-service-role', // Replace with your Glue service role name
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: `s3://${processedBucket.bucketName}/your-glue-script.py`,
      },
    });

    # Grant necessary permissions for Glue Job
    job.addToRolePolicy(new iam.PolicyStatement({
      actions: ['s3:GetObject', 's3:PutObject'],
      resources: [`${processedBucket.bucketArn}/*`],
    }));

    # Output
    new cdk.CfnOutput(this, 'RawDataBucketOutput', { value: rawBucket.bucketName });
    new cdk.CfnOutput(this, 'ProcessedDataBucketOutput', { value: processedBucket.bucketName });
  }
}

const app = new cdk.App();
new DataPipelineStack(app, 'DataPipelineStack');
