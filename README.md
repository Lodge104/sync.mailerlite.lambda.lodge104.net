# lambda-mailerlite-sync

Syncs membership data with MailerLite. Deployed with AWS SAM
(`template.yaml`) and a CodePipeline (`codepipeline.yaml`) that builds,
packages, and deploys the stack to the `prod` region on every push to
`main`.

## Architecture

```
lodge104-member_data (DynamoDB table + stream)
        │
        ▼
lodge104-member_data-SQS (EventBridge Pipe, managed outside this stack)
        │
        ▼
lodge104-member_data-mailerlite (SQS queue, owned by this stack)
        │
        ▼
lodge104-mailerlite-sync (this function)
        │
        ▼
MailerLite Subscribers API
```

Each DynamoDB change event for a member record is filtered and forwarded by
an EventBridge Pipe onto an SQS queue, which triggers this Lambda function
once per message (`BatchSize: 1`). The function upserts a MailerLite
subscriber for the member and, when a current-dues-year youth member has a
parent/guardian email on file, for the parent/guardian as well.

## MailerLite credentials

The MailerLite API bearer token is stored as a `SecureString` parameter in
SSM Parameter Store:

- `/lodge104/mailerlite-sync/bearer-token`

Store the **raw token value only, without the `Bearer ` prefix** — the
function adds that prefix itself when building the `Authorization` header.
`SyncFunction` reads the parameter directly at startup (`GetParameter` with
`WithDecryption: true`), using a policy on `SyncRole` scoped to exactly that
one parameter ARN plus `kms:Decrypt` restricted to `kms:ViaService:
ssm.<region>.amazonaws.com`. The credential value never passes through a
CloudFormation parameter or a Lambda environment variable in plaintext, so
it can't be read back via `DescribeStacks`, `GetFunctionConfiguration`, or
pipeline logs — only the SSM parameter *name* is, which is not sensitive.

To rotate the token, update the SSM parameter value directly (e.g. `aws ssm
put-parameter --name /lodge104/mailerlite-sync/bearer-token --type
SecureString --overwrite --value <new-token>`). No redeploy is required.
Because the value is cached for the lifetime of each Lambda execution
environment, an already-warm environment keeps using the old value until it
is recycled. To force immediate pickup, recycle the execution environments
(for example, by publishing a no-op update to `SyncFunction`); otherwise,
the new value is picked up when an environment is recycled, with no
guaranteed time.

The MailerLite group IDs (`ActiveMemberGroupId`, `InactiveMemberGroupId`,
`ParentGroupId`) and the `AlarmTopicArn`/`ErrorAlarmThreshold`/etc.
parameters are not secrets and remain plain CloudFormation parameters.

## Monitoring & alerts

The stack defines a CloudWatch alarm, `SyncErrorAlarm`, that watches the
`Errors` metric (`AWS/Lambda` namespace) for `SyncFunction`. By default it
fires when the function has three or more 15-minute periods with two or
more failed invocations within a 45-minute window
(`ErrorAlarmThreshold`/`ErrorAlarmPeriodSeconds`/`ErrorAlarmEvaluationPeriods`/`ErrorAlarmDatapointsToAlarm`).
It returns to `OK` once that many consecutive periods fall back under the
threshold; a window with no error datapoint is treated as healthy. Both the
`ALARM` and `OK` transitions publish to the SNS topic referenced by the
`AlarmTopicArn` parameter.

That topic — `lodge104-lambda` — is **shared** across other Lodge 104 Lambda
functions (not just this one) and already has email, SMS, and Slack
subscribers. For that reason, the topic itself is **not** defined/owned by
this stack; it's only referenced by ARN. Managing subscriptions (adding or
removing recipients) is done directly on the topic outside of this repo, so
that other stacks publishing to the same topic are unaffected.

To point at a different notification topic (e.g. for a non-prod stack),
override the `AlarmTopicArn` parameter at deploy time.

### One-time import for existing resources

The IAM role (`lodge104-mailerlite-sync-role-arkyxb4k`), the Lambda function
(`lodge104-mailerlite-sync`), the SQS queue
(`lodge104-member_data-mailerlite`), and the CloudWatch alarm (`AWS/Lambda
Errors FunctionName=lodge104-mailerlite-sync`) all pre-date this stack and
were created directly in the console. A normal deployment can overwrite an
existing resource with the same name/physical ID, including live
configuration such as the alarm's threshold and notification actions.
Import each resource when you need to adopt and preserve its live
configuration rather than allowing a normal deployment to reconcile it with
the template.

`AWS::Lambda::EventSourceMapping` does not support CloudFormation import.
Before the first deploy, delete the function's existing manually-created SQS
event source mapping (`aws lambda delete-event-source-mapping --uuid
<uuid>`); `sam deploy` will then create a new one declared in
`template.yaml`. Messages already in the queue are not lost — they are
simply picked up by the new mapping once it is created, within the
queue's `MessageRetentionPeriod`.

The CodePipeline deploys on every push to `main`. Before merging a change
that imports these resources, pause deployment by disabling the inbound
transition to `DeployProd`, and ensure no deployment is already in
progress:

```bash
aws codepipeline disable-stage-transition \
  --pipeline-name <pipeline-name> \
  --stage-name DeployProd \
  --transition-type Inbound \
  --reason "Import pre-existing resources before deployment"
```

Package the template and use the packaged template for the import change
set:

```bash
sam build --template-file template.yaml
sam package \
  --template-file .aws-sam/build/template.yaml \
  --s3-bucket <pipeline-artifact-bucket> \
  --region us-east-1 \
  --output-template-file packaged-prod.yaml

aws cloudformation create-change-set \
  --stack-name lodge104-mailerlite-sync \
  --change-set-type IMPORT \
  --change-set-name import-existing-resources \
  --template-body file://packaged-prod.yaml \
  --parameters <existing-parameter-overrides> \
  --resources-to-import \
    '[{"ResourceType":"AWS::IAM::Role","LogicalResourceId":"SyncRole","ResourceIdentifier":{"RoleName":"lodge104-mailerlite-sync-role-arkyxb4k"}},
      {"ResourceType":"AWS::SQS::Queue","LogicalResourceId":"MemberDataQueue","ResourceIdentifier":{"QueueUrl":"https://sqs.us-east-1.amazonaws.com/423971488961/lodge104-member_data-mailerlite"}},
      {"ResourceType":"AWS::Lambda::Function","LogicalResourceId":"SyncFunction","ResourceIdentifier":{"FunctionName":"lodge104-mailerlite-sync"}},
      {"ResourceType":"AWS::CloudWatch::Alarm","LogicalResourceId":"SyncErrorAlarm","ResourceIdentifier":{"AlarmName":"AWS/Lambda Errors FunctionName=lodge104-mailerlite-sync"}}]' \
  --capabilities CAPABILITY_NAMED_IAM

aws cloudformation execute-change-set \
  --stack-name lodge104-mailerlite-sync \
  --change-set-name import-existing-resources

aws codepipeline enable-stage-transition \
  --pipeline-name <pipeline-name> \
  --stage-name DeployProd \
  --transition-type Inbound
```

Resume deployment only after the import succeeds. Subsequent pipeline runs
manage these resources like any other stack resource. The SNS topic, the
EventBridge Pipe, and the `lodge104-member_data` DynamoDB table are never
imported or deleted by this stack — they are only read (by ARN or directly
by the pipe, outside CloudFormation).

## Deploying the pipeline

```bash
sam deploy -t codepipeline.yaml --stack-name lodge104-mailerlite-sync-pipeline \
  --capabilities CAPABILITY_NAMED_IAM --region us-east-1
```

The pipeline reuses the GitHub CodeStar Connection already authorized for
the Lodge104 org (see `CodeStarConnectionArn`'s default in
`codepipeline.yaml`). If that connection does not have access to this
repository, override `CodeStarConnectionArn` with an empty string to have
the stack create and own its own connection instead; that one starts in
`PENDING` status and needs a one-time GitHub OAuth handshake in the
console.
