/* global fetch */
/* global Headers */
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { SSMClient, GetParameterCommand } = require("@aws-sdk/client-ssm");

const REGION = process.env.AWS_REGION || "us-east-1";

// The MailerLite API token lives in SSM Parameter Store (SecureString),
// never in a plaintext Lambda environment variable or CloudFormation
// parameter. Fetched once per execution environment (cold start) and reused
// across warm invocations. Stored WITHOUT the "Bearer " prefix; that prefix
// is added at request time below.
const ssmClient = new SSMClient({ region: REGION });
const BEARER_TOKEN = await ssmClient
  .send(
    new GetParameterCommand({
      Name: process.env.BEARER_TOKEN_PARAM_NAME,
      WithDecryption: true,
    })
  )
  .then((res) => res.Parameter.Value);

// MailerLite group IDs, by audience. Configurable via template.yaml
// parameters so they can differ per deployment without a code change.
const ACTIVE_MEMBER_GROUP_ID = process.env.ACTIVE_MEMBER_GROUP_ID;
const INACTIVE_MEMBER_GROUP_ID = process.env.INACTIVE_MEMBER_GROUP_ID;
const PARENT_GROUP_ID = process.env.PARENT_GROUP_ID;

const MAILERLITE_SUBSCRIBERS_URL = "https://connect.mailerlite.com/api/subscribers";

console.log("Loading function");

/**
 * Creates/updates a single MailerLite subscriber. Rejects on any non-2xx
 * response; the handler collects rejections so every upsert for a record is
 * attempted, then fails the invocation so SQS redelivers the message.
 */
async function upsertSubscriber(email, fields, groupId) {
  if (!email) return;

  const headers = new Headers();
  headers.append("Content-Type", "application/json");
  headers.append("Authorization", `Bearer ${BEARER_TOKEN}`);

  const body = JSON.stringify({
    email,
    status: "active",
    fields,
    groups: [groupId],
  });

  const response = await fetch(MAILERLITE_SUBSCRIBERS_URL, {
    method: "POST",
    headers,
    body,
    redirect: "follow",
  });
  const result = await response.text();
  if (!response.ok) {
    throw new Error(
      `MailerLite request failed with status ${response.status}: ${result}`
    );
  }
  console.log(result);
}

// Maps the DynamoDB NewImage attribute shape onto the MailerLite custom
// field names expected by the lodge104-member_data integration.
function memberFields(image) {
  return {
    name: image.FirstName.S,
    last_name: image.LastName.S,
    country: "United States of America",
    city: image.AddressPrimaryCity.S,
    phone: image.PhonePrimaryNumber.S,
    state: image.AddressPrimaryState.S,
    z_i_p: image.AddressPrimaryZipCode.S,
    bsa_id: image.BsaPersonId.N,
    dues_year: image.DuesYear.N,
    position: image.Positions.S,
    street_1: image.AddressPrimaryStreet1.S,
    street_2: image.AddressPrimaryStreet2.S,
    chapter: image.Chapter.S,
    level: image.Level.S,
    date_of_birth: image.DateOfBirth.S,
    election_date: image.ElectionDate.S,
    ya: image.YouthAdult.S,
    bsa_reg: image.BsaLastCheckStatus.S,
    ypt_compliant: image.YouthProtectionCompliant.BOOL,
    committee: image.PositionLec.BOOL,
    bro_date: image.BrotherhoodDate.S,
    ordeal_date: image.OrdealDate.S,
    brot_eligible_date: image.BrotherhoodEligibleDate.S,
    unit: image.Unit.S,
    vigil_date: image.VigilInductionDate.S,
  };
}

function parentFields(image) {
  return {
    child_bsa_number: image.BsaPersonId.N,
    dues_year: image.DuesYear.N,
  };
}

export const handler = async (event) => {
  // SQS delivers a batch of records whose body is the JSON-serialized
  // DynamoDB stream record, forwarded by an EventBridge Pipe fed from the
  // lodge104-member_data table stream. BatchSize is 1, so there is exactly
  // one record per invocation.
  const record = JSON.parse(event.Records[0].body);
  console.log("Stream record: ", JSON.stringify(record, null, 2));

  const image = record.dynamodb.NewImage;
  const currentYear = new Date().getFullYear();
  const isCurrentDuesYear = image.DuesYear.N >= currentYear;
  const memberGroupId = isCurrentDuesYear ? ACTIVE_MEMBER_GROUP_ID : INACTIVE_MEMBER_GROUP_ID;

  const upserts = [
    [image.EmailPrimaryAddress.S, memberFields(image), memberGroupId],
  ];
  if (isCurrentDuesYear) {
    for (const email of [image.EmailParentAddress?.S, image.CustomEmailParent2Address?.S]) {
      if (email) upserts.push([email, parentFields(image), PARENT_GROUP_ID]);
    }
  }

  // Attempt every upsert (sequentially, to respect MailerLite rate limits) so
  // one failure doesn't skip the others, then throw if any failed. Throwing
  // returns the message to the queue (after the visibility timeout) for
  // retry, and eventually to the DLQ after DeadLetterMaxReceiveCount
  // receives. Upserts are idempotent, so re-running succeeded ones is safe.
  const failures = [];
  for (const [email, fields, groupId] of upserts) {
    try {
      await upsertSubscriber(email, fields, groupId);
    } catch (err) {
      console.error(`Upsert failed for ${email} (group ${groupId}):`, err);
      failures.push(err.message);
    }
  }
  if (failures.length > 0) {
    throw new Error(`${failures.length} upsert(s) failed: ${failures.join("; ")}`);
  }

  return { statusCode: 200 };
};
