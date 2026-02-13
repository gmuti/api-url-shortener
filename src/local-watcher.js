/**
 * Watcher local pour simuler les déclencheurs DynamoDB Streams.
 * Scanne les tables ou écoute les streams pour déclencher les Lambdas appropriées.
 */

import { DynamoDBClient, ScanCommand } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBStreamsClient,
  DescribeStreamCommand,
  GetShardIteratorCommand,
  GetRecordsCommand,
  ListStreamsCommand,
} from "@aws-sdk/client-dynamodb-streams";
import { Config } from "./lib/config.js";

const STREAMS_ENABLED = process.env.ENABLE_DDB_STREAMS === "true";
const TABLE_URLS = Config.dynamoDB.tables.urls;
const TABLE_CLICK_EVENTS = Config.dynamoDB.tables.clickEvents;

console.log(`[Watcher] Using DynamoDB Endpoint: ${Config.dynamoDB.endpoint}`);

/**
 * Clients AWS (DynamoDB + Streams)
 */
const clientConfig = {
  region: Config.region,
  endpoint: Config.dynamoDB.endpoint,
  credentials: Config.dynamoDB.credentials,
};

const dynamoClient = new DynamoDBClient(clientConfig);
const streamsClient = new DynamoDBStreamsClient(clientConfig);

/**
 * État interne des streams
 */
const state = {
  [TABLE_URLS]: { streamArn: null, shardId: null, iterator: null },
  [TABLE_CLICK_EVENTS]: { streamArn: null, shardId: null, iterator: null },
};

/**
 * Utils
 */
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function getLatestStreamArn(tableName) {
  try {
    const command = new ListStreamsCommand({ TableName: tableName });
    const response = await streamsClient.send(command);
    return response.Streams?.length ? response.Streams[0].StreamArn : null;
  } catch (e) {
    console.warn(
      `[Watcher] Could not list streams for ${tableName}: ${e.message}`
    );
    return null;
  }
}

async function initStream(tableName, handlerName) {
  console.log(
    `[${handlerName}] Initializing stream watcher for ${tableName}...`
  );

  const streamArn = await getLatestStreamArn(tableName);
  if (!streamArn) {
    console.log(`[${handlerName}] No stream found for ${tableName}. Waiting...`);
    return false;
  }

  const describe = await streamsClient.send(
    new DescribeStreamCommand({ StreamArn: streamArn })
  );
  const shards = describe.StreamDescription?.Shards || [];
  if (shards.length === 0) return false;

  const shardId = shards[shards.length - 1].ShardId;
  const iteratorCmd = new GetShardIteratorCommand({
    StreamArn: streamArn,
    ShardId: shardId,
    ShardIteratorType: "LATEST",
  });
  const iteratorRes = await streamsClient.send(iteratorCmd);

  state[tableName] = {
    streamArn,
    shardId,
    iterator: iteratorRes.ShardIterator,
  };
  console.log(`[${handlerName}] Listening on shard ${shardId}`);
  return true;
}

async function resetIterator(tableName, handlerName) {
  const s = state[tableName];
  if (!s.streamArn || !s.shardId) return;

  console.warn(`[${handlerName}] Resetting shard iterator (TRIM_HORIZON)`);
  const iteratorCmd = new GetShardIteratorCommand({
    StreamArn: s.streamArn,
    ShardId: s.shardId,
    ShardIteratorType: "TRIM_HORIZON",
  });
  const iteratorRes = await streamsClient.send(iteratorCmd);
  s.iterator = iteratorRes.ShardIterator;

  await wait(1000);
}

async function scanTable(tableName, handlerFunc, handlerName) {
  try {
    const data = await dynamoClient.send(
      new ScanCommand({ TableName: tableName })
    );
    if (data.Items?.length) {
      console.log(`[${handlerName}] Scan found ${data.Items.length} items`);
      const event = {
        Records: data.Items.map((item) => ({
          eventID:
            item.shortKey?.S || item.eventId?.S || Math.random().toString(),
          eventName: "MODIFY",
          dynamodb: { NewImage: item },
          eventSource: "aws:dynamodb",
          awsRegion: Config.region,
        })),
      };
      await handlerFunc(event);
    }
  } catch (err) {
    console.error(`[${handlerName}] Scan error:`, err.message);
  }
}

/**
 * Polling Streams ou Scan fallback
 */
async function poll(tableName, handlerFunc, handlerName) {
  if (!STREAMS_ENABLED) {
    await scanTable(tableName, handlerFunc, handlerName);
    await wait(3000);
    return;
  }

  const s = state[tableName];
  if (!s || !s.iterator) {
    await initStream(tableName, handlerName);
    return;
  }

  try {
    const recordsRes = await streamsClient.send(
      new GetRecordsCommand({ ShardIterator: s.iterator })
    );
    if (recordsRes.NextShardIterator) s.iterator = recordsRes.NextShardIterator;
    else {
      s.iterator = null;
      return;
    }

    if (recordsRes.Records?.length) {
      console.log(
        `[${handlerName}] Received ${recordsRes.Records.length} records`
      );
      const event = {
        Records: recordsRes.Records.map((r) => ({
          eventID: r.eventID,
          eventName: r.eventName,
          dynamodb: r.dynamodb,
          eventSource: "aws:dynamodb",
          awsRegion: Config.region,
        })),
      };
      await handlerFunc(event);
    }
  } catch (err) {
    const msg = err?.message || "";
    if (msg.includes("read past the oldest stream record")) {
      await resetIterator(tableName, handlerName);
      return;
    }
    if (err.name === "ExpiredIteratorException") {
      s.iterator = null;
      return;
    }
    console.warn(`[${handlerName}] Polling error: ${msg}`);
    await wait(1000);
  }
}

/**
 * Main
 */
async function main() {
  console.log("🚀 Local Stream Watcher started");
  console.log("STREAMS_ENABLED =", STREAMS_ENABLED);

  const { handler: processFavicon } = await import(
    "./handlers/process-favicon.js"
  );
  const { handler: processStats } = await import("./handlers/process-stats.js");

  setInterval(
    () => poll(TABLE_URLS, processFavicon, "PROCESS-FAVICON"),
    2000
  );
  setInterval(
    () => poll(TABLE_CLICK_EVENTS, processStats, "PROCESS-STATS"),
    2000
  );
}

main();
