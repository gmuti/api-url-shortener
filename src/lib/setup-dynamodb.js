import {
  DynamoDBClient,
  CreateTableCommand,
  DescribeTableCommand,
  waitUntilTableExists,
} from "@aws-sdk/client-dynamodb";
import { Config } from "./config.js";

/**
 * Initialise les tables DynamoDB au démarrage.
 */

const dynamoClient = new DynamoDBClient({
  region: Config.region,
  endpoint: Config.dynamoDB.hostEndpoint,
  credentials: Config.isLocal ? Config.dynamoDB.credentials : undefined,
});

const tables = [
  {
    TableName: Config.dynamoDB.tables.urls,
    AttributeDefinitions: [{ AttributeName: "shortKey", AttributeType: "S" }],
    KeySchema: [{ AttributeName: "shortKey", KeyType: "HASH" }],
    BillingMode: "PAY_PER_REQUEST",
    StreamSpecification: {
      StreamEnabled: true,
      StreamViewType: "NEW_IMAGE",
    },
  },
  {
    TableName: Config.dynamoDB.tables.clickEvents,
    AttributeDefinitions: [{ AttributeName: "eventId", AttributeType: "S" }],
    KeySchema: [{ AttributeName: "eventId", KeyType: "HASH" }],
    BillingMode: "PAY_PER_REQUEST",
    StreamSpecification: {
      StreamEnabled: true,
      StreamViewType: "NEW_IMAGE",
    },
  },
  {
    TableName: Config.dynamoDB.tables.dailyStats,
    AttributeDefinitions: [
      { AttributeName: "shortKey", AttributeType: "S" },
      { AttributeName: "statDate", AttributeType: "S" },
    ],
    KeySchema: [
      { AttributeName: "shortKey", KeyType: "HASH" },
      { AttributeName: "statDate", KeyType: "RANGE" },
    ],
    BillingMode: "PAY_PER_REQUEST",
  },
];

async function createTablesIfNotExist() {
  for (const table of tables) {
    try {
      await dynamoClient.send(
        new DescribeTableCommand({ TableName: table.TableName })
      );
      console.log(`✔ Table "${table.TableName}" existante.`);
    } catch (err) {
      if (err.name === "ResourceNotFoundException") {
        console.log(`➜ Création table "${table.TableName}"...`);
        await dynamoClient.send(new CreateTableCommand(table));
        await waitUntilTableExists(
          { client: dynamoClient, maxWaitTime: 30 },
          { TableName: table.TableName }
        );
        console.log(`✔ Table "${table.TableName}" créée.`);
      } else {
        throw err;
      }
    }
  }
}

createTablesIfNotExist()
  .then(() => {
    console.log("✅ DynamoDB prêt.");
    process.exit(0);
  })
  .catch((err) => {
    console.error("❌ Erreur init DynamoDB:", err);
    process.exit(1);
  });
