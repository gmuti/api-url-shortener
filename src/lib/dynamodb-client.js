import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { Config } from "./config.js";

// Client bas niveau
const dynamoClient = new DynamoDBClient({
  region: Config.region,
  endpoint: Config.dynamoDB.endpoint,
  credentials: Config.isLocal ? Config.dynamoDB.credentials : undefined,
});

// Client Document (CRUD simplifié)
export const dynamoDb = DynamoDBDocumentClient.from(dynamoClient, {
  marshallOptions: {
    removeUndefinedValues: true,
  },
});

/**
 * Retourne le client DynamoDB.
 */
export async function initDynamo() {
  return dynamoDb;
}
