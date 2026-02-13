/**
 * Lambda : GET /stats/{shortKey}
 * Récupère les stats journalières (30 derniers jours).
 */

import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import { initDynamo } from "../lib/dynamodb-client.js";
import { Config } from "../lib/config.js";

const TABLE_DAILY_STATS = Config.dynamoDB.tables.dailyStats;

export const handler = async (event) => {
  const dynamoDb = await initDynamo();

  // 1. Validation shortKey
  const shortKey = event.pathParameters?.shortKey;

  if (!shortKey) {
    return {
      statusCode: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "shortKey is required" }),
    };
  }

  // 2. Requête DynamoDB
  try {
    const result = await dynamoDb.send(
      new QueryCommand({
        TableName: TABLE_DAILY_STATS,
        KeyConditionExpression: "shortKey = :sk",
        ExpressionAttributeValues: {
          ":sk": shortKey,
        },
        ScanIndexForward: false, // Tri décroissant (plus récent en premier)
        Limit: 30,
      })
    );

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        shortKey,
        stats: result.Items || [],
      }),
    };
  } catch (error) {
    console.error("Erreur GET /stats:", error);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: "Internal server error",
        details: error.message,
      }),
    };
  }
};
