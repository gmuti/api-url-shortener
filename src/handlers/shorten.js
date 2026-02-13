/**
 * Lambda : POST /shorten
 * Crée une URL courte unique.
 */

import { PutCommand } from "@aws-sdk/lib-dynamodb";
import { initDynamo } from "../lib/dynamodb-client.js";
import { Config } from "../lib/config.js";

const TABLE_URLS = Config.dynamoDB.tables.urls;
const SHORT_KEY_LENGTH = 6;
const MAX_RETRIES = 5;

/**
 * Génère une clé aléatoire.
 */
function generateShortKey(length = SHORT_KEY_LENGTH) {
  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let key = "";
  for (let i = 0; i < length; i++) {
    key += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return key;
}

export const handler = async (event) => {
  const dynamoDb = await initDynamo();

  // 1. Validation input
  let body;
  try {
    body = JSON.parse(event.body);
  } catch {
    return {
      statusCode: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Invalid JSON body" }),
    };
  }

  if (!body?.url) {
    return {
      statusCode: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "URL is required" }),
    };
  }

  const longUrl = body.url;
  const createdAt = Date.now();
  let shortKey;
  let attempts = 0;

  // 2. Génération clé unique (avec retries)
  while (attempts < MAX_RETRIES) {
    shortKey = generateShortKey();

    try {
      await dynamoDb.send(
        new PutCommand({
          TableName: TABLE_URLS,
          Item: {
            shortKey,
            longUrl,
            createdAt,
            clickCount: 0,
          },
          ConditionExpression: "attribute_not_exists(shortKey)",
        })
      );
      break; // Succès
    } catch (err) {
      if (err.name === "ConditionalCheckFailedException") {
        attempts++;
        continue;
      }

      console.error("Erreur DynamoDB:", err);
      return {
        statusCode: 500,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          error: "Database error",
          details: err.message,
        }),
      };
    }
  }

  if (attempts === MAX_RETRIES) {
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: "Failed to generate unique short URL",
      }),
    };
  }

  // 3. Construction réponse
  const proto =
    event.headers?.["x-forwarded-proto"] ||
    event.headers?.["X-Forwarded-Proto"] ||
    "http";
  const host = event.headers?.host || "localhost:3000";
  const baseUrl = `${proto}://${host}`;

  return {
    statusCode: 201,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      shortKey,
      shortUrl: `${baseUrl}/${shortKey}`,
      longUrl,
      createdAt,
    }),
  };
};
