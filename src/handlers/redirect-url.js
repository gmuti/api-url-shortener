/**
 * Lambda : GET /{shortKey}
 * Redirection + Tracking clic.
 */

import { GetCommand, UpdateCommand, PutCommand } from "@aws-sdk/lib-dynamodb";
import { initDynamo } from "../lib/dynamodb-client.js";
import { randomUUID } from "crypto";
import { Config } from "../lib/config.js";

const TABLE_URLS = Config.dynamoDB.tables.urls;
const TABLE_CLICK_EVENTS = Config.dynamoDB.tables.clickEvents;

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

  // 2. Recherche URL
  let urlItem;
  try {
    const result = await dynamoDb.send(
      new GetCommand({
        TableName: TABLE_URLS,
        Key: { shortKey },
      })
    );
    urlItem = result.Item;
  } catch (error) {
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: "Internal server error",
        details: error.message,
      }),
    };
  }

  if (!urlItem) {
    return {
      statusCode: 404,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Short URL not found" }),
    };
  }

  // 3. Incrément compteur (non bloquant)
  try {
    await dynamoDb.send(
      new UpdateCommand({
        TableName: TABLE_URLS,
        Key: { shortKey },
        UpdateExpression:
          "SET clickCount = if_not_exists(clickCount, :zero) + :inc",
        ExpressionAttributeValues: {
          ":zero": 0,
          ":inc": 1,
        },
      })
    );
  } catch (error) {
    console.warn("Erreur incrément:", error.message);
  }

  // 4. Enregistrement événement (non bloquant)
  const clickEvent = {
    eventId: randomUUID(),
    shortKey,
    clickedAt: Date.now(),
    userAgent: event.headers?.["User-Agent"] || "unknown",
    ipAddress: event.requestContext?.identity?.sourceIp || "unknown",
  };

  try {
    await dynamoDb.send(
      new PutCommand({
        TableName: TABLE_CLICK_EVENTS,
        Item: clickEvent,
      })
    );
  } catch (error) {
    console.warn("Erreur event:", error.message);
  }

  // 5. Redirection 302
  return {
    statusCode: 302,
    headers: {
      Location: urlItem.longUrl,
      "Cache-Control": "no-cache",
      "Content-Type": "text/plain", // Requis pour SAM local
    },
    body: "",
  };
};
