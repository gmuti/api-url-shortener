/**
 * Lambda : GET /urls
 * Liste les URLs et leurs stats.
 */

import { ScanCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { initDynamo } from "../lib/dynamodb-client.js";
import { Config } from "../lib/config.js";

const TABLE_URLS = Config.dynamoDB.tables.urls;
const TABLE_CLICK_EVENTS = Config.dynamoDB.tables.clickEvents;

export const handler = async (event) => {
  const dynamoDb = await initDynamo();

  try {
    // 1. Récupération URLs
    const urlsResult = await dynamoDb.send(
      new ScanCommand({ TableName: TABLE_URLS })
    );

    const urls = urlsResult.Items || [];

    // 2. Enrichissement avec stats (parallélisé)
    const urlsWithStats = await Promise.all(
      urls.map(async (url) => {
        let totalClicks = url.clickCount || 0;

        try {
          const clicksResult = await dynamoDb.send(
            new QueryCommand({
              TableName: TABLE_CLICK_EVENTS,
              KeyConditionExpression: "shortKey = :sk",
              ExpressionAttributeValues: { ":sk": url.shortKey },
            })
          );
          totalClicks = clicksResult.Count || totalClicks;
        } catch (err) {
          console.warn(`Erreur stats ${url.shortKey}:`, err.message);
        }

        return {
          shortKey: url.shortKey,
          longUrl: url.longUrl,
          totalClicks,
          favicon: url.faviconPath || null,
        };
      })
    );

    // 3. Réponse JSON
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(urlsWithStats),
    };
  } catch (error) {
    console.error("Erreur GET /urls:", error);
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
