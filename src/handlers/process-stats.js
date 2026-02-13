/**
 * Lambda : process-stats
 * ------------------------
 * Déclenchée par INSERT dans la table click_events.
 * Met à jour les statistiques journalières dans daily_stats.
 */

import { UpdateCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { initDynamo } from "../lib/dynamodb-client.js";
import { Config } from "../lib/config.js";

const TABLE_CLICK_EVENTS = Config.dynamoDB.tables.clickEvents;
const TABLE_DAILY_STATS = Config.dynamoDB.tables.dailyStats;

/**
 * Convertit un timestamp en date YYYY-MM-DD.
 * @param {number} timestamp - Le timestamp à convertir.
 * @returns {string} La date formatée.
 */
function getStatDate(timestamp) {
  const date = new Date(Number(timestamp));
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

/**
 * Handler Lambda pour le traitement des statistiques.
 * @param {object} event - L'événement DynamoDB Stream.
 */
export const handler = async (event) => {
  console.log("[PROCESS-STATS] start");

  const dynamoDb = await initDynamo();

  let records = [];

  // 1. Mode local : scan de la table click_events si l'événement est vide
  if (Config.isLocal && (!event?.Records || event.Records.length === 0)) {
    console.log("[PROCESS-STATS] Local mode → scan click_events");

    const scanResult = await dynamoDb.send(
      new ScanCommand({ TableName: TABLE_CLICK_EVENTS })
    );

    records = (scanResult.Items || []).map((item) => ({
      shortKey: item.shortKey,
      clickedAt: item.clickedAt,
    }));
  }
  // 2. Mode production : utilisation des records du stream
  else {
    records = (event?.Records || [])
      .filter((r) => r.eventName === "INSERT")
      .map((r) => ({
        shortKey: r.dynamodb.NewImage.shortKey.S,
        clickedAt: Number(r.dynamodb.NewImage.clickedAt.N),
      }));
  }

  if (records.length === 0) {
    console.log("[PROCESS-STATS] No records to process");
    return;
  }

  console.log(`[PROCESS-STATS] Processing ${records.length} records`);

  // 3. Mettre à jour les statistiques journalières
  const updates = records.map(async ({ shortKey, clickedAt }) => {
    const statDate = getStatDate(clickedAt);

    await dynamoDb.send(
      new UpdateCommand({
        TableName: TABLE_DAILY_STATS,
        Key: { shortKey, statDate },
        UpdateExpression:
          "SET totalClicks = if_not_exists(totalClicks, :zero) + :inc, updatedAt = :now",
        ExpressionAttributeValues: {
          ":zero": 0,
          ":inc": 1,
          ":now": Date.now(),
        },
      })
    );
  });

  await Promise.all(updates);

  console.log("[PROCESS-STATS] Done ✅");
};
