/**
 * Lambda : process-favicon
 * ----------------------
 * Déclenchée par INSERT dans la table urls.
 * Récupère le favicon du site et le stocke dans S3/Minio.
 * Met ensuite à jour la table urls avec le chemin du favicon.
 */

import fetch from "node-fetch";
import { PutObjectCommand } from "@aws-sdk/client-s3";
import { UpdateCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { initDynamo } from "../lib/dynamodb-client.js";
import { s3Client, ensureBucketExists } from "../lib/s3-client.js";
import { Config } from "../lib/config.js";

const TABLE_URLS = Config.dynamoDB.tables.urls;
const BUCKET_NAME = Config.s3.bucketName;

/**
 * Handler Lambda pour récupérer et stocker le favicon.
 * @param {object} event - L'événement DynamoDB Stream.
 */
export const handler = async (event) => {
  const dynamoDb = await initDynamo();
  await ensureBucketExists();

  // 1. En local, on force le scan de toutes les URLs si l'événement est vide
  let records = event?.Records || [];
  if (Config.isLocal && records.length === 0) {
    console.log("[PROCESS-FAVICON] Mode local : scan table URLs");
    const scanResult = await dynamoDb.send(
      new ScanCommand({ TableName: TABLE_URLS })
    );
    records = (scanResult.Items || []).map((item) => ({
      eventName: "INSERT",
      dynamodb: {
        NewImage: {
          shortKey: { S: item.shortKey },
          longUrl: { S: item.longUrl },
        },
      },
    }));
  }

  if (records.length === 0) {
    console.log("[PROCESS-FAVICON] Aucun record à traiter");
    return;
  }

  console.log(`[PROCESS-FAVICON] Traitement de ${records.length} records`);

  // 2. Traiter chaque record
  for (const record of records) {
    if (record.eventName !== "INSERT") continue;

    const newImage = record.dynamodb.NewImage;
    const shortKey = newImage.shortKey.S;
    const longUrl = newImage.longUrl.S;

    try {
      const faviconUrl = new URL("/favicon.ico", longUrl).href;
      const response = await fetch(faviconUrl);

      if (!response.ok) {
        console.warn(`[${shortKey}] Favicon introuvable (${faviconUrl})`);
        continue;
      }

      const buffer = await response.arrayBuffer();
      const s3Key = `favicons/${shortKey}.ico`;

      // 3. Upload dans S3/Minio
      await s3Client.send(
        new PutObjectCommand({
          Bucket: BUCKET_NAME,
          Key: s3Key,
          Body: Buffer.from(buffer),
          ContentType: "image/x-icon",
        })
      );

      // 4. Mise à jour de la table URLs
      await dynamoDb.send(
        new UpdateCommand({
          TableName: TABLE_URLS,
          Key: { shortKey },
          UpdateExpression: "SET faviconPath = :path",
          ExpressionAttributeValues: { ":path": s3Key },
        })
      );

      console.log(`[${shortKey}] Favicon récupéré et stocké ✅`);
    } catch (error) {
      console.warn(
        `[${shortKey}] Impossible de récupérer le favicon :`,
        error.message
      );
    }
  }
};
