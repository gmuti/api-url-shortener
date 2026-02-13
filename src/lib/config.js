/**
 * Configuration centralisée de l'application.
 */

const isSamLocal = process.env.AWS_SAM_LOCAL === "true";
const isProduction = process.env.NODE_ENV === "production";
const isLocal = isSamLocal || !isProduction;

export const Config = {
  isLocal,
  region: process.env.AWS_REGION || "eu-west-1",
  
  s3: {
    endpoint: process.env.S3_ENDPOINT || (isLocal ? "http://localhost:9000" : undefined),
    bucketName: process.env.FAVICONS_BUCKET || (isLocal ? "favicons" : null),
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || "minioadmin",
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "minioadmin",
    },
  },

  dynamoDB: {
    endpoint: process.env.DYNAMODB_ENDPOINT || (isSamLocal ? "http://url-shortener-dynamodb:8000" : undefined),
    hostEndpoint: "http://localhost:8000", // Pour scripts hors conteneur
    tables: {
      urls: process.env.TABLE_URLS || "urls",
      clickEvents: process.env.TABLE_CLICK_EVENTS || "click_events",
      dailyStats: process.env.TABLE_DAILY_STATS || "daily_stats",
    },
    credentials: {
      accessKeyId: "test",
      secretAccessKey: "test",
    },
  },
};
