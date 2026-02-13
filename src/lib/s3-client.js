import {
  S3Client,
  HeadBucketCommand,
  CreateBucketCommand,
} from "@aws-sdk/client-s3";
import { Config } from "./config.js";

// Client S3 (AWS ou MinIO)
export const s3Client = new S3Client({
  region: Config.region,
  endpoint: Config.s3.endpoint,
  forcePathStyle: true, // Requis pour MinIO
  credentials: Config.s3.credentials,
});

/**
 * Vérifie ou crée le bucket S3.
 */
export async function ensureBucketExists() {
  const bucketName = Config.s3.bucketName;

  if (!bucketName) {
    throw new Error("Nom du bucket non défini !");
  }

  try {
    await s3Client.send(new HeadBucketCommand({ Bucket: bucketName }));
    console.log(`[S3] Bucket "${bucketName}" existant.`);
  } catch (err) {
    const statusCode = err.$metadata?.httpStatusCode;

    if (err.name === "NotFound" || statusCode === 404) {
      console.log(`[S3] Création du bucket "${bucketName}"...`);

      const createBucketParams = {
        Bucket: bucketName,
        // MinIO ne supporte pas LocationConstraint
        ...(Config.s3.endpoint
          ? {}
          : { CreateBucketConfiguration: { LocationConstraint: Config.region } }),
      };

      await s3Client.send(new CreateBucketCommand(createBucketParams));
      console.log(`[S3] Bucket "${bucketName}" créé.`);
    } else {
      console.error(`[S3] Erreur bucket "${bucketName}":`, err);
      throw err;
    }
  }
}
