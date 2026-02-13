import { ensureBucketExists } from "./s3-client.js";

/**
 * Initialise le bucket S3 au démarrage.
 */
async function main() {
  try {
    await ensureBucketExists();
    console.log("✅ S3 prêt.");
    process.exit(0);
  } catch (err) {
    console.error("❌ Erreur init S3:", err);
    process.exit(1);
  }
}

main();
