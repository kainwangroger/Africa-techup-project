from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv
from typing import Dict
import glob
from utils.logging import setup_logging, get_logger


# --------------------------------------------------
# LOGGING
# --------------------------------------------------
# def setup_logging(log_file: str = "app.log") -> None:
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s",
#         filename=log_file,
#         filemode="a"
#     )
#     logger = logging.getLogger("save_to_minio.py")

#     return logger


# logger = setup_logging()


setup_logging()
logger = get_logger(__name__)

logger.info("Sauvegarde démarré")


# --------------------------------------------------
# ENV
# --------------------------------------------------
def load_minio_config() -> Dict[str, str]:
    """Charge les variables d'environnement MinIO"""
    load_dotenv()

    config = {
        "endpoint": os.getenv("MINIO_ENDPOINT"),
        "access_key": os.getenv("MINIO_ACCESS_KEY"),
        "secret_key": os.getenv("MINIO_SECRET_KEY"),
        "bucket": os.getenv("MINIO_BUCKET"),
    }

    if not all(config.values()):
        raise ValueError("Variables MinIO manquantes dans le fichier .env")

    return config


# --------------------------------------------------
# CLIENT
# --------------------------------------------------
def create_minio_client(config: Dict[str, str]) -> Minio:
    """Crée et retourne le client MinIO"""
    return Minio(
        config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=False
    )


# --------------------------------------------------
# BUCKET
# --------------------------------------------------
def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """Crée le bucket s'il n'existe pas"""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Bucket créé : {bucket_name}")
    else:
        logger.info(f"Bucket existant : {bucket_name}")


# --------------------------------------------------
# UPLOAD
# --------------------------------------------------
def upload_file(
    client: Minio,
    bucket: str,
    object_name: str,
    local_path: str
) -> None:
    """Upload un fichier vers MinIO"""
    if not os.path.exists(local_path):
        logger.error(f"Fichier introuvable : {local_path}")
        return

    try:
        client.fput_object(bucket, object_name, local_path)
        logger.info(f"Fichier uploadé : {object_name}")
        print(f"⬆️  Upload : {object_name}")
    except S3Error as e:
        logger.error(f"Erreur upload {object_name} : {e}")


# --------------------------------------------------
# PIPELINE
# --------------------------------------------------
def upload_raw_files(client: Minio, bucket: str) -> None:
    """
    Upload tous les fichiers RAW vers MinIO
    """

    # 1️⃣ book_links.txt
    upload_file(
        client,
        bucket,
        "fichiers/raw/book_links.txt",
        "data/raw/book_links.txt"
    )

    # 2️⃣ fichier unique
    upload_file(
        client,
        bucket,
        "fichiers/raw/books_raw_unique.csv",
        "data/raw/books_raw_unique.csv"
    )

    # 3️⃣ batches
    batch_files = glob.glob("data/raw/books_raw_batch*.csv")

    for file_path in batch_files:
        object_name = f"fichiers/raw/{os.path.basename(file_path)}"
        upload_file(client, bucket, object_name, file_path)


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("Début des sauvaegardes dans Minio")
    logger.info("Début upload MinIO")

    try:
        config = load_minio_config()
        client = create_minio_client(config)
        ensure_bucket_exists(client, config["bucket"])
        upload_raw_files(client, config["bucket"])
    except Exception as e:
        logger.error(f"Erreur critique MinIO : {e}")
        print("❌ Erreur lors de l'upload MinIO")
        return

    logger.info("Upload MinIO terminé avec succès")
    print("✅ Fichiers sauvegardés dans MinIO")


if __name__ == "__main__":
    main()
