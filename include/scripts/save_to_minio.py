import pathlib
import os
# import glob
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
from typing import Dict

from include.utils.custom_logging import get_logger, setup_logging


# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# ENV
# -------------------------
load_dotenv()


def load_minio_config(from_docker: bool = True) -> Dict[str, str]:
    endpoint = os.getenv(
        "MINIO_ENDPOINT_DOCKER"
        if from_docker
        else "MINIO_ENDPOINT_HOST")
    config = {
        "endpoint": endpoint,
        "access_key": os.getenv("MINIO_ROOT_USER"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
        "bucket": os.getenv("MINIO_BUCKET"),
    }
    if not all(config.values()):
        raise ValueError("Variables MinIO manquantes dans le fichier .env")
    return config


# -------------------------
# CLIENT
# -------------------------
def create_minio_client(config: Dict[str, str]) -> Minio:
    return Minio(
        config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"], secure=False)


# -------------------------
# BUCKET
# -------------------------
def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Bucket créé : {bucket_name}")
    else:
        logger.info(f"Bucket existant : {bucket_name}")


# -------------------------
# CHEMINS
# -------------------------
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
PROJECT_ROOT.mkdir(parents=True, exist_ok=True)

RAW_FOLDER = PROJECT_ROOT / "data/raw"
PROCESSED_FOLDER = PROJECT_ROOT / "data/processed"


# -------------------------
# UPLOAD
# -------------------------
def upload_file(
    client: Minio,
    bucket: str,
    object_name: str,
    local_path: pathlib.Path
) -> bool:

    if not local_path.exists():
        msg = f"Fichier introuvable : {local_path}"
        logger.warning(msg)
        print(msg)
        return False
    try:
        client.fput_object(bucket, object_name, str(local_path))
        msg = f"Fichier uploadé : {object_name}"
        logger.info(msg)
        print(msg)
        return True
    except S3Error as e:
        msg = f"Erreur upload {object_name} : {e}"
        logger.error(msg)
        print(msg)
        return False


# -------------------------
# UPLOAD RAW FILES
# -------------------------
def upload_raw_files(client: Minio, bucket: str) -> None:
    files_to_upload = [
        "book_links.txt",
        "books_raw_unique.csv"
    ]

    success_count = 0
    for filename in files_to_upload:
        file_path = RAW_FOLDER / filename
        object_name = f"data/raw/{filename}"
        if upload_file(client, bucket, object_name, file_path):
            success_count += 1

    if success_count == 0:
        msg = "Aucun fichier n’a été uploadé vers MinIO (fichiers manquants ?)"
        logger.warning(msg)
        print(msg)


# -------------------------
# UPLOAD RAW BATCH FILES
# -------------------------
# def upload_raw_batch_files(client: Minio, bucket: str) -> None:
#     batch_files = RAW_FOLDER.glob("books_raw_batch*.csv")
#     success_count = 0
#     for file_path in batch_files:
#         object_name = f"data/raw/{file_path.name}"
#         if upload_file(client, bucket, object_name, file_path):
#             success_count += 1

#     if success_count == 0:
#         msg = "Aucun fichier batch n’a été uploadé vers MinIO (fichiers manquants ?)" # noqa 501
#         logger.warning(msg)
#         print(msg)


# -------------------------
# UPLOAD PROCESSED FILES

def upload_processed_files(client: Minio, bucket: str) -> None:
    files_to_upload = [
        "books_clean.csv"
    ]

    success_count = 0
    for filename in files_to_upload:
        file_path = PROCESSED_FOLDER / filename
        object_name = f"data/processed/{filename}"
        if upload_file(client, bucket, object_name, file_path):
            success_count += 1

    if success_count == 0:
        msg = "Aucun fichier traité n’a été uploadé vers MinIO (fichiers manquants ?)" # noqa 501
        logger.warning(msg)
        print(msg)


# -------------------------
# MAIN
# -------------------------
def main():
    print("Début des sauvegardes dans Minio")
    logger.info("Début upload MinIO")
    try:
        config = load_minio_config(from_docker=False)
        client = create_minio_client(config)
        ensure_bucket_exists(client, config["bucket"])
        upload_raw_files(client, config["bucket"])
        upload_processed_files(client, config["bucket"])
    except Exception as e:
        logger.error(f"Erreur critique MinIO : {e}")
        print("Erreur lors de l'upload MinIO")
        return

    logger.info("Upload MinIO terminé avec succès")
    print("✅ Fichiers sauvegardés dans MinIO")


if __name__ == "__main__":
    main()
