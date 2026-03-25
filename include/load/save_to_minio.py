import os
import pathlib
from typing import Dict
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv, find_dotenv

from include.utils.custom_logging import get_logger, setup_logging


# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# ENV
# -------------------------
env_path = find_dotenv(".env")

if env_path:
    logger.info(f"Fichier .env trouvé ici : {env_path}")
    load_dotenv(env_path)
else:
    logger.warning("Fichier .env introuvable. Utilisation des variables d'environnement système.")

load_dotenv()


# -------------------------
# CONFIG
# -------------------------
def load_minio_config(from_docker: bool = True) -> Dict[str, str]:
    endpoint = os.getenv(
        "MINIO_ENDPOINT_DOCKER" if from_docker else "MINIO_ENDPOINT_HOST"
    )

    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

    config = {
        "endpoint": endpoint,
        "access_key": os.getenv("MINIO_ROOT_USER"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
        "bucket": os.getenv("MINIO_BUCKET"),
        "secure": secure,
    }

    if not all([config["endpoint"], config["access_key"],
                config["secret_key"], config["bucket"]]):
        raise ValueError("Variables MinIO manquantes dans le fichier .env")

    return config


# -------------------------
# CLIENT
# -------------------------
def create_minio_client(config: Dict[str, str]) -> Minio:
    return Minio(
        config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=config["secure"],
    )


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
# PATHS
# -------------------------
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

RAW_FOLDER = PROJECT_ROOT / "data/raw"
PROCESSED_FOLDER = PROJECT_ROOT / "data/processed"

# On s'assure que les dossiers existent
# On s'assurera que les dossiers existent au moment du besoin


# -------------------------
# UPLOAD SINGLE FILE
# -------------------------
def upload_file(client: Minio, bucket: str,
                object_name: str, local_path: pathlib.Path) -> bool:

    try:
        client.fput_object(bucket, object_name, str(local_path))
        logger.info(f"Upload réussi : {object_name}")
        return True

    except S3Error as e:
        logger.error(f"Erreur upload {object_name} : {e}")
        return False


# -------------------------
# UPLOAD FOLDER (RECURSIVE)
# -------------------------
def upload_folder(client: Minio, bucket: str,
                  folder: pathlib.Path, layer: str) -> None:

    if not folder.exists():
        logger.warning(f"Dossier introuvable : {folder}")
        return

    # Utilisation de rglob("*") pour parcourir récursivement les dossiers (important pour Delta Lake)
    files = [f for f in folder.rglob("*") if f.is_file()]

    if not files:
        logger.warning(f"Aucun fichier à uploader dans {folder}")
        return

    success_count = 0

    for file_path in files:
        # Calculer l'object_name relatif au dossier source pour conserver la structure
        relative_path = file_path.relative_to(folder)
        object_name = f"{layer}/{relative_path.as_posix()}"

        if upload_file(client, bucket, object_name, file_path):
            success_count += 1

    logger.info(f"{success_count} fichiers uploadés depuis {folder} vers {layer}")


# -------------------------
# BACKWARD COMPATIBILITY
# -------------------------
def upload_raw_files(client: Minio, bucket: str) -> None:
    upload_folder(client, bucket, RAW_FOLDER, "bronze")


def upload_processed_files(client: Minio, bucket: str) -> None:
    upload_folder(client, bucket, PROCESSED_FOLDER, "silver")


# -------------------------
# MAIN
# -------------------------
def main():
    logger.info("Début upload MinIO")

    try:
        config = load_minio_config(from_docker=False)
        client = create_minio_client(config)

        ensure_bucket_exists(client, config["bucket"])

        # RAW -> bronze
        upload_folder(client, config["bucket"], RAW_FOLDER, "bronze")

        # PROCESSED & SILVER -> silver
        upload_folder(client, config["bucket"], PROCESSED_FOLDER, "silver")
        SILVER_DIR = PROJECT_ROOT / "data/silver"
        if SILVER_DIR.exists():
            upload_folder(client, config["bucket"], SILVER_DIR, "silver")

        # GOLD -> gold
        GOLD_DIR = PROJECT_ROOT / "data/gold"
        if GOLD_DIR.exists():
            upload_folder(client, config["bucket"], GOLD_DIR, "gold")

    except Exception as e:
        logger.error(f"Erreur critique MinIO : {e}")
        print("❌ Erreur lors de l'upload MinIO")
        return

    logger.info("Upload MinIO terminé avec succès")
    print("✅ Tous les fichiers ont été sauvegardés dans MinIO")


if __name__ == "__main__":
    main()