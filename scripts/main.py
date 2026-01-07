from scripts.scrap_links import save_book_links
from scripts.etl_scrap_books import save_books_info_to_csv
from scripts.save_to_minio import upload_raw_files, create_minio_client, load_minio_config  # noqa 501
from spark.transform_books import transform_books
from utils.logging import setup_logging, get_logger
from pathlib import Path

setup_logging()
logger = get_logger(__name__)

BATCH_SIZE = 100
TOTAL_BOOKS = 1000

file_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "book_links.txt" # noqa 501
file_path.parent.mkdir(parents=True, exist_ok=True)  # crée data/raw si besoin


def main():
    logger.info("🚀 Démarrage du pipeline ETL")

    # 1️⃣ Extract - liens
    logger.info("📌 Extraction des liens")
    save_book_links(file_path)

    # 2️⃣ Extract - détails (batch)
    logger.info("📚 Extraction des détails par batch")

    for i in range(0, TOTAL_BOOKS, BATCH_SIZE):
        batch_id = i // BATCH_SIZE + 1
        logger.info(f"➡️ Batch {batch_id} : liens {i} à {i + BATCH_SIZE}")

        save_books_info_to_csv(
            file_path,
            start_index=i,
            end_index=i + BATCH_SIZE,
            output_suffix=f"batch{batch_id}"
        )

    # 3️⃣ Transform (PySpark)
    logger.info("🧹 Transformation PySpark")
    transform_books()

    # # 4️⃣ Load MinIO
    # logger.info("☁️ Upload vers MinIO")
    # config = load_minio_config()
    # client = create_minio_client(config)
    # upload_raw_files(client, config["bucket"])

    # logger.info("✅ Pipeline ETL terminé avec succès")


if __name__ == "__main__":
    main()
