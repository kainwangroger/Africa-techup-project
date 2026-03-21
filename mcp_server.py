from fastmcp import FastMCP
import os

mcp = FastMCP("airflow-controller")

# Dossier par défaut (à adapter si besoin)
DAGS_FOLDER = "./dags"

@mcp.tool()
def list_dags(folder: str = DAGS_FOLDER):
    """Liste tous les DAG Airflow dans le dossier spécifié"""
    if os.path.exists(folder):
        return os.listdir(folder)
    return f"Dossier {folder} introuvable"

@mcp.tool()
def read_dag(dag_path: str):
    """Lire le contenu d'un DAG via son chemin complet"""
    if os.path.exists(dag_path):
        with open(dag_path, "r", encoding="utf-8") as f:
            return f.read()
    return "Fichier introuvable"

if __name__ == "__main__":
    mcp.run()
