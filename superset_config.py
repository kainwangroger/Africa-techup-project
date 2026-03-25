import os

# Read the database URI from the environment explicitly
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI") or os.getenv("SUPERSET_SQLALCHEMY_DATABASE_URI")

# Ensure the secret key is read
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY")

# Read the load examples setting
SUPERSET_LOAD_EXAMPLES = os.getenv("SUPERSET_LOAD_EXAMPLES", "no") == "yes"
