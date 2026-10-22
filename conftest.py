"""Root pytest config: load local secrets from .env so integration tests can find API keys."""
from dotenv import load_dotenv

# Don't override variables that are already set in the real environment
load_dotenv(override=False)
