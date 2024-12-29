from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent


# Watcher-specific configuration
CRAWLER_CONFIG = {
    "Fields_FOLDER": BASE_DIR / "uploads"
}

# Ensure directories exist
for folder in CRAWLER_CONFIG.values():
    folder.mkdir(parents=True, exist_ok=True)