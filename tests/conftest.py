import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SERVICE_ROOT = ROOT / "services" / "user_service"

sys.path.insert(0, str(SERVICE_ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://user:password@localhost:5432/user_service")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
