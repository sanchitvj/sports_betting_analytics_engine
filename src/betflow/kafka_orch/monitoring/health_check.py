from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

class DataSourceStatus(Enum):
    ACTIVE = "active"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"

@dataclass
class DataSourceHealth:
    status: DataSourceStatus
    last_success: Optional[datetime] = None
    failure_count: int = 0
    next_retry: Optional[datetime] = None