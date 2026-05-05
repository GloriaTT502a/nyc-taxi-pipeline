from dataclasses import dataclass, field
from typing import Dict

@dataclass
class DQResult:
    """
    Return object for data quality (Data Quality Result)
    """
    total_rows: int
    bad_rows: int = 0
    # record how many records for each rule
    bad_by_rule: Dict[str, int] = field(default_factory=dict)