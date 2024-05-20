from dataclasses import dataclass, field
from typing import List

@dataclass
class Table:
    schema: str
    name: str
    columns: List[str] = field(default_factory=list)
    pk_index: int = 0
    
    def __post_init__(self):
        self.pk = self.columns[self.pk_index]


    