import os
from dataclasses import dataclass
from google.cloud import bigquery


@dataclass
class Table:
    """
    structure: project_id.dataset_id.table_id
    """

    table_id: str
    dataset_id: str
    project_id: str = os.getenv("GCP_PROJECT_ID") or bigquery.Client().project

    def __str__(self):
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"
