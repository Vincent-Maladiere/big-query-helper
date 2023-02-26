import pandas as pd
from google.cloud import bigquery


# https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.type
schema_mapping = {
    "str": "STRING",
    "bytes": "BYTES",
    "int": "INTEGER",  # (or INT64)
    "float": "FLOAT",  # (or FLOAT64)
    "bool": "BOOL",  # BOOLEAN
    "datetime": "DATETIME",
    "object": "RECORD",
}


class Data:
    def __init__(self, data: pd.DataFrame, primary_keys: list[str]) -> None:
        if not isinstance(data, pd.DataFrame):
            raise ValueError(
                f" # [Data] data type must be pd.DataFrame, got {type(data)} instead"
            )
        self.data = data
        self.primary_keys = primary_keys
        self.schemas = self.infer_schema()

    def infer_schema(self):
        schema = []
        for col in self.data.columns:
            col_type = str(self.data[col].dtype)
            # remove numbers from col type
            col_type = "".join(c for c in col_type if not c.isnumeric())
            # if no schema is map, default is "STRING"
            schema_type = schema_mapping.get(col_type, "STRING")
            mode = "REQUIRED" if col in self.primary_keys else "NULLABLE"
            schema_field = bigquery.SchemaField(
                col,
                schema_type,
                mode=mode,
            )
            schema.append(schema_field)
        return schema
