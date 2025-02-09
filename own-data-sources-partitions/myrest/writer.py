# myrest/writer.py
import requests
from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from dataclasses import dataclass
from typing import List

@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    partition_id: int
    count: int

class MyRestDataSourceWriter(DataSourceWriter):
    def __init__(self, options, overwrite):
        self.options = options
        self.overwrite = overwrite

    def write(self, rows):
        from pyspark import TaskContext
        partition_id = TaskContext.get().partitionId()

        base_url = "https://jsonplaceholder.typicode.com"
        endpoint = self.options.get("endpoint", "posts")
        url = f"{base_url}/{endpoint}"

        count = 0
        for row in rows:
            count += 1
            payload = row.asDict() if hasattr(row, 'asDict') else dict(row)
            response = requests.post(url, json=payload)
            if not response.ok:
                raise RuntimeError(f"Error posting row: {payload}")

        return SimpleCommitMessage(partition_id, count)

    def commit(self, messages: List[SimpleCommitMessage]):
        total = sum(m.count for m in messages)
        print(f"Successfully wrote {total} records.")

    def abort(self, messages: List[SimpleCommitMessage]):
        print("Write aborted.")
