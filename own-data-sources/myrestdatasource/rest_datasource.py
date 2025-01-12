import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from typing import Iterator, List
from pyspark.sql.types import Row
from dataclasses import dataclass

# -----------------------------------------------------------------------------
# 1) Define a custom DataSource
# -----------------------------------------------------------------------------
class MyRestDataSource(DataSource):
    """
    A custom data source for reading (and optionally writing) data from a REST API.
    
    Example options:
    - endpoint: e.g. 'posts' => https://jsonplaceholder.typicode.com/posts
    - method: (for writes) e.g. 'POST', 'PUT'
    """

    @classmethod
    def name(cls):
        # The short name used in spark.read.format("myrestdatasource")
        return "myrestdatasource"

    def schema(self):
        """
        Return a schema string (or a StructType) that Spark can use.
        For this example, we assume a fixed schema for JSONPlaceholder 'posts':
          userId (int), id (int), title (string), body (string)
        """
        return "userId int, id int, title string, body string"

    def reader(self, schema: StructType):
        """
        Create and return a DataSourceReader for batch reads.
        """
        return MyRestDataSourceReader(schema, self.options)

    def writer(self, schema: StructType, overwrite: bool):
        """
        Create and return a DataSourceWriter for batch writes (if needed).
        """
        return MyRestDataSourceWriter(self.options, overwrite)


# -----------------------------------------------------------------------------
# 2) Define a DataSourceReader to handle reads
# -----------------------------------------------------------------------------
class MyRestDataSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        # options is a dictionary of strings
        self.options = options

    def read(self, partition):
        """
        Called on each partition to return an iterator of rows.
        For simplicity, this example does NOT implement multiple partitions.
        """
        base_url = "https://jsonplaceholder.typicode.com"
        endpoint = self.options.get("endpoint", "posts")  # default to 'posts'
        url = f"{base_url}/{endpoint}"

        # Make a GET request
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()

        # data is a list of dicts (JSONPlaceholder format).
        # We yield tuples matching the schema [userId, id, title, body].
        for item in data:
            yield (
                item.get("userId"),
                item.get("id"),
                item.get("title"),
                item.get("body"),
            )

    def partitions(self):
        """
        If you want multiple partitions, you would define them here.
        For now, we'll return a single partition.
        """
        from pyspark.sql.datasource import InputPartition
        return [InputPartition(0)]


# -----------------------------------------------------------------------------
# 3) (Optional) Define a DataSourceWriter to handle writes
# -----------------------------------------------------------------------------
@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    partition_id: int
    count: int

class MyRestDataSourceWriter(DataSourceWriter):
    """
    This is a minimal example of writing to a REST API.
    JSONPlaceholder won't actually persist the data, but let's illustrate.
    """

    def __init__(self, options: dict, overwrite: bool):
        self.options = options
        self.overwrite = overwrite

    def write(self, rows: Iterator[Row]) -> WriterCommitMessage:
        """
        Called on each partition to write data. 
        Return a commit message for the partition.
        """
        from pyspark import TaskContext
        context = TaskContext.get()
        partition_id = context.partitionId()

        base_url = "https://jsonplaceholder.typicode.com"
        endpoint = self.options.get("endpoint", "posts")
        url = f"{base_url}/{endpoint}"
        method = self.options.get("method", "POST").upper()

        count = 0
        for row in rows:
            count += 1
            # Convert row to a dict for JSON
            payload = row.asDict()
            if method == "POST":
                resp = requests.post(url, json=payload)
            elif method == "PUT":
                resp = requests.put(url, json=payload)
            else:
                raise NotImplementedError(f"Method {method} not supported.")
            if not resp.ok:
                raise RuntimeError(
                    f"Failed to write row {payload}, status: {resp.status_code}"
                )

        return SimpleCommitMessage(partition_id=partition_id, count=count)

    def commit(self, messages: List[SimpleCommitMessage]) -> None:
        total_count = sum(m.count for m in messages)
        print(f"SUCCESS: Wrote {total_count} rows to REST API.")

    def abort(self, messages: List[SimpleCommitMessage]) -> None:
        print(f"ABORT: Some tasks failed to write to the REST API.")
