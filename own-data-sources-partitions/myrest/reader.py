import requests
from pyspark.sql.datasource import DataSourceReader, InputPartition
from pyspark.sql.types import StructType
from pyspark import SparkContext
import math


class MyRestDataSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        
    def partitions(self):
        # Suppose the total range we want is 1..100
        start_id = int(self.options.get("startId", 1))
        end_id   = int(self.options.get("endId", 100))
        total_records = end_id - start_id + 1

        # Use the Spark default parallelism
        sc = SparkContext.getOrCreate()
        num_partitions = sc.defaultParallelism

        # Compute the size of each partition
        chunk_size = math.ceil(total_records / num_partitions)

        partitions = []
        current_start = start_id
        for i in range(num_partitions):
            current_end = min(current_start + chunk_size - 1, end_id)
            if current_start <= current_end:
                partitions.append(InputPartition({
                    "postIdRange": (current_start, current_end)
                }))
            current_start = current_end + 1
            if current_start > end_id:
                break

        return partitions

    def read(self, partition):
        """
        Called on each partition to fetch data from the REST API.
        We use the partition's value to filter or pick a subset of data.
        """
        # Retrieve partition info
        part_value = partition.value  # e.g. {'userIdRange': (1, 2)}
        (start_id, end_id) = part_value['postIdRange']

        url = "https://jsonplaceholder.typicode.com/posts/{id}"

        for post_id in range(start_id, end_id + 1):
            resp = requests.get(url.format(id=post_id))
            resp.raise_for_status()
            post = resp.json()
            yield (
                post.get("userId"),
                post.get("id"),
                post.get("title"),
                post.get("body")
            )
