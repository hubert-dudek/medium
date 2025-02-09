from pyspark.sql.datasource import DataSource
from pyspark.sql.types import StructType

from .reader import MyRestDataSourceReader
from .writer import MyRestDataSourceWriter

class MyRest(DataSource):

    @classmethod
    def name(cls):
        return "myrest"

    def schema(self):

        return "userId int, id int, title string, body string"

    def reader(self, schema: StructType):

        return MyRestDataSourceReader(schema, self.options)
    
    def writer(self, schema: StructType, overwrite: bool):
        """
        Create and return a DataSourceWriter for batch writes (if needed).
        """
        return MyRestDataSourceWriter(self.options, overwrite)
