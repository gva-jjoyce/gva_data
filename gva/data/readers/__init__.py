from .reader import Reader
from .internals.base_reader import BaseReader

# Implementations of the BaseReader for different resource types
from .file_reader import FileReader
from .mongodb_reader import MongoDbReader
from .minio_reader import MinioReader
from .gcs_reader import GoogleCloudStorageReader
