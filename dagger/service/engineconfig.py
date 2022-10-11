import typing
from enum import Enum
from typing import Any, NamedTuple

from faust.stores.aerospike import AeroSpikeStore  # type: ignore


class StoreEnum(Enum):
    AEROSPIKE = "aerospike://"
    ROCKSDB = "rocksdb://"


try:  # pragma: no cover
    import rocksdb  # type: ignore
except ImportError:  # pragma: no cover
    rocksdb = None  # noqa

if typing.TYPE_CHECKING:  # pragma: no cover
    from rocksdb import CompressionType
else:

    class CompressionType:  # noqa
        """Dummy CompressionType."""

        lz4_compression = "lz4_compression"


class EngineConfig(NamedTuple):
    """Engine Config NamedTuple"""

    BROKER: str
    DATADIR: typing.Optional[str]
    STORE: str
    APPLICATION_NAME: str
    PACKAGE_NAME: str
    KAFKA_PARTITIONS: int
    KWARGS: Any


class AerospikeConfig(NamedTuple):
    HOSTS: Any
    POLICIES: Any
    NAMESPACE: str
    USERNAME: str
    PASSWORD: str
    TTL: int
    KWARGS: Any

    def as_options(self):
        options = dict()
        options[AeroSpikeStore.HOSTS_KEY] = self.HOSTS
        options[AeroSpikeStore.POLICIES_KEY] = self.POLICIES
        options[AeroSpikeStore.TTL_KEY] = self.TTL
        options[AeroSpikeStore.USERNAME_KEY] = self.USERNAME
        options[AeroSpikeStore.PASSWORD_KEY] = self.PASSWORD
        options[AeroSpikeStore.NAMESPACE_KEY] = self.NAMESPACE
        options[AeroSpikeStore.CLIENT_OPTIONS_KEY] = {
            AeroSpikeStore.HOSTS_KEY: self.HOSTS,
            AeroSpikeStore.POLICIES_KEY: self.POLICIES,
        }
        if self.KWARGS:
            options[AeroSpikeStore.CLIENT_OPTIONS_KEY].update(**self.KWARGS)
        return options


ROCKS_DB_OPTIONS = {
    "write_buffer_size": 16 * 1024 * 1024,
    "max_write_buffer_number": 2,
    "block_cache_size": 64 * 1024 * 1024,
    "bloom_filter_size": 10,
    "max_open_files": 20000,
    "block_cache_compressed_size": 64 * 1024 * 1024,
    "compression": CompressionType.lz4_compression,
    "target_file_size_base": 67108864,
    "level0_file_num_compaction_trigger": 1,
    "max_bytes_for_level_base": 512 * 1024 * 1024,
}
