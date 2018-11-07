#!/usr/bin/python3
# coding: utf8

import json
import os
import array
import struct

from io import BytesIO
from typing import Any, List, Mapping, cast

from aiokafka.producer.producer import DefaultPartitioner
from faust.serializers import codecs
from faust import App as _App
from faust import Record

from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro import loads as loads_avro
from fastavro import schemaless_reader, parse_schema, schemaless_writer


MAGIC_BYTE = 0


dir_path = os.path.dirname(os.path.realpath(__file__))
sx_key_schema_file = os.path.join(dir_path, "key_schema.avro")
with open(sx_key_schema_file) as f:
    sx_key_schema_raw = f.read()
    sx_key_schema = parse_schema(json.loads(sx_key_schema_raw))


class AvroRecord(Record, abstract=True, serializer='raw'):
    """Base Class for Avro Records in Faust.

    Set __is_key__ to True and be very careful when using this as a Key Schema.

    """

    __avro_schema__ = None
    __topic__ = None
    __is_key__ = False
    __no_registry__ = False

    def to_representation(self) -> Mapping[str, Any]:
        payload = self.asdict()
        if not self.__avro_schema__:
            raise ValueError("AvroRecords needs to define __avro_schema__")
        if not self.__topic__ and not self.__no_registry__:
            raise ValueError(
                "AvroRecords needs to define __topic__ or __no_registry__")

        if self.__no_registry__:
            # TODO: Warning
            value_schema = json.loads(self.__avro_schema__)
            return AvroParser._write_avro(payload, value_schema, 1)

        value_schema = loads_avro(self.__avro_schema__)
        return AvroParser.encode(self.__topic__, value_schema, payload, is_key=self.__is_key__)

    @classmethod
    def from_data(cls, data: Mapping, *,
                  preferred_type: Any = None) -> 'AvroRecord':
        if not cls.__avro_schema__:
            raise ValueError("AvroRecords needs to define __avro_schema__")
        if not cls.__topic__ and not cls.__no_registry__:
            raise ValueError(
                "AvroRecords needs to define __topic__ or __no_registry__")

        if cls.__no_registry__:
            value_schema = json.loads(cls.__avro_schema__)
            d = AvroParser._parse_avro(data, schema=value_schema)
        else:
            d = AvroParser.decode(data)
        return cls(**d, __strict__=False)


class AvroParser(object):
    schema_registry_url = None
    _serializer = None
    _registry = None

    @classmethod
    def serializer(cls):
        if cls._serializer is None:
            cls._serializer = MessageSerializer(cls.registry())
        return cls._serializer

    @classmethod
    def registry(cls):
        if cls._registry is None:
            if not cls.schema_registry_url:
                raise RuntimeError("no schema registry set")
            cls._registry = CachedSchemaRegistryClient(
                url=cls.schema_registry_url)
        return cls._registry

    @classmethod
    def set_registry_url(cls, url):
        cls.schema_registry_url = url

    @classmethod
    def encode(cls, topic, value_schema, value, is_key=False):
        return cls.serializer().encode_record_with_schema(topic, value_schema, value, is_key=is_key)

    @classmethod
    def encode_with_schema_id(cls, schema_id, record, is_key=False):
        return cls.serializer().encode_record_with_schema_id(schema_id, record, is_key=is_key)

    @classmethod
    def decode(cls, message):
        return cls.serializer().decode_message(message)

    @classmethod
    def _parse_avro(cls, b, schema):
        payload = BytesIO(b)
        magic, schema_id = struct.unpack('>bI', payload.read(5))
        if magic != MAGIC_BYTE:
            raise ValueError("message does not start with magic byte")
        return schemaless_reader(payload, schema)

    @classmethod
    def _write_avro(cls, d, schema, schema_id=1):
        payload = BytesIO()
        payload.write(struct.pack('b', MAGIC_BYTE))
        payload.write(struct.pack('>I', schema_id))
        schemaless_writer(payload, schema, d)
        return payload.getvalue()


def monkeypatch_method(cls):
    def decorator(func):
        setattr(cls, func.__name__, func)
        return func
    return decorator


def smaxtec_partitioner(key: bytes, all_partitions: List[int],
                        available_partitions: List[int]) -> int:
    if len(key) < 1:
        raise ValueError(
            "key to small to parition ... must be at least one byte")

    partitions = len(all_partitions)
    last_byte_int = int.from_bytes(key[-1:], byteorder='big')
    return (last_byte_int % partitions)
    # Fallback Partitioner
    return DefaultPartitioner()(key, all_partitions, available_partitions)


class AvroKeyModel(AvroRecord):
    __avro_schema__ = sx_key_schema_raw
    __no_registry__ = True
    __is_key__ = True

    id: str
    organisation_id: str


class KeyModel(Record, serializer='sx_key'):
    id: str
    organisation_id: str

    def to_avro_key_model(self, topic_name=None):
        avro_key_model = AvroKeyModel(
            id=self.id, organisation_id=self.organisation_id)
        if topic_name:
            avro_key_model.__no_registry__ = False
            avro_key_model.__topic__ = topic_name
        return avro_key_model


class sx_avro_key(codecs.Codec):
    def _dumps(self, obj: Any) -> bytes:
        return AvroParser._write_avro(obj, sx_key_schema)

    def _loads(self, s: bytes) -> Any:
        return AvroParser._parse_avro(s, sx_key_schema)


class sx_data_raw(codecs.Codec):
    def _dumps(self, obj: Any) -> bytes:
        payload = BytesIO()
        size = len(obj["_timestamps"])
        payload.write(struct.pack('i', size))
        # payload.write(struct.pack('{}I'.format(size), *obj["_timestamps"]))
        # payload.write(struct.pack('{}i'.format(size), *obj["_timestamp_offsets"]))
        # payload.write(struct.pack('{}f'.format(size), *obj["_values"]))
        payload.write(obj["_timestamps"].tobytes())
        payload.write(obj["_timestamp_offsets"].tobytes())
        payload.write(obj["_values"].tobytes())
        return payload.getvalue()

    def _loads(self, s: bytes) -> Any:
        if len(s) < 4:
            return {}
        payload = BytesIO(s)
        size = struct.unpack("i", payload.read(4))[0]
        d = {}
        d["_timestamps"] = array.array("I", payload.read(size*4))
        d["_timestamp_offsets"] = array.array("i", payload.read(size*4))
        d["_values"] = array.array("f", payload.read(size*4))
        # d["_timestamps"] = list(struct.unpack('{}I'.format(size), payload.read(size*4)))
        # d["_timestamp_offsets"] = list(struct.unpack('{}i'.format(size), payload.read(size*4)))
        # d["_values"] = list(struct.unpack('{}f'.format(size), payload.read(size*4)))
        return d


codecs.register("sx_key", sx_avro_key())
codecs.register("sx_data_raw", sx_data_raw())


class App(_App):
    def __init__(self, *args, **kwargs):
        schema_registry_url = kwargs.pop('schema_registry_url', None)
        if schema_registry_url is not None:
            AvroParser.set_registry_url(schema_registry_url)
        if "key_serializer" not in kwargs:
            kwargs["key_serializer"] = "raw"
        if "value_serializer" not in kwargs:
            kwargs["value_serializer"] = "raw"
        kwargs["producer_partitioner"] = smaxtec_partitioner
        super().__init__(*args, **kwargs)
