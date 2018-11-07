#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import time
import mock
import json
import pytest

from sxfaust import AvroRecord, AvroParser, KeyModel, AvroKeyModel



class MyCustomAvroNoRegistry(AvroRecord):
    __avro_schema__ = """
        {
            "namespace": "com.smaxtec.my.avro.model",
            "type": "record",
            "name": "Id",
            "fields": [
                {"name": "myint", "type": "int"},
                {"name": "mystr", "type": "string"}
            ]
        }
        """
    __no_registry__ = True

    myint: int
    mystr: str


class MyCustomAvroNoRegistry(AvroRecord):
    __avro_schema__ = """
        {
            "namespace": "com.smaxtec.my.avro.model",
            "type": "record",
            "name": "Id",
            "fields": [
                {"name": "myint", "type": "int"},
                {"name": "mystr", "type": "string"}
            ]
        }
        """
    __no_registry__ = True

    myint: int
    mystr: str


class MyCustomAvro(AvroRecord):
    __avro_schema__ = """
        {
            "namespace": "com.smaxtec.my.avro.model",
            "type": "record",
            "name": "Id",
            "fields": [
                {"name": "myint", "type": "int"},
                {"name": "mystr", "type": "string"}
            ]
        }
        """
    __topic__ = "my_topic"

    myint: int
    mystr: str


class ModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_data_schema(self):
        m1 = KeyModel(id="animalid", organisation_id="organisation_id")
        b = m1.dumps()
        my_string1 = b[-15:].decode("ascii")
        my_string2 = b[6:14].decode("ascii")
        self.assertEqual(my_string1, "organisation_id")
        self.assertEqual(my_string2, "animalid")
        m2 = KeyModel.loads(b)
        self.assertEqual(m2.id, "animalid")
        self.assertEqual(m2.organisation_id, "organisation_id")
        self.assertEqual(m1, m2)
        m3 = m2.to_avro_key_model()
        self.assertTrue(m3)
        b2 = m3.dumps()
        self.assertEqual(b, b2)


    def test_custom_avro(self):
        expected_bytes = b'\x00\x00\x00\x00\x01\x0e\x0c\xc3\xb6\xc3\xa4\xc3\xbc'

        m1 = MyCustomAvroNoRegistry(myint=7, mystr="öäü")
        b1 = m1.dumps()
        assert b1 == expected_bytes

        m2 = MyCustomAvro(myint=7, mystr="öäü")
        with mock.patch('sxfaust.AvroParser.encode', return_value=b1) as mocked_encode:
            b = m2.dumps()
        assert b == expected_bytes

        with mock.patch('sxfaust.AvroParser.decode', return_value=dict(myint=7, mystr="öäü")) as mocked_encode:
            m3 = MyCustomAvro.loads(b)
        
        self.assertEqual(m3.myint, 7)
        self.assertEqual(m3.mystr, "öäü")
        self.assertEqual(m2, m3)

    @pytest.mark.skip(reason="only testable with schema registry")
    def test_avro_registry(self):
        AvroParser.set_registry_url("http://localhost:8081/")
        m1 = MyCustomAvro(myint=7, mystr="öäü")
        b = m1.dumps()
        m2 = MyCustomAvro.loads(b)
        assert m1 == m2

    def test_schema_evolution(self):
        evolution = """
        {
            "namespace": "com.smaxtec.my.avro.model",
            "type": "record",
            "name": "Id",
            "fields": [
                {"name": "myint", "type": "int"},
                {"name": "mystr", "type": "string"},
                {"name": "mystr2", "type": "string"}
            ]
        }
        """

        new_schema = json.loads(evolution)
        d = {"myint": 12, "mystr": "test", "mystr2": "test2"}
        new_bytes = AvroParser._write_avro(d, new_schema)

        with self.assertRaises(TypeError):
            t = MyCustomAvroNoRegistry(**{"mystr": "test", "myint": 2, "foo": "bar"})
        t = MyCustomAvroNoRegistry(**{"mystr": "test", "myint": 2, "foo": "bar"}, __strict__=False)
        assert t

        import struct
        from io import BytesIO
        from fastavro import schemaless_reader
        def my_parser(b, schema):
            payload = BytesIO(b)
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            return schemaless_reader(payload, new_schema)

        with mock.patch('sxfaust.AvroParser._parse_avro', side_effect=my_parser) as mocked_encode:
            di = AvroParser._parse_avro(new_bytes, "somthing")
            assert "myint" in di
            assert "mystr" in di
            assert "mystr2" in di

            m1 = MyCustomAvroNoRegistry.loads(new_bytes)

            assert m1.mystr == "test"
            assert m1.myint == 12