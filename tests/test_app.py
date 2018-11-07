#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import pendulum
import mock

from sxfaust import App, Record


class MyKeyModel(Record, serializer='sx_key'):
    id: str
    organisation_id: str = None


class SXFaustAppTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_app_creation(self):
        app = App("my_test_app", schema_registry_url="test")
        self.assertTrue(app)

    def test_key_serializer(self):
        m1 = MyKeyModel(id="animalid", organisation_id="organisation_id")
        b = m1.dumps()
        my_string1 = b[-15:].decode("ascii")
        my_string2 = b[6:14].decode("ascii")
        self.assertEqual(my_string1, "organisation_id")
        self.assertEqual(my_string2, "animalid")
        m2 = MyKeyModel.loads(b)
        self.assertEqual(m2.id, "animalid")
        self.assertEqual(m2.organisation_id, "organisation_id")
        self.assertEqual(m1, m2)
