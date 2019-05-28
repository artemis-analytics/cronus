#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Test Class for Artemis MetaStore
"""

import unittest
import logging
import tempfile
import os
from pathlib import Path
from cronus.core.cronus import Cronus
from cronus.core.cronus import BaseObjectStore
from cronus.io.protobuf.cronus_pb2 import CronusStore, CronusObjectStore, CronusObject
from cronus.io.protobuf.cronus_pb2 import DummyMessage, FileObjectInfo
import uuid

logging.getLogger().setLevel(logging.INFO)


class CronusTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_store(self):
        with tempfile.TemporaryDirectory() as dirpath:
            print(dirpath)
            store = Cronus('teststore', dirpath)
            self.assertEqual(store.name, 'teststore')
            p = Path(dirpath)
            self.assertEqual(store.path, str(p))
            store._save_store()

            loaded = Cronus('teststore', dirpath)
            self.assertEqual(store.store_id, loaded.store_id)

    def test_base(self):
        mymsg = DummyMessage()
        mymsg.name = "dummy"
        mymsg.description = "really dumb"

        store_id = uuid.uuid4()
        mystore = CronusObjectStore()
        mystore.name = 'test'
        mystore.uuid = str(store_id)
        mystore.parent_uuid = '' # top level store

        

        with tempfile.TemporaryDirectory() as dirpath:
            mystore.address = dirpath+'/test'
            _path = Path(mystore.address)
            _path.mkdir()
            store = BaseObjectStore(str(_path), 'test', msg=mystore ) # wrapper to the CronusStore message
            # Generate a CronusObject for content addressing mymsg
            #content = CronusObject()
            #content.name = mymsg.name
            #content.uuid = str(uuid.uuid4())
            #store[content.uuid] = content
            id_ = store.register(mymsg, '.dummy.dat')
            print(store[id_].address)
            store._put_object(id_, mymsg)
            # Add metadata
            fileinfo = FileObjectInfo()
            fileinfo.type = 0
            fileinfo.aux.description = 'Some dummy meta info'
            store[id_].file.CopyFrom(fileinfo)

            # Retrieve
            altmsg = DummyMessage()
            location = Path(store[id_].address)
            print(location)
            altmsg.ParseFromString(location.read_bytes())
            self.assertEqual(mymsg.name, altmsg.name)
            print(store[id_].file.aux)
            print(store[id_].WhichOneof("info"))








if __name__ == '__main__':
    unittest.main()
