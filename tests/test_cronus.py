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
import hashlib
from pathlib import Path
import pyarrow as pa
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

            buf = pa.py_buffer(mymsg.SerializeToString())
            stream = pa.input_stream(buf)
            print(type(stream))
            id_ = store.register_content(buf, '.dummy.dat')
            print(store[id_].address)
            store._put_object(id_, buf) 
            # Add metadata
            fileinfo = FileObjectInfo()
            fileinfo.type = 0
            fileinfo.aux.description = 'Some dummy meta info'
            store[id_].file.CopyFrom(fileinfo)

            # Retrieve
            altmsg = DummyMessage()
            location = Path(store[id_].address)
            altstream = location.read_bytes()
            hashobj = hashlib.new('sha1')
            hashobj.update(altstream)
            print(store[id_].uuid, hashobj.hexdigest())
            self.assertEqual(store[id_].uuid, hashobj.hexdigest())
            altmsg.ParseFromString(location.read_bytes())
            self.assertEqual(mymsg.name, altmsg.name)
            print(store[id_].file.aux)
            print(store[id_].WhichOneof("info"))
            _input = store._get_object(id_)
            altmsg.ParseFromString(_input.read())
            self.assertEqual(mymsg.name, altmsg.name)
            for obj in store._store.info.objects:
                print(obj.uuid)

            store.save_store()
            new_store = BaseObjectStore(str(_path), 'test', id_=str(store_id))
            _input = new_store._get_object(id_)
            _input = store._get_object(id_)
            altmsg.ParseFromString(_input.read())
            self.assertEqual(mymsg.name, altmsg.name)










if __name__ == '__main__':
    unittest.main()
