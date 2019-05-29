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
            fileinfo = FileObjectInfo()
            fileinfo.type = 0
            fileinfo.aux.description = 'Some dummy meta info'
            id_ = store.register_content(buf, fileinfo, '.dummy.dat').uuid
            print(store[id_].address)
            store._put_object(id_, buf) 
            # Add metadata
            #store[id_].file.CopyFrom(fileinfo)

            # Retrieve
            altmsg = DummyMessage()
            location = Path(store[id_].address)
            #altstream = location.read_bytes()
            altstream = store._get_object(id_)
            hashobj = hashlib.new('sha1')
            hashobj.update(altstream)
            print(store[id_].uuid, hashobj.hexdigest())
            self.assertEqual(store[id_].uuid, hashobj.hexdigest())
            #altmsg.ParseFromString(location.read_bytes())
            altmsg.ParseFromString(altstream)
            self.assertEqual(mymsg.name, altmsg.name)
            print(store[id_].file.aux)
            obj = store[id_]
            info=obj.WhichOneof('info')
            print(eval('obj.'+info))
            print(info)

            _input = store._get_object(id_)
            altmsg.ParseFromString(_input)
            self.assertEqual(mymsg.name, altmsg.name)
            for obj in store._mstore.info.objects:
                print(obj.uuid)

            store.save_store()
            new_store = BaseObjectStore(str(_path), 'test', id_=str(store_id))
            _input = new_store._get_object(id_)
            _input = store._get_object(id_)
            altmsg.ParseFromString(_input)
            self.assertEqual(mymsg.name, altmsg.name)
            info=obj.WhichOneof('info')
            print(eval('obj.'+info))
            print(info)

    def test_arrow(self):

        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
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
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.aux.description = 'Some dummy data'
            id_ = store.register_content(buf, fileinfo, '.dummy.arrow').uuid
            print(store[id_].address)
            store._put_object(id_, buf) 
            buf = pa.py_buffer(store._get_object(id_))
            reader = pa.ipc.open_file(buf)
            self.assertEqual(reader.num_record_batches, 10)

            reader = store._open_object(id_)
            self.assertEqual(reader.num_record_batches, 10)

    def test_register_file(self):
        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
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
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.aux.description = 'Some dummy data'

            path = dirpath+'/test/dummy.arrow'
            with pa.OSFile(str(path),'wb') as f:
                f.write(sink.getvalue())
            id_ = store.register_file(path, fileinfo, 'dummy.arrow' ).uuid
            print(store[id_].address)
            buf = pa.py_buffer(store._get_object(id_))
            reader = pa.ipc.open_file(buf)
            self.assertEqual(reader.num_record_batches, 10)
    
    def test_identical_files(self):
        print("Testing add file from path")
        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
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
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.aux.description = 'Some dummy data'

            path = dirpath+'/test/dummy.arrow'
            with pa.OSFile(str(path),'wb') as f:
                f.write(sink.getvalue())
            id_ = store.register_file(path, fileinfo, 'dummy.arrow' ).uuid
            print(id_,store[id_].address)
            buf = pa.py_buffer(store._get_object(id_))
            reader = pa.ipc.open_file(buf)
            self.assertEqual(reader.num_record_batches, 10)
            
            path = dirpath+'/test/dummy2.arrow'
            with pa.OSFile(str(path),'wb') as f:
                f.write(sink.getvalue())
            id_ = store.register_file(path, fileinfo, 'dummy2.arrow' ).uuid
            print(id_,store[id_].address)
            buf = pa.py_buffer(store._get_object(id_))
            reader = pa.ipc.open_file(buf)
            self.assertEqual(reader.num_record_batches, 10)
        print("Test Done ===========================")


    def test_dir_glob(self):
        print("Testing directory globbing")
        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
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
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.aux.description = 'Some dummy data'

            path = dirpath+'/test/dummy.arrow'
            with pa.OSFile(str(path),'wb') as f:
                f.write(sink.getvalue())
            path = dirpath+'/test/dummy2.arrow'
            with pa.OSFile(str(path),'wb') as f:
                f.write(sink.getvalue())
            
            objs_ = store.register_dir(mystore.address, '*arrow', fileinfo )
            for obj_ in objs_:
                print(obj_.uuid,store[obj_.uuid].address)
                buf = pa.py_buffer(store._get_object(obj_.uuid))
                reader = pa.ipc.open_file(buf)
                self.assertEqual(reader.num_record_batches, 10)
        print("Test Done ===========================")

if __name__ == '__main__':
    unittest.main()
