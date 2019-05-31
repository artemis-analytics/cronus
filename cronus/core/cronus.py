#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Interface to the Artemis Metadata Store
"""
from pathlib import Path
import uuid
import hashlib
import urllib.parse
from dataclasses import dataclass

import pyarrow as pa
from storefact import get_store_from_url
# from simplekv.fs import FilesystemStore

from cronus.io.protobuf.cronus_pb2 import CronusObjectStore, CronusObject
from cronus.logger import Logger
from cronus.core.book import BaseBook

# Import all the info objects to set the oneof of a CronusObject
# Annoying boiler plate
from cronus.io.protobuf.cronus_pb2 import MenuObjectInfo, \
        ConfigObjectInfo, \
        DatasetObjectInfo, \
        HistsObjectInfo, \
        JobObjectInfo, \
        LogObjectInfo, \
        PartitionObjectInfo, \
        FileObjectInfo, \
        TableObjectInfo


@dataclass
class MetaObject:
    '''
    Helper data class for accessing a content object metadata
    The returned class does not give access to the original protobuf
    that is only accesible via uuid (content's hash)
    '''

    name: str
    uuid: str
    parent_uuid: str
    address: str


@Logger.logged
class BaseObjectStore(BaseBook):

    def __init__(self,
                 root,
                 name,
                 store_uuid=None,
                 storetype='hfs',
                 algorithm='sha1',
                 alt_root=None):
        '''
        Loads a base store type
        Requires a root path where the store resides
        Create a store from persisted data
        Or create a new one
        '''
        self._mstore = CronusObjectStore()
        self._dstore = get_store_from_url(f"{storetype}://{root}")
        self._alt_dstore = None
        if alt_root is not None:
            self.__logger.info("Create alternative data store location")
            self._alt_dstore = get_store_from_url(f"{storetype}://{alt_root}")
        self._algorithm = algorithm
        if store_uuid is None:
            # Generate a new store
            self.__logger.info("Generating new metastore")
            
            self._mstore.uuid = str(uuid.uuid4())
            self._mstore.address = self._dstore.url_for(self._mstore.uuid)
            self._mstore.name = name
            self._mstore.info.created.GetCurrentTime()
            self.__logger.info("Metastore ID %s", self._mstore.uuid)
            self.__logger.info("Storage location %s", self._mstore.address)
            self.__logger.info("Created on %s", self._mstore.info.created.ToDatetime())
        elif store_uuid in self._dstore:
            self.__logger.info("Load metastore from path")
            self._load_from_path(name, store_uuid)
        else:
            self.__logger.error("Cannot retrieve metastore: %s from datastore %s", store_uuid, root)
            raise KeyError

        self._name = self._mstore.name
        self._uuid = self._mstore.uuid
        self._parent_uuid = self._mstore.parent_uuid
        self._info = self._mstore.info
        self._aux = self._info.aux

        self._dups = dict()
        self._child_stores = dict()

        objects = dict()

        for item in self._info.objects:
            self.__logger.debug("Loading object %s", item.uuid)
            objects[item.uuid] = item

        super().__init__(objects)

    @property
    def store_name(self):
        return self._name

    @property
    def store_uuid(self):
        return self._uuid

    @property
    def store_info(self):
        return self._info

    @property
    def store_aux(self):
        return self._aux

    def _load_from_path(self, name, id_):
        self.__logger.info("Loading from path")
        try:
            buf = self._dstore.get(id_)
        except FileNotFoundError:
            self.__logger.error("Metastore data not found")
            raise
        except Exception:
            self.__logger.error("Unknown error")
            raise

        self._mstore.ParseFromString(buf)
        if name != self._mstore.name:
            self.__logger.error("Name of store does not equal persisted store")
            raise ValueError


    def save_store(self):
        buf = self._mstore.SerializeToString()
        self._dstore.put(self._uuid, buf)

    def _set_object_info(self, obj, info):
        '''
        '''
        if isinstance(info, FileObjectInfo):
            obj.file.CopyFrom(info)
        elif isinstance(info, MenuObjectInfo):
            obj.menu.CopyFrom(info)
        elif isinstance(info, ConfigObjectInfo):
            obj.config.CopyFrom(info)
        elif isinstance(info, DatasetObjectInfo):
            obj.dataset.CopyFrom(info)
        elif isinstance(info, HistsObjectInfo):
            obj.hists.CopyFrom(info)
        elif isinstance(info, JobObjectInfo):
            obj.job.CopyFrom(info)
        elif isinstance(info, LogObjectInfo):
            obj.log.CopyFrom(info)
        elif isinstance(info, TableObjectInfo):
            obj.table.CopyFrom(info)
        elif isinstance(info, PartitionObjectInfo):
            obj.partition.CopyFrom(info)
        else:
            self.__logger.error("Unknown info object")
            raise ValueError
    
    def _register_content_type(self):
        '''
        Menu metadata
            Menu protobug
        Configuration metadata
            config protobuf
        Dataset metadata
            Dataset protobuf
            Log file
            Hists protobuf
            Job protobuf
            Partition
                Data file
                Table (Schema) protobuf
        '''
        pass

    def _register_menu(self, buf, menuinfo):
        pass

    def _register_config(self, buf, configinfo):
        pass

    def _register_dataset(self, buf, datasetinfo, menu_id, config_id):
        '''
        dataset uuid
        '''

    def _register_partition(self, buf, paritioninfo, partition_key):
        '''
        dataset id
        partition key from menu -- corresponds to leaf name
        '''

    def _register_partition_table(self, buf, tableinfo, parition_key):
        '''
        dataset uuid
        partition key
        job key
        file uuid
        '''
        pass

    def _register_partition_file(self, buf, fileinfo, partition_key):
        '''
        Requires 
        dataset uuid
        partition key
        job key
        file uuid
        '''
        pass

    def _register_log(self, buf, loginfo, dataset_id):
        '''
        Requires 
        uuid of dataset
        generate a hists uuid from buffer
        job key common to all jobs in a dataset
        keep an running index of hists?
        extension hists.data
        dataset_id.job_name.log_id.dat
        '''
        obj = self[dataset_id].info.logs.add()
        pass

    def _register_hists(self, buf, histsinfo, dataset_id):
        '''
        Requires 
        uuid of dataset
        generate a hists uuid from buffer
        job key common to all jobs in a dataset
        keep an running index of hists?
        extension hists.data
        dataset_id.job_name.hists_id.dat
        '''
        obj = self[dataset_id].info.hists.add()
        pass

    def _register_job(self, buf, jobinfo, dataset_id):
        '''
        Requires 
        uuid of dataset
        generate a job uuid from buffer
        job_name key common to all jobs in a dataset
        keep an running index of jobs?
        extension job.dat
        dataset_id.job_name.job_id.dat
        '''
        obj = self[dataset_id].info.jobs.add()
        pass
    
    def _build_name(self, buf, info):
        return ''
    
    def register_content(self, buf, info, 
                         dataset_uuid=None, 
                         job_id=None, 
                         partition_key=None):
        '''
        Returns the content identifier
        content is the raw data, e.g. serialized bytestream to be persisted
        hash the bytestream, see for example github.com/dgilland/hashfs

        info object can be used to call the correct
        register method and validate all the required inputs are received

        Parameters
        ----------
        buf : bytestream, object ready to be persisted
        info : associated metadata object describing the content of buf


        Optional
        --------
        dataset uuid : required for logs, files, tables, histst
        job_name or job_uuid
        partition_key : required for files and tables
        '''
        self.__logger.info("register")
        obj = self._mstore.info.objects.add()
        obj.name = self._build_name(buf, info)
        obj.uuid = self._compute_hash(pa.input_stream(buf))
        obj.parent_uuid = self._uuid
        # New data, get a url from the datastore
        obj.address = self._dstore.url_for(obj.uuid)
        self.__logger.info("Retrieving url %s", obj.address)
        self._set_object_info(obj, info)
        # self._register_object(uuid,obj)
        self[obj.uuid] = obj
        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def _compute_hash(self, stream):
        hashobj = hashlib.new(self._algorithm)
        hashobj.update(stream.read())
        return hashobj.hexdigest()

    def _register_object(self, obj):
        '''
        Identical files will generate same as
        while we should not support duplicate data
        could there be a reason?

        check that uuid is not in the store
        if so, create a counter extension to the uuid
        
        compute object hash
        check for duplicates and increment count
            duplicates not expected for artemis outputs
        check info object type
        validate kwargs
        set any metadata from info or kwargs -- use info objects to contain
            correct data to build the file name?

        '''
        pass
        
    
    def register_file(self, location, info, extension=''):
        '''
        Returns the content identifier
        for a file that is already in a store
        Requires a stream as bytes
        '''
        path = Path(location)
        if path.is_absolute() is False:
            path = path.resolve()
        obj = self._mstore.info.objects.add()
        obj.name = extension
        obj.uuid = self._compute_hash(pa.input_stream(str(path)))
        obj.parent_uuid = self._uuid
        # Create a Path object, ensure that location points to a file
        # Since we are using simplekv, new objects always registers as url
        # So make a file path as url
        obj.address = path.as_uri()
        self._set_object_info(obj, info)

        if obj.uuid in self:
            if obj.uuid in self._dups:
                self._dups[obj.uuid] += 1
            else:
                self._dups[obj.uuid] = 0
            obj.uuid = obj.uuid + '_' + str(self._dups[obj.uuid])

        self[obj.uuid] = obj
        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def register_dir(self, location, glob, info):
        '''
        Registers a directory of files in a store
        '''
        objs = []
        for file_ in Path(location).glob(glob):
            objs.append(self.register_file(file_, info))
        return objs

    def __setitem__(self, id_, msg):
        '''
        book[key] = value
        enfore immutible store
        '''
        if id_ in self:
            self.__logger.error("Key exists %s", id_)
            raise ValueError
        if not isinstance(id_, str):
            raise TypeError
        if not isinstance(msg, CronusObject):
            raise TypeError

        self._set(id_, msg)

    def _put_object(self, id_, buf):
        # bytestream to persist
        try:
            self._dstore.put(id_, buf.to_pybytes())
        except IOError:
            self.__logger.error("IO error %s", self[id_].address)
            raise
        except Exception:
            self.__logger.error("Unknown error put %s", self[id_].address)
            raise

    def _get_object(self, id_):
        # get object will read object into memory buffer
        try:
            return self._dstore.get(id_)
        except KeyError:
            # File resides outside of kv store
            # Used for registering files already existing in persistent storage
            return pa.input_stream(self._parse_url(id_)).read()

    def _parse_url(self, id_):
        url_data = urllib.parse.urlparse(self[id_].address)
        return urllib.parse.unquote(url_data.path)

    def _open_object(self, id_):
        # Returns pyarrow io handle
        if self[id_].WhichOneof('info') == 'file':
            # Arrow RecordBatchFile
            if self[id_].file.type == 5:
                # Convert the url to path
                return pa.ipc.open_file(self._parse_url(id_))
            # Arrow RecordBatchStream
            elif self[id_].file.type == 6:
                pa.ipc.open_stream(self[id_].address)
            else:
                return pa.input_stream(self[id_].address)
        else:
            # Anything else in the store is either a protobuf bytestream
            # or just text, e.g. a log file
            # Need to handle compressed files
            return pa.input_stream(self[id_].address)





@Logger.logged
class ArtemisSet(BaseObjectStore):
    '''
    A Dataset metaobject for the output of Artemis
    Persisted Arrow Files and Table metadata can be stored
    in another location than the Metastore
    '''

    def __init__(self, 
                 parent_store, 
                 parent_path, 
                 parent_id):

        self._ds = CronusObjectStore()
        self._ds.info = DatasetObjectInfo()
        self._is_raw = False  # Consume a raw file format dataset
        self._is_sim = False  # Consume a msg defining a simulation model
        self._is_derived = False  # Any derived dataset which comprises of arrow files
        self._path_to_store = Path()
        self._path_to_store / parent_path / parent_id

        # Path to actual data location
        # Path / dataset_uuid / 
        #       jobs
        #       logs
        #       hists
        #       partitions
        # Otherwise, held with rest of metadata
        self._storage = self._ds.info.storage_location
        if self._store != '':
            self._storage / self.parent_id / 'data' / self._ds.info.uuid
        else:
            self._storage = self._path_to_store / 'data' / self._ds.info.uuid

        # Now create the store wrapper
        super().__init__(self._ds)
                                

    def _set_config_object(self, name, id_=None):
        '''
        Reference to a config object that exists in the Cronus ConfigStore
        '''
        pass

    def _set_menu_object(self, name, id_=None):
        '''
        Reference to a menu object that exists in the Cronus MenuStore
        '''
        pass

    def _set_transform(self):
        '''
        Define the menu and config used to produce this dataset
        '''
    def _register_job_store(self):
        '''
        Create the path to output jobs
        '''
        pass

    def _register_hists_store(self):
        '''
        Create the path to output collections
        '''
        pass
    
    def _register_log_store(self):
        '''
        Creates the path to output logs
        '''

    def _register_partition(self, key):
        '''
        Create a path to a dataset partition
        '''
        # Create the path for files
        # Create the path for tables
        pass

    def _validate_dataset(self):
        '''
        After loading a dataset message
        Ensure that expected metadata exists
        Ensure that data exists
        '''
        # Dataset can have alternate path for storage of actual data and metaobjects
        # path_to_datastorage/
        #   jobs
        #   hists
        #   logs
        #   partitions
        #############
        # Transform must exist if dataset produced from artemis
        #   has a configuration
        #   has a menu
        #   configuration is found in configstore
        #   configuration is found in menustore
        #############
        # Partitions must exist
        #   Files must exist (Raw data, Arrow derived data, simulation config protobufs)
        #   Tables must exist i.e. a schema must be defined for using any tabular data
        #   
        pass






@Logger.logged
class Cronus():

    def __init__(self, store, path):
        '''
        Create or load a top-level CronusStore

        CronusStore must contain three additional stores
        MenuStore
            menus
        ConfigStore
            configs
        DatasetStore
            datasets
                tranform
                jobstore
                    jobs
                histsstore
                    hists
                logstore
                    logs
                partitions
                    files
                    tables
        '''
        self._store = CronusObjectStore()
        if self._store_exists(store, path):
            try:
                spath = self._get_store(store, path)
            except ValueError:
                self.__logger.error("Store does not have unique store file")
                raise

            self._store.ParseFromString(spath.read_bytes())
            self.__logger.info("Loaded Store from location %s", spath)
            self.__logger.info("Store %s ID %s",
                               self._store.name,
                               self._store.uuid)
        else:
            try:
                self._create_store(store, path)
            except Exception as e:
                self.__logger.error("Cannot create store")
                raise e

    @property
    def name(self):
        return self._store.name

    @property
    def store_id(self):
        return self._store.uuid

    @property
    def path(self):
        return self._store.address

    def _get_object(self,
                    store,
                    path,
                    object_key=None,
                    type_key=None,
                    partition_key=None,
                    subtype_key=None,
                    id_=None):
        '''
        Get an object path from the store

        Parameters
        ----------
        store : name of the base Metastore
        path : absolute path of the base Metastore
        object_key : name of the object
        type_key : optional argument for Metastore collections, must be
            accepted types : menus, datasets, configs
        partition_key : only valid for datasets type_ with name_
        subtype_key : accepted values files, tables, hists, logs, jobs
        id_ : optional uuid of object to retrieve

        path / store / type / item
                                partition
                                    files
                                    tables
                                    hists
                                    logs
                                    jobs
                                    table
                                    collection
        Returns
        -------
        path : pathlib.Path
        '''
        _base = Path(path, store)
        if _base.exists() is False:
            raise FileNotFoundError
        _path_to_object = None
        stores = []
        for child in _base.iterdir():
            stores.append(child)
        if len(stores) != 1:
            raise ValueError
        _path_to_object = stores[-1]

        if type_key is None:
            # Return a store path
            return _path_to_object

        _path_to_object = _path_to_object / type_key

        # Name of a menu item, config item or dataset
        if object_key is not None:
            _path_to_object = _path_to_object / object_key

        if _path_to_object.exists() is False:
            self.__logger.error("Requested a %s object, no path exists",
                                _path_to_object)
            raise FileNotFoundError
        
        if partition_key is not None:
            _path_to_object = _path_to_object / partition_key
            if subtype_key is not None:
                _path_to_object = _path_to_object / subtype_key

        if _path_to_object.exists() is False:
            self.__logger.error("Requested a %s object, no path exists",
                                _path_to_object)
            raise FileNotFoundError


        if id_ is not None:
            _path_to_object = _path_to_object / id_

            if _path_to_object.exists() is False:
                self.__logger.error("Requested a %s object, no path exists",
                                    _path_to_object)
                raise FileNotFoundError

            if _path_to_object.is_file() is False:
                self.__logger.error("Requested a %s file object, not a file",
                                    _path_to_object)
                raise TypeError

    def _object_exists(self, store, path, id_=None):
        p = Path(path)
        if p.exists():
            if p.is_dir():
                s = p / store
                if s.exists():
                    self.__logger.info("Store exists, loading from file")
                    return True

        else:
            self.__logger.info("Store path does not exist, creating new path")
            try:
                p.mkdir()
            except FileExistsError:
                self.__logger.error("Path exists, abort")
                raise

    def _create_path(self, path_to_object):
        _path_to_object = Path(path_)
        if _path_to_object.exists() is False:
            try:
                _path_to_object.mkdir()
            except FileExistsError:
                self.__logger.error("Cannot create path ", path_to_object)
                raise
    
    def _create_file(self, path_):
        pass

    def _create_object(self,
                    store,
                    path,
                    object_key=None,
                    type_key=None,
                    partition_key=None,
                    subtype_key=None,
                    id_=None):
        '''
        Create an object in the store

        Parameters
        ----------
        store : name of the base Metastore
        path : absolute path of the base Metastore
        object_key : name of the object
        type_key : optional argument for Metastore collections, must be
            accepted types : menus, datasets, configs
        partition_key : only valid for datasets type_ with name_
        subtype_key : accepted values files, tables, hists, logs, jobs
        id_ : optional uuid of object to retrieve

        path / store / type / item
                                partition
                                    files
                                    tables
                                    hists
                                    logs
                                    jobs
                                    table
                                    collection
        Returns
        -------
        path : pathlib.Path
        '''
        _base = Path(path, store)
        if _base.exists() is False:
            self.__logger.error("Cannot create object, store does not exist")
            raise FileNotFoundError

        # Get the object store 
        _path_to_object = self._get_object(store, path)

        if type_key is None:
            self.__logger.error("Provide a type to create or use")
            raise InputError

        _path_to_object = _path_to_object / type_key
        
        if _path_to_object.exists() is False:
            self._create_path(_path_to_object)
            
        if partition_key is not None:
            _path_to_object = _path_to_object / partition_key
            if subtype_key is not None:
                _path_to_object = _path_to_object / subtype_key

        if _path_to_object.exists() is False:
            self.__logger.error("Requested a %s object, no path exists",
                                _path_to_object)
            raise FileNotFoundError

        if object_key is not None:
            _path_to_object = _path_to_object / object_key

        if _path_to_object.exists() is False:
            self.__logger.error("Requested a %s object, no path exists",
                                _path_to_object)
            raise FileNotFoundError

        if id_ is not None:
            _path_to_object = _path_to_object / id_

            if _path_to_object.exists() is False:
                self.__logger.error("Requested a %s object, no path exists",
                                    _path_to_object)
                raise FileNotFoundError

            if _path_to_object.is_file() is False:
                self.__logger.error("Requested a %s file object, not a file",
                                    _path_to_object)
                raise TypeError
        pass

    def _set_object(self, store, path, id_=None):
        pass

    def _save_object(self, store, path, id_=None):
        pass

    def _load_object(self, store, path, id_=None):
        pass

    def _get_store(self, store, path):
        try:
            store = self._get_object(store, path)
        except ValueError:
            raise
        return store

    def _store_exists(self, store, path):
        return self._object_exists(store, path)

    def _create_store(self, store, path):
        self.__logger.info("Creating store")
        p = Path(path, store)
        if p.exists():
            if p.is_dir():
                s = p / store
                if s.exists():
                    self.__logger.error("Store exists %s %s", p, store)
                    raise FileExistsError
                else:
                    try:
                        self._set_store(store, path)
                    except Exception as e:
                        self.__logger.error("Error setting store")
                        raise e
            else:
                self.__logger.error("%s is not a directory", path)
                raise ValueError

        else:
            try:
                p.mkdir()
                self.__logger.info("Created new store location %s", p)
            except FileExistsError:
                self.__logger.error("Path exists")
                raise
            try:
                self._set_store(store, path)
            except Exception as e:
                self.__logger.error("Error setting store")
                raise e

    def _set_store(self, store, path):
        self.__logger.info("Set store")
        self._store.name = store
        id_ = uuid.uuid4()
        try:
            self._store.uuid = str(id_)
        except Exception as e:
            raise e
        self.__logger.info("Store %s %s", self._store.name, self._store.uuid)

        p = Path(path)
        # Create a file to persist the store
        s = p / self._store.name / self._store.uuid
        try:
            s.touch()
            self.__logger.info("Created a new store file %s", s)
        except FileExistsError:
            self.__logger.error("Cannot touch the new store file")
            raise FileExistsError
        try:
            self._store.address = path
        except ValueError:
            self.__logger.error("Path not absolute")
            raise ValueError

    def _save_store(self):
        p = Path(self._store.address)
        s = p / self._store.name / self._store.uuid
        try:
            s.write_bytes(self._store.SerializeToString())
        except FileNotFoundError:
            self.__logger.error("Error writing store, file not created")
            raise FileNotFoundError


            

        




