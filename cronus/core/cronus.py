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

from cronus.io.protobuf.cronus_pb2 import CronusStore, CronusObject
from cronus.io.protobuf cronus_pbs import DatasetObjectInfo
from cronus.logger import Logger
from cronus.core.book import BaseBook


@Logger.logged
class BaseStore(BaseBook):
    
    def __init__(self, 
                 path=None, 
                 msg=None, 
                 info=None):
        '''
        Loads a base store type 
        '''
        self._store = CronusStore()
        if path is not None:
            self._load_from_path(path)
        elif msg is not None:
            self._load_from_msg(msg)
        elif info is not None:
            self._store.info = 
        self._name = self._store.name
        self._uuid = self._store.uuid
        self._info = self._store.info
        self._aux = self._info.aux
        
        objects = dict()

        for item in self._info.objects:
            # Convert back to uuid4
            id_ = uuid.UUID(item.uuid)
            if id_ is None:
                self.__logger.error("Invalid UUID string")
                raise ValueError
            objects[item.name] = item.uuid

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
   
    def _load_from_msg(self, msg): # pathlib.Path
        self._store.CopyFrom(msg)

    def _load_from_path(self, path): # pathlib.Path
        self._store.ParseFromString(path.read_bytes())
    
    def _validate_uuid(self, id_):
        try:
            return uuid.UUID(id_)
        except ValueError:
            return None

    def __setitem__(self, name, value):
        '''
        book[key] = value
        enfore immutabible store
        '''
        if name in self:
            self.__logger.error("Object already exists in store, requires unique key")
            raise ValueError
        if not isinstance(name, str):
            raise TypeError
        if not isinstance(value, uuid.UUID) or not isinstance(value, str):
            raise TypeError
        try:
            self.register_object(name, value)
        self._set(name, value)

    def _register_object(self, name, value):
        id_ = str(value)
    
    def _put_object(self):
        pass

    def _get_object(self):
        pass


@Logger.logged
class ArtemisSet(BaseStore):
    '''
    A Dataset metaobject for the output of Artemis
    Persisted Arrow Files and Table metadata can be stored
    in another location than the Metastore
    '''

    def __init__(self, 
                 parent_store, 
                 parent_path, 
                 parent_id):

        self._ds = CronusStore()
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
        self._store = CronusStore()
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
        return self._store.info.path

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
            self._store.info.path = path
        except ValueError:
            self.__logger.error("Path not absolute")
            raise ValueError

    def _save_store(self):
        p = Path(self._store.info.path)
        s = p / self._store.name / self._store.uuid
        try:
            s.write_bytes(self._store.SerializeToString())
        except FileNotFoundError:
            self.__logger.error("Error writing store, file not created")
            raise FileNotFoundError


            

        




