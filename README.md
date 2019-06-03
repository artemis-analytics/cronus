# cronus
Metastore for Artemis


## Dependencies
Cronus uses [simplekv](https://github.com/mbr/simplekv) as a basic key-value store 
for persistency of data and metadata files to both local filesystems and cloud storage backends.
Easy factory class methods for setting up a new key-value store is supported by 
[storefact](https://github.com/blue-yonder/storefact).

## Getting started

```bash
git clone https://git.../cronus.git
python setup.py install
python -m unittest
```

Let's started by creating a test file to store. We create a Arrow RecordBatches from numpy arrays.
The RecordBatches are written to an Arrow RecordBatchFile (for random access reads of batches).
The file is closed and we get an in-memory bytestream buffer of serialized data. The buffer must be
written to persistent storage.

```python
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
```

Cronus' current scope is to provide content addressable metadata to access both metadata
required to run Artemis and metadata associated with Artemis data products.
Create an Artemis menu message and config message.

```python
mymenu = Menu() 
mymenu.uuid = str(uuid.uuid4())
mymenu.name = f"{mymenu.uuid}.menu.dat"

menuinfo = MenuObjectInfo()
menuinfo.created.GetCurrentTime()
bufmenu = pa.py_buffer(mymenu.SerializeToString())

myconfig = Configuration() 
myconfig.uuid = str(uuid.uuid4())
myconfig.name = f"{myconfig.uuid}.config.dat"

configinfo = ConfigObjectInfo()
configinfo.created.GetCurrentTime()
bufconfig = pa.py_buffer(myconfig.SerializeToString())
```

Now we can setup an object store to persist data and content address the data files
with Cronus metadata objects. The example below uses the local filesystem as a backend, 
however, storefact and simplekv provide support for cloud storage backends such as Azure Blob storage.

```python
dirpath = tempfile.mkdtemp()
_path = dirpath+'/test'

# Setup the metastore for content addressing data objects.
# Setup a backend datastore at _path
store = BaseObjectStore(str(_path), 'test') # wrapper to the CronusStore message

# Following puts the menu and config to the datastore
# Creates a metadata object for managing the configuration content
menu_uuid = store.register_content(mymenu, menuinfo).uuid
config_uuid = store.register_content(myconfig, configinfo).uuid

```

Now that we have a backing datastore and a way to content address the data with additional metadata,
we'll create a dataset and produce data.

```python
# A dataset must have a menu and configuration object
dataset = store.register_dataset(menu_uuid, config_uuid)

# Data outputs are always associated to a data stream or partition
# From the Artemis point of view a partition is a leaf node of the Artemis process model graph.

# Multiple streams
store.new_partition(dataset.uuid, 'key1')
store.new_partition(dataset.uuid, 'key2')
store.new_partition(dataset.uuid, 'key3')
```

Metadata content is retained in Info objects associated with the metaobject holding the data content address.
ObjectInfo protocol buffer models represent the basic object types for our metadata. Auxillary objects
can be added to extend the model.

```python
fileinfo = FileObjectInfo()
fileinfo.type = 5
fileinfo.aux.description = 'Some dummy data'
        
ids_ = []
parts = store.list_partitions(dataset.uuid)

# reload menu and config
# This simulates how an Artemis job works. At job start, both the menu and configuration are retrieved from the store.
newmenu = Menu()
store.get(menu_uuid, newmenu)
newconfig = Configuration()
store.get(config_uuid, newconfig)
print(parts)
```

Next, we create register content and put content to the datastore
```python
for _ in range(10):
    job_id = store.new_job(dataset.uuid)
    
    for key in parts:
        ids_.append(store.register_content(buf, 
                                     fileinfo, 
                                     dataset_id=dataset.uuid, 
                                     job_id=job_id, 
                                     partition_key=key ).uuid)
        store.put(ids_[-1], buf)
```

The following shows how to retrieve the content from the unique identifier. 
```python
for id_ in ids_:
    buf = pa.py_buffer(store.get(id_))
    reader = pa.ipc.open_file(buf)
    self.assertEqual(reader.num_record_batches, 10)
```

Now, we may want to backup the metastore and reload at a later time. 

```python
# Save the store, reload
store.save_store()
newstore = BaseObjectStore(str(_path), 'test', store_uuid=store.store_uuid) 
for id_ in ids_:
    print("Get object %s", id_)
    print(type(id_))
    buf = pa.py_buffer(newstore.get(id_))
    reader = pa.ipc.open_file(buf)
    self.assertEqual(reader.num_record_batches, 10)
```
