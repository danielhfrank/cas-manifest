cas-manifest provides a means to store data that is always immutable, stable when you want it to be, and flexible when you need it to be.

cas-manifest stores data artifacts via content addressible storage. It facilitates the use of CAS with standard, serializable wrappers that coexist with and support data.

## Why CAS?

In short, CAS enforces immutability. When using CAS, a key's contents can never be changed. The following comes naturally:
* No more `data_final__2_new` files - all objects are uniquely specified
* No cache invalidation - cache objects freely, knowing that their contents will never change upstream
* No more provenance questions - models can be robustly linked to the datasets used to train them

In a CAS store, instead of _put_-ing a Value at a Key, you _put_ a Value and get back the Key uniquely determined by that value.

## Why manifests?

It's all well and good to stuff some data into a binary artifact and keep a key that references it. The hard part is, when you're given a key, how do you know what's stored there? If you do settle on a standard serialization method, how do you let it evolve while maintaining backward compatibility?

cas-manifest encourages the use of manifest classes to address these challenges. These manifest classes include code to serialize and deserialize artifacts. They provide a place to store metadata about the artifacts - this may be used for deserialization, used to indicate how the loaded data should be used, or may simply be informational. Finally, fields in the manifest class may reference other objects in CAS, allowing objects to be composed and reused. In combination with CAS, you can ensure that your metadata and underlying data never go out of sync, since your metadata will refer to an immutable reference to underlying data.

## Example

### Implementing Serialization

Let's say that we wish to store datasets that we represent in memory as pandas Dataframes. We'll create a subclass of `cas_manifest.Serializable` to represent what we wish to store:
```python
class CSVSerializable(Serializable[pd.DataFrame]):

column_names: List[str]
path: Ref

@classmethod
def pack(cls, inst: pd.DataFrame, fs: HashFS) -> CSVSerializable:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / 'tmp.csv'
        with open(tmp_path, mode='w') as f:
            inst.to_csv(f, header=False, index=False)
        csv_addr = fs.put(tmp_path)
        return CSVSerializable(path=Ref(csv_addr.id), column_names=inst.columns.to_list())

def unpack(self, fs: HashFS) -> pd.DataFrame:
    addr = fs.get(self.path.hash_str)
    df = pd.read_csv(addr.abspath, names=self.column_names)
    return df
```
Let's break down the items one by one:
```python
class CSVSerializable(Serializable[pd.DataFrame]):
```
The type parameter of `Serializable` is the type that we use in memory. Whatever it is you use in application code, that's what you put here.
```python
column_names: List[str]
path: Ref
```
cas-manifest uses pydantic to define its manifest classes. This allows you to specify class fields at the top level. In order to ensure that they can be serialized, these fields need to be simple types, or other pydantic classes. `Ref` is a special wrapper class used to refer to other objects in cas.
```python
@classmethod
def pack(cls, inst: pd.DataFrame, fs: HashFS) -> CSVSerializable:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / 'tmp.csv'
        with open(tmp_path, mode='w') as f:
            inst.to_csv(f, header=False, index=False)
        csv_addr = fs.put(tmp_path)
        return CSVSerializable(path=Ref(csv_addr.id), column_names=inst.columns.to_list())
```
`pack` is a required method; this specifies how your data should be serialized. Take note of the types of the arguments.

In the body of this method, we save our dataframe as a csv, without a header row. We then `put` that into the `HashFS` instance that `pack` takes as an argument. `HashFS` provides the implementation of `CAS` that we use. That `put` operation returns the address (or key) for our csv. We can then construct an instance of our wrapper class, which contains a `Ref` to the csv file, and the column names as fields in the manifest class.
```python
def unpack(self, fs: HashFS) -> pd.DataFrame:
    addr = fs.get(self.path.hash_str)
    df = pd.read_csv(addr.abspath, names=self.column_names)
    return df
```
`unpack` is another required method; this indicates how your serialized data should be deserialized. Again, note the types of the arguments and the return types. In this case, we get the location on the filesystem for the csv file that we saved. We then call `pandas.read_csv` to read it, supplying the column names stored in the manifest class.

Note that, when this pattern is followed in the real world, it can often be confusing to keep track of whether column names are stored as a header row or kept elsewhere, whether there should be an index column, etc. This sounds silly, but it's a real problem - especially if you ever want to change your mind! cas-manifest standardizes these decisions by embedding the logic in the manifest class.

### Storing and retrieving data

Now that we've implemented all that, how do we use it?

First, we need to put something into cas. Let's say that we have a DataFrame named `df`. We'll also need an instance of `HashFS` from the `hashfs` package. AWS users may wish to make use of `S3HashFS` in this package, which provides an implementation of `HashFS` backed by S3. We can then do the following
```python
import pandas as pd
from hashfs import HashFS, HashAddress
df: pd.DataFrame = ...
fs_instance: HashFS = ...
addr: HashAddress = CSVSerializable.dump(df, fs_instance)
```
Given an instance of `HashFS`,  we can call `dump` on `CSVSerializable` to serialize our DataFrame and store it in `fs_instance`. The returned object is a `HashAddress`, which includes the immutable hash of the serialized object, as well as helper information like a path to its location on disk. If we wanted access to the serialized representation, we could also have called `CSVSerializable.pack(df, fs_instance)` to get an instance of `CSVSerializable`.

Now, how do we retrieve our serialized object from storage? Again with our instance of `HashFS`, we'll do the following
```python
hash_str = addr.id
# We create a Registry that knows what classes to expect
registry: SerializableRegistry[pd.DataFrame] = \
    SerializableRegistry(fs=fs_instance, classes=[CSVSerializable])
# We can `open` a hash address to get access to the dataframe
with registry.open(hash_str) as df:
    pass # df is the DataFrame that we saved before
# Or we can get the serialized form directly
serialized: CSVSerializable = registry.load(addr.id)
```

Why is `open` a context manager? Some implementations of `Serializable` may create temporary resources that need to be cleaned up, so we treat `open` like opening and closing a file.


### Evolving the serialization schema

Now, let's imagine that we decide we want to change our seralization format. Perhaps we'd like to make use of numpy's serialization methods to store the data in our dataframe. We can create another subclass of `Serializable`, implementing `pack` and `unpack` as before:
```python
class NPYSerializable(Serializable[pd.DataFrame]):
    ...
```
We'll skip the implementation for brevity here, but one is available in `tests/dataset.py`.

We can serialize a dataframe in this new format just as we did with `CSVSerializable`. When we want to load data, that's where things get interesting:
```python
registry_2: SerializableRegistry[pd.DataFrame] = \
    SerializableRegistry(fs=fs_instance, classes=[CSVSerializable, NPYSerializable])
```
`registry_2` now knows how to deserialize data stored in either format. You can pass it a hash string corresponding to either format, and it will correctly deserialize it into a DataFrame.

This means that you won't have to implement code to sniff out how data is stored on disk and sprinkle it around your codebase. You can consolidate your serde logic in a class, and let cas-manifest sort out how to handle it from there.

## Gotchas
* Regarding portability and schema evolution: keep in mind that your code is _not_ serialized. So, in order to load an object of type `X`, you must still have `X` available in your codebase. Instantiating your registry should make this part fairly clear
* Related to the above, if you make changes to a class, you must ensure that they are backward-compatible (e.g. adding optional fields) in order to be able to load older data.
* Typing: I've done my best to supply correct type annotations, but mypy struggles to infer return types of some generic functions. Explicit type annotations can be helpful.
