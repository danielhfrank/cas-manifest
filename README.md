# CAS-Manifest

This package facilitates storing artifacts in Content Addressable Storage via the `hashfs` library. In a CAS regime, the hash of the artifact's contents is used as the key.

It further requires that artifacts are `pydantic` models - this allows for stable serialization of the artifacts, and for data to be self-describing.

Consider an example usage profile: let's say that your application works with datasets, some of which are serialized as csv files, others of which are serialized as tsv files. Some have header rows, and some do not. Rather than write data-loading code that tries to infer the correct way to deserialize a dataset file, `cas-manifest` serializes all relevant
attributes of the dataset along with the data file itself. Your code might look like this:
```python
from hashfs import HashFS
from cas_manifest.registry import Registry
from my_classes import CSVDataset, TSVDataset

fs = HashFS('/path/to/data')
dataset_hash = '5fef4a'
registry = Registry(fs, [CSVDataset, TSVDataset])
obj = registry.load(dataset_hash)
# obj is an instance of either CSVDataset or TSVDataset
```

## Why CAS?

In short, CAS enforces immutability. When using CAS, a key's contents can never be changed. The following comes naturally:
* No more `data_final__2_new` files - all objects are uniquely specified
* No cache invalidation - cache objects freely, knowing that their contents will never change upstream
* No more provenance questions - models can be robustly linked to the datasets used to train them

## Why manifests?

In a CAS regime, keys are deliberately opaque. By using manifests, artifacts can be _self-descriptive_. It can include instructions for deserialization, links to other artifacts, and any other metadata you can think up. In combination with CAS, you can ensure that your metadata and underlying data never go out of sync, since your metadata will refer to an immutable reference to underlying data.
