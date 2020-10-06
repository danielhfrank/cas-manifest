import contextlib
from io import BytesIO, StringIO
import json
import zipfile

import pytest

from cas_manifest.ref import Ref
from cas_manifest.registry import Registry
from .dataset import CSVDataset, ZipDataset


@pytest.fixture
def registry(fs_instance):
    """Shared instance of a Registry using the available dataset
    sublasses

    :param fs_instance: instance of hashfs.HashFS (supplied via pytest fixture)
    :type fs_instance: HashFS
    """
    dataset_classes = [CSVDataset, ZipDataset]
    return Registry(fs_instance, dataset_classes)


def test_csv_dataset(registry, fs_instance):
    # Put test asset file into fs
    csv_addr = fs_instance.put('tests/assets/iris.csv')
    col_names = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'label']
    # Construct the dataset wrapper around the saved file
    orig_dataset = CSVDataset(path=Ref(csv_addr.id), column_names=col_names)
    # Save the wrapper to the fs
    addr = orig_dataset.dump(fs_instance)

    # Load the dataset
    dataset = registry.load(addr.id)
    if not isinstance(dataset, CSVDataset):
        raise Exception("just to prove to mypy that it's a CSVDataset")
    assert dataset.column_names == col_names
    # Load the dataframe referenced by the dataset
    df = dataset.load_from(fs_instance)
    # Check expected value in the dataframe
    assert df.sepal_length[0] == 5.1


def test_unsupported_objects(registry, fs_instance):
    bad_json = {'asdf': 123}
    buf = StringIO()
    json.dump(bad_json, buf)
    buf.seek(0)
    addr = fs_instance.put(buf)

    with pytest.raises(ValueError, match='Not a serialized object'):
        registry.load(addr.id)

    missing_class = {'class': 'DFDF', 'value': {}}
    buf = StringIO()
    json.dump(missing_class, buf)
    buf.seek(0)
    addr_2 = fs_instance.put(buf)

    with pytest.raises(ValueError, match='Not a recognized class'):
        registry.load(addr_2.id)


def test_zip_dataset(registry, fs_instance):
    buf = BytesIO()
    zf = zipfile.ZipFile(buf, mode='w')
    zf.writestr('df.txt', 'roolz')
    zf.close()
    buf.seek(0)
    zip_addr = fs_instance.put(buf)

    zd = ZipDataset(path=Ref(zip_addr.id))
    ds_addr = zd.dump(fs_instance)
    with contextlib.closing(registry.load(ds_addr.id)) as dataset:
        assert(isinstance(dataset, ZipDataset))
        tmpdir_path = dataset.load_from(fs_instance)
        assert(tmpdir_path.is_dir())
        assert(set(tmpdir_path.iterdir()) == {tmpdir_path / 'df.txt'})
        assert((tmpdir_path / 'df.txt').read_text() == 'roolz')
