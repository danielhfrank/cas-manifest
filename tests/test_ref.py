from io import StringIO

import pytest
from pydantic.error_wrappers import ValidationError

from cas_manifest import Ref


def test_ref(fs_instance):
    buf = StringIO('df')
    buf.seek(0)
    hash_addr = fs_instance.put(buf)
    ref = Ref(hash_addr)
    assert ref.hash_str == '32c220482c68413fbf8290e3b1e49b0a85901cfcd62ab0738760568a2a6e8a57'
    ref_2 = Ref(hash_addr.id)
    assert ref_2.hash_str == '32c220482c68413fbf8290e3b1e49b0a85901cfcd62ab0738760568a2a6e8a57'

    with pytest.raises(ValidationError):
        Ref({'a': 123})
