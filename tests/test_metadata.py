from pynapse.utils import METADATA_KEYS, combine_metadata, metadata_matches


def test_metadata_matches():
    assert metadata_matches({"a": "1"}, {"a": "1"}) is True
    assert metadata_matches({"a": "1"}, {"a": "2"}) is False
    assert metadata_matches({"a": "1", "b": "2"}, {"a": "1"}) is False


def test_combine_metadata_with_cdn():
    base = {"foo": "bar"}
    combined = combine_metadata(base, with_cdn=True)
    assert combined[METADATA_KEYS["WITH_CDN"]] == ""


def test_combine_metadata_no_override():
    base = {METADATA_KEYS["WITH_CDN"]: ""}
    combined = combine_metadata(base, with_cdn=True)
    assert combined is base
