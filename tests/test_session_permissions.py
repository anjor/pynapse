from pynapse.session.permissions import SESSION_KEY_PERMISSIONS, type_hash


def test_type_hashes_present():
    assert type_hash("CreateDataSet") == SESSION_KEY_PERMISSIONS["CreateDataSet"]
    assert type_hash("AddPieces") == SESSION_KEY_PERMISSIONS["AddPieces"]
