from pynapse.core import CALIBRATION, MAINNET, as_chain


def test_as_chain_by_id():
    assert as_chain(314).id == MAINNET.id
    assert as_chain(314159).id == CALIBRATION.id


def test_as_chain_by_name():
    assert as_chain("mainnet").id == MAINNET.id
    assert as_chain("calibration").id == CALIBRATION.id
