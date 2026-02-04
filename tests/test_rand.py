from pynapse.core import rand_index, rand_u256


def test_rand_u256_range():
    value = rand_u256()
    assert 0 <= value < 2**256


def test_rand_index_range():
    value = rand_index(10)
    assert 0 <= value < 10
