from pynapse.core import MAINNET, CALIBRATION
from pynapse.utils import create_piece_url, create_piece_url_pdp


def test_create_piece_url_pdp():
    url = create_piece_url_pdp("bafk", "https://pdp.example.com/")
    assert url == "https://pdp.example.com/piece/bafk"


def test_create_piece_url_cdn():
    url = create_piece_url("bafk", True, "0xabc", MAINNET, "https://pdp.example.com/")
    assert url.startswith("https://0xabc.")


def test_create_piece_url_fallback():
    url = create_piece_url("bafk", True, "0xabc", CALIBRATION, "https://pdp.example.com/")
    assert url == "https://pdp.example.com/piece/bafk"
