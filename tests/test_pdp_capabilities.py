from pynapse.sp_registry.pdp_capabilities import decode_pdp_capabilities, encode_pdp_capabilities
from pynapse.sp_registry.types import PDPOffering


def test_encode_decode_pdp_capabilities_roundtrip():
    offering = PDPOffering(
        service_url="https://example.com",
        min_piece_size_in_bytes=127,
        max_piece_size_in_bytes=1024,
        storage_price_per_tib_per_day=10,
        min_proving_period_in_epochs=2880,
        location="us-east",
        payment_token_address="0x0000000000000000000000000000000000000001",
        ipni_piece=True,
        ipni_ipfs=False,
    )
    keys, values = encode_pdp_capabilities(offering)
    caps = {k: value.hex() if hasattr(value, "hex") else value for k, value in zip(keys, values)}
    # normalize to 0x hex strings
    caps = {k: ("0x" + v if not str(v).startswith("0x") else str(v)) for k, v in caps.items()}
    decoded = decode_pdp_capabilities(caps)
    assert decoded.service_url == offering.service_url
    assert decoded.min_piece_size_in_bytes == offering.min_piece_size_in_bytes
    assert decoded.max_piece_size_in_bytes == offering.max_piece_size_in_bytes
