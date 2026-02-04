from __future__ import annotations

from urllib.parse import urljoin

from pynapse.core.chains import Chain


def create_piece_url_pdp(cid: str, pdp_url: str) -> str:
    return urljoin(pdp_url.rstrip("/") + "/", f"piece/{cid}")


def create_piece_url(cid: str, cdn: bool, address: str, chain: Chain, pdp_url: str) -> str:
    if cdn and chain.filbeam_domain:
        endpoint = f"https://{address}.{chain.filbeam_domain}"
        return urljoin(endpoint + "/", cid)
    return create_piece_url_pdp(cid, pdp_url)
