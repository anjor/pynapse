from __future__ import annotations

from dataclasses import dataclass

import httpx

from pynapse.core.chains import Chain, as_chain


@dataclass
class DataSetStats:
    cdn_egress_quota: int
    cache_miss_egress_quota: int


class FilBeamService:
    def __init__(self, chain: Chain, client: httpx.Client | None = None) -> None:
        self._chain = as_chain(chain)
        self._client = client or httpx.Client(timeout=30)

    def _stats_base_url(self) -> str:
        if self._chain.id == 314159:
            return "https://calibration.stats.filbeam.com"
        return "https://stats.filbeam.com"

    def get_data_set_stats(self, data_set_id: str | int) -> DataSetStats:
        url = f"{self._stats_base_url()}/data-set/{data_set_id}"
        resp = self._client.get(url)
        if resp.status_code == 404:
            raise RuntimeError(f"Data set not found: {data_set_id}")
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
        data = resp.json()
        if not isinstance(data, dict):
            raise RuntimeError("Invalid response from FilBeam")
        return DataSetStats(
            cdn_egress_quota=int(data.get("cdnEgressQuota", "0")),
            cache_miss_egress_quota=int(data.get("cacheMissEgressQuota", "0")),
        )
