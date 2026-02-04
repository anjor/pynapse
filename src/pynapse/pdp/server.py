from __future__ import annotations

import json
import re
from dataclasses import asdict
from typing import Iterable, Optional

import httpx

from .types import (
    AddPiecesResponse,
    CreateDataSetResponse,
    DataSetCreationStatus,
    PieceAdditionStatus,
    UploadPieceResponse,
)


class PDPServer:
    def __init__(self, endpoint: str, timeout_seconds: int = 300) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._client = httpx.Client(timeout=timeout_seconds)
        self._upload_client = httpx.Client(timeout=None)

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def create_data_set(self, record_keeper: str, extra_data: str) -> CreateDataSetResponse:
        resp = self._client.post(
            f"{self._endpoint}/pdp/data-sets",
            json={"recordKeeper": record_keeper, "extraData": extra_data},
        )
        if resp.status_code not in (201, 202):
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")
        location = resp.headers.get("Location")
        if not location:
            raise RuntimeError("missing Location header")
        tx_hash = location.split("/")[-1]
        if not tx_hash.startswith("0x"):
            raise RuntimeError(f"invalid txHash in Location header: {tx_hash}")
        return CreateDataSetResponse(tx_hash=tx_hash, status_url=f"{self._endpoint}{location}")

    def get_data_set_creation_status(self, tx_hash: str) -> DataSetCreationStatus:
        resp = self._client.get(f"{self._endpoint}/pdp/data-sets/created/{tx_hash}")
        if resp.status_code == 404:
            raise RuntimeError(f"data set creation not found for txHash: {tx_hash}")
        if resp.status_code != 200:
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")
        payload = resp.json()
        return DataSetCreationStatus(
            data_set_created=payload.get("dataSetCreated", False),
            data_set_id=payload.get("dataSetId"),
            message=payload.get("message"),
        )

    def add_pieces(self, data_set_id: int, piece_cids: Iterable[str], extra_data: str) -> AddPiecesResponse:
        pieces = [
            {
                "pieceCid": cid,
                "subPieces": [{"subPieceCid": cid}],
            }
            for cid in piece_cids
        ]
        resp = self._client.post(
            f"{self._endpoint}/pdp/data-sets/{data_set_id}/pieces",
            json={"pieces": pieces, "extraData": extra_data},
        )
        if resp.status_code not in (201, 202):
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")
        location = resp.headers.get("Location")
        if not location:
            raise RuntimeError("missing Location header")
        tx_hash = location.split("/")[-1]
        return AddPiecesResponse(
            message=f"Pieces added to data set ID {data_set_id}",
            tx_hash=tx_hash,
            status_url=f"{self._endpoint}{location}",
        )

    def get_piece_addition_status(self, data_set_id: int, tx_hash: str) -> PieceAdditionStatus:
        resp = self._client.get(f"{self._endpoint}/pdp/data-sets/{data_set_id}/pieces/added/{tx_hash}")
        if resp.status_code == 404:
            raise RuntimeError(f"piece addition not found for txHash: {tx_hash}")
        if resp.status_code != 200:
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")
        payload = resp.json()
        return PieceAdditionStatus(
            add_message_ok=payload.get("addMessageOk"),
            piece_count=payload.get("pieceCount"),
            confirmed_piece_ids=payload.get("confirmedPieceIds"),
            message=payload.get("message"),
        )

    def upload_piece(self, data: bytes, piece_cid: str) -> UploadPieceResponse:
        create_resp = self._client.post(f"{self._endpoint}/pdp/piece/uploads")
        if create_resp.status_code != 201:
            raise RuntimeError(f"failed to create upload session: {create_resp.text}")
        location = create_resp.headers.get("Location")
        if not location:
            raise RuntimeError("missing Location header in upload session response")
        match = re.search(r"/pdp/piece/uploads/([a-fA-F0-9-]+)", location)
        if not match:
            raise RuntimeError(f"invalid Location header format: {location}")
        upload_uuid = match.group(1)

        upload_resp = self._upload_client.put(
            f"{self._endpoint}/pdp/piece/uploads/{upload_uuid}",
            content=data,
            headers={"Content-Type": "application/octet-stream"},
        )
        if upload_resp.status_code != 204:
            raise RuntimeError(f"upload failed: {upload_resp.text}")

        finalize_resp = self._client.post(
            f"{self._endpoint}/pdp/piece/uploads/{upload_uuid}",
            json={"pieceCid": piece_cid},
        )
        if finalize_resp.status_code != 200:
            raise RuntimeError(f"finalize failed: {finalize_resp.text}")

        return UploadPieceResponse(piece_cid=piece_cid, size=len(data))

    def find_piece(self, piece_cid: str) -> None:
        resp = self._client.get(f"{self._endpoint}/pdp/piece", params={"pieceCid": piece_cid})
        if resp.status_code == 404:
            raise RuntimeError(f"piece not found: {piece_cid}")
        if resp.status_code != 200:
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")

    def download_piece(self, piece_cid: str) -> bytes:
        resp = self._client.get(f"{self._endpoint}/pdp/piece/{piece_cid}")
        if resp.status_code == 404:
            raise RuntimeError(f"piece not found: {piece_cid}")
        if resp.status_code != 200:
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")
        return resp.content


class AsyncPDPServer:
    def __init__(self, endpoint: str, timeout_seconds: int = 300) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout_seconds)
        self._upload_client = httpx.AsyncClient(timeout=None)

    @property
    def endpoint(self) -> str:
        return self._endpoint

    async def create_data_set(self, record_keeper: str, extra_data: str) -> CreateDataSetResponse:
        resp = await self._client.post(
            f"{self._endpoint}/pdp/data-sets",
            json={"recordKeeper": record_keeper, "extraData": extra_data},
        )
        if resp.status_code not in (201, 202):
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")
        location = resp.headers.get("Location")
        if not location:
            raise RuntimeError("missing Location header")
        tx_hash = location.split("/")[-1]
        if not tx_hash.startswith("0x"):
            raise RuntimeError(f"invalid txHash in Location header: {tx_hash}")
        return CreateDataSetResponse(tx_hash=tx_hash, status_url=f"{self._endpoint}{location}")

    async def get_data_set_creation_status(self, tx_hash: str) -> DataSetCreationStatus:
        resp = await self._client.get(f"{self._endpoint}/pdp/data-sets/created/{tx_hash}")
        if resp.status_code == 404:
            raise RuntimeError(f"data set creation not found for txHash: {tx_hash}")
        if resp.status_code != 200:
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")
        payload = resp.json()
        return DataSetCreationStatus(
            data_set_created=payload.get("dataSetCreated", False),
            data_set_id=payload.get("dataSetId"),
            message=payload.get("message"),
        )

    async def add_pieces(self, data_set_id: int, piece_cids: Iterable[str], extra_data: str) -> AddPiecesResponse:
        pieces = [
            {
                "pieceCid": cid,
                "subPieces": [{"subPieceCid": cid}],
            }
            for cid in piece_cids
        ]
        resp = await self._client.post(
            f"{self._endpoint}/pdp/data-sets/{data_set_id}/pieces",
            json={"pieces": pieces, "extraData": extra_data},
        )
        if resp.status_code not in (201, 202):
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")
        location = resp.headers.get("Location")
        if not location:
            raise RuntimeError("missing Location header")
        tx_hash = location.split("/")[-1]
        return AddPiecesResponse(
            message=f"Pieces added to data set ID {data_set_id}",
            tx_hash=tx_hash,
            status_url=f"{self._endpoint}{location}",
        )

    async def get_piece_addition_status(self, data_set_id: int, tx_hash: str) -> PieceAdditionStatus:
        resp = await self._client.get(f"{self._endpoint}/pdp/data-sets/{data_set_id}/pieces/added/{tx_hash}")
        if resp.status_code == 404:
            raise RuntimeError(f"piece addition not found for txHash: {tx_hash}")
        if resp.status_code != 200:
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")
        payload = resp.json()
        return PieceAdditionStatus(
            add_message_ok=payload.get("addMessageOk"),
            piece_count=payload.get("pieceCount"),
            confirmed_piece_ids=payload.get("confirmedPieceIds"),
            message=payload.get("message"),
        )

    async def upload_piece(self, data: bytes, piece_cid: str) -> UploadPieceResponse:
        create_resp = await self._client.post(f"{self._endpoint}/pdp/piece/uploads")
        if create_resp.status_code != 201:
            raise RuntimeError(f"failed to create upload session: {create_resp.text}")
        location = create_resp.headers.get("Location")
        if not location:
            raise RuntimeError("missing Location header in upload session response")
        match = re.search(r"/pdp/piece/uploads/([a-fA-F0-9-]+)", location)
        if not match:
            raise RuntimeError(f"invalid Location header format: {location}")
        upload_uuid = match.group(1)

        upload_resp = await self._upload_client.put(
            f"{self._endpoint}/pdp/piece/uploads/{upload_uuid}",
            content=data,
            headers={"Content-Type": "application/octet-stream"},
        )
        if upload_resp.status_code != 204:
            raise RuntimeError(f"upload failed: {upload_resp.text}")

        finalize_resp = await self._client.post(
            f"{self._endpoint}/pdp/piece/uploads/{upload_uuid}",
            json={"pieceCid": piece_cid},
        )
        if finalize_resp.status_code != 200:
            raise RuntimeError(f"finalize failed: {finalize_resp.text}")

        return UploadPieceResponse(piece_cid=piece_cid, size=len(data))

    async def find_piece(self, piece_cid: str) -> None:
        resp = await self._client.get(f"{self._endpoint}/pdp/piece", params={"pieceCid": piece_cid})
        if resp.status_code == 404:
            raise RuntimeError(f"piece not found: {piece_cid}")
        if resp.status_code != 200:
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")

    async def download_piece(self, piece_cid: str) -> bytes:
        resp = await self._client.get(f"{self._endpoint}/pdp/piece/{piece_cid}")
        if resp.status_code == 404:
            raise RuntimeError(f"piece not found: {piece_cid}")
        if resp.status_code != 200:
            raise RuntimeError(f"unexpected status {resp.status_code}: {resp.text}")
        return resp.content
