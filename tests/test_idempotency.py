"""Tests for idempotency behavior in dataset creation and piece addition."""
import pytest
from unittest.mock import MagicMock, patch, Mock
import httpx

from pynapse.pdp.server import PDPServer, AsyncPDPServer, AlreadyExistsError, IdempotencyError
from pynapse.pdp.types import CreateDataSetResponse, AddPiecesResponse


class TestIdempotencyKeyGeneration:
    """Test idempotency key generation logic."""

    def test_generate_idempotency_key(self):
        """Test that idempotency keys are deterministic and different for different inputs."""
        from pynapse.storage.context import StorageContext

        # Same inputs should produce same key
        key1 = StorageContext._generate_idempotency_key("create_dataset", "addr1", "provider1", "metadata")
        key2 = StorageContext._generate_idempotency_key("create_dataset", "addr1", "provider1", "metadata")
        assert key1 == key2

        # Different inputs should produce different keys
        key3 = StorageContext._generate_idempotency_key("create_dataset", "addr2", "provider1", "metadata")
        assert key1 != key3

        key4 = StorageContext._generate_idempotency_key("add_pieces", "addr1", "provider1", "metadata")
        assert key1 != key4

    def test_async_idempotency_key_generation(self):
        """Test async version generates same keys as sync version."""
        from pynapse.storage.context import StorageContext
        from pynapse.storage.async_context import AsyncStorageContext

        sync_key = StorageContext._generate_idempotency_key("test", "arg1", "arg2")
        async_key = AsyncStorageContext._generate_idempotency_key("test", "arg1", "arg2")
        assert sync_key == async_key

    def test_idempotency_key_length(self):
        """Test that idempotency keys are appropriately sized."""
        from pynapse.storage.context import StorageContext

        key = StorageContext._generate_idempotency_key("test", "arg")
        assert len(key) == 32  # 32 hex characters
        assert all(c in '0123456789abcdef' for c in key)


class TestPDPServerIdempotency:
    """Test PDP server idempotency handling."""

    def test_create_data_set_success(self):
        """Test successful dataset creation with idempotency key."""
        pdp = PDPServer("http://test.com")

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.headers = {"Location": "/pdp/data-sets/created/0x123abc"}

        with patch.object(pdp._client, 'post', return_value=mock_response) as mock_post:
            response = pdp.create_data_set("keeper", "extra_data", "idem_key_123")

            # Verify idempotency header was sent
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            assert call_args[1]['headers']['Idempotency-Key'] == "idem_key_123"

            assert response.tx_hash == "0x123abc"

    def test_create_data_set_already_exists(self):
        """Test dataset creation when dataset already exists (409 response)."""
        pdp = PDPServer("http://test.com")

        mock_response = Mock()
        mock_response.status_code = 409
        mock_response.text = "Dataset already exists"
        mock_response.json.return_value = {"existingDataSetId": "456"}

        with patch.object(pdp._client, 'post', return_value=mock_response):
            with pytest.raises(AlreadyExistsError) as exc_info:
                pdp.create_data_set("keeper", "extra_data", "idem_key_123")

            assert exc_info.value.existing_resource_id == "456"

    def test_create_data_set_idempotency_conflict(self):
        """Test dataset creation with idempotency key conflict (422 response)."""
        pdp = PDPServer("http://test.com")

        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = "Idempotency key conflict"

        with patch.object(pdp._client, 'post', return_value=mock_response):
            with pytest.raises(IdempotencyError):
                pdp.create_data_set("keeper", "extra_data", "idem_key_123")

    def test_add_pieces_success(self):
        """Test successful piece addition with idempotency key."""
        pdp = PDPServer("http://test.com")

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.headers = {"Location": "/pdp/data-sets/123/pieces/added/0x456def"}

        with patch.object(pdp._client, 'post', return_value=mock_response) as mock_post:
            response = pdp.add_pieces(123, ["cid1", "cid2"], "extra_data", "idem_key_456")

            # Verify idempotency header was sent
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            assert call_args[1]['headers']['Idempotency-Key'] == "idem_key_456"

            assert response.tx_hash == "0x456def"

    def test_add_pieces_already_exists(self):
        """Test piece addition when pieces already exist (409 response)."""
        pdp = PDPServer("http://test.com")

        mock_response = Mock()
        mock_response.status_code = 409
        mock_response.text = "Some pieces already exist"
        mock_response.json.return_value = {"existingPieces": ["cid1"]}

        with patch.object(pdp._client, 'post', return_value=mock_response):
            with pytest.raises(AlreadyExistsError) as exc_info:
                pdp.add_pieces(123, ["cid1", "cid2"], "extra_data", "idem_key_456")

            assert exc_info.value.existing_resource_id == "123"


class TestAsyncPDPServerIdempotency:
    """Test async PDP server idempotency handling."""

    @pytest.mark.asyncio
    async def test_create_data_set_success_async(self):
        """Test successful async dataset creation with idempotency key."""
        pdp = AsyncPDPServer("http://test.com")

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.headers = {"Location": "/pdp/data-sets/created/0x123abc"}

        with patch.object(pdp._client, 'post', return_value=mock_response) as mock_post:
            response = await pdp.create_data_set("keeper", "extra_data", "idem_key_123")

            # Verify idempotency header was sent
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            assert call_args[1]['headers']['Idempotency-Key'] == "idem_key_123"

            assert response.tx_hash == "0x123abc"

    @pytest.mark.asyncio
    async def test_add_pieces_already_exists_async(self):
        """Test async piece addition when pieces already exist (409 response)."""
        pdp = AsyncPDPServer("http://test.com")

        mock_response = Mock()
        mock_response.status_code = 409
        mock_response.text = "Some pieces already exist"
        mock_response.json.return_value = {"existingPieces": ["cid1"]}

        with patch.object(pdp._client, 'post', return_value=mock_response):
            with pytest.raises(AlreadyExistsError) as exc_info:
                await pdp.add_pieces(123, ["cid1", "cid2"], "extra_data", "idem_key_456")

            assert exc_info.value.existing_resource_id == "123"


class TestStorageContextIdempotency:
    """Test StorageContext idempotency integration."""

    def test_upload_generates_idempotency_key(self):
        """Test that upload operations generate idempotency keys."""
        from pynapse.storage.context import StorageContext
        from pynapse.core.piece import PieceCidInfo

        # Create a storage context
        ctx = StorageContext(
            pdp_endpoint="http://test.com",
            chain=Mock(),
            private_key="0x" + "a" * 64,
            data_set_id=123,
            client_data_set_id=456,
        )

        # Mock the piece CID calculation
        mock_info = PieceCidInfo(
            piece_cid="baga6ea4seaqtest",
            piece_cid_v1="baga6ea4seaqtest",
            payload_size=1000,
            unpadded_piece_size=1000,
            padded_piece_size=1024
        )

        # Mock the PDP server methods
        ctx._pdp.upload_piece = Mock()
        ctx._pdp.wait_for_piece = Mock()
        ctx._pdp.add_pieces = Mock(return_value=Mock(tx_hash="0x123"))

        with patch('pynapse.storage.context.calculate_piece_cid', return_value=mock_info):
            with patch('pynapse.storage.context.sign_add_pieces_extra_data', return_value="signed_data"):
                result = ctx.upload(b"x" * 300)  # Use data larger than MIN_UPLOAD_SIZE (256 bytes)

                # Verify add_pieces was called with idempotency key
                ctx._pdp.add_pieces.assert_called_once()
                call_args = ctx._pdp.add_pieces.call_args
                assert len(call_args[0]) == 3  # data_set_id, piece_cids, extra_data
                assert 'idempotency_key' in call_args[1]
                assert len(call_args[1]['idempotency_key']) == 32

    def test_upload_handles_already_exists(self):
        """Test that upload handles AlreadyExistsError gracefully."""
        from pynapse.storage.context import StorageContext
        from pynapse.core.piece import PieceCidInfo

        # Create a storage context
        ctx = StorageContext(
            pdp_endpoint="http://test.com",
            chain=Mock(),
            private_key="0x" + "a" * 64,
            data_set_id=123,
            client_data_set_id=456,
        )

        # Mock the piece CID calculation
        mock_info = PieceCidInfo(
            piece_cid="baga6ea4seaqtest",
            piece_cid_v1="baga6ea4seaqtest",
            payload_size=1000,
            unpadded_piece_size=1000,
            padded_piece_size=1024
        )

        # Mock the PDP server methods
        ctx._pdp.upload_piece = Mock()
        ctx._pdp.wait_for_piece = Mock()
        ctx._pdp.add_pieces = Mock(side_effect=AlreadyExistsError("Piece already exists"))

        with patch('pynapse.storage.context.calculate_piece_cid', return_value=mock_info):
            with patch('pynapse.storage.context.sign_add_pieces_extra_data', return_value="signed_data"):
                result = ctx.upload(b"x" * 300)  # Use data larger than MIN_UPLOAD_SIZE (256 bytes)

                # Should succeed even though piece already exists
                assert result.piece_cid == mock_info.piece_cid
                assert result.tx_hash is None  # Mock response has no tx_hash


class TestAsyncStorageContextIdempotency:
    """Test AsyncStorageContext idempotency integration."""

    @pytest.mark.asyncio
    async def test_upload_handles_already_exists_async(self):
        """Test that async upload handles AlreadyExistsError gracefully."""
        from pynapse.storage.async_context import AsyncStorageContext
        from pynapse.core.piece import PieceCidInfo

        # Create a storage context
        ctx = AsyncStorageContext(
            pdp_endpoint="http://test.com",
            chain=Mock(),
            private_key="0x" + "a" * 64,
            data_set_id=123,
            client_data_set_id=456,
        )

        # Mock the piece CID calculation
        mock_info = PieceCidInfo(
            piece_cid="baga6ea4seaqtest",
            piece_cid_v1="baga6ea4seaqtest",
            payload_size=1000,
            unpadded_piece_size=1000,
            padded_piece_size=1024
        )

        # Mock the PDP server methods with async coroutines
        async def mock_upload_piece(*args, **kwargs):
            return Mock()

        async def mock_wait_for_piece(*args, **kwargs):
            return None

        async def mock_add_pieces(*args, **kwargs):
            raise AlreadyExistsError("Piece already exists")

        ctx._pdp.upload_piece = mock_upload_piece
        ctx._pdp.wait_for_piece = mock_wait_for_piece
        ctx._pdp.add_pieces = mock_add_pieces

        with patch('pynapse.storage.async_context.calculate_piece_cid', return_value=mock_info):
            with patch('pynapse.storage.async_context.sign_add_pieces_extra_data', return_value="signed_data"):
                result = await ctx.upload(b"x" * 300)  # Use data larger than MIN_UPLOAD_SIZE (256 bytes)

                # Should succeed even though piece already exists
                assert result.piece_cid == mock_info.piece_cid
                assert result.tx_hash is None  # Mock response has no tx_hash


class TestIdempotencyErrorHandling:
    """Test various error conditions and edge cases."""

    def test_idempotency_error_propagation(self):
        """Test that IdempotencyError is properly propagated."""
        pdp = PDPServer("http://test.com")

        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = "Idempotency key conflict - different payload"

        with patch.object(pdp._client, 'post', return_value=mock_response):
            with pytest.raises(IdempotencyError) as exc_info:
                pdp.create_data_set("keeper", "extra_data", "conflicting_key")

            assert "Idempotency key conflict" in str(exc_info.value)

    def test_malformed_409_response(self):
        """Test handling of 409 response without proper JSON structure."""
        pdp = PDPServer("http://test.com")

        mock_response = Mock()
        mock_response.status_code = 409
        mock_response.text = "Conflict - malformed response"
        mock_response.json.side_effect = ValueError("Invalid JSON")

        with patch.object(pdp._client, 'post', return_value=mock_response):
            with pytest.raises(AlreadyExistsError) as exc_info:
                pdp.create_data_set("keeper", "extra_data", "test_key")

            assert exc_info.value.existing_resource_id is None
            assert "malformed response" in str(exc_info.value)

    def test_no_idempotency_key_provided(self):
        """Test behavior when no idempotency key is provided."""
        pdp = PDPServer("http://test.com")

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.headers = {"Location": "/pdp/data-sets/created/0x123abc"}

        with patch.object(pdp._client, 'post', return_value=mock_response) as mock_post:
            response = pdp.create_data_set("keeper", "extra_data")  # No idempotency key

            # Verify no idempotency header was sent
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            headers = call_args[1].get('headers', {})
            assert 'Idempotency-Key' not in headers

            assert response.tx_hash == "0x123abc"