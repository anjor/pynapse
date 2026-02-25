"""Tests for async storage manager and context."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass

# Skip all tests if required dependencies are missing
pytest.importorskip("httpx")


@dataclass
class MockProviderInfo:
    provider_id: int
    service_provider: str
    payee: str
    name: str
    description: str
    is_active: bool


@dataclass
class MockDataSetInfo:
    pdp_rail_id: int
    cache_miss_rail_id: int
    cdn_rail_id: int
    payer: str
    payee: str
    service_provider: str
    commission_bps: int
    client_data_set_id: int
    pdp_end_epoch: int
    provider_id: int
    data_set_id: int


@dataclass
class MockEnhancedDataSetInfo(MockDataSetInfo):
    active_piece_count: int = 0
    is_live: bool = True
    is_managed: bool = True
    with_cdn: bool = False
    metadata: dict = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class MockProviderWithProduct:
    provider_id: int
    provider_info: MockProviderInfo
    product: MagicMock
    product_capability_values: list


class TestAsyncStorageContext:
    """Tests for AsyncStorageContext."""

    @pytest.mark.asyncio
    async def test_validate_size_min(self):
        """Test that data below minimum size is rejected."""
        from pynapse.storage.async_context import AsyncStorageContext

        with pytest.raises(ValueError, match="below minimum"):
            AsyncStorageContext._validate_size(100, "test")

    @pytest.mark.asyncio
    async def test_validate_size_max(self):
        """Test that data above maximum size is rejected."""
        from pynapse.storage.async_context import AsyncStorageContext

        with pytest.raises(ValueError, match="exceeds maximum"):
            AsyncStorageContext._validate_size(300 * 1024 * 1024, "test")

    @pytest.mark.asyncio
    async def test_validate_size_valid(self):
        """Test that valid sizes pass validation."""
        from pynapse.storage.async_context import AsyncStorageContext

        # Should not raise
        AsyncStorageContext._validate_size(1000, "test")
        AsyncStorageContext._validate_size(254 * 1024 * 1024, "test")

    @pytest.mark.asyncio
    async def test_ping_provider_success(self):
        """Test successful provider ping."""
        from pynapse.storage.async_context import AsyncStorageContext

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client.head = AsyncMock(return_value=MagicMock(status_code=200))
            mock_client_class.return_value = mock_client

            result = await AsyncStorageContext._ping_provider("http://test.com")
            assert result is True

    @pytest.mark.asyncio
    async def test_ping_provider_failure(self):
        """Test failed provider ping."""
        from pynapse.storage.async_context import AsyncStorageContext

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client.head = AsyncMock(side_effect=Exception("Connection failed"))
            mock_client_class.return_value = mock_client

            result = await AsyncStorageContext._ping_provider("http://test.com")
            assert result is False


class TestAsyncStorageManager:
    """Tests for AsyncStorageManager."""

    @pytest.fixture
    def mock_chain(self):
        chain = MagicMock()
        chain.contracts.warm_storage = "0xWarmStorage"
        chain.contracts.warm_storage_state_view = "0xStateView"
        chain.contracts.sp_registry = "0xSPRegistry"
        return chain

    @pytest.fixture
    def mock_warm_storage(self):
        ws = AsyncMock()
        ws.get_current_pricing_rates = AsyncMock(return_value=[
            1000000,  # price no CDN
            2000000,  # price with CDN
            86400,    # epochs per month
            "0xToken"
        ])
        ws.get_approved_provider_ids = AsyncMock(return_value=[1, 2, 3])
        return ws

    @pytest.fixture
    def mock_sp_registry(self):
        sp = AsyncMock()
        sp.get_all_active_providers = AsyncMock(return_value=[
            MockProviderInfo(
                provider_id=1,
                service_provider="0xProvider1",
                payee="0xPayee1",
                name="Provider 1",
                description="Test provider",
                is_active=True,
            ),
            MockProviderInfo(
                provider_id=2,
                service_provider="0xProvider2",
                payee="0xPayee2",
                name="Provider 2",
                description="Test provider 2",
                is_active=True,
            ),
        ])
        sp.get_provider = AsyncMock(side_effect=lambda pid: MockProviderInfo(
            provider_id=pid,
            service_provider=f"0xProvider{pid}",
            payee=f"0xPayee{pid}",
            name=f"Provider {pid}",
            description="Test provider",
            is_active=True,
        ))
        return sp

    @pytest.mark.asyncio
    async def test_preflight_with_pricing(self, mock_chain, mock_warm_storage, mock_sp_registry):
        """Test preflight cost estimation."""
        from pynapse.storage.async_manager import AsyncStorageManager

        manager = AsyncStorageManager(
            chain=mock_chain,
            private_key="0x" + "1" * 64,
            sp_registry=mock_sp_registry,
            warm_storage=mock_warm_storage,
        )

        result = await manager.preflight(
            size_bytes=1024 * 1024,  # 1 MiB
            provider_count=1,
            duration_epochs=2880,
        )

        assert result.size_bytes == 1024 * 1024
        assert result.provider_count == 1
        assert len(result.providers) == 1

    @pytest.mark.asyncio
    async def test_select_providers(self, mock_chain, mock_warm_storage, mock_sp_registry):
        """Test provider selection."""
        from pynapse.storage.async_manager import AsyncStorageManager

        manager = AsyncStorageManager(
            chain=mock_chain,
            private_key="0x" + "1" * 64,
            sp_registry=mock_sp_registry,
            warm_storage=mock_warm_storage,
        )

        providers = await manager.select_providers(count=2)
        assert len(providers) == 2

    @pytest.mark.asyncio
    async def test_get_storage_info(self, mock_chain, mock_warm_storage, mock_sp_registry):
        """Test getting storage service info."""
        from pynapse.storage.async_manager import AsyncStorageManager

        manager = AsyncStorageManager(
            chain=mock_chain,
            private_key="0x" + "1" * 64,
            sp_registry=mock_sp_registry,
            warm_storage=mock_warm_storage,
        )

        info = await manager.get_storage_info()
        assert info.token_symbol == "USDFC"
        assert len(info.approved_provider_ids) == 3

    @pytest.mark.asyncio
    async def test_context_creation_requires_services(self, mock_chain):
        """Test that context creation requires warm_storage and sp_registry."""
        from pynapse.storage.async_manager import AsyncStorageManager

        manager = AsyncStorageManager(
            chain=mock_chain,
            private_key="0x" + "1" * 64,
        )

        with pytest.raises(ValueError, match="warm_storage required"):
            await manager.get_context()


class TestAsyncChainRetriever:
    """Tests for AsyncChainRetriever."""

    @pytest.fixture
    def mock_warm_storage(self):
        ws = AsyncMock()
        ws.get_client_data_sets_with_details = AsyncMock(return_value=[
            MockEnhancedDataSetInfo(
                pdp_rail_id=1,
                cache_miss_rail_id=0,
                cdn_rail_id=0,
                payer="0xClient",
                payee="0xPayee",
                service_provider="0xProvider",
                commission_bps=0,
                client_data_set_id=1,
                pdp_end_epoch=0,
                provider_id=1,
                data_set_id=1,
                active_piece_count=10,
                is_live=True,
                is_managed=True,
            ),
        ])
        return ws

    @pytest.fixture
    def mock_sp_registry(self):
        sp = AsyncMock()
        sp.get_provider = AsyncMock(return_value=MockProviderInfo(
            provider_id=1,
            service_provider="0xProvider",
            payee="0xPayee",
            name="Test Provider",
            description="Test",
            is_active=True,
        ))
        sp.get_provider_by_address = AsyncMock(return_value=MockProviderInfo(
            provider_id=1,
            service_provider="0xProvider",
            payee="0xPayee",
            name="Test Provider",
            description="Test",
            is_active=True,
        ))

        # Mock product with serviceURL
        mock_product = MagicMock()
        mock_product.capability_keys = ["serviceURL"]
        sp.get_provider_with_product = AsyncMock(return_value=MockProviderWithProduct(
            provider_id=1,
            provider_info=MagicMock(),
            product=mock_product,
            product_capability_values=["http://pdp.test.com"],
        ))
        return sp

    @pytest.mark.asyncio
    async def test_find_providers(self, mock_warm_storage, mock_sp_registry):
        """Test finding providers for a client."""
        from pynapse.retriever.async_chain import AsyncChainRetriever

        retriever = AsyncChainRetriever(mock_warm_storage, mock_sp_registry)
        providers = await retriever._find_providers("0xClient")

        assert len(providers) == 1
        assert providers[0].provider_id == 1

    @pytest.mark.asyncio
    async def test_find_providers_specific_address(self, mock_warm_storage, mock_sp_registry):
        """Test finding a specific provider by address."""
        from pynapse.retriever.async_chain import AsyncChainRetriever

        retriever = AsyncChainRetriever(mock_warm_storage, mock_sp_registry)
        providers = await retriever._find_providers("0xClient", "0xProvider")

        assert len(providers) == 1
        mock_sp_registry.get_provider_by_address.assert_called_once_with("0xProvider")

    @pytest.mark.asyncio
    async def test_get_pdp_endpoint(self, mock_warm_storage, mock_sp_registry):
        """Test getting PDP endpoint for a provider."""
        from pynapse.retriever.async_chain import AsyncChainRetriever

        retriever = AsyncChainRetriever(mock_warm_storage, mock_sp_registry)
        endpoint = await retriever._get_pdp_endpoint(1)

        assert endpoint == "http://pdp.test.com"


class TestAsyncResolveByDataSetId:
    """Tests for async _resolve_by_data_set_id metadata and provider consistency checks."""

    def _make_mock_sp_registry(self, provider_id=1):
        sp = AsyncMock()
        sp.get_provider = AsyncMock(return_value=MockProviderInfo(
            provider_id=provider_id,
            service_provider=f"0xProvider{provider_id}",
            payee=f"0xPayee{provider_id}",
            name=f"Provider {provider_id}",
            description="Test provider",
            is_active=True,
        ))
        sp.get_provider_by_address = AsyncMock(return_value=MockProviderInfo(
            provider_id=provider_id,
            service_provider=f"0xProvider{provider_id}",
            payee=f"0xPayee{provider_id}",
            name=f"Provider {provider_id}",
            description="Test provider",
            is_active=True,
        ))
        mock_product = MagicMock()
        mock_product.capability_keys = ["serviceURL"]
        mock_with_product = MagicMock()
        mock_with_product.product = mock_product
        mock_with_product.product_capability_values = ["http://pdp.test.com"]
        sp.get_provider_with_product = AsyncMock(return_value=mock_with_product)
        return sp

    def _make_mock_warm_storage(self, payer="0xClient", provider_id=1, metadata=None):
        ws = AsyncMock()
        ds_info = MockDataSetInfo(
            pdp_rail_id=1,
            cache_miss_rail_id=0,
            cdn_rail_id=0,
            payer=payer,
            payee="0xPayee",
            service_provider=f"0xProvider{provider_id}",
            commission_bps=0,
            client_data_set_id=1,
            pdp_end_epoch=0,
            provider_id=provider_id,
            data_set_id=42,
        )
        ws.validate_data_set = AsyncMock()
        ws.get_data_set = AsyncMock(return_value=ds_info)
        ws.get_all_data_set_metadata = AsyncMock(return_value=metadata if metadata is not None else {})
        ws.is_provider_approved = AsyncMock(return_value=True)
        return ws

    @pytest.mark.asyncio
    async def test_resolve_by_data_set_id_metadata_mismatch_raises(self):
        """Specifying data_set_id + with_cdn=True but dataset has no CDN metadata should raise."""
        from pynapse.storage.async_context import AsyncStorageContext, AsyncStorageContextOptions

        ws = self._make_mock_warm_storage(metadata={})
        sp = self._make_mock_sp_registry()

        with pytest.raises(ValueError, match="does not match requested metadata"):
            await AsyncStorageContext._resolve_by_data_set_id(
                data_set_id=42,
                client_address="0xClient",
                warm_storage=ws,
                sp_registry=sp,
                requested_metadata={"withCDN": ""},
                options=AsyncStorageContextOptions(data_set_id=42, with_cdn=True),
            )

    @pytest.mark.asyncio
    async def test_resolve_by_data_set_id_metadata_match_succeeds(self):
        """Specifying data_set_id + metadata that matches the dataset should succeed."""
        from pynapse.storage.async_context import AsyncStorageContext, AsyncStorageContextOptions

        ws = self._make_mock_warm_storage(metadata={"withCDN": ""})
        sp = self._make_mock_sp_registry()

        result = await AsyncStorageContext._resolve_by_data_set_id(
            data_set_id=42,
            client_address="0xClient",
            warm_storage=ws,
            sp_registry=sp,
            requested_metadata={"withCDN": ""},
            options=AsyncStorageContextOptions(data_set_id=42, with_cdn=True),
        )
        assert result.data_set_id == 42
        assert result.is_existing is True

    @pytest.mark.asyncio
    async def test_resolve_by_data_set_id_no_metadata_skips_check(self):
        """Specifying data_set_id with no metadata options should succeed regardless of dataset metadata."""
        from pynapse.storage.async_context import AsyncStorageContext, AsyncStorageContextOptions

        ws = self._make_mock_warm_storage(metadata={"withCDN": ""})
        sp = self._make_mock_sp_registry()

        result = await AsyncStorageContext._resolve_by_data_set_id(
            data_set_id=42,
            client_address="0xClient",
            warm_storage=ws,
            sp_registry=sp,
            requested_metadata={},
            options=AsyncStorageContextOptions(data_set_id=42),
        )
        assert result.data_set_id == 42
        assert result.is_existing is True

    @pytest.mark.asyncio
    async def test_resolve_by_data_set_id_provider_id_mismatch_raises(self):
        """Specifying data_set_id + provider_id that doesn't match the dataset's provider should raise."""
        from pynapse.storage.async_context import AsyncStorageContext, AsyncStorageContextOptions

        ws = self._make_mock_warm_storage(provider_id=1)
        sp = self._make_mock_sp_registry(provider_id=1)

        with pytest.raises(ValueError, match="belongs to provider 1.*not the requested provider 99"):
            await AsyncStorageContext._resolve_by_data_set_id(
                data_set_id=42,
                client_address="0xClient",
                warm_storage=ws,
                sp_registry=sp,
                requested_metadata={},
                options=AsyncStorageContextOptions(data_set_id=42, provider_id=99),
            )

    @pytest.mark.asyncio
    async def test_resolve_by_provider_id_not_approved_raises(self):
        """Specifying a provider that is not approved should raise early."""
        from pynapse.storage.async_context import AsyncStorageContext

        ws = AsyncMock()
        ws.is_provider_approved = AsyncMock(return_value=False)
        sp = self._make_mock_sp_registry(provider_id=1)

        with pytest.raises(ValueError, match="not approved for Warm Storage"):
            await AsyncStorageContext._resolve_by_provider_id(
                provider_id=1,
                client_address="0xClient",
                warm_storage=ws,
                sp_registry=sp,
                requested_metadata={},
                force_create=False,
            )


class TestAsyncStoragePreflightChecks:
    """Tests for async preflight checks before upload/add operations."""

    @pytest.fixture
    def mock_chain(self):
        chain = MagicMock()
        chain.contracts.warm_storage = "0xWarmStorage"
        chain.contracts.warm_storage_state_view = "0xStateView"
        chain.contracts.sp_registry = "0xSPRegistry"
        return chain

    @pytest.fixture
    def mock_sp_registry(self):
        sp = AsyncMock()
        sp.get_all_active_providers = AsyncMock(return_value=[
            MockProviderInfo(
                provider_id=1,
                service_provider="0xProvider1",
                payee="0xPayee1",
                name="Provider 1",
                description="Test provider",
                is_active=True,
            ),
        ])
        sp.get_provider = AsyncMock(side_effect=lambda pid: MockProviderInfo(
            provider_id=pid,
            service_provider=f"0xProvider{pid}",
            payee=f"0xPayee{pid}",
            name=f"Provider {pid}",
            description="Test provider",
            is_active=True,
        ))
        return sp

    @pytest.mark.asyncio
    async def test_upload_preflight_blocks_insufficient_allowance(self, mock_chain, mock_sp_registry):
        """Upload should fail early when allowances are insufficient."""
        from pynapse.storage.async_manager import AsyncStorageManager

        mock_warm_storage = AsyncMock()
        mock_warm_storage.get_current_pricing_rates = AsyncMock(return_value=[
            2000000,  # price no CDN
            2000000,  # price with CDN
            1,        # epochs per month
            "0xToken"
        ])

        payments_service = AsyncMock()
        payments_service.service_approval = AsyncMock(return_value=MagicMock(
            is_approved=True,
            rate_allowance=0,
            lockup_allowance=0,
        ))

        manager = AsyncStorageManager(
            chain=mock_chain,
            private_key="0x" + "1" * 64,
            sp_registry=mock_sp_registry,
            warm_storage=mock_warm_storage,
        )

        context = AsyncMock()
        with pytest.raises(ValueError, match="Insufficient allowances"):
            await manager.upload(
                data=b"x" * (1024 * 1024),
                context=context,
                payments_service=payments_service,
            )
        context.upload.assert_not_called()


class TestAsyncUploadResult:
    """Tests for AsyncUploadResult dataclass."""

    def test_upload_result_fields(self):
        """Test that AsyncUploadResult has expected fields."""
        from pynapse.storage.async_context import AsyncUploadResult

        result = AsyncUploadResult(
            piece_cid="bafk...",
            size=1024,
            tx_hash="0x123",
            piece_id=1,
        )

        assert result.piece_cid == "bafk..."
        assert result.size == 1024
        assert result.tx_hash == "0x123"
        assert result.piece_id == 1

    def test_upload_result_optional_fields(self):
        """Test that optional fields can be None."""
        from pynapse.storage.async_context import AsyncUploadResult

        result = AsyncUploadResult(
            piece_cid="bafk...",
            size=1024,
        )

        assert result.tx_hash is None
        assert result.piece_id is None
