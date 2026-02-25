"""Tests for StorageManager and StorageContext."""
import pytest
from unittest.mock import MagicMock, patch
from dataclasses import dataclass


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


class TestStorageContext:
    """Tests for StorageContext."""

    def test_validate_size_min(self):
        """Test that data below minimum size is rejected."""
        from pynapse.storage.context import StorageContext

        with pytest.raises(ValueError, match="below minimum"):
            StorageContext._validate_size(100, "test")

    def test_validate_size_max(self):
        """Test that data above maximum size is rejected."""
        from pynapse.storage.context import StorageContext

        with pytest.raises(ValueError, match="exceeds maximum"):
            StorageContext._validate_size(300 * 1024 * 1024, "test")

    def test_validate_size_valid(self):
        """Test that valid sizes pass validation."""
        from pynapse.storage.context import StorageContext

        # Should not raise
        StorageContext._validate_size(1000, "test")
        StorageContext._validate_size(254 * 1024 * 1024, "test")

    def test_ping_provider_success(self):
        """Test successful provider ping."""
        from pynapse.storage.context import StorageContext

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=None)
            mock_client.head = MagicMock(return_value=MagicMock(status_code=200))
            mock_client_class.return_value = mock_client

            result = StorageContext._ping_provider("http://test.com")
            assert result is True

    def test_ping_provider_failure(self):
        """Test failed provider ping."""
        from pynapse.storage.context import StorageContext

        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=None)
            mock_client.head = MagicMock(side_effect=Exception("Connection failed"))
            mock_client_class.return_value = mock_client

            result = StorageContext._ping_provider("http://test.com")
            assert result is False


class TestStorageManager:
    """Tests for StorageManager."""

    @pytest.fixture
    def mock_chain(self):
        chain = MagicMock()
        chain.contracts.warm_storage = "0xWarmStorage"
        chain.contracts.warm_storage_state_view = "0xStateView"
        chain.contracts.sp_registry = "0xSPRegistry"
        return chain

    @pytest.fixture
    def mock_warm_storage(self):
        ws = MagicMock()
        ws.get_current_pricing_rates = MagicMock(return_value=[
            1000000,  # price no CDN
            2000000,  # price with CDN
            86400,    # epochs per month
            "0xToken"
        ])
        ws.get_approved_provider_ids = MagicMock(return_value=[1, 2, 3])
        return ws

    @pytest.fixture
    def mock_sp_registry(self):
        sp = MagicMock()
        sp.get_all_active_providers = MagicMock(return_value=[
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
        sp.get_provider = MagicMock(side_effect=lambda pid: MockProviderInfo(
            provider_id=pid,
            service_provider=f"0xProvider{pid}",
            payee=f"0xPayee{pid}",
            name=f"Provider {pid}",
            description="Test provider",
            is_active=True,
        ))
        return sp

    def test_preflight_with_pricing(self, mock_chain, mock_warm_storage, mock_sp_registry):
        """Test preflight cost estimation."""
        from pynapse.storage.manager import StorageManager

        manager = StorageManager(
            chain=mock_chain,
            private_key="0x" + "1" * 64,
            sp_registry=mock_sp_registry,
            warm_storage=mock_warm_storage,
        )

        result = manager.preflight(
            size_bytes=1024 * 1024,  # 1 MiB
            provider_count=1,
            duration_epochs=2880,
        )

        assert result.size_bytes == 1024 * 1024
        assert result.provider_count == 1
        assert len(result.providers) == 1

    def test_select_providers(self, mock_chain, mock_warm_storage, mock_sp_registry):
        """Test provider selection."""
        from pynapse.storage.manager import StorageManager

        manager = StorageManager(
            chain=mock_chain,
            private_key="0x" + "1" * 64,
            sp_registry=mock_sp_registry,
            warm_storage=mock_warm_storage,
        )

        providers = manager.select_providers(count=2)
        assert len(providers) == 2

    def test_get_storage_info(self, mock_chain, mock_warm_storage, mock_sp_registry):
        """Test getting storage service info."""
        from pynapse.storage.manager import StorageManager

        manager = StorageManager(
            chain=mock_chain,
            private_key="0x" + "1" * 64,
            sp_registry=mock_sp_registry,
            warm_storage=mock_warm_storage,
        )

        info = manager.get_storage_info()
        assert info.token_symbol == "USDFC"
        assert len(info.approved_provider_ids) == 3

    def test_context_creation_requires_services(self, mock_chain):
        """Test that context creation requires warm_storage and sp_registry."""
        from pynapse.storage.manager import StorageManager

        manager = StorageManager(
            chain=mock_chain,
            private_key="0x" + "1" * 64,
        )

        with pytest.raises(ValueError, match="warm_storage required"):
            manager.get_context()


class TestChainRetriever:
    """Tests for ChainRetriever."""

    @pytest.fixture
    def mock_warm_storage(self):
        ws = MagicMock()
        ws.get_client_data_sets_with_details = MagicMock(return_value=[
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
        sp = MagicMock()
        sp.get_provider = MagicMock(return_value=MockProviderInfo(
            provider_id=1,
            service_provider="0xProvider",
            payee="0xPayee",
            name="Test Provider",
            description="Test",
            is_active=True,
        ))
        sp.get_provider_by_address = MagicMock(return_value=MockProviderInfo(
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
        mock_with_product = MagicMock()
        mock_with_product.product = mock_product
        mock_with_product.product_capability_values = ["http://pdp.test.com"]
        sp.get_provider_with_product = MagicMock(return_value=mock_with_product)
        return sp

    def test_find_providers(self, mock_warm_storage, mock_sp_registry):
        """Test finding providers for a client."""
        from pynapse.retriever.chain import ChainRetriever

        retriever = ChainRetriever(mock_warm_storage, mock_sp_registry)
        providers = retriever._find_providers("0xClient")

        assert len(providers) == 1
        assert providers[0].provider_id == 1

    def test_find_providers_specific_address(self, mock_warm_storage, mock_sp_registry):
        """Test finding a specific provider by address."""
        from pynapse.retriever.chain import ChainRetriever

        retriever = ChainRetriever(mock_warm_storage, mock_sp_registry)
        providers = retriever._find_providers("0xClient", "0xProvider")

        assert len(providers) == 1
        mock_sp_registry.get_provider_by_address.assert_called_once_with("0xProvider")

    def test_get_pdp_endpoint(self, mock_warm_storage, mock_sp_registry):
        """Test getting PDP endpoint for a provider."""
        from pynapse.retriever.chain import ChainRetriever

        retriever = ChainRetriever(mock_warm_storage, mock_sp_registry)
        endpoint = retriever._get_pdp_endpoint(1)

        assert endpoint == "http://pdp.test.com"


class TestUploadResult:
    """Tests for UploadResult dataclass."""

    def test_upload_result_fields(self):
        """Test that UploadResult has expected fields."""
        from pynapse.storage.context import UploadResult

        result = UploadResult(
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
        from pynapse.storage.context import UploadResult

        result = UploadResult(
            piece_cid="bafk...",
            size=1024,
        )

        assert result.tx_hash is None
        assert result.piece_id is None


class TestPreflightInfo:
    """Tests for PreflightInfo dataclass."""

    def test_preflight_info_fields(self):
        """Test that PreflightInfo has expected fields."""
        from pynapse.storage.manager import PreflightInfo

        info = PreflightInfo(
            size_bytes=1024,
            estimated_cost_per_epoch=100,
            estimated_total_cost=10000,
            duration_epochs=100,
            provider_count=2,
            providers=[1, 2],
        )

        assert info.size_bytes == 1024
        assert info.provider_count == 2
        assert len(info.providers) == 2


class TestResolveByDataSetId:
    """Tests for _resolve_by_data_set_id metadata and provider consistency checks."""

    def _make_mock_sp_registry(self, provider_id=1):
        sp = MagicMock()
        sp.get_provider = MagicMock(return_value=MockProviderInfo(
            provider_id=provider_id,
            service_provider=f"0xProvider{provider_id}",
            payee=f"0xPayee{provider_id}",
            name=f"Provider {provider_id}",
            description="Test provider",
            is_active=True,
        ))
        sp.get_provider_by_address = MagicMock(return_value=MockProviderInfo(
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
        sp.get_provider_with_product = MagicMock(return_value=mock_with_product)
        return sp

    def _make_mock_warm_storage(self, payer="0xClient", provider_id=1, metadata=None):
        ws = MagicMock()
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
        ws.validate_data_set = MagicMock()
        ws.get_data_set = MagicMock(return_value=ds_info)
        ws.get_all_data_set_metadata = MagicMock(return_value=metadata if metadata is not None else {})
        ws.is_provider_approved = MagicMock(return_value=True)
        return ws

    def test_resolve_by_data_set_id_metadata_mismatch_raises(self):
        """Specifying data_set_id + with_cdn=True but dataset has no CDN metadata should raise."""
        from pynapse.storage.context import StorageContext, StorageContextOptions

        ws = self._make_mock_warm_storage(metadata={})
        sp = self._make_mock_sp_registry()

        with pytest.raises(ValueError, match="does not match requested metadata"):
            StorageContext._resolve_by_data_set_id(
                data_set_id=42,
                client_address="0xClient",
                warm_storage=ws,
                sp_registry=sp,
                requested_metadata={"withCDN": ""},
                options=StorageContextOptions(data_set_id=42, with_cdn=True),
            )

    def test_resolve_by_data_set_id_metadata_match_succeeds(self):
        """Specifying data_set_id + metadata that matches the dataset should succeed."""
        from pynapse.storage.context import StorageContext, StorageContextOptions

        ws = self._make_mock_warm_storage(metadata={"withCDN": ""})
        sp = self._make_mock_sp_registry()

        result = StorageContext._resolve_by_data_set_id(
            data_set_id=42,
            client_address="0xClient",
            warm_storage=ws,
            sp_registry=sp,
            requested_metadata={"withCDN": ""},
            options=StorageContextOptions(data_set_id=42, with_cdn=True),
        )
        assert result.data_set_id == 42
        assert result.is_existing is True

    def test_resolve_by_data_set_id_no_metadata_skips_check(self):
        """Specifying data_set_id with no metadata options should succeed regardless of dataset metadata."""
        from pynapse.storage.context import StorageContext, StorageContextOptions

        ws = self._make_mock_warm_storage(metadata={"withCDN": ""})
        sp = self._make_mock_sp_registry()

        result = StorageContext._resolve_by_data_set_id(
            data_set_id=42,
            client_address="0xClient",
            warm_storage=ws,
            sp_registry=sp,
            requested_metadata={},
            options=StorageContextOptions(data_set_id=42),
        )
        assert result.data_set_id == 42
        assert result.is_existing is True

    def test_resolve_by_data_set_id_provider_id_mismatch_raises(self):
        """Specifying data_set_id + provider_id that doesn't match the dataset's provider should raise."""
        from pynapse.storage.context import StorageContext, StorageContextOptions

        ws = self._make_mock_warm_storage(provider_id=1)
        sp = self._make_mock_sp_registry(provider_id=1)

        with pytest.raises(ValueError, match="belongs to provider 1.*not the requested provider 99"):
            StorageContext._resolve_by_data_set_id(
                data_set_id=42,
                client_address="0xClient",
                warm_storage=ws,
                sp_registry=sp,
                requested_metadata={},
                options=StorageContextOptions(data_set_id=42, provider_id=99),
            )

    def test_resolve_by_provider_id_not_approved_raises(self):
        """Specifying a provider that is not approved should raise early."""
        from pynapse.storage.context import StorageContext

        ws = MagicMock()
        ws.is_provider_approved = MagicMock(return_value=False)
        sp = self._make_mock_sp_registry(provider_id=1)

        with pytest.raises(ValueError, match="not approved for Warm Storage"):
            StorageContext._resolve_by_provider_id(
                provider_id=1,
                client_address="0xClient",
                warm_storage=ws,
                sp_registry=sp,
                requested_metadata={},
                force_create=False,
            )


class TestStoragePreflightChecks:
    """Tests for preflight checks before upload/add operations."""

    @pytest.fixture
    def mock_chain(self):
        chain = MagicMock()
        chain.contracts.warm_storage = "0xWarmStorage"
        chain.contracts.warm_storage_state_view = "0xStateView"
        chain.contracts.sp_registry = "0xSPRegistry"
        return chain

    @pytest.fixture
    def mock_sp_registry(self):
        sp = MagicMock()
        sp.get_all_active_providers = MagicMock(return_value=[
            MockProviderInfo(
                provider_id=1,
                service_provider="0xProvider1",
                payee="0xPayee1",
                name="Provider 1",
                description="Test provider",
                is_active=True,
            ),
        ])
        sp.get_provider = MagicMock(side_effect=lambda pid: MockProviderInfo(
            provider_id=pid,
            service_provider=f"0xProvider{pid}",
            payee=f"0xPayee{pid}",
            name=f"Provider {pid}",
            description="Test provider",
            is_active=True,
        ))
        return sp

    def test_upload_preflight_blocks_insufficient_allowance(self, mock_chain, mock_sp_registry):
        """Upload should fail early when allowances are insufficient."""
        from pynapse.storage.manager import StorageManager

        mock_warm_storage = MagicMock()
        mock_warm_storage.get_current_pricing_rates = MagicMock(return_value=[
            2000000,  # price no CDN
            2000000,  # price with CDN
            1,        # epochs per month
            "0xToken"
        ])

        payments_service = MagicMock()
        payments_service.service_approval = MagicMock(return_value=MagicMock(
            is_approved=True,
            rate_allowance=0,
            lockup_allowance=0,
        ))

        manager = StorageManager(
            chain=mock_chain,
            private_key="0x" + "1" * 64,
            sp_registry=mock_sp_registry,
            warm_storage=mock_warm_storage,
        )

        context = MagicMock()
        with pytest.raises(ValueError, match="Insufficient allowances"):
            manager.upload(
                data=b"x" * (1024 * 1024),
                context=context,
                payments_service=payments_service,
            )
        context.upload.assert_not_called()


class TestStoragePricing:
    """Tests for StoragePricing dataclass."""

    def test_storage_pricing_fields(self):
        """Test that StoragePricing has expected fields."""
        from pynapse.storage.manager import StoragePricing

        pricing = StoragePricing(
            per_tib_per_month=1000000,
            per_tib_per_day=33333,
            per_tib_per_epoch=115,
        )

        assert pricing.per_tib_per_month == 1000000
        assert pricing.per_tib_per_day == 33333
        assert pricing.per_tib_per_epoch == 115
