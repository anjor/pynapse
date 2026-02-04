from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from eth_account import Account
from web3 import AsyncWeb3, Web3

from pynapse.contracts import SERVICE_PROVIDER_REGISTRY_ABI
from pynapse.core.chains import Chain
from .pdp_capabilities import encode_pdp_capabilities
from .types import PDPOffering, ProviderInfo, ProviderRegistrationInfo, ProviderWithProduct, ServiceProduct


class SyncSPRegistryService:
    def __init__(self, web3: Web3, chain: Chain, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._private_key = private_key
        self._contract = web3.eth.contract(address=chain.contracts.sp_registry, abi=SERVICE_PROVIDER_REGISTRY_ABI)

    def get_provider(self, provider_id: int) -> ProviderInfo:
        info = self._contract.functions.getProvider(provider_id).call()
        provider_id = int(info[0])
        inner = info[1]
        return ProviderInfo(
            provider_id=provider_id,
            service_provider=inner[0],
            payee=inner[1],
            name=inner[2],
            description=inner[3],
            is_active=inner[4],
        )

    def get_provider_by_address(self, address: str) -> Optional[ProviderInfo]:
        info = self._contract.functions.getProviderByAddress(address).call()
        provider_id = int(info[0])
        if provider_id == 0:
            return None
        inner = info[1]
        return ProviderInfo(
            provider_id=provider_id,
            service_provider=inner[0],
            payee=inner[1],
            name=inner[2],
            description=inner[3],
            is_active=inner[4],
        )

    def get_provider_id_by_address(self, address: str) -> int:
        return int(self._contract.functions.getProviderIdByAddress(address).call())

    def get_provider_count(self) -> int:
        return int(self._contract.functions.getProviderCount().call())

    def is_provider_active(self, provider_id: int) -> bool:
        return bool(self._contract.functions.isProviderActive(provider_id).call())

    def is_registered_provider(self, address: str) -> bool:
        return bool(self._contract.functions.isRegisteredProvider(address).call())

    def get_provider_with_product(self, provider_id: int, product_type: int) -> ProviderWithProduct:
        data = self._contract.functions.getProviderWithProduct(provider_id, product_type).call()
        provider_id = int(data[0])
        provider_info_tuple = data[1]
        product_tuple = data[2]
        values = data[3]

        provider_info = ProviderInfo(
            provider_id=provider_id,
            service_provider=provider_info_tuple[0],
            payee=provider_info_tuple[1],
            name=provider_info_tuple[2],
            description=provider_info_tuple[3],
            is_active=provider_info_tuple[4],
        )
        product = ServiceProduct(
            product_type=int(product_tuple[0]),
            capability_keys=list(product_tuple[1]),
            is_active=product_tuple[2],
        )
        return ProviderWithProduct(
            provider_id=provider_id,
            provider_info=provider_info,
            product=product,
            product_capability_values=list(values),
        )

    def get_providers_by_product_type(self, product_type: int, only_active: bool, offset: int, limit: int):
        result = self._contract.functions.getProvidersByProductType(product_type, only_active, offset, limit).call()
        providers = []
        for item in result[0]:
            providers.append(self._convert_provider_with_product(item))
        return providers, bool(result[1])

    def get_all_active_providers(self) -> List[ProviderInfo]:
        providers: List[ProviderInfo] = []
        limit = 50
        offset = 0
        has_more = True
        while has_more:
            result = self._contract.functions.getAllActiveProviders(offset, limit).call()
            for info in result[0]:
                providers.append(
                    ProviderInfo(
                        provider_id=int(info[0]),
                        service_provider=info[1][0],
                        payee=info[1][1],
                        name=info[1][2],
                        description=info[1][3],
                        is_active=info[1][4],
                    )
                )
            has_more = bool(result[1])
            offset += limit
        return providers

    def register_provider(self, account: str, info: ProviderRegistrationInfo, product_type: int = 1) -> str:
        if not self._private_key:
            raise ValueError("private_key required for register_provider")
        keys, values = encode_pdp_capabilities(info.pdp_offering, info.capabilities)
        txn = self._contract.functions.registerProvider(
            info.payee,
            info.name,
            info.description,
            product_type,
            keys,
            values,
        ).build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def update_provider_info(self, account: str, name: str, description: str) -> str:
        if not self._private_key:
            raise ValueError("private_key required for update_provider_info")
        txn = self._contract.functions.updateProviderInfo(name, description).build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def remove_provider(self, account: str) -> str:
        if not self._private_key:
            raise ValueError("private_key required for remove_provider")
        txn = self._contract.functions.removeProvider().build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def add_product(self, account: str, product_type: int, pdp_offering: PDPOffering, capabilities: Optional[dict] = None) -> str:
        if not self._private_key:
            raise ValueError("private_key required for add_product")
        keys, values = encode_pdp_capabilities(pdp_offering, capabilities)
        txn = self._contract.functions.addProduct(product_type, keys, values).build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def update_product(self, account: str, product_type: int, pdp_offering: PDPOffering, capabilities: Optional[dict] = None) -> str:
        if not self._private_key:
            raise ValueError("private_key required for update_product")
        keys, values = encode_pdp_capabilities(pdp_offering, capabilities)
        txn = self._contract.functions.updateProduct(product_type, keys, values).build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def remove_product(self, account: str, product_type: int) -> str:
        if not self._private_key:
            raise ValueError("private_key required for remove_product")
        txn = self._contract.functions.removeProduct(product_type).build_transaction(
            {
                "from": account,
                "nonce": self._web3.eth.get_transaction_count(account),
            }
        )
        signed = self._web3.eth.account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def _convert_provider_with_product(self, data) -> ProviderWithProduct:
        provider_id = int(data[0])
        provider_info_tuple = data[1]
        product_tuple = data[2]
        values = data[3]
        provider_info = ProviderInfo(
            provider_id=provider_id,
            service_provider=provider_info_tuple[0],
            payee=provider_info_tuple[1],
            name=provider_info_tuple[2],
            description=provider_info_tuple[3],
            is_active=provider_info_tuple[4],
        )
        product = ServiceProduct(
            product_type=int(product_tuple[0]),
            capability_keys=list(product_tuple[1]),
            is_active=product_tuple[2],
        )
        return ProviderWithProduct(
            provider_id=provider_id,
            provider_info=provider_info,
            product=product,
            product_capability_values=list(values),
        )


class AsyncSPRegistryService:
    def __init__(self, web3: AsyncWeb3, chain: Chain, private_key: Optional[str] = None) -> None:
        self._web3 = web3
        self._chain = chain
        self._private_key = private_key
        self._contract = web3.eth.contract(address=chain.contracts.sp_registry, abi=SERVICE_PROVIDER_REGISTRY_ABI)

    async def get_provider(self, provider_id: int) -> ProviderInfo:
        info = await self._contract.functions.getProvider(provider_id).call()
        provider_id = int(info[0])
        inner = info[1]
        return ProviderInfo(
            provider_id=provider_id,
            service_provider=inner[0],
            payee=inner[1],
            name=inner[2],
            description=inner[3],
            is_active=inner[4],
        )

    async def get_provider_by_address(self, address: str) -> Optional[ProviderInfo]:
        info = await self._contract.functions.getProviderByAddress(address).call()
        provider_id = int(info[0])
        if provider_id == 0:
            return None
        inner = info[1]
        return ProviderInfo(
            provider_id=provider_id,
            service_provider=inner[0],
            payee=inner[1],
            name=inner[2],
            description=inner[3],
            is_active=inner[4],
        )

    async def get_provider_id_by_address(self, address: str) -> int:
        return int(await self._contract.functions.getProviderIdByAddress(address).call())

    async def get_provider_count(self) -> int:
        return int(await self._contract.functions.getProviderCount().call())

    async def is_provider_active(self, provider_id: int) -> bool:
        return bool(await self._contract.functions.isProviderActive(provider_id).call())

    async def is_registered_provider(self, address: str) -> bool:
        return bool(await self._contract.functions.isRegisteredProvider(address).call())

    async def get_provider_with_product(self, provider_id: int, product_type: int) -> ProviderWithProduct:
        data = await self._contract.functions.getProviderWithProduct(provider_id, product_type).call()
        provider_id = int(data[0])
        provider_info_tuple = data[1]
        product_tuple = data[2]
        values = data[3]

        provider_info = ProviderInfo(
            provider_id=provider_id,
            service_provider=provider_info_tuple[0],
            payee=provider_info_tuple[1],
            name=provider_info_tuple[2],
            description=provider_info_tuple[3],
            is_active=provider_info_tuple[4],
        )
        product = ServiceProduct(
            product_type=int(product_tuple[0]),
            capability_keys=list(product_tuple[1]),
            is_active=product_tuple[2],
        )
        return ProviderWithProduct(
            provider_id=provider_id,
            provider_info=provider_info,
            product=product,
            product_capability_values=list(values),
        )

    async def get_providers_by_product_type(self, product_type: int, only_active: bool, offset: int, limit: int):
        result = await self._contract.functions.getProvidersByProductType(product_type, only_active, offset, limit).call()
        providers = []
        for item in result[0]:
            providers.append(self._convert_provider_with_product(item))
        return providers, bool(result[1])

    async def get_all_active_providers(self) -> List[ProviderInfo]:
        providers: List[ProviderInfo] = []
        limit = 50
        offset = 0
        has_more = True
        while has_more:
            result = await self._contract.functions.getAllActiveProviders(offset, limit).call()
            for info in result[0]:
                providers.append(
                    ProviderInfo(
                        provider_id=int(info[0]),
                        service_provider=info[1][0],
                        payee=info[1][1],
                        name=info[1][2],
                        description=info[1][3],
                        is_active=info[1][4],
                    )
                )
            has_more = bool(result[1])
            offset += limit
        return providers

    async def register_provider(self, account: str, info: ProviderRegistrationInfo, product_type: int = 1) -> str:
        if not self._private_key:
            raise ValueError("private_key required for register_provider")
        keys, values = encode_pdp_capabilities(info.pdp_offering, info.capabilities)
        txn = await self._contract.functions.registerProvider(
            info.payee,
            info.name,
            info.description,
            product_type,
            keys,
            values,
        ).build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def update_provider_info(self, account: str, name: str, description: str) -> str:
        if not self._private_key:
            raise ValueError("private_key required for update_provider_info")
        txn = await self._contract.functions.updateProviderInfo(name, description).build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def remove_provider(self, account: str) -> str:
        if not self._private_key:
            raise ValueError("private_key required for remove_provider")
        txn = await self._contract.functions.removeProvider().build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def add_product(self, account: str, product_type: int, pdp_offering: PDPOffering, capabilities: Optional[dict] = None) -> str:
        if not self._private_key:
            raise ValueError("private_key required for add_product")
        keys, values = encode_pdp_capabilities(pdp_offering, capabilities)
        txn = await self._contract.functions.addProduct(product_type, keys, values).build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def update_product(self, account: str, product_type: int, pdp_offering: PDPOffering, capabilities: Optional[dict] = None) -> str:
        if not self._private_key:
            raise ValueError("private_key required for update_product")
        keys, values = encode_pdp_capabilities(pdp_offering, capabilities)
        txn = await self._contract.functions.updateProduct(product_type, keys, values).build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    async def remove_product(self, account: str, product_type: int) -> str:
        if not self._private_key:
            raise ValueError("private_key required for remove_product")
        txn = await self._contract.functions.removeProduct(product_type).build_transaction(
            {
                "from": account,
                "nonce": await self._web3.eth.get_transaction_count(account),
            }
        )
        signed = Account.sign_transaction(txn, private_key=self._private_key)
        tx_hash = await self._web3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()

    def _convert_provider_with_product(self, data) -> ProviderWithProduct:
        provider_id = int(data[0])
        provider_info_tuple = data[1]
        product_tuple = data[2]
        values = data[3]
        provider_info = ProviderInfo(
            provider_id=provider_id,
            service_provider=provider_info_tuple[0],
            payee=provider_info_tuple[1],
            name=provider_info_tuple[2],
            description=provider_info_tuple[3],
            is_active=provider_info_tuple[4],
        )
        product = ServiceProduct(
            product_type=int(product_tuple[0]),
            capability_keys=list(product_tuple[1]),
            is_active=product_tuple[2],
        )
        return ProviderWithProduct(
            provider_id=provider_id,
            provider_info=provider_info,
            product=product,
            product_capability_values=list(values),
        )
