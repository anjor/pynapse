#!/usr/bin/env python3
"""
pynapse Demo - Test end-to-end storage on Filecoin Calibration testnet
"""
import asyncio
import os
from pynapse import AsyncSynapse

# Demo wallet (Calibration testnet only - no real value)
DEMO_PRIVATE_KEY = "0x7666381bff20b2f0819c77d3846b1d22637be4996012aaaff25e8b93f73a6985"
RPC_URL = "https://api.calibration.node.glif.io/rpc/v1"


async def main():
    print("=" * 60)
    print("pynapse Demo - Filecoin Calibration Testnet")
    print("=" * 60)
    
    # 1. Connect to network
    print("\n[1/6] Connecting to Calibration testnet...")
    synapse = await AsyncSynapse.create(
        rpc_url=RPC_URL,
        chain="calibration",
        private_key=DEMO_PRIVATE_KEY
    )
    print(f"  ✓ Connected as {synapse.account}")
    print(f"  ✓ Chain: {synapse.chain.name} (ID: {synapse.chain.id})")

    # 2. Check balances
    print("\n[2/6] Checking balances...")
    try:
        usdfc_balance = await synapse.payments.balance()
        print(f"  ✓ USDFC balance: {usdfc_balance / 1e6:.2f} USDFC")
    except Exception as e:
        print(f"  ⚠ Could not fetch USDFC balance: {e}")
        usdfc_balance = 0
    
    try:
        fil_balance = await synapse.payments.wallet_balance()
        print(f"  ✓ FIL balance: {fil_balance / 1e18:.4f} tFIL")
    except Exception as e:
        print(f"  ⚠ Could not fetch FIL balance: {e}")
        fil_balance = 0

    # 3. Get provider info
    print("\n[3/6] Fetching storage providers...")
    try:
        info = await synapse.storage.get_storage_info()
        providers = getattr(info, 'providers', []) if hasattr(info, 'providers') else []
        print(f"  ✓ Found {len(providers)} approved providers")
        for i, p in enumerate(providers[:3]):
            pid = getattr(p, 'id', 'N/A') if hasattr(p, 'id') else p.get('id', 'N/A')
            paddr = getattr(p, 'address', 'N/A') if hasattr(p, 'address') else p.get('address', 'N/A')
            print(f"    - Provider {pid}: {str(paddr)[:20]}...")
    except Exception as e:
        print(f"  ⚠ Could not fetch storage info: {e}")

    # 4. Check if we can proceed with upload
    if fil_balance == 0:
        print("\n[!] No tFIL balance - cannot proceed with upload demo")
        print("    Fund the wallet at: https://faucet.calibnet.chainsafe-fil.io/")
        print(f"    Address: {synapse.account}")
        return

    # 5. Upload test data
    print("\n[4/6] Uploading test data...")
    test_data = b"Hello from pynapse! " + bytes(range(256)) * 10  # ~2.5KB
    print(f"  → Uploading {len(test_data)} bytes...")
    
    try:
        ctx = await synapse.storage.get_context()
        provider_id = getattr(ctx, 'provider_id', 'auto')
        print(f"  ✓ Created storage context with provider {provider_id}")
        
        piece_cid = await ctx.upload(test_data)
        print(f"  ✓ Uploaded! PieceCID: {piece_cid}")
    except Exception as e:
        print(f"  ✗ Upload failed: {e}")
        return

    # 6. Download and verify
    print("\n[5/6] Downloading and verifying...")
    try:
        downloaded = await synapse.storage.download(piece_cid)
        if downloaded == test_data:
            print(f"  ✓ Downloaded {len(downloaded)} bytes")
            print("  ✓ Data integrity verified!")
        else:
            print(f"  ✗ Data mismatch! Expected {len(test_data)}, got {len(downloaded)}")
    except Exception as e:
        print(f"  ✗ Download failed: {e}")
        return

    # Done!
    print("\n[6/6] Summary")
    print("=" * 60)
    print("  🎉 pynapse is working!")
    print(f"  → Wallet: {synapse.account}")
    print(f"  → PieceCID: {piece_cid}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
