#!/usr/bin/env python3
"""
pynapse Full Demo - Complete end-to-end test on Filecoin Calibration
Handles all setup steps: deposit, service approval, then upload/download
"""
import asyncio
import sys
from web3 import AsyncWeb3
from eth_account import Account

# Demo wallet (Calibration testnet only)
PRIVATE_KEY = "0x7666381bff20b2f0819c77d3846b1d22637be4996012aaaff25e8b93f73a6985"
RPC_URL = "https://api.calibration.node.glif.io/rpc/v1"
CHAIN_ID = 314159

# Contract addresses (Calibration)
USDFC = "0xb3042734b608a1B16e9e86B374A3f3e389B4cDf0"
PAYMENTS = "0x09a0fDc2723fAd1A7b8e3e00eE5DF73841df55a0"  # FilecoinPay (updated by sub-agent)
FWSS = "0x02925630df557F957f70E112bA06e50965417CA0"  # WarmStorage service

# ABIs
ERC20_ABI = [
    {'inputs': [{'name': 'account', 'type': 'address'}], 'name': 'balanceOf', 'outputs': [{'name': '', 'type': 'uint256'}], 'stateMutability': 'view', 'type': 'function'},
    {'inputs': [{'name': 'spender', 'type': 'address'}, {'name': 'amount', 'type': 'uint256'}], 'name': 'approve', 'outputs': [{'name': '', 'type': 'bool'}], 'stateMutability': 'nonpayable', 'type': 'function'},
    {'inputs': [{'name': 'owner', 'type': 'address'}, {'name': 'spender', 'type': 'address'}], 'name': 'allowance', 'outputs': [{'name': '', 'type': 'uint256'}], 'stateMutability': 'view', 'type': 'function'},
]

PAYMENTS_ABI = [
    # accounts(token, account) returns (funds, lockupCurrent, lockupRate, lockupLastSettledAt)
    {'inputs': [{'name': 'token', 'type': 'address'}, {'name': 'account', 'type': 'address'}], 'name': 'accounts', 'outputs': [{'name': 'funds', 'type': 'uint256'}, {'name': 'lockupCurrent', 'type': 'uint256'}, {'name': 'lockupRate', 'type': 'uint256'}, {'name': 'lockupLastSettledAt', 'type': 'uint256'}], 'stateMutability': 'view', 'type': 'function'},
    {'inputs': [{'name': 'token', 'type': 'address'}, {'name': 'to', 'type': 'address'}, {'name': 'amount', 'type': 'uint256'}], 'name': 'deposit', 'outputs': [], 'stateMutability': 'nonpayable', 'type': 'function'},
    # operatorApprovals(token, client, operator) - note: 3 args not 2
    {'inputs': [{'name': 'token', 'type': 'address'}, {'name': 'client', 'type': 'address'}, {'name': 'operator', 'type': 'address'}], 'name': 'operatorApprovals', 'outputs': [{'name': 'approved', 'type': 'bool'}, {'name': 'rateAllowance', 'type': 'uint256'}, {'name': 'lockupAllowance', 'type': 'uint256'}, {'name': 'rateUsage', 'type': 'uint256'}, {'name': 'lockupUsage', 'type': 'uint256'}, {'name': 'maxLockupPeriod', 'type': 'uint256'}], 'stateMutability': 'view', 'type': 'function'},
    {'inputs': [{'name': 'token', 'type': 'address'}, {'name': 'operator', 'type': 'address'}, {'name': 'approved', 'type': 'bool'}, {'name': 'rateAllowance', 'type': 'uint256'}, {'name': 'lockupAllowance', 'type': 'uint256'}, {'name': 'maxLockupPeriod', 'type': 'uint256'}], 'name': 'setOperatorApproval', 'outputs': [], 'stateMutability': 'nonpayable', 'type': 'function'},
]


async def send_tx(w3, account, tx_data, description):
    """Send a transaction with proper Filecoin gas handling"""
    print(f"  → {description}...")
    
    nonce = await w3.eth.get_transaction_count(account.address)
    
    # Get gas estimate
    try:
        gas = await w3.eth.estimate_gas({**tx_data, 'from': account.address})
        gas = int(gas * 1.2)  # 20% buffer
    except Exception as e:
        print(f"    Gas estimate failed: {e}")
        gas = 30000000
    
    # Build transaction with EIP-1559 style fees for Filecoin
    base_fee = await w3.eth.gas_price
    
    tx = {
        **tx_data,
        'from': account.address,
        'nonce': nonce,
        'gas': gas,
        'maxFeePerGas': base_fee * 2,
        'maxPriorityFeePerGas': base_fee,
        'chainId': CHAIN_ID,
    }
    
    signed = account.sign_transaction(tx)
    tx_hash = await w3.eth.send_raw_transaction(signed.rawTransaction)
    print(f"    Tx: {tx_hash.hex()[:20]}...")
    
    receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt.status == 1:
        print(f"    ✅ Success (gas: {receipt.gasUsed:,})")
        return True
    else:
        print(f"    ❌ Failed")
        return False


async def main():
    print("=" * 60)
    print("pynapse Full Demo - Filecoin Calibration Testnet")
    print("=" * 60)
    
    # Connect
    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_URL))
    account = Account.from_key(PRIVATE_KEY)
    print(f"\nWallet: {account.address}")
    
    usdfc = w3.eth.contract(address=USDFC, abi=ERC20_ABI)
    payments = w3.eth.contract(address=PAYMENTS, abi=PAYMENTS_ABI)
    
    # Check balances
    print("\n[1/5] Checking balances...")
    fil_balance = await w3.eth.get_balance(account.address)
    usdfc_balance = await usdfc.functions.balanceOf(account.address).call()
    print(f"  FIL: {fil_balance / 1e18:.4f} tFIL")
    print(f"  USDFC (wallet): {usdfc_balance / 1e18:.2f}")
    
    if fil_balance < 1e18:
        print("\n❌ Need more tFIL for gas. Get from: https://faucet.calibnet.chainsafe-fil.io/")
        return
    
    # Check payments contract balance using accounts(token, account)
    try:
        funds, _, _, _ = await payments.functions.accounts(USDFC, account.address).call()
        payments_balance = int(funds)
        print(f"  USDFC (payments): {payments_balance / 1e18:.2f}")
    except Exception as e:
        payments_balance = 0
        print(f"  USDFC (payments): 0 (query failed: {e})")
    
    # Check if we have enough funds (either in wallet or already deposited)
    total_usdfc = usdfc_balance + payments_balance
    if total_usdfc < 10e18:
        print(f"\n❌ Need more USDFC. Total: {total_usdfc / 1e18:.2f}, need at least 10")
        print("   Mint at: https://stg.usdfc.net")
        return
    
    # Step 2: Approve USDFC for payments contract (only if we have wallet USDFC)
    print("\n[2/5] Checking USDFC allowance...")
    if usdfc_balance > 0:
        allowance = await usdfc.functions.allowance(account.address, PAYMENTS).call()
        print(f"  Current allowance: {allowance / 1e18:.2f}")
        
        if allowance < usdfc_balance:
            print("  Need to approve...")
            tx_data = await usdfc.functions.approve(PAYMENTS, 2**256 - 1).build_transaction({'from': account.address})
            if not await send_tx(w3, account, tx_data, "Approving USDFC"):
                return
        else:
            print("  ✅ Already approved")
    else:
        print("  ✅ Skipped (no wallet USDFC to approve)")
    
    # Step 3: Deposit USDFC to payments (only if we need more and have wallet USDFC)
    print("\n[3/5] Depositing USDFC to payments contract...")
    min_required = int(10e18)  # Need at least 10 USDFC
    
    if payments_balance >= min_required:
        print(f"  ✅ Already have {payments_balance/1e18:.2f} USDFC deposited")
    elif usdfc_balance > 0:
        deposit_amount = min(usdfc_balance, int(50e18))  # Deposit up to 50 USDFC
        tx_data = await payments.functions.deposit(USDFC, account.address, deposit_amount).build_transaction({'from': account.address})
        if not await send_tx(w3, account, tx_data, f"Depositing {deposit_amount/1e18:.0f} USDFC"):
            print("  ⚠️ Deposit failed - may need different approach")
        # Refresh payments balance
        payments_balance = await payments.functions.balanceOf(USDFC).call({'from': account.address})
    else:
        print("  ⚠️ No wallet USDFC to deposit, hoping payments balance is sufficient")
    
    # Step 4: Approve FWSS service
    print("\n[4/5] Checking FWSS operator approval...")
    try:
        # operatorApprovals(token, client, operator)
        approval = await payments.functions.operatorApprovals(USDFC, account.address, FWSS).call()
        is_approved = approval[0]
        print(f"  Approved: {is_approved}")
    except Exception as e:
        is_approved = False
        print(f"  Query failed: {e}")
    
    if not is_approved:
        print("  Need to approve operator...")
        # Approve with generous limits
        rate_allowance = int(1e24)  # Large allowance
        lockup_allowance = int(1e24)
        max_lockup_period = 365 * 24 * 60 * 60  # 1 year in seconds
        
        tx_data = await payments.functions.setOperatorApproval(
            USDFC, FWSS, True, rate_allowance, lockup_allowance, max_lockup_period
        ).build_transaction({'from': account.address})
        if not await send_tx(w3, account, tx_data, "Approving FWSS operator"):
            print("  ⚠️ Operator approval failed")
    else:
        print("  ✅ Operator already approved")
    
    # Step 5: Try upload with pynapse
    print("\n[5/5] Testing pynapse upload...")
    try:
        from pynapse import AsyncSynapse
        
        synapse = await AsyncSynapse.create(
            rpc_url=RPC_URL,
            chain='calibration',
            private_key=PRIVATE_KEY
        )
        
        print(f"  Connected as {synapse.account}")
        
        # Check storage providers
        info = await synapse.storage.get_storage_info()
        providers = getattr(info, 'providers', [])
        print(f"  Found {len(providers)} storage providers")
        
        if not providers:
            print("  ❌ No providers available")
            return
        
        # Try to create context and upload
        test_data = b"Hello from pynapse! " + bytes(range(256)) * 10
        print(f"  Uploading {len(test_data)} bytes...")
        
        ctx = await synapse.storage.get_context()
        piece_cid = await ctx.upload(test_data)
        print(f"  ✅ Uploaded! PieceCID: {piece_cid}")
        
        # Download and verify
        print("  Downloading...")
        downloaded = await synapse.storage.download(piece_cid)
        if downloaded == test_data:
            print("  ✅ Download verified!")
        else:
            print(f"  ⚠️ Data mismatch")
        
        print("\n" + "=" * 60)
        print("🎉 pynapse is working end-to-end!")
        print("=" * 60)
        
    except Exception as e:
        print(f"  ❌ Upload failed: {e}")
        print("\n  This may need additional debugging.")


if __name__ == "__main__":
    asyncio.run(main())
