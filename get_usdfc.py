#!/usr/bin/env python3
"""Get USDFC from faucet on Calibration testnet"""
import asyncio
from web3 import AsyncWeb3
from eth_account import Account

PRIVATE_KEY = '0x7666381bff20b2f0819c77d3846b1d22637be4996012aaaff25e8b93f73a6985'
USDFC = '0xb3042734b608a1B16e9e86B374A3f3e389B4cDf0'

async def main():
    print("Connecting...")
    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider('https://api.calibration.node.glif.io/rpc/v1'))
    account = Account.from_key(PRIVATE_KEY)
    print(f"Wallet: {account.address}")
    
    faucet_abi = [
        {'inputs': [{'name': 'to', 'type': 'address'}], 'name': 'faucet', 'outputs': [], 'stateMutability': 'nonpayable', 'type': 'function'},
        {'inputs': [{'name': 'account', 'type': 'address'}], 'name': 'balanceOf', 'outputs': [{'name': '', 'type': 'uint256'}], 'stateMutability': 'view', 'type': 'function'},
    ]
    
    contract = w3.eth.contract(address=USDFC, abi=faucet_abi)
    
    balance_before = await contract.functions.balanceOf(account.address).call()
    print(f'USDFC Balance before: {balance_before / 1e18}')
    
    print("Building faucet transaction...")
    nonce = await w3.eth.get_transaction_count(account.address)
    gas_price = await w3.eth.gas_price
    
    tx = await contract.functions.faucet(account.address).build_transaction({
        'from': account.address,
        'nonce': nonce,
        'gas': 1000000,
        'gasPrice': gas_price,
        'chainId': 314159,
    })
    
    print("Signing and sending...")
    signed = account.sign_transaction(tx)
    raw = signed.rawTransaction
    tx_hash = await w3.eth.send_raw_transaction(raw)
    print(f'Tx hash: {tx_hash.hex()}')
    
    print('Waiting for confirmation (up to 2 min)...')
    receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    status = "✅ SUCCESS" if receipt.status == 1 else "❌ FAILED"
    print(f'Status: {status}')
    
    balance_after = await contract.functions.balanceOf(account.address).call()
    print(f'USDFC Balance after: {balance_after / 1e18}')

if __name__ == "__main__":
    asyncio.run(main())
