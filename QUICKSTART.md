# pynapse Quickstart - Upload to Filecoin Calibration Testnet

## Prerequisites

1. **Python 3.11+** with venv
2. **Go** (to build stream-commp helper)
3. **Test wallet** with:
   - tFIL for gas (get from faucet: https://faucet.calibnet.chainsafe-fil.io/)
   - USDFC for storage payments (get from https://stg.usdfc.net - mint by depositing tFIL)

---

## Step 1: Clone and Setup

```bash
# Clone pynapse
git clone https://github.com/anjor/pynapse.git
cd pynapse

# Create venv and install
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Step 2: Build stream-commp Helper

pynapse needs `stream-commp` to calculate Filecoin piece commitments:

```bash
# Clone and build
cd /tmp
git clone https://github.com/filecoin-project/go-fil-commp-hashhash.git
cd go-fil-commp-hashhash/cmd/stream-commp
go build -o stream-commp .

# Install to your PATH (pick one)
sudo cp stream-commp /usr/local/bin/
# OR
cp stream-commp ~/go/bin/  # if ~/go/bin is in PATH
# OR
export PYNAPSE_COMMP_HELPER=/tmp/go-fil-commp-hashhash/cmd/stream-commp/stream-commp
```

Verify: `stream-commp --help`

## Step 3: Fund Your Wallet

You need:
- **tFIL** for gas (~1 tFIL is plenty)
- **USDFC** for storage payments (~10-50 USDFC for testing)

### Get tFIL
Go to https://faucet.calibnet.chainsafe-fil.io/ and request tFIL for your wallet.

### Get USDFC
1. Go to https://stg.usdfc.net
2. Connect wallet (Calibration network)
3. Mint USDFC by depositing tFIL as collateral
4. You'll get ~1.2x your deposit in USDFC

## Step 4: Deposit USDFC & Approve Operator

Before uploading, you need to:
1. Approve USDFC spending by the Payments contract
2. Deposit USDFC to the Payments contract
3. Approve the FWSS operator

Run this setup script (replace with your private key):

```python
import asyncio
from web3 import AsyncWeb3

# CONFIG - Replace these!
PRIVATE_KEY = "0xYOUR_PRIVATE_KEY_HERE"
RPC_URL = "https://api.calibration.node.glif.io/rpc/v1"

# Contract addresses (Calibration)
USDFC = "0xb3042734b608a1B16e9e86B374A3f3e389B4cDf0"
PAYMENTS = "0x09a0fDc2723fAd1A7b8e3e00eE5DF73841df55a0"
FWSS = "0x02925630df557F957f70E112bA06e50965417CA0"

async def setup():
    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(RPC_URL))
    acct = w3.eth.account.from_key(PRIVATE_KEY)
    print(f"Wallet: {acct.address}")
    
    # ERC20 ABI (minimal)
    erc20_abi = [
        {"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"name":"approve","outputs":[{"type":"bool"}],"stateMutability":"nonpayable","type":"function"},
        {"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"type":"uint256"}],"stateMutability":"view","type":"function"},
    ]
    
    # Payments ABI (minimal)
    payments_abi = [
        {"inputs":[{"name":"token","type":"address"},{"name":"to","type":"address"},{"name":"amount","type":"uint256"}],"name":"deposit","outputs":[],"stateMutability":"nonpayable","type":"function"},
        {"inputs":[{"name":"token","type":"address"},{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"type":"uint256"}],"stateMutability":"view","type":"function"},
        {"inputs":[{"name":"operator","type":"address"},{"name":"approved","type":"bool"}],"name":"setOperatorApproval","outputs":[],"stateMutability":"nonpayable","type":"function"},
        {"inputs":[{"name":"account","type":"address"},{"name":"operator","type":"address"}],"name":"isOperatorFor","outputs":[{"type":"bool"}],"stateMutability":"view","type":"function"},
    ]
    
    usdfc = w3.eth.contract(address=USDFC, abi=erc20_abi)
    payments = w3.eth.contract(address=PAYMENTS, abi=payments_abi)
    
    # Check balances
    wallet_bal = await usdfc.functions.balanceOf(acct.address).call()
    deposit_bal = await payments.functions.balanceOf(USDFC, acct.address).call()
    print(f"USDFC in wallet: {wallet_bal / 1e18:.2f}")
    print(f"USDFC deposited: {deposit_bal / 1e18:.2f}")
    
    # 1. Approve USDFC spending (if needed)
    if wallet_bal > 0:
        print("\n1. Approving USDFC for Payments contract...")
        tx = await usdfc.functions.approve(PAYMENTS, 2**256 - 1).build_transaction({
            'from': acct.address,
            'nonce': await w3.eth.get_transaction_count(acct.address),
            'gas': 100000,
            'maxFeePerGas': await w3.eth.gas_price,
            'maxPriorityFeePerGas': 1000000000,
        })
        signed = acct.sign_transaction(tx)
        tx_hash = await w3.eth.send_raw_transaction(signed.raw_transaction)
        await w3.eth.wait_for_transaction_receipt(tx_hash)
        print(f"   Approved: {tx_hash.hex()}")
    
    # 2. Deposit USDFC (if wallet has balance)
    if wallet_bal > 0:
        deposit_amount = wallet_bal  # Deposit all
        print(f"\n2. Depositing {deposit_amount / 1e18:.2f} USDFC...")
        tx = await payments.functions.deposit(USDFC, acct.address, deposit_amount).build_transaction({
            'from': acct.address,
            'nonce': await w3.eth.get_transaction_count(acct.address),
            'gas': 200000,
            'maxFeePerGas': await w3.eth.gas_price,
            'maxPriorityFeePerGas': 1000000000,
        })
        signed = acct.sign_transaction(tx)
        tx_hash = await w3.eth.send_raw_transaction(signed.raw_transaction)
        await w3.eth.wait_for_transaction_receipt(tx_hash)
        print(f"   Deposited: {tx_hash.hex()}")
    
    # 3. Approve FWSS operator
    is_approved = await payments.functions.isOperatorFor(acct.address, FWSS).call()
    if not is_approved:
        print("\n3. Approving FWSS operator...")
        tx = await payments.functions.setOperatorApproval(FWSS, True).build_transaction({
            'from': acct.address,
            'nonce': await w3.eth.get_transaction_count(acct.address),
            'gas': 100000,
            'maxFeePerGas': await w3.eth.gas_price,
            'maxPriorityFeePerGas': 1000000000,
        })
        signed = acct.sign_transaction(tx)
        tx_hash = await w3.eth.send_raw_transaction(signed.raw_transaction)
        await w3.eth.wait_for_transaction_receipt(tx_hash)
        print(f"   Approved: {tx_hash.hex()}")
    else:
        print("\n3. FWSS operator already approved ✓")
    
    print("\n✅ Setup complete! Ready to upload.")

asyncio.run(setup())
```

## Step 5: Upload a File!

```python
import asyncio
from pynapse import AsyncSynapse

PRIVATE_KEY = "0xYOUR_PRIVATE_KEY_HERE"

async def upload_file():
    # Connect to Calibration testnet
    synapse = await AsyncSynapse.create(
        rpc_url="https://api.calibration.node.glif.io/rpc/v1",
        chain="calibration",
        private_key=PRIVATE_KEY
    )
    print(f"Connected as {synapse.account}")
    
    # Get storage context (auto-selects provider)
    ctx = await synapse.storage.get_context()
    print(f"Using provider: {ctx.provider.name}")
    print(f"Dataset ID: {ctx.data_set_id}")
    
    # Upload some data (min 256 bytes)
    data = b"Hello, Filecoin! This is my first pynapse upload. " * 10
    print(f"\nUploading {len(data)} bytes...")
    
    result = await ctx.upload(data)
    
    print(f"\n✅ Upload successful!")
    print(f"   Piece CID: {result.piece_cid}")
    print(f"   Size: {result.size} bytes")
    print(f"   TX Hash: {result.tx_hash}")

asyncio.run(upload_file())
```

## Step 6: Upload from a File

```python
import asyncio
from pynapse import AsyncSynapse

PRIVATE_KEY = "0xYOUR_PRIVATE_KEY_HERE"

async def upload_from_file(filepath: str):
    synapse = await AsyncSynapse.create(
        rpc_url="https://api.calibration.node.glif.io/rpc/v1",
        chain="calibration",
        private_key=PRIVATE_KEY
    )
    
    ctx = await synapse.storage.get_context()
    
    # Read file
    with open(filepath, "rb") as f:
        data = f.read()
    
    # Min size is 256 bytes, max is 254 MiB
    if len(data) < 256:
        data = data + b'\x00' * (256 - len(data))  # Pad if needed
    
    print(f"Uploading {filepath} ({len(data)} bytes)...")
    result = await ctx.upload(data)
    
    print(f"✅ Uploaded: {result.piece_cid}")
    return result

asyncio.run(upload_from_file("./myfile.txt"))
```

---

## Troubleshooting

### "stream-commp helper not found"
Set the path: `export PYNAPSE_COMMP_HELPER=/path/to/stream-commp`

### "insufficient funds"
- Check tFIL balance for gas
- Check USDFC deposited balance in Payments contract

### "operator not approved"
Run Step 4 to approve the FWSS operator.

### Upload hangs
- Network latency to Calibration RPC can be slow
- Storage context creation involves multiple contract calls

---

## Key Addresses (Calibration Testnet)

| Contract | Address |
|----------|---------|
| USDFC Token | `0xb3042734b608a1B16e9e86B374A3f3e389B4cDf0` |
| Payments | `0x09a0fDc2723fAd1A7b8e3e00eE5DF73841df55a0` |
| FWSS (Operator) | `0x02925630df557F957f70E112bA06e50965417CA0` |
| WarmStorage | `0xa9fEdb4e4acd6434adBE163b2e25f05E54bb4319` |
| SP Registry | `0xc21cbbF2a8F94f2C9C6D4a6A75C571eB45052e8A` |

---

## Next Steps

- Try uploading larger files (up to 254 MiB)
- Explore batch uploads with `ctx.upload_batch([data1, data2, ...])`
- Check the SDK source for more options
