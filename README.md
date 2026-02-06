# Pynapse

Python SDK for Filecoin Onchain Cloud (Synapse).

This project mirrors the JS SDK in `FilOzone/synapse-sdk` and references the Go implementation in `data-preservation-programs/go-synapse` for parity.

## Status

Work in progress. Parity is being implemented in incremental commits.

## Install (dev)

```bash
uv venv
uv pip install -e .[test]
```

PyPI package name: `synapse-filecoin-sdk`  
Python import: `pynapse`

## Install (PyPI)

```bash
pip install synapse-filecoin-sdk
```

## Supported Networks

Both **Filecoin Mainnet** and **Calibration testnet** are supported:

```python
from pynapse import AsyncSynapse

# Mainnet
synapse = await AsyncSynapse.create(
    rpc_url="https://api.node.glif.io/rpc/v1",
    chain="mainnet",
    private_key=PRIVATE_KEY
)

# Calibration testnet (for testing)
synapse = await AsyncSynapse.create(
    rpc_url="https://api.calibration.node.glif.io/rpc/v1",
    chain="calibration",
    private_key=PRIVATE_KEY
)
```

See [QUICKSTART.md](QUICKSTART.md) for a full tutorial using Calibration testnet.

## CommP / PieceCID

`pynapse` uses `stream-commp` from `go-fil-commp-hashhash` for PieceCID calculation.

### Install `stream-commp`

`stream-commp` is an external runtime dependency and is not installed by `pip`.

```bash
git clone https://github.com/filecoin-project/go-fil-commp-hashhash.git
cd go-fil-commp-hashhash/cmd/stream-commp
go build -o stream-commp .
```

Install it to your PATH (for example `/usr/local/bin`) or set:

```bash
export PYNAPSE_COMMP_HELPER=/absolute/path/to/stream-commp
```

Verify:

```bash
stream-commp --help
```

## License

Dual-licensed under Apache-2.0 OR MIT. See `LICENSE.md`.

## Publishing to PyPI

Publishing is automated via GitHub Actions in `.github/workflows/publish-pypi.yml`.

1. In PyPI, create the project (or use an existing one) and configure a Trusted Publisher for this GitHub repository and workflow.
2. In GitHub, optionally protect the `pypi` environment for manual approval.
3. Tag a release and push the tag:

```bash
git tag v0.2.0
git push origin v0.2.0
```

The workflow builds the package, runs `twine check`, and publishes to PyPI via OIDC (no API token required).
