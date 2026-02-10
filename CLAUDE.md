# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Pynapse is a Python SDK for Filecoin Onchain Cloud (Synapse). It mirrors the JS SDK in `FilOzone/synapse-sdk` and references the Go implementation in `data-preservation-programs/go-synapse` for parity.

- **PyPI package**: `synapse-filecoin-sdk`
- **Python import**: `pynapse`
- **Python**: 3.11+
- **Build system**: Hatchling, managed with `uv`

## Commands

```bash
# Setup
uv venv && uv pip install -e .[test]

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_piece.py

# Run a specific test
uv run pytest tests/test_piece.py::test_function_name -v

# Build distribution
uv run python -m build

# Install with LangChain integration
uv pip install -e .[langchain]
```

## Architecture

### Entry Point and Dual Sync/Async Pattern

The SDK exposes two top-level clients: `Synapse` (sync) and `AsyncSynapse` (async) in `src/pynapse/synapse.py`. Both are facades that wire together all subsystems via `create()` factory methods.

**Nearly every module has both sync and async variants** with identical APIs (except async/await). The sync variants use `Web3`/`httpx.Client`, async variants use `AsyncWeb3`/`httpx.AsyncClient`. When adding new functionality, both variants must be implemented.

### Subsystem Map

All subsystems are accessed as properties on the `Synapse`/`AsyncSynapse` client:

| Property | Module | Purpose |
|---|---|---|
| `storage` | `storage/` | Upload/download orchestration (StorageManager → StorageContext → PDPServer) |
| `payments` | `payments/` | Payment rail approvals, settlement, ERC20 permits |
| `providers` | `sp_registry/` | Service provider discovery and capability queries |
| `warm_storage` | `warm_storage/` | On-chain dataset/piece operations via smart contract |
| `session_registry` | `session/` | Delegated session key management |
| `retriever` | `retriever/` | SP-agnostic piece downloads (queries chain → finds providers → fetches) |
| `filbeam` | `filbeam/` | FilBeam CDN stats (mainnet only) |

### Storage Upload Flow

```
StorageManager.upload(data)
  → Get/create StorageContext (provider + dataset selection)
  → PieceCID calculation via external `stream-commp` binary
  → PDPServer: 3-phase upload (create session → PUT bytes → finalize with CID)
  → Poll until indexed
  → EIP-712 sign add-pieces → on-chain transaction
```

### Storage Download Flow

Direct (known endpoint): `StorageContext.download(cid)` → HTTP GET from PDP server

SP-agnostic: `ChainRetriever` queries warm_storage for datasets → finds providers via sp_registry → tries PDP endpoints until success.

### Contracts System

Smart contract ABIs live in `src/pynapse/contracts/` as JSON files. `addresses.json` maps chain IDs (314=mainnet, 314159=calibration) to deployed contract addresses. ABIs are exported as Python constants via `generated.py`.

Key contracts: FilecoinWarmStorageService (datasets/pieces), FilecoinPayV1 (payments), ServiceProviderRegistry (SP discovery), PDPVerifier (proof verification), SessionKeyRegistry.

### Core Primitives

- **Chain** (`core/chains.py`): Frozen dataclass with network config, contract addresses, genesis timestamps. Two predefined: `MAINNET`, `CALIBRATION`.
- **PieceCID** (`core/piece.py`): Handles PieceCIDv1 (CommP) ↔ PieceCIDv2 (FRC-0069) conversion. Requires external `stream-commp` binary.
- **Typed Data** (`core/typed_data.py`): EIP-712 structured signing for dataset creation, piece additions, permits.

### LangChain Integration

Optional (`pip install synapse-filecoin-sdk[langchain]`). Located in `src/pynapse/integrations/langchain.py`. Provides `FilecoinDocumentLoader` and `FilecoinStorageTool` for LLM agent workflows.

## External Runtime Dependency

PieceCID calculation requires `stream-commp` from `go-fil-commp-hashhash` (not bundled). The binary must be on PATH or pointed to via `PYNAPSE_COMMP_HELPER` env var.

## Publishing

Tag-driven via GitHub Actions: `git tag v0.X.Y && git push origin v0.X.Y` triggers `.github/workflows/publish-pypi.yml` (OIDC publish, no API token).
