# Idempotency Behavior in Pynapse SDK

This document defines the idempotency guarantees and retry semantics for the Pynapse SDK, specifically for dataset creation and piece addition operations that are prone to network failures and retries.

## Overview

The Pynapse SDK implements idempotency for critical operations to ensure safe retries and prevent duplicate resources when network issues or timeouts occur. This is especially important for blockchain-related operations where duplicate transactions can be costly or problematic.

## Supported Operations

### Dataset Creation (`create_data_set`)

**Operation**: Creating new datasets via `StorageContext.create()` and `AsyncStorageContext.create()`

**Idempotency Strategy**:
- Automatic idempotency key generation based on client address, provider ID, client dataset ID, and metadata
- Server-side detection of duplicate dataset creation attempts
- Graceful handling of "already exists" responses

**Guarantees**:
- ✅ Safe to retry dataset creation operations
- ✅ Duplicate datasets are not created when retrying with identical parameters
- ✅ Existing datasets are discovered and reused when appropriate
- ✅ Metadata consistency is verified when reusing existing datasets

**Behavior on Retry**:
- If dataset already exists with matching metadata: reuse existing dataset
- If dataset already exists with different metadata: raise `ValueError`
- If idempotency key conflicts: raise `IdempotencyError`

### Piece Addition (`add_pieces`)

**Operation**: Adding pieces to datasets via `StorageContext.upload()` and `AsyncStorageContext.upload()`

**Idempotency Strategy**:
- Automatic idempotency key generation based on dataset ID, piece CID, and metadata
- Server-side detection of duplicate piece additions
- Graceful handling of "already exists" responses

**Guarantees**:
- ✅ Safe to retry piece addition operations
- ✅ Duplicate pieces are not added when retrying with identical parameters
- ✅ Existing pieces are detected without error
- ✅ Batch uploads handle partial duplicates gracefully

**Behavior on Retry**:
- If piece already exists in dataset: operation succeeds (no-op)
- If some pieces in batch already exist: operation succeeds for new pieces
- If idempotency key conflicts: raise `IdempotencyError`

## Error Handling

### New Exception Types

```python
from pynapse.pdp.server import AlreadyExistsError, IdempotencyError

try:
    context = StorageContext.create(...)
except AlreadyExistsError as e:
    # Resource already exists - may be acceptable
    existing_id = e.existing_resource_id

except IdempotencyError as e:
    # Idempotency key conflict - retry with new key or investigate
    pass
```

### HTTP Status Code Mapping

| Status Code | Exception | Meaning |
|-------------|-----------|---------|
| 409 Conflict | `AlreadyExistsError` | Resource already exists (acceptable for idempotency) |
| 422 Unprocessable Entity | `IdempotencyError` | Idempotency key conflicts with different payload |
| Other 4xx/5xx | `RuntimeError` | Other server errors (may be retryable) |

## Implementation Details

### Idempotency Key Generation

Idempotency keys are deterministically generated using SHA-256 hashing of operation parameters:

```python
def _generate_idempotency_key(operation: str, *args: str) -> str:
    content = f"{operation}:" + ":".join(args)
    return hashlib.sha256(content.encode('utf-8')).hexdigest()[:32]
```

**Dataset Creation Key**: Includes client address, provider ID, client dataset ID, and sorted metadata

**Piece Addition Key**: Includes dataset ID, piece CID(s), and sorted metadata

### Automatic Retry Logic

The SDK handles `AlreadyExistsError` automatically by:

1. **Dataset Creation**: Searching for existing datasets that match the criteria
2. **Piece Addition**: Treating existing pieces as successful operations

This allows transparent retries without client code changes.

## Non-Guarantees

### What is NOT Guaranteed

- ❌ **Upload piece data idempotency**: The same piece data uploaded multiple times may create separate upload sessions
- ❌ **Cross-provider idempotency**: Idempotency keys are provider-specific
- ❌ **Long-term idempotency**: Keys may expire based on server policy
- ❌ **Transaction idempotency**: Blockchain transactions themselves are not idempotent (handled at higher level)

### Operations Without Idempotency

The following operations do not currently support idempotency:
- `upload_piece()` raw data uploads (use `upload()` instead)
- `download_piece()` and other read operations (naturally idempotent)
- Provider health checks and discovery

## Best Practices

### For Application Developers

1. **Use the high-level APIs**: `StorageContext.upload()` provides automatic idempotency
2. **Handle exceptions appropriately**: Distinguish between `AlreadyExistsError` (often acceptable) and other errors
3. **Don't disable retries**: The SDK's retry logic is designed to be safe with idempotency
4. **Monitor for `IdempotencyError`**: This may indicate a bug in retry logic or concurrent operations

### For SDK Maintenance

1. **Preserve key generation logic**: Changes to `_generate_idempotency_key()` break existing operations
2. **Test error paths**: Ensure both success and error scenarios work with idempotency
3. **Document breaking changes**: Any changes to idempotency behavior are breaking changes

## Alignment with Filecoin Ecosystem

This idempotency implementation aligns with:

- **Filecoin Curio semantics**: Task-based retries with bounded attempts
- **Industry standards**: HTTP idempotency patterns (RFC 7231, draft specifications)
- **Blockchain best practices**: Preventing duplicate on-chain operations

The design allows Pynapse to integrate seamlessly with Curio storage providers that implement similar retry and idempotency behaviors.

---

*See [GitHub Issue #9](https://github.com/anjor/pynapse/issues/9) for the original requirements and implementation discussion.*