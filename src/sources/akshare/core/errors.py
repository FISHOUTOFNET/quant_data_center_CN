"""AkShare source error types."""


class AkShareError(RuntimeError):
    """Base class for AkShare source errors with manifest-friendly typing."""

    error_type = "unknown"


class AkShareNetworkError(AkShareError):
    error_type = "network"


class AkShareCircuitOpen(AkShareError):
    error_type = "circuit_open"


class AkShareSchemaDriftError(AkShareError):
    error_type = "schema_drift"


class AkShareEmptyDataError(AkShareError):
    error_type = "empty_data"
