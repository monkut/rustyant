"""Python client for rustyant (Redis-compatible KV over AWS Lambda + S3)."""

from rustyant.client import Client, RustyAntError

__all__ = ["Client", "RustyAntError"]
__version__ = "0.1.0"
