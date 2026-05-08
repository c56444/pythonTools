#!/usr/bin/env python3
"""
List Azure Blob Storage files where blob metadata contains delete=1.

Authentication (preferred):
- Microsoft Entra ID via DefaultAzureCredential

Optional fallback:
- Connection string
- Storage account name + key

Examples:
  python list_blobs_with_delete_metadata.py --account-url https://mystorage.blob.core.windows.net
  python list_blobs_with_delete_metadata.py --account-url https://mystorage.blob.core.windows.net --container my-container
    python list_blobs_with_delete_metadata.py --container my-container --prefix reportJSON/eng-doc-source
  python list_blobs_with_delete_metadata.py --connection-string "DefaultEndpointsProtocol=https;..."
  python list_blobs_with_delete_metadata.py --account-name mystorage --account-key "<storage-account-key>"
    python list_blobs_with_delete_metadata.py --container my-container --output-file matched_blobs.txt

Environment file:
- Create/edit a .env file in this folder and set AZURE_STORAGE_ACCOUNT_NAME and
    AZURE_STORAGE_ACCOUNT_KEY.
"""

import argparse
import os
import sys
from typing import Dict, Iterable, Optional, Tuple

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import AzureError


def load_dotenv_file(file_path: str = ".env") -> None:
    """Load simple KEY=VALUE pairs from a local .env file into os.environ."""
    if not os.path.exists(file_path):
        return

    try:
        with open(file_path, "r", encoding="utf-8") as env_file:
            for line in env_file:
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue

                key, value = stripped.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError as exc:
        print(f"Warning: Could not read .env file: {exc}", file=sys.stderr)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print blobs that have metadata delete=1"
    )

    auth_group = parser.add_mutually_exclusive_group(required=False)
    auth_group.add_argument(
        "--account-url",
        help="Storage account blob URL, for example: https://mystorage.blob.core.windows.net",
    )
    auth_group.add_argument(
        "--connection-string",
        help="Azure Storage connection string (fallback auth method)",
    )
    auth_group.add_argument(
        "--account-name",
        help="Storage account name (used with --account-key)",
    )

    parser.add_argument(
        "--account-key",
        help="Storage account key (used with --account-name)",
    )

    parser.add_argument(
        "--container",
        help="Optional container name. If omitted, scans all containers.",
    )
    parser.add_argument(
        "--prefix",
        help="Optional blob name prefix within the container.",
    )
    parser.add_argument(
        "--output-file",
        default="matched_blobs.txt",
        help="Text file path for matched blob names.",
    )

    return parser.parse_args()


def write_matches_to_file(output_file: str, matches: Iterable[str]) -> None:
    with open(output_file, "w", encoding="utf-8") as file_handle:
        for match in matches:
            file_handle.write(f"{match}\n")


def normalize_container_and_prefix(
    container_name: Optional[str], prefix: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    if prefix or not container_name or "/" not in container_name:
        return container_name, prefix

    normalized_container, normalized_prefix = container_name.split("/", 1)
    return normalized_container, normalized_prefix


def create_blob_service_client(args: argparse.Namespace) -> BlobServiceClient:
    # Allow env var fallback so the script works well in CI/local shells.
    connection_string = args.connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    account_url = args.account_url or os.getenv("AZURE_STORAGE_ACCOUNT_URL")
    account_name = args.account_name or os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = args.account_key or os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string)

    if account_name or account_key:
        if not (account_name and account_key):
            raise ValueError(
                "For account key auth, provide both --account-name and --account-key "
                "(or AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY)."
            )
        return BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
        )

    if not account_url:
        raise ValueError(
            "Provide one auth option: --connection-string, --account-name with --account-key, "
            "or --account-url. Env vars are also supported: AZURE_STORAGE_CONNECTION_STRING, "
            "AZURE_STORAGE_ACCOUNT_NAME with AZURE_STORAGE_ACCOUNT_KEY, or AZURE_STORAGE_ACCOUNT_URL."
        )

    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url=account_url, credential=credential)


def iter_target_containers(
    blob_service_client: BlobServiceClient, container_name: Optional[str]
) -> Iterable[Tuple[str, ContainerClient]]:
    if container_name:
        yield container_name, blob_service_client.get_container_client(container_name)
        return

    for container in blob_service_client.list_containers():
        name = container["name"] if isinstance(container, dict) else container.name
        yield name, blob_service_client.get_container_client(name)


def metadata_has_delete_1(metadata: Optional[Dict[str, str]]) -> bool:
    if not metadata:
        return False

    normalized = {str(k).lower(): str(v).strip() for k, v in metadata.items()}
    return normalized.get("delete") == "1"


def main() -> int:
    load_dotenv_file()
    args = parse_args()

    try:
        container_name, prefix = normalize_container_and_prefix(args.container, args.prefix)
        blob_service_client = create_blob_service_client(args)

        matched_blobs = []
        for current_container_name, container_client in iter_target_containers(blob_service_client, container_name):
            blobs = container_client.list_blobs(name_starts_with=prefix, include=["metadata"])
            for blob in blobs:
                if metadata_has_delete_1(blob.metadata):
                    matched_blobs.append(f"{current_container_name}/{blob.name}")

        write_matches_to_file(args.output_file, matched_blobs)

        if not matched_blobs:
            print(f"No blobs found with metadata delete=1. Wrote empty file: {args.output_file}")
        else:
            print(f"Wrote {len(matched_blobs)} matched blobs to {args.output_file}")

        return 0
    except AzureError as exc:
        print(f"Azure error: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:  # defensive catch for unexpected failures
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
