#!/usr/bin/env python3
"""GCP Resource Reset Tool.

A simple interactive CLI for safely resetting GCP resources and BigQuery datasets.

Features:
- Load environment variables from .env file
- Interactive dataset selection
- Optional backup before deletion
- Terraform state cleanup
"""

import json
import os
import shutil
import subprocess  # nosec B404 - subprocess used safely with validated input
import sys
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv


class Colors:
    """ANSI color codes for terminal output."""

    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"


def print_header(text: str) -> None:
    """Print a formatted header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.END}\n")


def print_success(text: str) -> None:
    """Print success message."""
    print(f"{Colors.GREEN}✔️ {text}{Colors.END}")


def print_error(text: str) -> None:
    """Print error message."""
    print(f"{Colors.RED}❌ {text}{Colors.END}")


def print_warning(text: str) -> None:
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.END}")


def print_info(text: str) -> None:
    """Print info message."""
    print(f"{Colors.CYAN}ℹ️  {text}{Colors.END}")


def load_environment() -> Dict[str, str]:
    """Load and validate environment variables from .env file."""
    print_header("Loading Environment Configuration")

    env_path = Path("../.env")

    if not env_path.exists():
        print_error("ERROR: .env file not found in parent directory")
        print_info("Please create a .env file with the following variables:")
        print("  - GCP_PROJECT_ID")
        print("  - REGION")
        print("  - BACKUP_BUCKET")
        sys.exit(1)

    print_info(f"Loading from: {env_path.absolute()}")
    load_dotenv(env_path)

    gcp_project_id = os.getenv("GCP_PROJECT_ID")
    region = os.getenv("REGION", "US")
    backup_bucket = os.getenv("BACKUP_BUCKET")

    # Validate and display configuration
    print("\nConfiguration Status:")
    all_valid = True

    if gcp_project_id:
        print_success(f"GCP_PROJECT_ID: {gcp_project_id}")
    else:
        print_error("GCP_PROJECT_ID: NOT SET")
        all_valid = False

    if region:
        print_success(f"REGION: {region}")
    else:
        print_error("REGION: NOT SET")
        all_valid = False

    if backup_bucket:
        print_success(f"BACKUP_BUCKET: {backup_bucket}")
    else:
        print_error("BACKUP_BUCKET: NOT SET")
        all_valid = False

    if not all_valid:
        print_error("\nMissing required environment variables!")
        sys.exit(1)

    # Type assertion: all values are guaranteed to be strings at this point
    return {
        "GCP_PROJECT_ID": gcp_project_id or "",  # This will never be empty due to validation
        "REGION": region or "US",
        "BACKUP_BUCKET": backup_bucket or "",
    }


def load_datasets(project_id: str) -> List[str]:
    """Load BigQuery datasets from GCP."""
    print_header("Loading BigQuery Datasets")

    try:
        cmd = [
            "bq",
            "ls",
            f"--project_id={project_id}",
            "--format=prettyjson",
        ]

        result = subprocess.run(  # nosec B603 B607 - trusted bq command with validated input
            cmd, capture_output=True, text=True, timeout=30, check=True
        )

        if result.stdout.strip():
            datasets_json = json.loads(result.stdout)
            datasets = [ds["datasetReference"]["datasetId"] for ds in datasets_json]
            print_success(f"Found {len(datasets)} dataset(s)")
            return datasets
        else:
            print_info("No BigQuery datasets found")
            return []

    except subprocess.TimeoutExpired:
        print_error("Timeout loading datasets")
        return []
    except subprocess.CalledProcessError as e:
        print_error(f"Error loading datasets: {e.stderr}")
        return []
    except json.JSONDecodeError:
        print_error("Error parsing dataset list")
        return []
    except FileNotFoundError:
        print_error("'bq' command not found. Is Google Cloud SDK installed?")
        return []


def select_datasets(datasets: List[str]) -> List[str]:
    """Interactively select datasets for deletion."""
    if not datasets:
        return []

    print_header("Select Datasets for Deletion")
    print("Available BigQuery datasets:\n")

    for i, dataset in enumerate(datasets, 1):
        print(f"  {i}) {dataset}")

    print("\nOptions:")
    print("  a - Select all datasets")
    print("  n - Select none (skip deletion)")
    print("  Or enter dataset numbers separated by spaces (e.g., 1 3 5)")

    while True:
        choice = input(f"\n{Colors.BOLD}Your choice:{Colors.END} ").strip()

        if choice.lower() == "a":
            return datasets
        elif choice.lower() == "n":
            return []
        else:
            try:
                indices = [int(x) for x in choice.split()]
                selected = []
                for idx in indices:
                    if 1 <= idx <= len(datasets):
                        selected.append(datasets[idx - 1])
                    else:
                        print_warning(f"Invalid index: {idx}")

                if selected:
                    return selected
                else:
                    print_error("No valid datasets selected. Try again.")
            except ValueError:
                print_error("Invalid input. Please enter numbers, 'a', or 'n'")


def confirm_action(message: str) -> bool:
    """Ask user for confirmation."""
    while True:
        response = input(f"\n{Colors.YELLOW}{message} (y/N):{Colors.END} ").strip().lower()
        if response in ["y", "yes"]:
            return True
        elif response in ["n", "no", ""]:
            return False
        else:
            print_error("Please enter 'y' or 'n'")


def cleanup_terraform() -> None:
    """Clean up Terraform resources and state files."""
    print_header("Terraform Cleanup")

    # Check if terraform.tfstate exists
    if Path("terraform.tfstate").exists():
        print_info("Destroying Terraform-managed resources...")

        try:
            result = subprocess.run(  # nosec B603 B607 - trusted terraform command
                ["terraform", "destroy", "-auto-approve"],
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode == 0:
                print_success("Terraform resources destroyed")
            else:
                print_warning(f"Terraform destroy failed: {result.stderr}")

        except subprocess.TimeoutExpired:
            print_error("Terraform destroy timed out")
        except FileNotFoundError:
            print_warning("'terraform' command not found")
    else:
        print_info("No terraform.tfstate found, skipping destroy")

    # Clean up Terraform files
    print_info("Cleaning up Terraform state files...")
    files_to_remove = [
        "terraform.tfstate",
        "terraform.tfstate.backup",
        ".terraform",
    ]

    for file_path in files_to_remove:
        path = Path(file_path)
        try:
            if path.is_file():
                path.unlink()
                print(f"  ✔️  Removed {file_path}")
            elif path.is_dir():
                shutil.rmtree(path)
                print(f"  ✔️  Removed {file_path}/")
        except Exception as e:
            print_warning(f"Failed to remove {file_path}: {e}")


def backup_dataset(dataset: str, project_id: str, bucket: str) -> None:
    """Backup a single dataset to GCS."""
    print(f"\n  📦 Backing up to {bucket}/{dataset}/")

    try:
        # List tables in dataset
        cmd = [
            "bq",
            "ls",
            "-n",
            "1000",
            "--format=prettyjson",
            f"--project_id={project_id}",
            dataset,
        ]

        result = subprocess.run(  # nosec B603 B607 - trusted bq command with validated input
            cmd, capture_output=True, text=True, timeout=30, check=True
        )

        if result.stdout.strip():
            tables_json = json.loads(result.stdout)
            tables = [t["tableReference"]["tableId"] for t in tables_json]

            print(f"  Found {len(tables)} table(s) to backup")

            for table in tables:
                print(f"    📄 Backing up table: {dataset}.{table}")

                export_cmd = [
                    "bq",
                    "extract",
                    "--compression=GZIP",
                    "--destination_format=CSV",
                    f"--project_id={project_id}",
                    f"{dataset}.{table}",
                    f"{bucket}/{dataset}/{table}/*.csv.gz",
                ]

                export_result = subprocess.run(  # nosec B603 B607 - trusted bq command
                    export_cmd,
                    capture_output=True,
                    text=True,
                    timeout=120,
                )

                if export_result.returncode == 0:
                    print(f"      ✔️  Backed up {table}")
                else:
                    print_warning(f"Failed to backup {table}")
        else:
            print_info("No tables found in dataset")

    except subprocess.TimeoutExpired:
        print_error(f"Timeout backing up dataset {dataset}")
    except subprocess.CalledProcessError as e:
        print_error(f"Error listing tables: {e.stderr}")
    except json.JSONDecodeError:
        print_error("Error parsing table list")


def delete_dataset(dataset: str, project_id: str) -> None:
    """Delete a BigQuery dataset."""
    print(f"  🗑️  Deleting dataset: {dataset}")

    try:
        cmd = ["bq", "rm", "-f", "-r", "-d", f"{project_id}:{dataset}"]

        result = subprocess.run(  # nosec B603 B607 - trusted bq command with validated input
            cmd, capture_output=True, text=True, timeout=60
        )

        if result.returncode == 0:
            print_success(f"Deleted {dataset}")
        else:
            print_warning(f"Failed to delete {dataset}")

    except subprocess.TimeoutExpired:
        print_error(f"Timeout deleting dataset {dataset}")


def process_datasets(datasets: List[str], config: Dict[str, str], backup: bool) -> None:
    """Backup and delete selected datasets."""
    print_header(f"Processing {len(datasets)} Dataset(s)")

    project_id = config["GCP_PROJECT_ID"]
    bucket = config["BACKUP_BUCKET"]

    for idx, dataset in enumerate(datasets, 1):
        print(f"\n[{idx}/{len(datasets)}] Processing dataset: {dataset}")

        # Backup if enabled
        if backup:
            backup_dataset(dataset, project_id, bucket)

        # Delete dataset
        delete_dataset(dataset, project_id)


def main() -> None:
    """Main entry point."""
    print_header("GCP Resource Reset Tool")

    # Load environment (exits on failure)
    config = load_environment()

    # Load datasets
    datasets = load_datasets(config["GCP_PROJECT_ID"])

    # Show warning
    print_warning(
        "This will reset Terraform-managed resources and delete selected BigQuery datasets"
    )

    if not confirm_action("Are you sure you want to continue?"):
        print_info("Aborting reset")
        sys.exit(0)

    # Get user options
    backup_enabled = confirm_action("Do you want to back up datasets before deletion?")
    terraform_cleanup = confirm_action("Do you want to cleanup Terraform state?")

    # Select datasets
    selected_datasets = []
    if datasets:
        selected_datasets = select_datasets(datasets)

    # Confirm actions
    if not selected_datasets and not terraform_cleanup:
        print_info("No actions selected. Nothing to do.")
        sys.exit(0)

    # Execute cleanup
    if terraform_cleanup:
        cleanup_terraform()

    if selected_datasets:
        process_datasets(selected_datasets, config, backup_enabled)

    # Final message
    print_header("Reset Complete!")
    print_success("All selected operations completed")

    if config.get("BACKUP_BUCKET") and backup_enabled:
        print_info(f"Backups saved to: {config['BACKUP_BUCKET']}")

    print_info("You can now run 'terraform init' and 'terraform apply'")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Operation cancelled by user{Colors.END}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        sys.exit(1)
