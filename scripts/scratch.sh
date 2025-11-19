#!/bin/bash
set -e  # Exit on first error

# Load environment variables from .env file
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "⚠️ .env file not found, using default configuration."
fi

# CONFIGURATION (loaded from .env or defaults)
PROJECT_ID="${PROJECT_ID:-your-gcp-project-id}"
REGION="${REGION:-your-region}"
BACKUP_BUCKET="${BACKUP_BUCKET:-gs://your-backup-bucket}"

# CONFIRMATION
echo "⚠️ This will reset Terraform-managed resources and optionally delete BigQuery datasets in project $PROJECT_ID."
echo "You will be asked whether to back up selected datasets before deletion."
read -p "Are you sure you want to continue? (y/N): " confirm
if [[ "$confirm" != "y" ]]; then
    echo "Aborting reset."
    exit 0
fi

# 1️⃣ Ask if the user wants to back up datasets
read -p "Do you want to back up datasets before deletion? (y/N): " backup_confirm
if [[ "$backup_confirm" == "y" ]]; then
    BACKUP=true
else
    BACKUP=false
fi

# 2️⃣ Destroy Terraform-managed resources
if [ -f "terraform.tfstate" ]; then
    echo "🚀 Destroying Terraform-managed resources..."
    terraform destroy -auto-approve
else
    echo "No Terraform state found. Skipping terraform destroy."
fi

# 3️⃣ List BigQuery datasets
datasets=($(gcloud bigquery datasets list --project "$PROJECT_ID" --format="value(datasetId)"))

if [ ${#datasets[@]} -eq 0 ]; then
    echo "No BigQuery datasets found."
else
    echo ""
    echo "📋 Available BigQuery datasets:"
    for i in "${!datasets[@]}"; do
        printf "%d) %s\n" "$((i+1))" "${datasets[$i]}"
    done

    echo ""
    echo "Options:"
    echo "  a - delete all datasets"
    echo "  n - delete none (skip all)"
    echo "  Or enter dataset numbers separated by spaces (e.g., 1 3 5)"
    read -p "Your choice: " -a selections

    # Check if user chose 'a' or 'n'
    if [[ "${selections[0]}" == "a" ]]; then
        selections=($(seq 1 ${#datasets[@]}))
    elif [[ "${selections[0]}" == "n" ]]; then
        selections=()
    fi

    # Backup and delete selected datasets
    for index in "${selections[@]}"; do
        if [[ "$index" -ge 1 && "$index" -le "${#datasets[@]}" ]]; then
            dataset="${datasets[$((index-1))]}"

            if $BACKUP; then
                echo "📦 Backing up dataset '$dataset' to $BACKUP_BUCKET/$dataset..."
                gcloud bigquery extract \
                    --destination_format=CSV \
                    --project "$PROJECT_ID" \
                    --compression GZIP \
                    --destination_uri "$BACKUP_BUCKET/$dataset/*.csv.gz" \
                    "$dataset.*" || echo "⚠️ Failed to backup dataset $dataset, skipping deletion."
            else
                echo "Skipping backup for dataset '$dataset'."
            fi

            echo "🗑️ Deleting BigQuery dataset: $dataset"
            gcloud bigquery datasets delete "$dataset" \
                --project "$PROJECT_ID" \
                --quiet \
                --delete_contents || echo "⚠️ Failed to delete dataset $dataset, skipping."
        else
            echo "⚠️ Invalid selection: $index, skipping."
        fi
    done
fi

# 4️⃣ Cleanup Terraform local files
echo "🧹 Cleaning up Terraform state and local files..."
rm -f terraform.tfstate terraform.tfstate.backup
rm -rf .terraform

echo "✅ Reset complete. Selected datasets backed up to $BACKUP_BUCKET (if you chose backup), and Terraform reset done."
echo "You can now start fresh with 'terraform init' and 'terraform apply'."
