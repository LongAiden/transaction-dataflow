#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://github.com/LongAiden/transaction-dataflow.git"
REPO_DIR="transaction-dataflow" # Added for clarity
AIRFLOW_UID="${AIRFLOW_UID:-50000}"
AIRFLOW_ADMIN_USER="admin"
AIRFLOW_ADMIN_PASS="admin"
AIRFLOW_ADMIN_EMAIL="admin@example.com"
AIRFLOW_ADMIN_FIRST="Admin"
AIRFLOW_ADMIN_LAST="User"
GROUP_ID=$(id -g) # Get current group id

# ----------------------------------------
echo "1. Cloning repository..."
if [ -d "${REPO_DIR}" ]; then
  echo "Removing existing directory: ${REPO_DIR}"
  rm -rf "${REPO_DIR}"
fi
git clone "${REPO_URL}"
cd "${REPO_DIR}"

echo "2. Building Docker images..."
docker-compose -f docker-airflow.yaml build --no-cache
docker-compose -f docker-airflow.yaml up -d

echo "3. Creating Airflow directories and setting permissions..."
mkdir -p ./dags ./logs ./plugins ./scripts ./external_scripts ./results ./feature_store
sudo chown -R "${AIRFLOW_UID}:${GROUP_ID}" ./dags ./logs ./plugins ./scripts ./external_scripts ./results ./feature_store
sudo chmod -R 775 ./dags ./logs ./plugins ./scripts ./external_scripts ./results ./feature_store

echo "4. Exporting AIRFLOW_UID=${AIRFLOW_UID}..."
export AIRFLOW_UID

echo "5. Checking status of Airflow containers..."
docker-compose -f docker-airflow.yaml ps  # Check container status

echo "6. Creating Airflow admin user..."
if docker-compose exec airflow-webserver airflow users create \
  --username "${AIRFLOW_ADMIN_USER}" \
  --firstname "${AIRFLOW_ADMIN_FIRST}" \
  --lastname "${AIRFLOW_ADMIN_LAST}" \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}" \
  --password "${AIRFLOW_ADMIN_PASS}"; then
  echo "Airflow admin user created successfully."
else
  echo "Error creating Airflow admin user."
  exit 1
fi

echo "Setup complete!"