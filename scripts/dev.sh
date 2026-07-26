#!/bin/bash

set -e

# =========================================
# 1. 定位脚本所在目录
# =========================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# =========================================
# 2. 定位项目根目录（scripts 的上一层）
# =========================================
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# =========================================
# 3. 强制切换到项目根目录 ⭐关键修复点
# =========================================
cd "$PROJECT_ROOT"

echo ""
echo "========================================="
echo "NYC Taxi Development Pipeline"
echo "========================================="

echo ""
echo "Current Directory:"
pwd

echo ""
echo "[1/4] Cleaning..."

rm -rf dist build *.egg-info

echo ""
echo "[2/4] Building..."

python -m build

echo ""
echo "[3/4] Deploying..."

databricks bundle deploy -t dev

echo ""
echo "[4/4] Running..."

# databricks bundle run run_spatial_pipeline -t dev
# databricks bundle run run_bronze_pipeline -t dev
# databricks bundle run run_silver_pipeline -t dev
#databricks bundle run run_spatial_pipeline -t dev


##############
## Run Spatial
##############

# databricks bundle run run_spatial_pipeline -t dev 


##############
## Run Bronze and Silver 
############## 

TARGET_MONTHS=("201003" "201004" "201101" "201102")



for month in "${TARGET_MONTHS[@]}"; do
  echo ""
  echo "-----------------------------------------"
  echo ">>> [4.2] Processing Target Month: $month"
  echo "-----------------------------------------"
  
  echo "-> Triggering Bronze Pipeline for $month..."
  databricks bundle run run_bronze_pipeline -t dev --params "target_month=$month"
  
  echo "-> Triggering Silver Pipeline for $month..."
  databricks bundle run run_silver_pipeline -t dev --params "target_month=$month"
done



echo ""
echo "========================================="
echo "Pipeline Finished Successfully!"
echo "========================================="