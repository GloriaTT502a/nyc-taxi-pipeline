#!/bin/bash

set -e

echo ""
echo "========================================="
echo "Running Spatial Pipeline..."
echo "========================================="

databricks bundle run run_spatial_pipeline -t dev
