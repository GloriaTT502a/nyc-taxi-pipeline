#!/bin/bash

set -e

echo ""
echo "========================================="
echo "Deploying Databricks Bundle..."
echo "========================================="

databricks bundle deploy -t dev

echo ""
echo "Deploy completed."