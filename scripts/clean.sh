#!/bin/bash

set -e

echo ""
echo "========================================="
echo "Cleaning build artifacts..."
echo "========================================="

rm -rf dist
rm -rf build
rm -rf *.egg-info

echo "Done."