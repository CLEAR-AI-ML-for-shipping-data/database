#!/bin/bash

set -e  # Exit on error

# Set the threshold for available RAM in MB
RAM_THRESHOLD=2048  # Set this to your desired threshold (e.g., 1024 MB)

# Get available RAM in MB
AVAILABLE_RAM=$(free -m | awk '/^Mem:/ {print $7}')

echo "Available RAM: ${AVAILABLE_RAM}MB"

# Check if available RAM is above the threshold
if (( AVAILABLE_RAM < RAM_THRESHOLD )); then
    echo "Warning: Available RAM is less than $RAM_THRESHOLD MB. Aborting swap clearing to avoid memory issues."
    exit 1
else
    echo "Available RAM is sufficient. Proceeding to clear swap..."

    # Disable and then re-enable swap
    sudo swapoff -a
    sudo swapon -a
    echo "Swap cleared successfully."
fi
