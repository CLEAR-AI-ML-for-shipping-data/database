#!/bin/bash

# Usage: ./insert_csv_directory_serial.sh /path/to/csv_directory
PYTHON_SCRIPT="src/ais_data_processor.py"

INPUT="$1"

if [[ -z "$INPUT" ]]; then
    echo "Error: No input provided."
    echo "Usage: $0 /path/to/csv_directory_or_file"
    exit 1
fi

# Check if input is a CSV file
if [[ -f "$INPUT" && "$INPUT" == *.csv ]]; then
    echo "[1/1] Processing file: $INPUT"
    python3 "$PYTHON_SCRIPT" --datapath "$INPUT"
    echo "Done."
    exit 0
fi

# Check if input is a directory
if [[ -d "$INPUT" ]]; then
    CSV_FILES=("$INPUT"/*.csv)

    if [[ ! -e "${CSV_FILES[0]}" ]]; then
        echo "No CSV files found in directory '$INPUT'."
        exit 0
    fi

    total=${#CSV_FILES[@]}
    count=1

    for file in "${CSV_FILES[@]}"; do
        echo "[$count/$total] Processing: $file"
        python3 "$PYTHON_SCRIPT" --datapath "$file"
        ((count++))
    done

    echo "All files processed."
    exit 0
fi

echo "Error: '$INPUT' is neither a CSV file nor a directory."
exit 1
