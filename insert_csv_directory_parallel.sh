#!/bin/bash

set -e  # Exit on error
set -u  # Treat unset variables as errors

NUM_SPLITS=4 # for 100 csv 4 splits would be 25 files per split processed in parallel

PYTHON_SCRIPT="src/ais_data_processor.py"
INPUT_DIR="${1:-}"

# Check if input was provided
if [ -z "$INPUT_DIR" ]; then
    echo "Usage: $0 /path/to/csv_files_directory"
    exit 1
fi

# Check if it's a valid directory
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: '$INPUT_DIR' is not a valid directory."
    exit 1
fi

echo "Processing directory: $INPUT_DIR"



# Store PIDs of background jobs
PIDS=()

# Cleanup function to kill background jobs
cleanup() {
    echo "Caught interrupt. Terminating background jobs..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
        fi
    done
    exit 1
}

# Trap Ctrl+C (SIGINT)
trap cleanup SIGINT

# Get list of files and count
# FILES=($(find "$INPUT_DIR" -type f))
FILES=()
while IFS= read -r -d '' file; do
    FILES+=("$file")
done < <(find "$INPUT_DIR" -type f -print0 | sort -z)

TOTAL_FILES=${#FILES[@]}

if [ "$TOTAL_FILES" -eq 0 ]; then
    echo "No files found in $INPUT_DIR"
    exit 1
fi

# Calculate number of files per chunk (ceiling division)
FILES_PER_CHUNK=$(( (TOTAL_FILES + NUM_SPLITS - 1) / NUM_SPLITS ))

echo "Total files: $TOTAL_FILES"
echo "Splitting into $NUM_SPLITS chunks of up to $FILES_PER_CHUNK files each..."

# Create temp dirs and distribute files
for ((i=0; i<NUM_SPLITS; i++)); do
    TEMP_DIR="./tmp/chunk_$i"
    mkdir -p "$TEMP_DIR"

    START_INDEX=$((i * $FILES_PER_CHUNK))
    END_INDEX=$((START_INDEX + $FILES_PER_CHUNK))
    
    echo "Chunk $i -> $TEMP_DIR (files $START_INDEX to $END_INDEX)"

    echo "${FILES[1]}"

    for ((j=START_INDEX; j<START_INDEX + FILES_PER_CHUNK && j<TOTAL_FILES; j++)); do
        # cp "${FILES[j]}" "$TEMP_DIR/"
        TARGET="$TEMP_DIR/$(basename "${FILES[j]}")"
        if [[ ! -e "$TARGET" ]]; then
            ln -s "$(realpath "${FILES[j]}")" "$TARGET"
        fi
    done

    # Run the Python script on the chunk
   
    echo "Running Python script on $TEMP_DIR"
    python3 "$PYTHON_SCRIPT" --datapath "$TEMP_DIR" &
    PIDS+=($!)
done

wait
echo "All jobs completed."