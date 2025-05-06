#!/bin/bash

# Check if two arguments were provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <all_samples_file> <new_samples_file>"
    exit 1
fi

# Assign arguments to variables
all_samples_file="$1"
new_samples_file="$2"

# Check if the files exist
if [ ! -f "$all_samples_file" ] || [ ! -f "$new_samples_file" ]; then
    echo "Both files must exist."
    exit 1
fi

# Find duplicates in the second file
duplicates_in_new_file=$(sort "$new_samples_file" | uniq -d)

if [ -n "$duplicates_in_new_file" ]; then
    echo "New samples file contains duplicate samples. These samples are:"
    echo "$duplicates_in_new_file"
    exit 0
fi

# Find repeated samples between the two files
duplicates=$(grep -Fxf "$all_samples_file" "$new_samples_file")

if [ -n "$duplicates" ]; then
    # If there are duplicate samples between the two files
    echo "The second file contains samples that are already in the first file. These samples are:"
    echo "$duplicates"
else
    # If all samples are new
    echo "All samples have been added to the first file."
    cat "$new_samples_file" >> "$all_samples_file"
fi