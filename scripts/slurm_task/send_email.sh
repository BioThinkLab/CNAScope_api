#!/bin/bash

# Load micromamba
module load micromamba
eval "$(micromamba shell hook --shell bash)"
micromamba activate CNAScope

# Path to your Python script
SCRIPT_PATH="/home/platform/workspace/CNAScope/CNAScope_api/analysis/utils/email_utils.py"

# Get command line arguments
EMAIL="$1"
SUBJECT="$2"
BODY="$3"

# Check if arguments are provided
if [ -z "$EMAIL" ] || [ -z "$SUBJECT" ] || [ -z "$BODY" ]; then
  echo "Usage: $0 <email_address> <subject> <body>"
  exit 1
fi

# Call the Python script with the provided arguments
python "$SCRIPT_PATH" --email "$EMAIL" --subject "$SUBJECT" --body "$BODY"

# Deactivate the environment
micromamba deactivate