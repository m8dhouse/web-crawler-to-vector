#!/bin/bash

   # Define variables
   VENV_DIR="venv"
   REQUIREMENTS_FILE="requirements.txt"
   SCRIPT_FILE="web_crawler.py"

   # Print the current directory for debugging
   echo "Current directory: $(pwd)"

   # List files in the current directory for debugging
   ls

   # Check if the script file exists
   if [ ! -f "$SCRIPT_FILE" ]; then
     echo "Error: $SCRIPT_FILE not found!"
     exit 1
   fi

   # Create a virtual environment
   python3 -m venv $VENV_DIR

   # Activate the virtual environment
   source $VENV_DIR/bin/activate

   # Upgrade pip
   pip install --upgrade pip

   # Install required packages
   pip install -r $REQUIREMENTS_FILE

   # Run the main script
   python $SCRIPT_FILE

   # Deactivate the virtual environment
   deactivate