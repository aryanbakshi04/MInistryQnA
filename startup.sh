#!/bin/bash
echo "Starting Ministry QnA Application with PostgreSQL..."

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Set up database
echo "Setting up PostgreSQL database..."
python scripts/setup_azure_db.py

# Start the Streamlit application
echo "Starting Streamlit app..."
streamlit run app.py --server.port 8000 --server.address 0.0.0.0
