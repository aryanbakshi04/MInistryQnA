echo "Starting Ministry QnA Application with PostgreSQL..."

echo "Installing dependencies..."
pip install -r requirements.txt

echo "Setting up PostgreSQL database..."
python scripts/setup_azure_db.py

echo "Starting Streamlit app..."
streamlit run app.py --server.port 8000 --server.address 0.0.0.0