#!/bin/bash
# Setup script for Blockchain Database Chat

# Create virtual environment
echo "Creating Python virtual environment..."
python -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install required packages
echo "Installing required packages..."
pip install psycopg2-binary python-dotenv pandas vanna[chromadb,openai,postgres] streamlit matplotlib seaborn

# Create .env file
echo "Creating .env file..."
cat > .env << EOL
# Database configuration
DB_NAME=blockchain_db
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
CHAIN=base

# OpenAI configuration
OPENAI_API_KEY=your_openai_api_key_here

# Data generation
NUM_RECORDS=1000
EOL

echo ".env file created. Please edit it with your OpenAI API key and database credentials."

echo "Setup complete!"
echo ""
echo "Instructions:"
echo "1. Edit the .env file with your OpenAI API key and database credentials"
echo "2. Make sure PostgreSQL is running"
echo "3. Run 'python generate_data.py' to create the database and populate it with sample data"
echo "4. Run 'streamlit run chat.py' to start the chat interface"