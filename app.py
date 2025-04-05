from flask import Flask, request, jsonify
import os
import pandas as pd
import json
from dotenv import load_dotenv
from openai import OpenAI
from flask_swagger_ui import get_swaggerui_blueprint

# Import functions from cli_oai.py
from cli_oai import (
    connect_to_database,
    execute_query, 
    generate_sql, 
    generate_response,
    SCHEMA_INFO,
    BLOCKCHAIN_CONTEXT,
    EXAMPLE_QUERIES,
    OPENAI_API_KEY,
    OPENAI_MODEL,
    CHAIN
)

# Load environment variables
load_dotenv()

# Create Flask app
app = Flask(__name__)

# Initialize OpenAI client
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Configure Swagger UI
SWAGGER_URL = '/docs'  # URL for exposing Swagger UI
API_URL = '/static/swagger.json'  # Our API url (can of course be a local resource)

# Call factory function to create our blueprint
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={  # Swagger UI config overrides
        'app_name': "Blockchain Database API"
    }
)

# Register blueprint at URL
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Create /static folder and swagger.json file if they don't exist
os.makedirs('static', exist_ok=True)
swagger_json = {
    "swagger": "2.0",
    "info": {
        "title": "Blockchain Database API",
        "description": "API for querying blockchain data using natural language",
        "version": "1.0"
    },
    "basePath": "/",
    "schemes": ["http"],
    "paths": {
        "/api/examples": {
            "get": {
                "summary": "Get example queries",
                "description": "Returns a list of example natural language queries",
                "produces": ["application/json"],
                "responses": {
                    "200": {
                        "description": "Successful operation",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "status": {"type": "string"},
                                "examples": {
                                    "type": "array",
                                    "items": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }
        },
        "/api/query": {
            "post": {
                "summary": "Execute natural language query",
                "description": "Converts natural language to SQL, executes query, and returns results with insights",
                "produces": ["application/json"],
                "consumes": ["application/json"],
                "parameters": [
                    {
                        "in": "body",
                        "name": "body",
                        "description": "Query parameters",
                        "required": True,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "Natural language query"
                                },
                                "chain": {
                                    "type": "string",
                                    "description": "Blockchain name"
                                }
                            },
                            "required": ["query"]
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Successful operation"
                    },
                    "400": {
                        "description": "Invalid input"
                    },
                    "500": {
                        "description": "Server error"
                    }
                }
            }
        },
        "/api/sql": {
            "post": {
                "summary": "Execute raw SQL query",
                "description": "Executes a raw SQL query against the database",
                "produces": ["application/json"],
                "consumes": ["application/json"],
                "parameters": [
                    {
                        "in": "body",
                        "name": "body",
                        "description": "SQL query",
                        "required": True,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "sql": {
                                    "type": "string",
                                    "description": "SQL query to execute"
                                }
                            },
                            "required": ["sql"]
                        }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Successful operation"
                    },
                    "400": {
                        "description": "Invalid input"
                    },
                    "500": {
                        "description": "Server error"
                    }
                }
            }
        },
        "/health": {
            "get": {
                "summary": "Health check",
                "description": "Check the health of the API and its dependencies",
                "produces": ["application/json"],
                "responses": {
                    "200": {
                        "description": "Successful operation"
                    }
                }
            }
        }
    }
}

with open('static/swagger.json', 'w') as f:
    json.dump(swagger_json, f)

# Endpoint to get example questions
@app.route('/api/examples', methods=['GET'])
def get_examples():
    examples = [
        "What are the 10 most recent blocks?",
        "Show me the top 5 transactions by value",
        "Which wallets have the most token transfers?",
        "What's the average gas used per block?",
        "Show me all ERC-20 transfers with value over 1000",
        "Which tokens have the highest price?",
        "Show me the total transaction count per day",
        "Which addresses have the most NFTs?",
        "How many unique tokens are there in the database?",
        "What's the distribution of transaction values?",
        "Which miners have produced the most blocks?",
        "What's the average number of transactions per block?",
        "Which token has the highest total transfer value?",
        "Show me the most active wallet addresses"
    ]
    return jsonify({"status": "success", "examples": examples})

# Endpoint to execute natural language query and get insights
@app.route('/api/query', methods=['POST'])
def execute_nl_query():
    data = request.json
    if not data or 'query' not in data:
        return jsonify({"status": "error", "message": "Missing 'query' parameter"}), 400
    
    prompt = data['query']
    chain = data.get('chain', CHAIN)
    
    # Generate SQL from natural language
    sql, sql_error = generate_sql(openai_client, prompt, SCHEMA_INFO, chain, OPENAI_MODEL)
    if sql_error:
        return jsonify({"status": "error", "message": sql_error}), 500
    
    # Connect to database
    conn = connect_to_database()
    if not conn:
        return jsonify({"status": "error", "message": "Failed to connect to database"}), 500
    
    # Execute SQL
    df, db_error = execute_query(conn, sql)
    
    # Format results for JSON
    if df is not None and not df.empty:
        results = {
            "status": "success",
            "row_count": len(df),
            "data": df.to_dict(orient='records')
        }
    else:
        results = {
            "status": "success" if not db_error else "error",
            "message": db_error if db_error else "No results found",
            "data": []
        }
    
    conn.close()
    
    if db_error and "no results" not in db_error.lower():
        return jsonify({"status": "error", "message": db_error, "sql": sql}), 500
    
    # Generate insights
    insights, insights_error = generate_response(openai_client, prompt, sql, df, OPENAI_MODEL)
    if insights_error:
        return jsonify({
            "status": "partial_success", 
            "message": insights_error,
            "sql": sql,
            "results": results
        }), 200
    
    # Return successful response with all data
    return jsonify({
        "status": "success",
        "sql": sql,
        "results": results,
        "insights": insights
    })

# Endpoint to execute raw SQL query
@app.route('/api/sql', methods=['POST'])
def execute_raw_sql():
    data = request.json
    if not data or 'sql' not in data:
        return jsonify({"status": "error", "message": "Missing 'sql' parameter"}), 400
    
    sql = data['sql']
    
    # Connect to database
    conn = connect_to_database()
    if not conn:
        return jsonify({"status": "error", "message": "Failed to connect to database"}), 500
    
    # Execute SQL
    df, error = execute_query(conn, sql)
    conn.close()
    
    if error and "no results" not in error.lower():
        return jsonify({"status": "error", "message": error, "sql": sql}), 500
    
    # Format results for JSON
    if df is not None and not df.empty:
        results = {
            "status": "success",
            "row_count": len(df),
            "data": df.to_dict(orient='records')
        }
    else:
        results = {
            "status": "success" if not error else "error",
            "message": error if error else "No results found",
            "data": []
        }
    
    # Return successful response
    return jsonify({
        "status": "success",
        "sql": sql,
        "results": results
    })

# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    # Check OpenAI API connection
    openai_status = "ok"
    try:
        # Simple model test
        openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": "test"}],
            max_tokens=5
        )
    except Exception as e:
        openai_status = f"error: {str(e)}"
    
    # Check database connection
    db_status = "ok"
    try:
        conn = connect_to_database()
        if conn:
            conn.close()
        else:
            db_status = "connection failed"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return jsonify({
        "status": "online",
        "openai": openai_status,
        "database": db_status,
        "model": OPENAI_MODEL,
        "chain": CHAIN
    })

# Error handling
@app.errorhandler(404)
def not_found(e):
    return jsonify({"status": "error", "message": "Endpoint not found"}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"status": "error", "message": "Internal server error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)