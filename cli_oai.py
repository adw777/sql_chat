import os
import pandas as pd
import psycopg2
import psycopg2.extras
from openai import OpenAI
from dotenv import load_dotenv
import json
import traceback
import sys
import textwrap

# Load environment variables from .env file
load_dotenv()

# Database connection settings from environment variables
DB_HOST = os.getenv("DB_HOST", "aws-0-us-east-1.pooler.supabase.com")
DB_PORT = os.getenv("DB_PORT", "6543")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres.cbxagjoxdgzpfknkpswj")
DB_PASSWORD = os.getenv("DB_PASSWORD", "amit")
CHAIN = os.getenv("CHAIN", "base")

# OpenAI API Key 
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
if not OPENAI_API_KEY:
    print("Error: OpenAI API key is required. Please set the OPENAI_API_KEY environment variable.")
    sys.exit(1)

# OpenAI Model to use
OPENAI_MODEL = "gpt-4o-mini"  # Default to GPT-4o-mini

# Database schema information for the LLM
SCHEMA_INFO = """
The database schema consists of the following tables:
1. **blocks_base**: Stores blockchain block details.
   - Columns: id, hash, parentHash, timestamp, number, gasLimit, gasUsed, transactionsRoot, receiptsRoot, logsBloom, difficulty, baseFeePerGas, withdrawalsRoot, blobGasUsed, excessBlobGas, parentBeaconBlockRoot, sha3Uncles, miner, stateRoot, nonce, mixHash, extraData, createdAt, updatedAt.
   - Unique Index: (hash, id).
2. **deposit_erc20_base**: Stores ERC-20 token deposit logs.
   - Columns: id, address, topics, data, block_hash, block_number, transaction_hash, transaction_index, log_index, removed, from, to, value.
   - Unique Index: (transaction_hash, from, id).
3. **pools_base**: Stores details of token pools.
   - Columns: id, pool_address, token_a_address, token_b_address, token_a_details, token_b_details, created_at, updated_at.
   - Unique Index: (id).
4. **tokens_base**: Stores token details.
   - Columns: id, token_address, name, symbol, decimals, total_supply, price, created_at, updated_at.
   - Unique Index: (token_address).
5. **token_trades_base**: Stores trade records of tokens.
   - Columns: id, token_address, name, symbol, decimals, total_supply, price, created_at, updated_at, pool_address, transaction_hash.
   - Unique Index: (token_address, transaction_hash, id).
6. **transactions_base**: Stores blockchain transaction details.
   - Columns: id, type, chain_id, nonce, gas_price, gas, to, value, input, r, s, v, hash, block_hash, block_number, transaction_index, from.
   - Index: (hash, from, id).
7. **transfer_erc20_base**: Stores ERC-20 token transfer logs.
   - Columns: id, contract_address, topics, data, block_hash, block_number, transaction_hash, transaction_index, log_index, removed, from, to, value.
   - Unique Index: (transaction_hash, from, id).
8. **transfer_erc721_base**: Stores ERC-721 (NFT) transfer logs.
   - Columns: id, contract_address, topics, data, block_hash, block_number, transaction_hash, transaction_index, log_index, removed, from_address, to_address, token_id.
   - Unique Index: (transaction_hash, from_address, id).
9. **users**: Stores user details.
   - Columns: id, email, password, project_name, limit, created_at, updated_at.
   - Unique Constraint: (email, id).
10. **users_base**: Stores blockchain user balances.
    - Columns: id, address, balances, created_at, updated_at.
    - Index: (address, id).
11. **withdrawal_erc20_base**: Stores ERC-20 withdrawal logs.
    - Columns: id, address, topics, data, block_hash, block_number, transaction_hash, transaction_index, log_index, removed, from, to, value.
    - Unique Index: (transaction_hash, from, id).
"""

# Blockchain context for better understanding
BLOCKCHAIN_CONTEXT = """
Common blockchain terms and concepts:
- Address: A 42-character hexadecimal string starting with '0x' that represents an account or contract
- Hash: A 66-character hexadecimal string starting with '0x' that uniquely identifies blocks or transactions
- Token: A fungible asset on the blockchain (ERC-20 standard)
- NFT: A non-fungible token on the blockchain (ERC-721 standard)
- Block: A container for transactions, each with a unique hash and number
- Transaction: A transfer of value or data between addresses
- Gas: Cost to execute operations on the blockchain
- Base Chain: An Ethereum Layer 2 blockchain built with the OP Stack
- In this database, 'value' columns for tokens often use wei (1 ETH = 10^18 wei)
- Timestamps are typically stored in Unix epoch time (seconds since Jan 1, 1970)
"""

# Example queries to help the model generate better SQL
EXAMPLE_QUERIES = """
Example SQL queries for common blockchain questions:

-- Get the 10 most recent blocks
SELECT hash, number, timestamp, gasUsed, gasLimit, miner
FROM blocks_base
ORDER BY CAST(number AS BIGINT) DESC
LIMIT 10;

-- Get the top 10 tokens by price
SELECT token_address, name, symbol, decimals, price
FROM tokens_base
WHERE price IS NOT NULL
ORDER BY CAST(price AS DECIMAL) DESC
LIMIT 10;

-- Get the top 10 addresses by number of transactions
SELECT "from" as address, COUNT(*) as tx_count
FROM transactions_base
GROUP BY "from"
ORDER BY tx_count DESC
LIMIT 10;

-- Get the total value of ERC-20 transfers for each token
SELECT contract_address, SUM(CAST(value AS DECIMAL)) as total_value
FROM transfer_erc20_base
GROUP BY contract_address
ORDER BY total_value DESC
LIMIT 10;

-- Get average gas used per block over time (by day)
SELECT 
    TO_TIMESTAMP(CAST(timestamp AS BIGINT)) as block_date,
    AVG(CAST(gasUsed AS DECIMAL)) as avg_gas_used
FROM blocks_base
GROUP BY TO_CHAR(TO_TIMESTAMP(CAST(timestamp AS BIGINT)), 'YYYY-MM-DD')
ORDER BY block_date;

-- Get all ERC-721 (NFT) transfers for a specific token contract
SELECT from_address, to_address, token_id, transaction_hash
FROM transfer_erc721_base
WHERE contract_address = '0x123abc...'
ORDER BY block_number DESC
LIMIT 20;
"""

class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(text):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{text}{Colors.ENDC}\n")

def print_error(text):
    print(f"{Colors.FAIL}{text}{Colors.ENDC}")

def print_success(text):
    print(f"{Colors.GREEN}{text}{Colors.ENDC}")

def print_info(text):
    print(f"{Colors.CYAN}{text}{Colors.ENDC}")

def print_code(text):
    print(f"{Colors.BLUE}{text}{Colors.ENDC}")

def connect_to_database():
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print_error(f"Database connection error: {str(e)}")
        return None

def execute_query(conn, sql):
    """Execute a SQL query and return the results as a DataFrame."""
    try:
        # Create a new cursor for each query to avoid transaction issues
        conn.autocommit = True  # This prevents transaction block errors
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sql)
        
        if cursor.description:  # Check if the query returns results
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            
            # Convert results to DataFrame
            df = pd.DataFrame(results, columns=columns)
            
            # Close cursor
            cursor.close()
            
            return df, None
        else:
            # For non-SELECT queries
            affected_rows = cursor.rowcount
            cursor.close()
            return pd.DataFrame(), f"Query executed successfully. Rows affected: {affected_rows}"
            
    except Exception as e:
        # Get detailed error
        error_message = str(e)
        return None, error_message

def generate_sql(client, prompt, schema_info, chain, model):
    """Generate SQL from natural language using OpenAI."""
    # Create system message with schema information
    system_message = f"""You are an expert SQL generator for a blockchain database. 
Your task is to convert natural language questions into PostgreSQL queries.

{schema_info}

{BLOCKCHAIN_CONTEXT}

{EXAMPLE_QUERIES}

Important SQL writing guidelines:
1. Always generate valid PostgreSQL syntax
2. Use table names with the suffix '_{chain}' unless referring to the 'users' table
3. Handle quoting properly for reserved words like "from" and "to" in table columns
4. Cast numeric strings to appropriate types (DECIMAL, BIGINT) when needed for math operations
5. Use TO_TIMESTAMP() for timestamp conversions when needed
6. Format timestamps in human-readable format when returning them
7. Use proper JOIN syntax when combining tables
8. Add LIMIT clauses (usually LIMIT 10 or 20) unless asked for all records
9. Add proper WHERE clauses to filter data based on the user's question
10. NEVER use column names that don't exist in the tables

OUTPUT FORMAT: Return ONLY the SQL query, no explanations or markdown formatting."""

    # Call OpenAI API
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": f"Generate PostgreSQL query for: {prompt}"}
            ],
            temperature=0.1  # Low temperature for more deterministic outputs
        )
        
        sql = response.choices[0].message.content.strip()
        
        # Remove markdown code blocks if the model included them
        if sql.startswith("```sql"):
            sql = sql.replace("```sql", "").replace("```", "").strip()
        elif sql.startswith("```"):
            sql = sql.replace("```", "").strip()
            
        return sql, None
    except Exception as e:
        return None, f"Error generating SQL: {str(e)}"

def generate_response(client, prompt, sql, df, model):
    """Generate natural language response from query results."""
    # Create a context with the results
    result_sample = df.head(5).to_string() if df is not None and not df.empty else "No results found"
    row_count = len(df) if df is not None else 0
    col_names = ", ".join(df.columns) if df is not None and not df.empty else ""
    
    # Create system message
    system_message = f"""You are an expert blockchain data analyst who provides clear, concise explanations.
Your task is to explain query results from a blockchain database in natural language.
Focus on providing insights and interpreting the data for the user.

Include relevant statistics like:
- Key metrics and totals
- Interesting patterns or outliers
- Time-based trends if applicable
- Relationships between different data points

Make your response conversational and informative for non-technical users.
If no results were found, explain why there might be no data and suggest alternatives.

OUTPUT FORMAT: A conversational paragraph or two explaining the results, plus 2-3 bullet points highlighting key insights.
DO NOT include the SQL query in your response unless it's specifically requested."""

    # User message with query and results
    user_message = f"""User question: {prompt}
SQL query used: {sql}
Number of rows returned: {row_count}
Column names: {col_names}
Sample of results (first 5 rows):
{result_sample}

Please provide a natural language explanation of these results, focusing on insights that answer the user's question."""

    # Call OpenAI API
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message}
            ],
            temperature=0.7  # Slightly higher temperature for more natural responses
        )
        
        return response.choices[0].message.content, None
    except Exception as e:
        return None, f"Error generating response: {str(e)}"

def display_example_questions():
    """Display example questions that users can ask."""
    print_header("Example questions you can ask:")
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
    
    for i, example in enumerate(examples, 1):
        print(f"{i}. {example}")
    print()

def main():
    # Print welcome message
    print_header("Blockchain Database Chat CLI")
    print("Ask questions about blockchain data in natural language, and get insights!\n")
    
    # Initialize OpenAI client
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        print_error(f"Error initializing OpenAI client: {str(e)}")
        return
    
    # Connect to database
    print_info("Connecting to database...")
    conn = connect_to_database()
    if not conn:
        print_error("Failed to connect to database. Please check your credentials.")
        return
    
    print_success("Successfully connected to database!\n")
    
    # Display example questions
    display_example_questions()
    
    # Main interaction loop
    while True:
        try:
            prompt = input(f"{Colors.BOLD}Ask a question (or type 'exit' to quit): {Colors.ENDC}")
            
            if prompt.lower() in ('exit', 'quit'):
                break
                
            if not prompt.strip():
                continue
                
            print_info("\nGenerating SQL query...")
            
            # Generate SQL using OpenAI
            sql, sql_error = generate_sql(client, prompt, SCHEMA_INFO, CHAIN, OPENAI_MODEL)
            
            if sql_error:
                print_error(f"Failed to generate SQL: {sql_error}")
                continue
                
            # Display generated SQL
            print_info("Generated SQL:")
            print_code(textwrap.indent(sql, '  '))
            print()
            
            # Execute SQL
            print_info("Executing query...")
            result_df, error_message = execute_query(conn, sql)
            
            if error_message:
                if "no results" in error_message.lower():
                    print_info("Query executed, but no results were found.")
                else:
                    print_error(f"Failed to execute query: {error_message}")
                    error_type = "Syntax error" if "syntax" in error_message.lower() else "Database error"
                    print_info(f"Error type: {error_type}")
                continue
                
            if result_df is None or result_df.empty:
                print_info("The query returned no results.")
                continue
                
            # Display results
            print_success(f"Query returned {len(result_df)} rows.")
            print_info("Results preview:")
            
            # Display dataframe with a maximum width to avoid wrapping issues in terminal
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 120)
            pd.set_option('display.max_colwidth', 30)
            
            print(result_df.head(10).to_string())
            print()
            
            # Generate natural language response
            print_info("Generating insights...")
            response, response_error = generate_response(client, prompt, sql, result_df, OPENAI_MODEL)
            
            if response_error:
                print_error(f"Failed to generate insights: {response_error}")
            else:
                print_header("Insights")
                print(response)
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print_error(f"An error occurred: {str(e)}")
    
    # Close connection
    if conn:
        conn.close()
    
    print_info("\nThank you for using Blockchain Database Chat CLI!")

if __name__ == "__main__":
    main()