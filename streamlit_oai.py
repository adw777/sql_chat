import os
import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras
from openai import OpenAI
from dotenv import load_dotenv
import json
import traceback
import time

# STREAMLIT DIRECT OAI CHAT!

# Load environment variables from .env file
load_dotenv()

# Initialize Streamlit app
st.set_page_config(
    page_title="Blockchain Database Chat",
    page_icon="🔗",
    layout="wide",
)

st.title("Chat with Your Blockchain Database")
st.write("Ask questions about blockchain data in natural language, and get insights!")

# Sidebar for configuration
st.sidebar.title("Configuration")

# Database connection settings
db_host = st.sidebar.text_input("Database Host", value=os.getenv("DB_HOST", "aws-0-us-east-1.pooler.supabase.com"))
db_port = st.sidebar.text_input("Database Port", value=os.getenv("DB_PORT", "6543"))
db_name = st.sidebar.text_input("Database Name", value=os.getenv("DB_NAME", "postgres"))
db_user = st.sidebar.text_input("Database Username", value=os.getenv("DB_USER", "postgres.cbxagjoxdgzpfknkpswj"))
db_password = st.sidebar.text_input("Database Password", value=os.getenv("DB_PASSWORD", "amit"), type="password")

# Chain selection (this corresponds to the suffix in table names)
chain = st.sidebar.text_input("Blockchain Chain", value=os.getenv("CHAIN", "base"))

# LLM Configuration
openai_api_key = st.sidebar.text_input(
    "OpenAI API Key", 
    value=os.getenv("OPENAI_API_KEY", ""),
    type="password"
)

llm_model = st.sidebar.selectbox(
    "OpenAI Model",
    ["gpt-4o", "gpt-4o-mini", "gpt-3.5-turbo"],
    index=1  # Default to gpt-4o-mini for good balance of cost/performance
)

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

# Add blockchain context
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

# Example queries to help the model
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

# Function to connect to the database
def connect_to_database():
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        return conn
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        return None

# Function to execute SQL queries
def execute_query(conn, sql):
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        
        # Convert results to DataFrame
        df = pd.DataFrame(results, columns=columns)
        
        # Close cursor
        cursor.close()
        
        return df
    except Exception as e:
        # Get detailed error
        error_message = str(e)
        traceback_details = traceback.format_exc()
        return None, error_message, traceback_details

# Function to generate SQL from natural language using OpenAI
def generate_sql(client, prompt, schema_info, chain, model):
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
            
        return sql
    except Exception as e:
        return f"Error generating SQL: {str(e)}"

# Function to handle SQL errors and suggest fixes
def handle_sql_error(client, error_message, sql, prompt, model):
    try:
        error_system_prompt = f"""You are an expert SQL troubleshooter for a PostgreSQL blockchain database.
A SQL query has failed with the following error. Fix the query based on the error message.

Database schema:
{SCHEMA_INFO}

OUTPUT FORMAT: Return ONLY the corrected SQL query, no explanations."""

        error_user_prompt = f"""Original query: {sql}
Error message: {error_message}
User's original question: {prompt}

Please fix the SQL query to resolve this error."""

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": error_system_prompt},
                {"role": "user", "content": error_user_prompt}
            ],
            temperature=0.1
        )
        
        fixed_sql = response.choices[0].message.content.strip()
        
        # Remove markdown code blocks if the model included them
        if fixed_sql.startswith("```sql"):
            fixed_sql = fixed_sql.replace("```sql", "").replace("```", "").strip()
        elif fixed_sql.startswith("```"):
            fixed_sql = fixed_sql.replace("```", "").strip()
            
        return fixed_sql
    except Exception as e:
        return f"Error fixing SQL: {str(e)}"

# Function to generate natural language response from query results
def generate_response(client, prompt, sql, df, model):
    # Create a context with the results
    result_sample = df.head(5).to_string() if not df.empty else "No results found"
    row_count = len(df)
    col_names = ", ".join(df.columns)
    
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
If no results were found, suggest refining the search or explain why there might be no data.

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
        
        return response.choices[0].message.content
    except Exception as e:
        return f"Error generating response: {str(e)}"

# Initialize OpenAI client when button is clicked
if st.sidebar.button("Connect to Services"):
    with st.spinner("Connecting to database and OpenAI..."):
        try:
            # Initialize OpenAI client
            if not openai_api_key:
                st.error("Please provide your OpenAI API key in the sidebar.")
            else:
                client = OpenAI(api_key=openai_api_key)
                st.session_state.openai_client = client
                
                # Test OpenAI connection
                test_response = client.chat.completions.create(
                    model=llm_model,
                    messages=[
                        {"role": "user", "content": "Reply with 'OpenAI connection successful' if you can read this."}
                    ],
                    max_tokens=20,
                )
                
                # Test database connection
                conn = connect_to_database()
                if conn:
                    st.session_state.db_conn = conn
                    st.session_state.connected = True
                    st.session_state.chain = chain
                    st.success("Successfully connected to database and OpenAI!")
                    
                    # Get sample table row counts to verify connection
                    try:
                        cursor = conn.cursor()
                        cursor.execute(f"SELECT COUNT(*) FROM blocks_{chain} LIMIT 1")
                        block_count = cursor.fetchone()[0]
                        cursor.close()
                        st.info(f"Database contains data - found {block_count} rows in blocks_{chain} table.")
                    except Exception as e:
                        st.warning(f"Connected to database but couldn't verify table contents: {str(e)}")
                else:
                    st.error("Failed to connect to database. Please check your credentials.")
        except Exception as e:
            st.error(f"Error connecting to services: {str(e)}")

# Initialize chat history
if 'messages' not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])
        if message["role"] == "assistant" and "dataframe" in message:
            st.dataframe(message["dataframe"])

# Chat interface - only show if connected
if st.session_state.get('connected', False):
    st.header("Ask Questions About Your Blockchain Data")
    
    # Chat input
    if prompt := st.chat_input("Ask a question about blockchain data..."):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Display user message
        with st.chat_message("user"):
            st.write(prompt)
        
        # Generate response
        with st.chat_message("assistant"):
            # Generate SQL using OpenAI
            with st.spinner("Generating SQL..."):
                sql = generate_sql(
                    st.session_state.openai_client, 
                    prompt, 
                    SCHEMA_INFO, 
                    st.session_state.chain,
                    llm_model
                )
                
                st.write("I'm analyzing your question...")
                st.code(sql, language="sql")
                
                # Execute SQL
                try:
                    result_df = execute_query(st.session_state.db_conn, sql)
                    
                    # Check if there was an error
                    if isinstance(result_df, tuple) and len(result_df) == 3:
                        df, error_message, traceback_details = result_df
                        st.error(f"Error executing SQL: {error_message}")
                        
                        # Try to fix the SQL
                        st.write("Let me try to fix that query...")
                        fixed_sql = handle_sql_error(
                            st.session_state.openai_client,
                            error_message,
                            sql,
                            prompt,
                            llm_model
                        )
                        
                        if fixed_sql and fixed_sql != sql:
                            st.code(fixed_sql, language="sql")
                            
                            # Try the fixed query
                            try:
                                result_df = execute_query(st.session_state.db_conn, fixed_sql)
                                
                                if isinstance(result_df, tuple) and len(result_df) == 3:
                                    df, error_message, _ = result_df
                                    st.error(f"Still getting an error: {error_message}")
                                    st.write("I'll need more specific information. Could you please clarify your question?")
                                    
                                    # Add to chat history
                                    message_content = f"I tried to answer your question, but encountered database errors. Here's what I tried:\n\nFirst query:\n```sql\n{sql}\n```\n\nThen I tried:\n```sql\n{fixed_sql}\n```\n\nBut I still got an error. Could you please clarify your question with more specific details?"
                                    st.session_state.messages.append({
                                        "role": "assistant", 
                                        "content": message_content
                                    })
                                else:
                                    # Success with fixed query
                                    df = result_df
                            except Exception as e2:
                                st.error(f"Error executing fixed SQL: {str(e2)}")
                                message_content = f"I tried to answer your question, but encountered database errors. Could you please clarify your question?"
                                st.session_state.messages.append({
                                    "role": "assistant", 
                                    "content": message_content
                                })
                        else:
                            message_content = f"I tried to analyze your question but couldn't generate a working SQL query. Could you please rephrase your question to focus specifically on the blockchain data available in the database?"
                            st.session_state.messages.append({
                                "role": "assistant", 
                                "content": message_content
                            })
                    elif result_df is None:
                        st.error("Failed to execute query")
                        message_content = f"I tried to answer your question with this SQL query:\n```sql\n{sql}\n```\nBut I encountered an error executing it. Could you please rephrase your question?"
                        st.session_state.messages.append({
                            "role": "assistant", 
                            "content": message_content
                        })
                    else:
                        # Success - no errors
                        df = result_df
                        
                        if df.empty:
                            st.write("The query returned no results. You might want to refine your question or check if there's data that matches your criteria.")
                            message_content = f"I searched the blockchain database with this query:\n```sql\n{sql}\n```\nBut I didn't find any matching data. This could be because:\n\n- The specific data you're looking for might not be in the database\n- The criteria might be too restrictive\n- You might want to try a broader search term\n\nCould you please refine your question?"
                            st.session_state.messages.append({
                                "role": "assistant", 
                                "content": message_content
                            })
                        else:
                            # Display results
                            st.write("Here are the results:")
                            st.dataframe(df)
                            
                            # Generate natural language response
                            response = generate_response(
                                st.session_state.openai_client,
                                prompt,
                                sql,
                                df,
                                llm_model
                            )
                            
                            st.write(response)
                            
                            # Add to chat history with dataframe
                            st.session_state.messages.append({
                                "role": "assistant", 
                                "content": response,
                                "dataframe": df
                            })
                
                except Exception as e:
                    st.error(f"Error: {str(e)}")
                    st.session_state.messages.append({
                        "role": "assistant", 
                        "content": f"I encountered an error while trying to process your question: {str(e)}"
                    })
else:
    # Show a message if not connected
    st.info("Please connect to the blockchain database and OpenAI using the sidebar to get started.")
    
    # Add sample questions as examples
    st.subheader("Example questions you can ask after connecting:")
    sample_questions = [
        "What are the most recent 10 blocks?",
        "Show me the top 5 transactions by value",
        "Which wallets have the most token transfers?",
        "What's the average gas used per block?",
        "Show me all ERC-20 transfers with value over 1000",
        "Which tokens have the highest price?",
        "Show me the total transaction count per day",
        "Which addresses have the most NFTs?",
    ]
    
    for question in sample_questions:
        st.markdown(f"- *{question}*")

# Footer
st.markdown("---")
st.markdown("Blockchain Database Chat • Powered by OpenAI & Streamlit")