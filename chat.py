import os
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Vanna.ai imports
from vanna.openai import OpenAI_Chat
from vanna.chromadb import ChromaDB_VectorStore

# Custom Vanna class that combines OpenAI and ChromaDB
class BlockchainVanna(ChromaDB_VectorStore, OpenAI_Chat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        OpenAI_Chat.__init__(self, config=config)

# Initialize Streamlit app
st.set_page_config(
    page_title="Blockchain Database Chat",
    page_icon="🔗",
    layout="wide",
)

st.title("Chat with Your Blockchain Database")
st.write("Ask questions about blockchain data in natural language, and get SQL queries and results!")

# Sidebar for configuration
st.sidebar.title("Configuration")

# Database connection settings
db_host = st.sidebar.text_input("Database Host", value=os.getenv("DB_HOST", "localhost"))
db_port = st.sidebar.text_input("Database Port", value=os.getenv("DB_PORT", "5432"))
db_name = st.sidebar.text_input("Database Name", value=os.getenv("DB_NAME", "blockchain_db"))
db_user = st.sidebar.text_input("Database Username", value=os.getenv("DB_USER", "postgres"))
db_password = st.sidebar.text_input("Database Password", value=os.getenv("DB_PASSWORD", "postgres"), type="password")

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

# Initialize Vanna instance when user clicks connect
if st.sidebar.button("Connect to Database"):
    with st.spinner("Connecting to database..."):
        try:
            # Initialize Vanna with OpenAI config
            vn = BlockchainVanna(config={
                'api_key': openai_api_key,
                'model': llm_model
            })
            
            # Connect to PostgreSQL
            vn.connect_to_postgres(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                dbname=db_name
            )
            
            # Store in session state
            st.session_state.vn = vn
            st.session_state.connected = True
            st.session_state.chain = chain
            st.success(f"Successfully connected to {db_name}!")
            
            # Get schema info for training
            with st.expander("Database Schema Information"):
                try:
                    # Get the schema information for the tables with the chain suffix
                    df_schema = vn.run_sql(f"""
                    SELECT 
                        table_name, 
                        column_name, 
                        data_type, 
                        is_nullable,
                        column_default
                    FROM 
                        information_schema.columns
                    WHERE 
                        table_name LIKE '%{chain}%' OR table_name = 'users'
                    ORDER BY 
                        table_name, ordinal_position
                    """)
                    
                    st.dataframe(df_schema)
                    
                    # Create a more structured schema description for training
                    schema_description = ""
                    current_table = ""
                    
                    for _, row in df_schema.iterrows():
                        if row['table_name'] != current_table:
                            current_table = row['table_name']
                            schema_description += f"\n\nTABLE: {current_table}\n"
                            schema_description += "-" * 50 + "\n"
                            schema_description += "Column Name | Data Type | Nullable | Default\n"
                            schema_description += "-" * 50 + "\n"
                        
                        schema_description += f"{row['column_name']} | {row['data_type']} | {row['is_nullable']} | {row['column_default']}\n"
                    
                    st.text_area("Schema Description", schema_description, height=300)
                    
                    # Generate training plan
                    plan = vn.get_training_plan_generic(df_schema)
                    st.write("Generated Training Plan:")
                    st.write(plan)
                    
                    if st.button("Train System with Schema"):
                        with st.spinner("Training system with your database schema..."):
                            vn.train(plan=plan)
                            
                            # Add additional training data for blockchain-specific context
                            vn.train(documentation=f"""
                            # Blockchain Database Context
                            
                            This database stores blockchain data from the {chain} network. Here's what the tables represent:
                            
                            - blocks_{chain}: Contains block data including hash, parent hash, timestamp, etc.
                            - transactions_{chain}: Contains transaction data including hash, from, to, value, etc.
                            - transfer_erc20_{chain}: Records ERC-20 token transfers with from, to, and value.
                            - transfer_erc721_{chain}: Records ERC-721 NFT transfers with from_address, to_address, and token_id.
                            - tokens_{chain}: Contains token information including address, name, symbol, and price.
                            - deposit_erc20_{chain}: Records deposits of ERC-20 tokens.
                            - withdrawal_erc20_{chain}: Records withdrawals of ERC-20 tokens.
                            - pools_{chain}: Contains liquidity pool information with token pair details.
                            - token_trades_{chain}: Records token trades with price information.
                            - users_{chain}: Contains wallet user information with token balances.
                            - users: Contains application user information.
                            
                            Common blockchain terms:
                            - Address: A 42-character hexadecimal string starting with '0x'
                            - Hash: A 66-character hexadecimal string starting with '0x'
                            - Token: A fungible asset on the blockchain (ERC-20)
                            - NFT: A non-fungible token on the blockchain (ERC-721)
                            - Block: A container for transactions, each with a unique hash
                            - Transaction: A transfer of value or data between addresses
                            - Gas: Cost to execute operations on the blockchain
                            """)
                            
                            # Add example SQL queries for training
                            vn.train(sql=f"""
                            -- Get the 10 most recent blocks
                            SELECT 
                                hash, 
                                number, 
                                timestamp,
                                gas_used, 
                                gas_limit, 
                                miner
                            FROM 
                                blocks_{chain}
                            ORDER BY 
                                CAST(number AS BIGINT) DESC
                            LIMIT 10
                            """)
                            
                            vn.train(sql=f"""
                            -- Get the top 10 tokens by price
                            SELECT 
                                token_address,
                                name,
                                symbol,
                                decimals,
                                price
                            FROM 
                                tokens_{chain}
                            ORDER BY 
                                CAST(price AS DECIMAL) DESC
                            LIMIT 10
                            """)
                            
                            vn.train(sql=f"""
                            -- Get the top 10 addresses by number of transactions
                            SELECT 
                                "from" as address,
                                COUNT(*) as tx_count
                            FROM 
                                transactions_{chain}
                            GROUP BY 
                                "from"
                            ORDER BY 
                                tx_count DESC
                            LIMIT 10
                            """)
                            
                            vn.train(sql=f"""
                            -- Get the top 10 wallets by ERC-20 value transferred
                            SELECT 
                                "from",
                                SUM(CAST(value AS DECIMAL)) as total_value_transferred
                            FROM 
                                transfer_erc20_{chain}
                            GROUP BY 
                                "from"
                            ORDER BY 
                                total_value_transferred DESC
                            LIMIT 10
                            """)
                            
                            st.session_state.trained = True
                            st.success("Training complete! You can now start asking questions.")
                except Exception as e:
                    st.error(f"Error fetching schema: {str(e)}")
            
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")

# Training section - only show if connected
if st.session_state.get('connected', False):
    
    st.subheader("Add Custom Training Data")
    
    training_tab1, training_tab2, training_tab3 = st.tabs(["SQL Examples", "DDL Statements", "Documentation"])
    
    with training_tab1:
        sql_example = st.text_area("Enter example SQL queries that are common in blockchain analysis:", key="sql_examples")
        if st.button("Add SQL Example"):
            if sql_example:
                print(f"Adding SQL: {sql_example}")
                st.session_state.vn.train(sql=sql_example)
                st.success("SQL example added to training data!")
    
    with training_tab2:
        ddl_statement = st.text_area("Enter DDL statements (CREATE TABLE, etc.):", key="ddl_statement")
        if st.button("Add DDL Statement"):
            if ddl_statement:
                print(f"Adding ddl: {ddl_statement}")
                st.session_state.vn.train(ddl=ddl_statement)
                st.success("DDL statement added to training data!")
    
    with training_tab3:
        documentation = st.text_area("Enter blockchain terminology, patterns, or business logic:", key="documentation")
        if st.button("Add Documentation"):
            if documentation:
                print(f"Adding documentation....")
                st.session_state.vn.train(documentation=documentation)
                st.success("Documentation added to training data!")
    
    # View and manage training data
    if st.button("View Training Data"):
        training_data = st.session_state.vn.get_training_data()
        st.dataframe(training_data)
        
        # Option to remove training data
        if not training_data.empty:
            training_id = st.selectbox("Select training data to remove:", training_data['id'].tolist())
            if st.button("Remove Selected Training Data"):
                st.session_state.vn.remove_training_data(id=training_id)
                st.success(f"Training data {training_id} removed!")

# Chat interface - only show if connected
if st.session_state.get('connected', False):
    st.header("Ask Questions About Your Blockchain Data")
    
    # Initialize chat history
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            if message["role"] == "assistant" and "dataframe" in message:
                st.write(message["content"])
                st.dataframe(message["dataframe"])
            else:
                st.write(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask a question about blockchain data..."):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Display user message
        with st.chat_message("user"):
            st.write(prompt)
        
        # Generate response
        with st.chat_message("assistant"):
            with st.spinner("Generating SQL..."):
                try:
                    # Generate SQL using Vanna 
                    # We need to handle different versions of Vanna API responses
                    response = st.session_state.vn.generate_sql(prompt)
                    
                    # Check what type of response we got (could be dict, tuple, or string)
                    if isinstance(response, dict) and 'sql' in response:
                        sql = response['sql']
                    elif isinstance(response, tuple) and len(response) > 0:
                        sql = response[0]  # First element might be the SQL
                    elif isinstance(response, str):
                        sql = response
                    else:
                        # Try to handle a different API shape
                        try:
                            sql = st.session_state.vn.ask(question=prompt)
                            if isinstance(sql, dict) and 'sql' in sql:
                                sql = sql['sql']
                            elif isinstance(sql, tuple) and len(sql) > 0:
                                sql = sql[0]
                        except:
                            # Try one more approach: direct ask
                            sql = prompt
                            
                    # Once we have SQL (or think we do), display it
                    if sql:
                        st.write(f"I've generated the following SQL to answer your question:")
                        st.code(sql, language="sql")
                        
                        # Execute the SQL
                        try:
                            # First, try to run the SQL directly
                            result_df = st.session_state.vn.run_sql(sql)
                            
                            if result_df is None or result_df.empty:
                                st.write("The query returned no results. You might want to refine your question or check if there's data that matches your criteria.")
                                
                                # Add to chat history
                                message_content = f"I've generated the following SQL for your question:\n```sql\n{sql}\n```\nThe query returned no results. You might want to refine your question or check if there's data that matches your criteria."
                                st.session_state.messages.append({
                                    "role": "assistant", 
                                    "content": message_content
                                })
                            else:
                                st.write("Here are the results:")
                                st.dataframe(result_df)
                                
                                # Basic analysis
                                num_rows = len(result_df)
                                num_cols = len(result_df.columns)
                                
                                analysis = f"This query returned {num_rows} rows with {num_cols} columns."
                                
                                # Simple stats without matplotlib
                                num_columns = result_df.select_dtypes(include=['number']).columns
                                if len(num_columns) > 0:
                                    for col in num_columns[:3]:  # Limit to first 3 numeric columns
                                        analysis += f"\n\nStatistics for {col}:"
                                        analysis += f"\n- Min: {result_df[col].min()}"
                                        analysis += f"\n- Max: {result_df[col].max()}"
                                        analysis += f"\n- Mean: {result_df[col].mean():.2f}"
                                        analysis += f"\n- Median: {result_df[col].median():.2f}"
                                
                                st.write(analysis)
                                
                                # Add to chat history with dataframe
                                message_content = f"I've generated the following SQL for your question:\n```sql\n{sql}\n```\nHere are the results:\n\n{analysis}"
                                st.session_state.messages.append({
                                    "role": "assistant", 
                                    "content": message_content,
                                    "dataframe": result_df
                                })
                        except Exception as e:
                            st.error(f"Error executing SQL: {str(e)}")
                            st.session_state.messages.append({
                                "role": "assistant", 
                                "content": f"I generated this SQL:\n```sql\n{sql}\n```\nBut encountered an error: {str(e)}"
                            })
                    else:
                        st.write("I couldn't generate SQL for your question. Please try rephrasing to focus on blockchain data stored in the database.")
                        st.session_state.messages.append({
                            "role": "assistant", 
                            "content": "I couldn't generate SQL for your question. Please try rephrasing to focus on blockchain data stored in the database."
                        })
                
                except Exception as e:
                    st.error(f"Error: {str(e)}")
                    # Try a different approach - direct SQL generation
                    try:
                        # Generate SQL using Vanna's underlying methods (bypassing the normal ask method)
                        sql_prompt = [
                            {"role": "system", "content": f"Generate SQL query for PostgreSQL to answer this question about blockchain data. Tables include: blocks_{chain}, transactions_{chain}, transfer_erc20_{chain}, transfer_erc721_{chain}, tokens_{chain}, pools_{chain}, token_trades_{chain}, users_{chain}. Provide ONLY the SQL query with no explanations."},
                            {"role": "user", "content": prompt}
                        ]
                        
                        # Try to generate SQL using the raw prompt
                        sql = st.session_state.vn.generate_response(sql_prompt)
                        
                        if isinstance(sql, str) and "SELECT" in sql.upper():
                            st.write(f"I've generated the following SQL to answer your question using a direct approach:")
                            st.code(sql, language="sql")
                            
                            # Try to execute it
                            try:
                                result_df = st.session_state.vn.run_sql(sql)
                                if result_df is not None and not result_df.empty:
                                    st.write("Here are the results:")
                                    st.dataframe(result_df)
                                    
                                    # Add to chat history with dataframe
                                    message_content = f"I've generated SQL using a direct approach:\n```sql\n{sql}\n```\nHere are the results."
                                    st.session_state.messages.append({
                                        "role": "assistant", 
                                        "content": message_content,
                                        "dataframe": result_df
                                    })
                                else:
                                    st.write("The query returned no results.")
                                    st.session_state.messages.append({
                                        "role": "assistant", 
                                        "content": f"I generated SQL using a direct approach:\n```sql\n{sql}\n```\nBut the query returned no results."
                                    })
                            except Exception as e2:
                                st.error(f"Error executing SQL from direct approach: {str(e2)}")
                                st.session_state.messages.append({
                                    "role": "assistant", 
                                    "content": f"I generated SQL using a direct approach:\n```sql\n{sql}\n```\nBut encountered an error: {str(e2)}"
                                })
                        else:
                            st.session_state.messages.append({
                                "role": "assistant", 
                                "content": f"I encountered an error and couldn't generate SQL: {str(e)}"
                            })
                    except Exception as e3:
                        st.session_state.messages.append({
                            "role": "assistant", 
                            "content": f"I encountered an error and couldn't generate SQL: {str(e)}"
                        })

# Show a message if not connected
if not st.session_state.get('connected', False):
    st.info("Please connect to your blockchain database using the sidebar to get started.")
    
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
st.markdown("Blockchain Database Chat")