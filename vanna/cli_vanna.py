import os
from dotenv import load_dotenv
import pandas as pd
from vanna.openai import OpenAI_Chat
from vanna.chromadb import ChromaDB_VectorStore

# CLI CHAT with vanna!

# Load environment variables from .env file
load_dotenv()

class BlockchainVanna(ChromaDB_VectorStore, OpenAI_Chat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        OpenAI_Chat.__init__(self, config=config)

class BlockchainChat:
    def __init__(self):
        self.vn = None
        self.chain = None
        self.connected = False

    def connect_to_database(self):
        print("\n=== Database Connection Setup ===")
        db_host = os.getenv("DB_HOST", "aws-0-us-east-1.pooler.supabase.com")
        db_port = os.getenv("DB_PORT", "6543")
        db_name = os.getenv("DB_NAME", "postgres")
        db_user = os.getenv("DB_USER", "postgres.cbxagjoxdgzpfknkpswj")
        db_password = os.getenv("DB_PASSWORD", "")
        self.chain = os.getenv("CHAIN", "base")
        openai_api_key = os.getenv("OPENAI_API_KEY", "")

        # Initialize Vanna
        try:
            self.vn = BlockchainVanna(config={
                'api_key': openai_api_key,
                'model': 'gpt-4o-mini',
                'allow_llm_to_see_data': True
            })
            
            # Connect to PostgreSQL
            self.vn.connect_to_postgres(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                dbname=db_name
            )
            
            self.connected = True
            print(f"\nSuccessfully connected to {db_name}!")
            self.train_system()
            
        except Exception as e:
            print(f"\nConnection failed: {str(e)}")
            return False
        
        return True

    def train_system(self):
        print("\nTraining system with schema information...")
        try:
            # Get schema info
            df_schema = self.vn.run_sql(f"""
            SELECT 
                table_name, 
                column_name, 
                data_type, 
                is_nullable,
                column_default
            FROM 
                information_schema.columns
            WHERE 
                table_name LIKE '%{self.chain}%' OR table_name = 'users'
            ORDER BY 
                table_name, ordinal_position
            """)
            
            # Train with schema
            plan = self.vn.get_training_plan_generic(df_schema)
            self.vn.train(plan=plan)
            
            # Train with blockchain context
            self.vn.train(documentation=f"""
            # Blockchain Database Context
            
            This database stores blockchain data from the {self.chain} network. Here's what the tables represent:
            
            - blocks_{self.chain}: Contains block data including hash, parent_hash, timestamp, etc.
            - transactions_{self.chain}: Contains transaction data including hash, from, to, value, etc.
            - transfer_erc20_{self.chain}: Records ERC-20 token transfers with from, to, and value.
            - transfer_erc721_{self.chain}: Records ERC-721 NFT transfers with from_address, to_address, and token_id.
            - tokens_{self.chain}: Contains token information including address, name, symbol, and price.
            - deposit_erc20_{self.chain}: Records deposits of ERC-20 tokens.
            - withdrawal_erc20_{self.chain}: Records withdrawals of ERC-20 tokens.
            - pools_{self.chain}: Contains liquidity pool information with token pair details.
            - token_trades_{self.chain}: Records token trades with price information.
            - users_{self.chain}: Contains wallet user information with token balances.
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
            
            # Train with example queries
            example_queries = [
                f"""
                -- Get the 10 most recent blocks
                SELECT hash, number, timestamp, gasUsed, gasLimit, miner
                FROM blocks_{self.chain}
                ORDER BY CAST(number AS BIGINT) DESC
                LIMIT 10
                """,
                # Add other corrected example queries here
            ]
            
            for query in example_queries:
                self.vn.train(sql=query)
            
            print("Training complete!")
            
        except Exception as e:
            print(f"Error during training: {str(e)}")
            
    def display_results(self, df):
        if df is None or df.empty:
            print("\nNo results found.")
            return

        print("\nResults:")
        print("-" * 80)
        print(df.to_string())
        print("-" * 80)
        
        # Display basic statistics
        num_rows = len(df)
        num_cols = len(df.columns)
        print(f"\nFound {num_rows} rows with {num_cols} columns.")
        
        # Show basic stats for numeric columns
        num_columns = df.select_dtypes(include=['number']).columns
        if len(num_columns) > 0:
            print("\nBasic statistics:")
            for col in num_columns[:3]:
                print(f"\n{col}:")
                print(f"  Min: {df[col].min()}")
                print(f"  Max: {df[col].max()}")
                print(f"  Mean: {df[col].mean():.2f}")
                print(f"  Median: {df[col].median():.2f}")

    def chat_loop(self):
        if not self.connected:
            print("Please connect to the database first!")
            return

        print("\nBlockchain Database Chat")
        print("Type 'exit' to quit")
        print("\nExample questions:")
        print("- What are the most recent 10 blocks?")
        print("- Show me the top 5 transactions by value")
        print("- Which wallets have the most token transfers?")
        
        while True:
            print("\n" + "=" * 80)
            question = input("\nAsk a question about blockchain data: ")
            
            if question.lower() in ['exit', 'quit']:
                break
                
            try:
                # Generate SQL
                sql = self.vn.generate_sql(question)
                
                print("\nGenerated SQL:")
                print("-" * 80)
                print(sql)
                print("-" * 80)
                
                # Execute SQL
                result_df = self.vn.run_sql(sql)
                self.display_results(result_df)
                
            except Exception as e:
                print(f"\nError: {str(e)}")

def main():
    chat = BlockchainChat()
    if chat.connect_to_database():
        chat.chat_loop()

if __name__ == "__main__":
    main()