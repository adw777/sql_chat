import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import random
import string
import json
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection parameters - replace with your own
DB_NAME = os.getenv("DB_NAME", "blockchain_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# Chain for table names (similar to config.chain in the TS code)
CHAIN = "base"

def create_database():
    """Create the database if it doesn't exist"""
    
    # Connect to PostgreSQL server
    conn = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # Check if database exists
    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
    exists = cursor.fetchone()
    
    if not exists:
        print(f"Creating database {DB_NAME}...")
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"Database {DB_NAME} created successfully")
    else:
        print(f"Database {DB_NAME} already exists")
    
    cursor.close()
    conn.close()

def connect_to_db():
    """Connect to the database"""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def create_tables():
    """Create all the tables based on the TypeScript schema"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    # Create blocks table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS blocks_{CHAIN} (
        id SERIAL PRIMARY KEY,
        hash TEXT NOT NULL,
        parent_hash TEXT,
        timestamp TEXT,
        number TEXT,
        gas_limit TEXT,
        gas_used TEXT,
        transactions_root TEXT,
        receipts_root TEXT,
        logs_bloom TEXT,
        difficulty TEXT,
        base_fee_per_gas TEXT,
        withdrawals_root TEXT,
        blob_gas_used TEXT,
        excess_blob_gas TEXT,
        parent_beacon_block_root TEXT,
        sha3_uncles TEXT,
        miner TEXT,
        state_root TEXT,
        nonce TEXT,
        mix_hash TEXT,
        extra_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT blocks_unique_hash_{CHAIN} UNIQUE (hash, id)
    )
    """)
    
    # Create deposit_erc20 table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS deposit_erc20_{CHAIN} (
        id SERIAL PRIMARY KEY,
        address TEXT,
        topics TEXT[],
        data TEXT,
        block_hash TEXT,
        block_number TEXT,
        transaction_hash TEXT,
        transaction_index INTEGER,
        log_index INTEGER,
        removed BOOLEAN,
        "from" TEXT,
        "to" TEXT,
        value TEXT,
        CONSTRAINT deposit_erc20_unique_hash_{CHAIN} UNIQUE (transaction_hash, "from", id)
    )
    """)
    
    # Create pools table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS pools_{CHAIN} (
        id SERIAL PRIMARY KEY NOT NULL,
        pool_address TEXT NOT NULL,
        token_a_address TEXT NOT NULL,
        token_b_address TEXT NOT NULL,
        token_a_details JSONB,
        token_b_details JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        CONSTRAINT pool_address_index_{CHAIN} UNIQUE (id)
    )
    """)
    
    # Create tokens table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS tokens_{CHAIN} (
        id SERIAL PRIMARY KEY NOT NULL,
        token_address VARCHAR NOT NULL UNIQUE,
        name TEXT,
        symbol TEXT,
        decimals INTEGER,
        total_supply TEXT,
        price TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        CONSTRAINT token_address_index_{CHAIN} UNIQUE (token_address)
    )
    """)
    
    # Create token_trades table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS token_trades_{CHAIN} (
        id SERIAL PRIMARY KEY NOT NULL,
        token_address TEXT,
        name TEXT,
        symbol TEXT,
        decimals INTEGER,
        total_supply TEXT,
        price TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        pool_address TEXT NOT NULL,
        transaction_hash TEXT NOT NULL,
        CONSTRAINT token_trades_{CHAIN}_token_address_index UNIQUE (token_address, transaction_hash, id)
    )
    """)
    
    # Create transactions table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS transactions_{CHAIN} (
        id SERIAL PRIMARY KEY,
        type INTEGER NOT NULL,
        chain_id TEXT,
        nonce TEXT,
        gas_price TEXT,
        gas TEXT,
        "to" TEXT,
        value TEXT,
        input TEXT,
        r TEXT,
        s TEXT,
        v TEXT,
        hash TEXT NOT NULL,
        block_hash TEXT NOT NULL,
        block_number TEXT,
        transaction_index INTEGER,
        "from" TEXT NOT NULL
    )
    """)
    
    # Create index on transactions table
    cursor.execute(f"""
    CREATE INDEX IF NOT EXISTS transactions_hash_index_{CHAIN} ON transactions_{CHAIN} (hash, "from", id)
    """)
    
    # Create transfer_erc20 table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS transfer_erc20_{CHAIN} (
        id SERIAL PRIMARY KEY,
        contract_address TEXT NOT NULL,
        topics TEXT[] NOT NULL,
        data TEXT NOT NULL,
        block_hash TEXT NOT NULL,
        block_number TEXT NOT NULL,
        transaction_hash TEXT NOT NULL,
        transaction_index TEXT NOT NULL,
        log_index TEXT NOT NULL,
        removed BOOLEAN NOT NULL,
        "from" TEXT,
        "to" TEXT,
        value TEXT,
        CONSTRAINT transfer_erc20_unique_hash_{CHAIN} UNIQUE (transaction_hash, "from", id)
    )
    """)
    
    # Create transfer_erc721 table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS transfer_erc721_{CHAIN} (
        id SERIAL PRIMARY KEY,
        contract_address TEXT NOT NULL,
        topics TEXT[] NOT NULL,
        data TEXT NOT NULL,
        block_hash TEXT NOT NULL,
        block_number TEXT NOT NULL,
        transaction_hash TEXT NOT NULL,
        transaction_index TEXT NOT NULL,
        log_index TEXT NOT NULL,
        removed BOOLEAN NOT NULL,
        from_address TEXT NOT NULL,
        to_address TEXT NOT NULL,
        token_id TEXT NOT NULL,
        CONSTRAINT transfer_erc721_unique_hash_{CHAIN} UNIQUE (transaction_hash, from_address, id)
    )
    """)
    
    # Create users table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        password VARCHAR(255) NOT NULL,
        project_name VARCHAR(255) NOT NULL,
        "limit" INTEGER DEFAULT 0,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
        CONSTRAINT Users_email_idx_ UNIQUE (email, id)
    )
    """)
    
    # Create users_{CHAIN} table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS users_{CHAIN} (
        id SERIAL PRIMARY KEY,
        address VARCHAR(255) NOT NULL,
        balances JSONB NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
    )
    """)
    
    # Create index on users_{CHAIN} table
    cursor.execute(f"""
    CREATE INDEX IF NOT EXISTS users_address_idx_{CHAIN} ON users_{CHAIN} (address, id)
    """)
    
    # Create withdrawal_erc20 table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS withdrawal_erc20_{CHAIN} (
        id SERIAL PRIMARY KEY,
        address TEXT,
        topics TEXT[],
        data TEXT,
        block_hash TEXT,
        block_number TEXT,
        transaction_hash TEXT,
        transaction_index TEXT,
        log_index TEXT,
        removed BOOLEAN,
        "from" TEXT,
        "to" TEXT,
        value TEXT,
        CONSTRAINT withdrawal_erc20_unique_hash_index_{CHAIN} UNIQUE (transaction_hash, "from", id)
    )
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Tables created successfully")

def generate_random_hex(length):
    """Generate a random hex string of specified length"""
    return '0x' + ''.join(random.choice(string.hexdigits.lower()) for _ in range(length))

def generate_blocks(n):
    """Generate and insert n blocks into the blocks table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} blocks...")
    
    batch_size = 1000
    blocks = []
    
    for i in range(n):
        block = {
            'hash': generate_random_hex(64),
            'parent_hash': generate_random_hex(64),
            'timestamp': str(int(time.time()) - random.randint(0, 10000000)),
            'number': str(i),
            'gas_limit': str(random.randint(10000000, 100000000)),
            'gas_used': str(random.randint(1000000, 30000000)),
            'transactions_root': generate_random_hex(64),
            'receipts_root': generate_random_hex(64),
            'logs_bloom': generate_random_hex(256),
            'difficulty': '0',
            'base_fee_per_gas': str(random.randint(1000000, 50000000)),
            'withdrawals_root': generate_random_hex(64),
            'blob_gas_used': '0',
            'excess_blob_gas': '0',
            'parent_beacon_block_root': generate_random_hex(64),
            'sha3_uncles': generate_random_hex(64),
            'miner': generate_random_hex(40),
            'state_root': generate_random_hex(64),
            'nonce': '0',
            'mix_hash': generate_random_hex(64),
            'extra_data': generate_random_hex(2)
        }
        
        blocks.append(block)
        
        if len(blocks) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
                block['hash'], 
                block['parent_hash'],
                block['timestamp'],
                block['number'],
                block['gas_limit'],
                block['gas_used'],
                block['transactions_root'],
                block['receipts_root'],
                block['logs_bloom'],
                block['difficulty'],
                block['base_fee_per_gas'],
                block['withdrawals_root'],
                block['blob_gas_used'],
                block['excess_blob_gas'],
                block['parent_beacon_block_root'],
                block['sha3_uncles'],
                block['miner'],
                block['state_root'],
                block['nonce'],
                block['mix_hash'],
                block['extra_data']
            )).decode('utf-8') for block in blocks)
            
            cursor.execute(f"""
            INSERT INTO blocks_{CHAIN} (
                hash, parent_hash, timestamp, number, gas_limit, gas_used, 
                transactions_root, receipts_root, logs_bloom, difficulty, 
                base_fee_per_gas, withdrawals_root, blob_gas_used, excess_blob_gas, 
                parent_beacon_block_root, sha3_uncles, miner, state_root, nonce, 
                mix_hash, extra_data
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            blocks = []
            print(f"Inserted {i+1}/{n} blocks ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} blocks in {time.time() - start_time:.2f} seconds")

def generate_token_record():
    """Generate a random token record"""
    return {
        'token_address': generate_random_hex(40),
        'name': ''.join(random.choices(string.ascii_letters, k=random.randint(3, 10))),
        'symbol': ''.join(random.choices(string.ascii_uppercase, k=random.randint(2, 5))),
        'decimals': random.randint(1, 18),
        'total_supply': str(random.randint(1000000, 1000000000000)),
        'price': str(round(random.uniform(0.01, 1000), random.randint(1, 8)))
    }

def generate_tokens(n):
    """Generate and insert n tokens into the tokens table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} tokens...")
    
    batch_size = 1000
    tokens = []
    
    for i in range(n):
        token = generate_token_record()
        tokens.append(token)
        
        if len(tokens) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s)", (
                token['token_address'],
                token['name'],
                token['symbol'],
                token['decimals'],
                token['total_supply'],
                token['price']
            )).decode('utf-8') for token in tokens)
            
            cursor.execute(f"""
            INSERT INTO tokens_{CHAIN} (
                token_address, name, symbol, decimals, total_supply, price
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            tokens = []
            print(f"Inserted {i+1}/{n} tokens ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} tokens in {time.time() - start_time:.2f} seconds")

def generate_pools(n):
    """Generate and insert n pools into the pools table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} pools...")
    
    batch_size = 1000
    pools = []
    
    for i in range(n):
        token_a = generate_token_record()
        token_b = generate_token_record()
        
        pool = {
            'pool_address': generate_random_hex(40),
            'token_a_address': token_a['token_address'],
            'token_b_address': token_b['token_address'],
            'token_a_details': json.dumps(token_a),
            'token_b_details': json.dumps(token_b)
        }
        
        pools.append(pool)
        
        if len(pools) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s)", (
                pool['pool_address'],
                pool['token_a_address'],
                pool['token_b_address'],
                pool['token_a_details'],
                pool['token_b_details']
            )).decode('utf-8') for pool in pools)
            
            cursor.execute(f"""
            INSERT INTO pools_{CHAIN} (
                pool_address, token_a_address, token_b_address, token_a_details, token_b_details
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            pools = []
            print(f"Inserted {i+1}/{n} pools ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} pools in {time.time() - start_time:.2f} seconds")

def generate_transactions(n):
    """Generate and insert n transactions into the transactions table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} transactions...")
    
    batch_size = 1000
    transactions = []
    
    for i in range(n):
        transaction = {
            'type': random.randint(0, 2),
            'chain_id': str(random.randint(1, 10000)),
            'nonce': str(random.randint(0, 1000)),
            'gas_price': str(random.randint(1000000, 50000000)),
            'gas': str(random.randint(21000, 500000)),
            'to': generate_random_hex(40),
            'value': str(random.randint(0, 10**18)),
            'input': generate_random_hex(random.randint(10, 200)),
            'r': generate_random_hex(64),
            's': generate_random_hex(64),
            'v': str(random.randint(0, 20000)),
            'hash': generate_random_hex(64),
            'block_hash': generate_random_hex(64),
            'block_number': str(random.randint(1, 1000000)),
            'transaction_index': random.randint(0, 200),
            'from': generate_random_hex(40)
        }
        
        transactions.append(transaction)
        
        if len(transactions) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
                transaction['type'],
                transaction['chain_id'],
                transaction['nonce'],
                transaction['gas_price'],
                transaction['gas'],
                transaction['to'],
                transaction['value'],
                transaction['input'],
                transaction['r'],
                transaction['s'],
                transaction['v'],
                transaction['hash'],
                transaction['block_hash'],
                transaction['block_number'],
                transaction['transaction_index'],
                transaction['from']
            )).decode('utf-8') for transaction in transactions)
            
            cursor.execute(f"""
            INSERT INTO transactions_{CHAIN} (
                type, chain_id, nonce, gas_price, gas, "to", value, input, r, s, v, hash, 
                block_hash, block_number, transaction_index, "from"
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            transactions = []
            print(f"Inserted {i+1}/{n} transactions ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} transactions in {time.time() - start_time:.2f} seconds")

def generate_transfer_erc20(n):
    """Generate and insert n ERC20 transfers into the transfer_erc20 table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} ERC20 transfers...")
    
    batch_size = 1000
    transfers = []
    
    for i in range(n):
        transfer = {
            'contract_address': generate_random_hex(40),
            'topics': [generate_random_hex(64)],
            'data': generate_random_hex(64),
            'block_hash': generate_random_hex(64),
            'block_number': str(random.randint(1, 1000000)),
            'transaction_hash': generate_random_hex(64),
            'transaction_index': str(random.randint(0, 200)),
            'log_index': str(random.randint(0, 1000)),
            'removed': random.choice([True, False]),
            'from': generate_random_hex(40),
            'to': generate_random_hex(40),
            'value': str(random.randint(1, 10**18))
        }
        
        transfers.append(transfer)
        
        if len(transfers) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
                transfer['contract_address'],
                transfer['topics'],
                transfer['data'],
                transfer['block_hash'],
                transfer['block_number'],
                transfer['transaction_hash'],
                transfer['transaction_index'],
                transfer['log_index'],
                transfer['removed'],
                transfer['from'],
                transfer['to'],
                transfer['value']
            )).decode('utf-8') for transfer in transfers)
            
            cursor.execute(f"""
            INSERT INTO transfer_erc20_{CHAIN} (
                contract_address, topics, data, block_hash, block_number, transaction_hash, 
                transaction_index, log_index, removed, "from", "to", value
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            transfers = []
            print(f"Inserted {i+1}/{n} ERC20 transfers ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} ERC20 transfers in {time.time() - start_time:.2f} seconds")

def generate_transfer_erc721(n):
    """Generate and insert n ERC721 transfers into the transfer_erc721 table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} ERC721 transfers...")
    
    batch_size = 1000
    transfers = []
    
    for i in range(n):
        transfer = {
            'contract_address': generate_random_hex(40),
            'topics': [generate_random_hex(64)],
            'data': generate_random_hex(64),
            'block_hash': generate_random_hex(64),
            'block_number': str(random.randint(1, 1000000)),
            'transaction_hash': generate_random_hex(64),
            'transaction_index': str(random.randint(0, 200)),
            'log_index': str(random.randint(0, 1000)),
            'removed': random.choice([True, False]),
            'from_address': generate_random_hex(40),
            'to_address': generate_random_hex(40),
            'token_id': str(random.randint(1, 10000))
        }
        
        transfers.append(transfer)
        
        if len(transfers) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
                transfer['contract_address'],
                transfer['topics'],
                transfer['data'],
                transfer['block_hash'],
                transfer['block_number'],
                transfer['transaction_hash'],
                transfer['transaction_index'],
                transfer['log_index'],
                transfer['removed'],
                transfer['from_address'],
                transfer['to_address'],
                transfer['token_id']
            )).decode('utf-8') for transfer in transfers)
            
            cursor.execute(f"""
            INSERT INTO transfer_erc721_{CHAIN} (
                contract_address, topics, data, block_hash, block_number, transaction_hash, 
                transaction_index, log_index, removed, from_address, to_address, token_id
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            transfers = []
            print(f"Inserted {i+1}/{n} ERC721 transfers ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} ERC721 transfers in {time.time() - start_time:.2f} seconds")

def generate_users(n):
    """Generate and insert n users into the users table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} users...")
    
    batch_size = 1000
    users = []
    
    for i in range(n):
        user = {
            'email': f"user{i}_{random.randint(10000, 99999)}@example.com",
            'password': ''.join(random.choices(string.ascii_letters + string.digits, k=12)),
            'project_name': f"Project_{random.randint(1000, 9999)}_{i}",
            'limit': random.randint(0, 10000)
        }
        
        users.append(user)
        
        if len(users) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s)", (
                user['email'],
                user['password'],
                user['project_name'],
                user['limit']
            )).decode('utf-8') for user in users)
            
            cursor.execute(f"""
            INSERT INTO users (
                email, password, project_name, "limit"
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            users = []
            print(f"Inserted {i+1}/{n} users ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} users in {time.time() - start_time:.2f} seconds")

def generate_wallet_users(n):
    """Generate and insert n wallet users into the users_{CHAIN} table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} wallet users...")
    
    batch_size = 1000
    users = []
    
    for i in range(n):
        # Generate random balances
        num_tokens = random.randint(1, 10)
        balances = []
        
        for _ in range(num_tokens):
            balances.append({
                'token': generate_random_hex(40),
                'balance': str(random.randint(1, 10**18))
            })
        
        user = {
            'address': generate_random_hex(40),
            'balances': json.dumps(balances)
        }
        
        users.append(user)
        
        if len(users) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s)", (
                user['address'],
                user['balances']
            )).decode('utf-8') for user in users)
            
            cursor.execute(f"""
            INSERT INTO users_{CHAIN} (
                address, balances
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            users = []
            print(f"Inserted {i+1}/{n} wallet users ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} wallet users in {time.time() - start_time:.2f} seconds")

def generate_deposits(n):
    """Generate and insert n deposits into the deposit_erc20 table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} deposits...")
    
    batch_size = 1000
    deposits = []
    
    for i in range(n):
        deposit = {
            'address': generate_random_hex(40),
            'topics': [generate_random_hex(64), generate_random_hex(64)],
            'data': generate_random_hex(64),
            'block_hash': generate_random_hex(64),
            'block_number': str(random.randint(1, 1000000)),
            'transaction_hash': generate_random_hex(64),
            'transaction_index': random.randint(0, 200),
            'log_index': random.randint(0, 1000),
            'removed': random.choice([True, False]),
            'from': generate_random_hex(40),
            'to': generate_random_hex(40),
            'value': str(random.randint(1, 10**18))
        }
        
        deposits.append(deposit)
        
        if len(deposits) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
                deposit['address'],
                deposit['topics'],
                deposit['data'],
                deposit['block_hash'],
                deposit['block_number'],
                deposit['transaction_hash'],
                deposit['transaction_index'],
                deposit['log_index'],
                deposit['removed'],
                deposit['from'],
                deposit['to'],
                deposit['value']
            )).decode('utf-8') for deposit in deposits)
            
            cursor.execute(f"""
            INSERT INTO deposit_erc20_{CHAIN} (
                address, topics, data, block_hash, block_number, transaction_hash, 
                transaction_index, log_index, removed, "from", "to", value
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            deposits = []
            print(f"Inserted {i+1}/{n} deposits ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} deposits in {time.time() - start_time:.2f} seconds")

def generate_withdrawals(n):
    """Generate and insert n withdrawals into the withdrawal_erc20 table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} withdrawals...")
    
    batch_size = 1000
    withdrawals = []
    
    for i in range(n):
        withdrawal = {
            'address': generate_random_hex(40),
            'topics': [generate_random_hex(64), generate_random_hex(64)],
            'data': generate_random_hex(64),
            'block_hash': generate_random_hex(64),
            'block_number': str(random.randint(1, 1000000)),
            'transaction_hash': generate_random_hex(64),
            'transaction_index': str(random.randint(0, 200)),
            'log_index': str(random.randint(0, 1000)),
            'removed': random.choice([True, False]),
            'from': generate_random_hex(40),
            'to': generate_random_hex(40),
            'value': str(random.randint(1, 10**18))
        }
        
        withdrawals.append(withdrawal)
        
        if len(withdrawals) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
                withdrawal['address'],
                withdrawal['topics'],
                withdrawal['data'],
                withdrawal['block_hash'],
                withdrawal['block_number'],
                withdrawal['transaction_hash'],
                withdrawal['transaction_index'],
                withdrawal['log_index'],
                withdrawal['removed'],
                withdrawal['from'],
                withdrawal['to'],
                withdrawal['value']
            )).decode('utf-8') for withdrawal in withdrawals)
            
            cursor.execute(f"""
            INSERT INTO withdrawal_erc20_{CHAIN} (
                address, topics, data, block_hash, block_number, transaction_hash, 
                transaction_index, log_index, removed, "from", "to", value
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            withdrawals = []
            print(f"Inserted {i+1}/{n} withdrawals ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} withdrawals in {time.time() - start_time:.2f} seconds")

def generate_token_trades(n):
    """Generate and insert n token trades into the token_trades table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    start_time = time.time()
    print(f"Generating {n} token trades...")
    
    batch_size = 1000
    trades = []
    
    for i in range(n):
        token = generate_token_record()
        trade = {
            **token,
            'pool_address': generate_random_hex(40),
            'transaction_hash': generate_random_hex(64)
        }
        
        trades.append(trade)
        
        if len(trades) >= batch_size or i == n - 1:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", (
                trade['token_address'],
                trade['name'],
                trade['symbol'],
                trade['decimals'],
                trade['total_supply'],
                trade['price'],
                trade['pool_address'],
                trade['transaction_hash']
            )).decode('utf-8') for trade in trades)
            
            cursor.execute(f"""
            INSERT INTO token_trades_{CHAIN} (
                token_address, name, symbol, decimals, total_supply, price,
                pool_address, transaction_hash
            ) VALUES {args_str} ON CONFLICT DO NOTHING
            """)
            
            conn.commit()
            trades = []
            print(f"Inserted {i+1}/{n} token trades ({(i+1)/n*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print(f"Generated {n} token trades in {time.time() - start_time:.2f} seconds")

def generate_all_data(record_count=100):
    """Generate all data with the specified number of records per table"""
    create_database()
    create_tables()
    
    # Generate data for all tables
    generate_blocks(record_count)
    generate_tokens(record_count)
    generate_pools(record_count)
    generate_transactions(record_count)
    generate_transfer_erc20(record_count)
    generate_transfer_erc721(record_count)
    generate_users(record_count)
    generate_wallet_users(record_count)
    generate_deposits(record_count)
    generate_withdrawals(record_count)
    generate_token_trades(record_count)
    
    print(f"All data generated successfully with {record_count} records per table!")

if __name__ == "__main__":
    # Define how many records to generate per table
    num_records = int(os.getenv("NUM_RECORDS", "1000"))
    generate_all_data(num_records)