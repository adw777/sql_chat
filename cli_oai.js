// blockchain-db-cli.js
import { config } from 'dotenv';
import pg from 'pg';
import OpenAI from 'openai';
import chalk from 'chalk';
import readline from 'readline';
import { createObjectCsvWriter } from 'csv-writer';
import Table from 'cli-table3';

// Load environment variables from .env file
config();

// Database connection settings from environment variables
const DB_HOST = process.env.DB_HOST || 'aws-0-us-east-1.pooler.supabase.com';
const DB_PORT = process.env.DB_PORT || '6543';
const DB_NAME = process.env.DB_NAME || 'postgres';
const DB_USER = process.env.DB_USER || 'postgres.cbxagjoxdgzpfknkpswj';
const DB_PASSWORD = process.env.DB_PASSWORD || 'amit';
const CHAIN = process.env.CHAIN || 'base';

// OpenAI API Key 
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
if (!OPENAI_API_KEY) {
    console.error(chalk.red("Error: OpenAI API key is required. Please set the OPENAI_API_KEY environment variable."));
    process.exit(1);
}

// OpenAI Model to use
const OPENAI_MODEL = "gpt-4o-mini";  // Default to GPT-4o-mini

// Database schema information for the LLM
const SCHEMA_INFO = `
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
`;

// Blockchain context for better understanding
const BLOCKCHAIN_CONTEXT = `
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
`;

// Example queries to help the model generate better SQL
const EXAMPLE_QUERIES = `
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
`;

/**
 * Connect to the PostgreSQL database
 * @returns {Promise<pg.Client>} Database client
 */
async function connectToDatabase() {
    try {
        const client = new pg.Client({
            host: DB_HOST,
            port: DB_PORT,
            database: DB_NAME,
            user: DB_USER,
            password: DB_PASSWORD
        });

        await client.connect();
        return client;
    } catch (error) {
        console.error(chalk.red(`Database connection error: ${error.message}`));
        return null;
    }
}

/**
 * Execute a SQL query and return the results
 * @param {pg.Client} client - Database client
 * @param {string} sql - SQL query to execute
 * @returns {Promise<{rows: Array, error: string|null}>}
 */
async function executeQuery(client, sql) {
    try {
        const result = await client.query(sql);
        
        if (result.rows) {
            return { 
                rows: result.rows, 
                error: null, 
                columnNames: result.fields.map(field => field.name),
                rowCount: result.rowCount
            };
        } else {
            return { 
                rows: [], 
                error: null, 
                affectedRows: result.rowCount,
                message: `Query executed successfully. Rows affected: ${result.rowCount}`
            };
        }
    } catch (error) {
        return { rows: null, error: error.message };
    }
}

/**
 * Generate SQL from natural language using OpenAI
 * @param {OpenAI} client - OpenAI client
 * @param {string} prompt - User's natural language prompt
 * @param {string} schemaInfo - Database schema information
 * @param {string} chain - Blockchain chain name
 * @param {string} model - OpenAI model to use
 * @returns {Promise<{sql: string|null, error: string|null}>}
 */
async function generateSQL(client, prompt, schemaInfo, chain, model) {
    // Create system message with schema information
    const systemMessage = `You are an expert SQL generator for a blockchain database. 
Your task is to convert natural language questions into PostgreSQL queries.

${schemaInfo}

${BLOCKCHAIN_CONTEXT}

${EXAMPLE_QUERIES}

Important SQL writing guidelines:
1. Always generate valid PostgreSQL syntax
2. Use table names with the suffix '_${chain}' unless referring to the 'users' table
3. Handle quoting properly for reserved words like "from" and "to" in table columns
4. Cast numeric strings to appropriate types (DECIMAL, BIGINT) when needed for math operations
5. Use TO_TIMESTAMP() for timestamp conversions when needed
6. Format timestamps in human-readable format when returning them
7. Use proper JOIN syntax when combining tables
8. Add LIMIT clauses (usually LIMIT 10 or 20) unless asked for all records
9. Add proper WHERE clauses to filter data based on the user's question
10. NEVER use column names that don't exist in the tables

OUTPUT FORMAT: Return ONLY the SQL query, no explanations or markdown formatting.`;

    // Call OpenAI API
    try {
        const response = await client.chat.completions.create({
            model: model,
            messages: [
                { role: "system", content: systemMessage },
                { role: "user", content: `Generate PostgreSQL query for: ${prompt}` }
            ],
            temperature: 0.1  // Low temperature for more deterministic outputs
        });
        
        let sql = response.choices[0].message.content.trim();
        
        // Remove markdown code blocks if the model included them
        if (sql.startsWith("```sql")) {
            sql = sql.replace("```sql", "").replace("```", "").trim();
        } else if (sql.startsWith("```")) {
            sql = sql.replace(/```/g, "").trim();
        }
            
        return { sql, error: null };
    } catch (error) {
        return { sql: null, error: `Error generating SQL: ${error.message}` };
    }
}

/**
 * Generate natural language response from query results
 * @param {OpenAI} client - OpenAI client
 * @param {string} prompt - User's original prompt
 * @param {string} sql - Executed SQL query
 * @param {Array} results - Query results
 * @param {string} model - OpenAI model to use
 * @returns {Promise<{response: string|null, error: string|null}>}
 */
async function generateResponse(client, prompt, sql, results, model) {
    // Create a context with the results
    const resultSample = results && results.rows && results.rows.length > 0 
        ? JSON.stringify(results.rows.slice(0, 5), null, 2) 
        : "No results found";
    const rowCount = results && results.rows ? results.rows.length : 0;
    const colNames = results && results.columnNames ? results.columnNames.join(", ") : "";
    
    // Create system message
    const systemMessage = `You are an expert blockchain data analyst who provides clear, concise explanations.
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
DO NOT include the SQL query in your response unless it's specifically requested.`;

    // User message with query and results
    const userMessage = `User question: ${prompt}
SQL query used: ${sql}
Number of rows returned: ${rowCount}
Column names: ${colNames}
Sample of results (first 5 rows):
${resultSample}

Please provide a natural language explanation of these results, focusing on insights that answer the user's question.`;

    // Call OpenAI API
    try {
        const response = await client.chat.completions.create({
            model: model,
            messages: [
                { role: "system", content: systemMessage },
                { role: "user", content: userMessage }
            ],
            temperature: 0.7  // Slightly higher temperature for more natural responses
        });
        
        return { response: response.choices[0].message.content, error: null };
    } catch (error) {
        return { response: null, error: `Error generating response: ${error.message}` };
    }
}

/**
 * Display example questions that users can ask
 */
function displayExampleQuestions() {
    console.log(chalk.magenta.bold("\nExample questions you can ask:"));
    const examples = [
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
    ];
    
    examples.forEach((example, i) => {
        console.log(`${i + 1}. ${example}`);
    });
    console.log();
}

/**
 * Format query results as a nice table for display
 * @param {Array} rows - Query result rows
 * @param {Array} columnNames - Column names
 * @returns {string} Formatted table
 */
function formatResultTable(rows, columnNames) {
    if (!rows || rows.length === 0 || !columnNames) {
        return "No results";
    }

    // Create table with column headers
    const table = new Table({
        head: columnNames,
        style: { head: ['cyan'] }
    });

    // Add rows to table (limit to first 10 rows)
    rows.slice(0, 10).forEach(row => {
        const rowValues = columnNames.map(col => {
            const value = row[col];
            if (value === null) return 'NULL';
            if (typeof value === 'object') return JSON.stringify(value);
            return String(value).substring(0, 30) + (String(value).length > 30 ? '...' : '');
        });
        table.push(rowValues);
    });

    return table.toString();
}

/**
 * Create a readline interface for user input
 * @returns {readline.Interface}
 */
function createInterface() {
    return readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });
}

/**
 * Export query results to CSV
 * @param {Array} rows - Query result rows 
 * @param {Array} columnNames - Column names
 */
async function exportToCSV(rows, columnNames) {
    if (!rows || rows.length === 0) {
        console.log(chalk.yellow("No data to export"));
        return;
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `query_results_${timestamp}.csv`;
    
    const csvWriter = createObjectCsvWriter({
        path: filename,
        header: columnNames.map(column => ({id: column, title: column}))
    });
    
    try {
        await csvWriter.writeRecords(rows);
        console.log(chalk.green(`Results exported to ${filename}`));
    } catch (error) {
        console.error(chalk.red(`Failed to export results: ${error.message}`));
    }
}

/**
 * Main application function
 */
async function main() {
    // Print welcome message
    console.log(chalk.magenta.bold("\nBlockchain Database Chat CLI"));
    console.log("Ask questions about blockchain data in natural language, and get insights!\n");
    
    // Initialize OpenAI client
    let openai;
    try {
        openai = new OpenAI({
            apiKey: OPENAI_API_KEY
        });
    } catch (error) {
        console.error(chalk.red(`Error initializing OpenAI client: ${error.message}`));
        process.exit(1);
    }
    
    // Connect to database
    console.log(chalk.cyan("Connecting to database..."));
    const dbClient = await connectToDatabase();
    if (!dbClient) {
        console.error(chalk.red("Failed to connect to database. Please check your credentials."));
        process.exit(1);
    }
    
    console.log(chalk.green("Successfully connected to database!\n"));
    
    // Display example questions
    displayExampleQuestions();
    
    // Create interface for user input
    const rl = createInterface();
    
    // Main interaction loop
    const promptUser = () => {
        rl.question(chalk.bold("Ask a question (or type 'exit' to quit): "), async (prompt) => {
            try {
                if (prompt.toLowerCase() === 'exit' || prompt.toLowerCase() === 'quit') {
                    // Close connection and exit
                    await dbClient.end();
                    rl.close();
                    console.log(chalk.cyan("\nThank you for using Blockchain Database Chat CLI!"));
                    return;
                }
                
                if (!prompt.trim()) {
                    promptUser();
                    return;
                }
                
                console.log(chalk.cyan("\nGenerating SQL query..."));
                
                // Generate SQL using OpenAI
                const { sql, error: sqlError } = await generateSQL(openai, prompt, SCHEMA_INFO, CHAIN, OPENAI_MODEL);
                
                if (sqlError) {
                    console.error(chalk.red(`Failed to generate SQL: ${sqlError}`));
                    promptUser();
                    return;
                }
                
                // Display generated SQL
                console.log(chalk.cyan("Generated SQL:"));
                console.log(chalk.blue(sql));
                console.log();
                
                // Execute SQL
                console.log(chalk.cyan("Executing query..."));
                const queryResult = await executeQuery(dbClient, sql);
                
                if (queryResult.error) {
                    console.error(chalk.red(`Failed to execute query: ${queryResult.error}`));
                    const errorType = queryResult.error.toLowerCase().includes("syntax") 
                        ? "Syntax error" 
                        : "Database error";
                    console.log(chalk.cyan(`Error type: ${errorType}`));
                    promptUser();
                    return;
                }
                
                if (!queryResult.rows || queryResult.rows.length === 0) {
                    console.log(chalk.cyan("The query returned no results."));
                    promptUser();
                    return;
                }
                
                // Display results
                console.log(chalk.green(`Query returned ${queryResult.rowCount} rows.`));
                console.log(chalk.cyan("Results preview:"));
                
                // Display formatted table
                console.log(formatResultTable(queryResult.rows, queryResult.columnNames));
                console.log();
                
                // Ask if user wants to export results
                rl.question(chalk.cyan("Would you like to export these results to CSV? (y/n): "), async (answer) => {
                    if (answer.toLowerCase() === 'y') {
                        await exportToCSV(queryResult.rows, queryResult.columnNames);
                    }
                    
                    // Generate natural language response
                    console.log(chalk.cyan("Generating insights..."));
                    const { response, error: responseError } = await generateResponse(
                        openai, 
                        prompt, 
                        sql, 
                        queryResult, 
                        OPENAI_MODEL
                    );
                    
                    if (responseError) {
                        console.error(chalk.red(`Failed to generate insights: ${responseError}`));
                    } else {
                        console.log(chalk.magenta.bold("\nInsights"));
                        console.log(response);
                    }
                    
                    promptUser();
                });
                
            } catch (error) {
                console.error(chalk.red(`An error occurred: ${error.message}`));
                console.error(error.stack);
                promptUser();
            }
        });
    };
    
    // Start the prompt loop
    promptUser();
}

// Run the application
main().catch(error => {
    console.error(chalk.red(`Fatal error: ${error.message}`));
    console.error(error.stack);
    process.exit(1);
});