from typing import Any, List, Dict, Optional
import httpx
from mcp.server.fastmcp import FastMCP
import asyncio
import json

# Initialize FastMCP server
mcp = FastMCP("blockchain_sql")

# Constants
API_BASE = "http://192.168.1.36:5000"  # Flask API base URL

async def make_api_request(url: str, method: str = "GET", json_data: dict = None) -> dict[str, Any] | None:
    """Make a request to the Blockchain SQL API with proper error handling."""
    async with httpx.AsyncClient() as client:
        try:
            if method == "GET":
                response = await client.get(url, timeout=30.0)
            else:
                response = await client.post(url, json=json_data, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}

@mcp.tool()
async def get_example_queries() -> str:
    """Get a list of example blockchain database queries that can be asked."""
    url = f"{API_BASE}/api/examples"
    data = await make_api_request(url)
    
    if "error" in data:
        return f"Failed to get example queries: {data['error']}"
    
    examples = data.get("examples", [])
    return "Example queries you can ask:\n" + "\n".join(f"- {example}" for example in examples)

@mcp.tool()
async def natural_language_query(query: str, chain: Optional[str] = None) -> str:
    """Execute a natural language query against the blockchain database.

    Args:
        query: The natural language query to execute
        chain: Optional blockchain name (defaults to 'base')
    """
    url = f"{API_BASE}/api/query"
    request_data = {"query": query}
    
    if chain:
        request_data["chain"] = chain
    
    response = await make_api_request(url, "POST", request_data)
    
    if "error" in response:
        return f"Query execution failed: {response['error']}"
    
    # Format and return the results
    result_text = []
    
    # Add SQL generated
    sql = response.get("sql", "")
    if sql:
        result_text.append(f"Generated SQL:\n```sql\n{sql}\n```\n")
    
    # Add insights if available
    insights = response.get("insights", "")
    if insights:
        result_text.append(f"Insights:\n{insights}\n")
    
    # Add result data
    results = response.get("results", {})
    row_count = results.get("row_count", 0)
    data = results.get("data", [])
    
    if row_count > 0:
        result_text.append(f"Results: {row_count} rows returned")
        
        # Format the first 5 rows as a table
        preview_rows = data[:5]
        if preview_rows:
            result_text.append("Preview of results:")
            
            # Get all keys
            all_keys = set()
            for row in preview_rows:
                all_keys.update(row.keys())
            
            # Convert to markdown table
            header = " | ".join(all_keys)
            separator = " | ".join(["---"] * len(all_keys))
            
            table_rows = [f"{header}", f"{separator}"]
            
            for row in preview_rows:
                values = []
                for key in all_keys:
                    values.append(str(row.get(key, "N/A")))
                table_rows.append(" | ".join(values))
            
            result_text.append("\n".join(table_rows))
            
            if row_count > 5:
                result_text.append(f"\n(Showing 5 of {row_count} rows)")
    else:
        result_text.append("No results found")
    
    return "\n".join(result_text)

@mcp.tool()
async def execute_sql(sql: str) -> str:
    """Execute a raw SQL query against the blockchain database.

    Args:
        sql: The SQL query to execute
    """
    url = f"{API_BASE}/api/sql"
    request_data = {"sql": sql}
    
    response = await make_api_request(url, "POST", request_data)
    
    if "error" in response:
        return f"SQL execution failed: {response['error']}"
    
    # Format and return the results
    result_text = []
    
    # Add SQL used
    result_text.append(f"Executed SQL:\n```sql\n{sql}\n```\n")
    
    # Add result data
    results = response.get("results", {})
    row_count = results.get("row_count", 0)
    data = results.get("data", [])
    
    if row_count > 0:
        result_text.append(f"Results: {row_count} rows returned")
        
        # Format the first 5 rows as a table
        preview_rows = data[:5]
        if preview_rows:
            result_text.append("Preview of results:")
            
            # Get all keys
            all_keys = set()
            for row in preview_rows:
                all_keys.update(row.keys())
            
            # Convert to markdown table
            header = " | ".join(all_keys)
            separator = " | ".join(["---"] * len(all_keys))
            
            table_rows = [f"{header}", f"{separator}"]
            
            for row in preview_rows:
                values = []
                for key in all_keys:
                    values.append(str(row.get(key, "N/A")))
                table_rows.append(" | ".join(values))
            
            result_text.append("\n".join(table_rows))
            
            if row_count > 5:
                result_text.append(f"\n(Showing 5 of {row_count} rows)")
    else:
        result_text.append("No results found")
    
    return "\n".join(result_text)

@mcp.tool()
async def check_api_health() -> str:
    """Check the health of the blockchain database API and its dependencies."""
    url = f"{API_BASE}/health"
    response = await make_api_request(url)
    
    if "error" in response:
        return f"Health check failed: {response['error']}"
    
    status = response.get("status", "unknown")
    openai_status = response.get("openai", "unknown")
    database_status = response.get("database", "unknown")
    model = response.get("model", "unknown")
    chain = response.get("chain", "unknown")
    
    return f"""API Health Check:
- Status: {status}
- OpenAI Connection: {openai_status}
- Database Connection: {database_status}
- Model: {model}
- Blockchain: {chain}
"""

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')

# mcp-cli chat --server sql_server --config-file sql_config.json --provider openai --model gpt-4o-mini