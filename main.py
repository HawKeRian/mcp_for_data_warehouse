from fastapi import FastAPI, Request
from fastapi_mcp import FastApiMCP
import socket
import time
from datetime import datetime
import os
import asyncio # New: Required for creating and cancelling tasks
from fastapi.middleware.cors import CORSMiddleware
import logging

# ----- import router -----
# Assuming routers/dwh_router.py exists
from routers import dwh_router 

# --- Configuration ---
logger = logging.getLogger(__name__)

# Limit to 3 concurrent workers/tasks
MAX_CONCURRENT_TASKS = 3
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# --- Global Task Tracking ---
# This holds the asyncio Task object for managing the background process
db_status_task: asyncio.Task | None = None 
            
# Create the FastAPI application instance
app = FastAPI(title="MTLW-DWH Agent API", version="1.0.0")

# --- Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# ----- include app router -----
app.include_router(dwh_router.router)

#################################################################################
########################   Helper Function Section   ############################
#################################################################################


# Custom logging function for routes
def log_route_access(request, response_time: float, status_code: int | None=None, error: str | None=None):
    """Log route access without ASGI middleware conflicts"""
    try:
        if error:
            logger.error(f"{request.method} {request.url.path} - ERROR - {response_time:.2f}s - {error}")
        else:
            logger.info(f"{request.method} {request.url.path} - {status_code or 200} - {response_time:.2f}s")
    except Exception as log_error:
        logger.debug(f"Logging error for {request.url.path}: {log_error}")

async def worker(name):
    print(f"[{time.strftime('%H:%M:%S')}] Task {name}: Waiting to acquire semaphore...")

    # 2. Acquire the semaphore using 'async with'
    # The task will pause here if the limit is reached
    async with semaphore:
        print(f"[{time.strftime('%H:%M:%S')}] Task {name}: >>> ACQUIRED. Working for 1 second...")
        await asyncio.sleep(1)  # Simulate I/O-bound work (e.g., API call)
        print(f"[{time.strftime('%H:%M:%S')}] Task {name}: <<< RELEASED. Finished work.")
    
async def main():
    """Main async function to demonstrate async functionality."""

    # Create 10 tasks, but only 3 will run concurrently
    tasks = [worker(i) for i in range(10)]
    await asyncio.gather(*tasks)
        
#################################################################################
########################        Main Section         ############################
#################################################################################

# Set up MCP without conflicting middleware
mcp = FastApiMCP(
    app, 
    include_operations=[
        "helper_mapping_info", "helper_process_mapper","main_execute_sql",
        "main_summary_each_process_data", "main_summary_each_process_data_defective"
        # "generate_sql", "execute_sql",
        # "common_info", "lot_in_process", "quality_info", "machine_info", "summary_lot_data"
    ],
    # base_url="http://127.0.0.1:8000" # Uncomment if needed for correct documentation generation
)
# Mount the MCP server directly to FastAPI app
mcp.mount()

if __name__ == "__main__":
    asyncio.run(main())