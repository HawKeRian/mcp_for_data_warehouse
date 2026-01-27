from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, Depends
from typing import Optional, Any, List

# Import the controller layer functions
from controllers.dwh_controller import helper_mapping_info_func, helper_process_mapper_func, \
    main_execute_sql_func, main_summary_each_process_data_func, main_summary_each_process_data_def_func
# , common_lot_info_func, lot_in_process_func, lot_defective_func, summary_lot_func
import logging
import json

router = APIRouter(
    prefix="/dwh_agent",
    tags=["DWH Agent - SQL with AI"],
)

oauth2_scheme = HTTPBearer()

logger = logging.getLogger(__name__)

#################################################################################
########################     Class for Base Model    ############################
#################################################################################

class SQLcommonRequest(BaseModel):
    """
        Schema for the incoming user message. Use for operations in DWH database
        chatInput: use for mapping tools to convert input into actual & correct data from Database
        sql_statement: use for executing sql_query from LLM or MCP Client site.
    """
    chatInput: str | None = None
    arguments: dict | None = None
    
class MappingResponse(BaseModel):
    """
        Schema for the response MAPPING DATA to MCP Client site (LLM, chatbot, application etc.)
        resnponse schema:
            {
                "success": bool,
                "error": Optional[str],
                "mapping_prompt": dict,
            }
    """
    success: bool
    error: Optional[str] = None
    mapping_param: dict

# class GenerateSQLResponse(BaseModel):
#     """
#         Schema for the response GENERATE SQL QUERY to MCP Client site (LLM, chatbot, application etc.)
#         resnponse schema:
#             {
#                 "success": bool,
#                 "error": Optional[str],
#                 "mapping_data: dict,
#                 "generate_sql_prompt": str,
#             }
#     """
#     success: bool
#     error: Optional[str] = None
#     mapping_data: dict
#     generate_sql_prompt: str

# class ChatResponse(BaseModel):
#     """
#         Schema for the response output to MCP Client site (LLM, chatbot, application etc.)
#         resnponse schema:
#             {
#                 "success": bool,
#                 "error": Optional[str],
#                 "mapping_param": dict,
#                 "content": dict,
#             }
#     """
#     success: bool
#     error: Optional[str] = None
#     mapping_data: dict
#     content: List[List[Any]]
    
    
#################################################################################
########################        Router Section       ############################
#################################################################################
    
# Route Mapping Information
@router.post(
    "/helper_mapping_info",
    operation_id="helper_mapping_info",
    name="DWH Mapping Information Tool"
)
async def helper_mapping_info(
    request: SQLcommonRequest,
    token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)
):
    """
        Retrieves manufacturing data. 
        Input: A description containing [Lot No., Process Name, Process Code, Product Code, Table Name, Department].
        Example: 'Get data about 2354ABC of Racking Process'
    """
    try:
        session_token = token.credentials
        logger.info(f"Received Session Token: {session_token[:5]}...")
        print(f"🔒 User requested mapping information from DWH")
        

        # Call the Main controller function
        result = helper_mapping_info_func(request.chatInput)

        # Handle validation errors from controller
        if not result["success"] and "error" in result:
            error_msg = result["error"]

            if "Invalid time_back_unit" in error_msg:
                # raise HTTPException(status_code=400, detail=error_msg)
                return
            
        # Convert Row objects to list of dicts
        content = result["content"]

        return {"success": result["success"], "content": content}

    except HTTPException:
        # raise
        return
    except Exception as e:
        logger.error(f"Failed to get mapping information from DWH: {str(e)}")
        return
 
# Route Process Mapper
@router.post(
    "/helper_process_mapper",
    operation_id="helper_process_mapper",
    name="DWH Process Mapper Tool"
)
async def helper_process_mapper(
    request: SQLcommonRequest,
    token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)
):
    """
        Retrieves manufacturing data. 
        Input: Relation with the LOTNO between the tables in DWH.
        Example: 'Summary data of 25XPB0062' (find list of tables before goes summary data)
    """
    try:
        session_token = token.credentials
        logger.info(f"Received Session Token: {session_token[:5]}...")
        print(f"🔒 User requested process mapper from DWH")
        

        # Call the Main controller function
        result = helper_process_mapper_func(request.chatInput, request.arguments)

        # Handle validation errors from controller
        if not result["success"] and "error" in result:
            error_msg = result["error"]

            if "Invalid time_back_unit" in error_msg:
                # raise HTTPException(status_code=400, detail=error_msg)
                return
            
        # Convert Row objects to list of dicts
        if result["content"] and isinstance(result["content"], list):
            content = [dict(row._mapping) if hasattr(row, '_mapping') else row for row in result["content"]]
        else:
            content = result["content"]

        return {"success": result["success"], "content": content}

    except HTTPException:
        # raise
        return
    except Exception as e:
        logger.error(f"Failed to get process mapper from DWH: {str(e)}")
        return
 
# Route Execute SQL Query String
@router.post(
    "/main_execute_sql",
    operation_id="main_execute_sql",
    name="DWH Execute SQL Tool"
)
async def main_execute_sql(
    request: SQLcommonRequest,
    token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)
):
    """
        Retrieves data from Data Warehouse based on target table. 
        Input: User's request data from <table_name>
        Example: 'Get data from pac_info'
        Output: 'Example data of target table as JSON format'
    """
    try:
        session_token = token.credentials
        logger.info(f"Received Session Token: {session_token[:5]}...")
        print(f"🔒 User requested to execute sql query from DWH")
        

        # Call the Main controller function
        result = main_execute_sql_func(request.chatInput, request.arguments)

        # Handle validation errors from controller
        if not result["success"] and "error" in result:
            error_msg = result["error"]

            if "Invalid time_back_unit" in error_msg:
                return
                # raise HTTPException(status_code=400, detail=error_msg)

        # Convert Row objects to list of dicts
        content = [dict(row._mapping) for row in result["content"]] if result["content"] else []
        
        return {"success": result["success"], "content": content}

    except HTTPException:
        # raise
        return {"success": False, "content": []}
    
    except Exception as e:
        logger.error(f"Failed to execute sql query from DWH: {str(e)}")
        return {"success": False, "content": []}
        

# Route Summary Each Process Data
@router.post(
    "/main_summary_each_process_data",
    operation_id="main_summary_each_process_data",
    name="DWH Summary Each Process Data Tool"
)
async def main_summary_each_process_data(
    request: SQLcommonRequest,
    token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)
):
    """
        Retrieves data from Data Warehouse based on target table. 
        Input: User's request data from <table_name>
        Example: 'Summary Data of lot 25XPB0062' -> for-loop each table with this tool
        Output: 'Example data of target table as JSON format'
        
        Remark: Exclude DEFECTIVE column
    """
    try:
        session_token = token.credentials
        logger.info(f"Received Session Token: {session_token[:5]}...")
        print(f"🔒 User requested to summary lot data (each process) from DWH")
        

        # Call the Main controller function
        result = main_summary_each_process_data_func(request.chatInput, request.arguments)

        # Handle validation errors from controller
        if not result["success"] and "error" in result:
            error_msg = result["error"]

            if "Invalid time_back_unit" in error_msg:
                return
                # raise HTTPException(status_code=400, detail=error_msg)

        # Convert Row objects to list of dicts
        if result["content"] and isinstance(result["content"], list):
            content = []
            for item in result["content"]:
                if isinstance(item, list):  # Handle nested lists of Row objects
                    converted_list = []
                    for row in item:
                        if hasattr(row, '_mapping'):
                            converted_list.append(dict(row._mapping))
                        else:
                            converted_list.append(row)
                    content.append(converted_list)
                elif hasattr(item, '_mapping'):
                    content.append(dict(item._mapping))
                else:
                    content.append(item)
        else:
            content = result["content"]
        
        return {"success": result["success"], "content": content}

    except HTTPException:
        # raise
        return {"success": False, "content": []}
    
    except Exception as e:
        logger.error(f"Failed to summary process lot data from DWH: {str(e)}")
        return {"success": False, "content": []}
   
# Route Summary Each Process Data Defective
@router.post(
    "/main_summary_each_process_data_defective",
    operation_id="main_summary_each_process_data_defective",
    name="DWH Summary Each Process Data Tool"
)
async def main_summary_each_process_data_defective(
    request: SQLcommonRequest,
    token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)
):
    """
        Retrieves data from Data Warehouse based on target table. 
        Input: User's request data from <table_name>
        Example: 'Summary Defective/NG/NC Reject Data of lot 25XPB0062' -> for-loop each table with this tool
        Output: 'Example data of target table as JSON format'
        
        Remark: Only DEFECTIVE column
    """
    try:
        session_token = token.credentials
        logger.info(f"Received Session Token: {session_token[:5]}...")
        print(f"🔒 User requested to summary lot data (each process) from DWH")
        

        # Call the Main controller function
        result = main_summary_each_process_data_def_func(request.chatInput, request.arguments)

        # Handle validation errors from controller
        if not result["success"] and "error" in result:
            error_msg = result["error"]

            if "Invalid time_back_unit" in error_msg:
                return
                # raise HTTPException(status_code=400, detail=error_msg)

        # Convert Row objects to list of dicts
        if result["content"] and isinstance(result["content"], list):
            content = []
            for item in result["content"]:
                if isinstance(item, list):  # Handle nested lists of Row objects
                    converted_list = []
                    for row in item:
                        if hasattr(row, '_mapping'):
                            converted_list.append(dict(row._mapping))
                        else:
                            converted_list.append(row)
                    content.append(converted_list)
                elif hasattr(item, '_mapping'):
                    content.append(dict(item._mapping))
                else:
                    content.append(item)
        else:
            content = result["content"]
        
        return {"success": result["success"], "content": content}

    except HTTPException:
        # raise
        return {"success": False, "content": []}
    
    except Exception as e:
        logger.error(f"Failed to summary process lot data defective from DWH: {str(e)}")
        return {"success": False, "content": []}
   


# # Route Generate SQL Query String
# @router.post(
#     "/generate_sql",
#     response_model=GenerateSQLResponse,
#     operation_id="generate_sql",
#     name="DWH Generate SQL Query Tool"
# )
# async def generate_sql(
#     request: SQLcommonRequest,
#     token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)
# ):
#     """
#         This is a tool for generating sql query base-on user's input and mapping information from database.
        
#         Return output as PROMPT MESSAGE
        
#     """
#     try:
#         session_token = token.credentials
#         logger.info(f"Received Session Token: {session_token[:5]}...")

#         print(f"🔒 User requested generating sql query from DWH: {request}")

#         # Call the Main controller function
#         result = generate_sql_func(request.chatInput, request.sql_statement, request.mapping_data)

#         # Handle validation errors from controller
#         if not result["success"] and "error" in result:
#             error_msg = result["error"]

#             if "Invalid time_back_unit" in error_msg:
#                 raise HTTPException(status_code=400, detail=error_msg)

#         return GenerateSQLResponse(
#             success=result["success"],
#             error=result.get("error"),
#             mapping_data=request.mapping_data or {},
#             generate_sql_prompt=result["generate_sql_prompt"]
#         )

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to get information to generate sql from DWH: {str(e)}")
        
        
# # Route Execute SQL Query String
# @router.post(
#     "/execute_sql",
#     response_model=GenerateSQLResponse,
#     operation_id="execute_sql",
#     name="DWH Execute SQL Query Tool"
# )
# async def execute_sql(
#     request: SQLcommonRequest,
#     token: HTTPAuthorizationCredentials = Depends(oauth2_scheme)
# ):
#     """
#         This is a tool for executing sql query base-on sql_statement and user'input and mapping information (if need)
        
#         Return output as Table Schema format
        
#     """
#     try:
#         session_token = token.credentials
#         logger.info(f"Received Session Token: {session_token[:5]}...")

#         print(f"🔒 User requested executing sql query from DWH: {request}")

#         # Call the Main controller function
#         result = execute_sql_func(request.chatInput, request.sql_statement, request.mapping_data)

#         # Handle validation errors from controller
#         if not result["success"] and "error" in result:
#             error_msg = result["error"]

#             if "Invalid time_back_unit" in error_msg:
#                 raise HTTPException(status_code=400, detail=error_msg)

#         return ChatResponse(
#             success=result["success"],
#             error=result.get("error"),
#             mapping_data=request.mapping_data or {},
#             content=result["output"]
#         )

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Failed to execute sql from DWH: {str(e)}")