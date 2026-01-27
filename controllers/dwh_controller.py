from sqlalchemy import func, case, and_, select, create_engine, desc, text, String, DateTime, Integer
from sqlalchemy.orm import Session, sessionmaker, DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import QueuePool
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from psycopg2.extras import Json

# get postgres engine from database module
from models.database import get_pg_engine

from typing import List
import json
import os
import logging
from dotenv import load_dotenv
import requests
import re

load_dotenv()
logger = logging.getLogger(__name__)



#########################################################################################################
######################################## --- Component Function --- ########################################

def info_mapping():
    """
        Info Mapping Function: A component function use for retrieving all table_name and related information in Data Warehouse
        
        Return: data from database format
    """
    
    # Query for LOT_INFO
    lot_info_query = text(f"""
                SELECT DISTINCT 
                    t1.TABLE_NAME, t1.DEPARTMENT, t1.PRODUCT_CODE, t1.PROCESS_CODE,
                    t2.DG_PROCESS_NAME as PROCESS_NAME
                FROM CONFIG_WAREHOUSE_TABLE t1
                LEFT JOIN CONFIG_LINK_CODE t2 ON t1.PRODUCT_CODE = t2.PRODUCT_SUBGROUP AND t1.PROCESS_CODE = t2.DG_PROCESS_CODE
                WHERE t1.IS_ACTIVE = 1
                union
                select distinct upper(target_table_name) as table_name, department,  
                    upper(split_part(lower(target_table_name), '_lot_info', 1)) AS product_code, null as process_code, null as process_name 
                from config_table_field
                where target_table_name is not null
            """)
    
    pg_engine = get_pg_engine()
    pg_session = sessionmaker(bind=pg_engine)

    with pg_session() as pg_session:
        lot_info = pg_session.execute(lot_info_query).fetchall()

    pg_session.close()
    pg_engine.dispose()
    
    if lot_info.__len__() > 0:
        return lot_info
    else:
        return []

def lot_mapper(
    lotno: str
):
    """
        Lot Mapper Function: A component function use for finding all significant information by using lotno as key
        
        Return: mapping_data format
    """
    
    target_table = ""
    
    if lotno:
        # Query for LOT_INFO
        lot_info_qry = text(f"""
                    SELECT DISTINCT UPPER(TARGET_TABLE_NAME) AS TABLE_NAME
                    FROM CONFIG_TABLE_FIELD
                    WHERE TARGET_TABLE_NAME IS NOT NULL
                """)

        pg_engine = get_pg_engine()
        pg_session = sessionmaker(bind=pg_engine)

        with pg_session() as pg_session:
            table_list = pg_session.execute(lot_info_qry).fetchall()

            # For-loop to find <lotno> in each table. If found, then return table_name. Else continue
            for item in table_list:
                # Inial Query from <table_name>. Select only lotno
                item_qry = text(f"SELECT LOTNO FROM {item.table_name} WHERE LOTNO = '{lotno}'")
                
                # If lotno was found, mark table_name as target then break the loop
                if(pg_session.execute(item_qry).fetchall().__len__() > 0):
                    target_table = item.table_name
                    break
                else: continue   
                
            # FIND Product from "target_table"
            # Inial Query from <table_name>. Select product
            product_qry = text(f"SELECT DISTINCT PRODUCT FROM {target_table}")
            target_product = pg_session.execute(product_qry).fetchall()[0][0]
                
            # Find mapping_data from target_table
            find_mapping_qry = text(f"""
                SELECT DISTINCT
                    t1.TABLE_NAME, t1.DEPARTMENT, t1.PRODUCT_CODE, t1.PROCESS_CODE,
                    t2.DG_PROCESS_NAME as PROCESS_NAME
                FROM CONFIG_WAREHOUSE_TABLE t1
                LEFT JOIN CONFIG_LINK_CODE t2 ON t1.PROCESS_CODE = t2.DG_PROCESS_CODE
                WHERE t1.IS_ACTIVE = 1 AND t1.PRODUCT_CODE = '{target_product}'
            """)
                
            find_mapping_data = pg_session.execute(find_mapping_qry).fetchall()
            
            
            # Fill mapping_data with target_table filter
            if find_mapping_data:
                row = find_mapping_data[0]
                mapping_data = {
                    "lotno": lotno,
                    "table_name": target_table,
                    "department": row.department,
                    "product_code": row.product_code,
                    "process_code": row.process_code,
                    "process_name": row.process_name,
                }
            else:
                mapping_data = {
                    "lotno": lotno,
                    "table_name": target_table,
                    "department": None,
                    "product_code": None,
                    "process_code": None,
                    "process_name": None,
                }


        pg_session.close()
        pg_engine.dispose()

        return mapping_data

    else:
        mapping_data = {
            "lotno": lotno,
            "table_name": target_table,
            "department": None,
            "product_code": None,
            "process_code": None,
            "process_name": None,
        }
                        
        return mapping_data

def process_mapper(
    product_code: str,
    department: str,
):
    """
        Process Mapper Function: A component function use for listing all table that include in the same product.
        By using product_code in mapping_data
        
        Return: process_table_lists
    """
    
    if product_code and department:
        # Query for PROCESS_MAPPER
        process_mapper_qry = text(f"""
                    SELECT DISTINCT TABLE_NAME
                    FROM CONFIG_WAREHOUSE_TABLE
                    WHERE PRODUCT_CODE = '{product_code}'
                    AND DEPARTMENT = '{department}'
                """)

        pg_engine = get_pg_engine()
        pg_session = sessionmaker(bind=pg_engine)

        with pg_session() as pg_session:
            table_list = pg_session.execute(process_mapper_qry).fetchall()

        pg_session.close()
        pg_engine.dispose()

        return table_list

    else:
        return []

def process_retrieving_data(
    table_name: str,
    lotno: str,
):
    """
        Process Retrieving Data Function: A component function use for execute data from each table based on 
        table_name & lotno from mapping data.
        Remark: Exclude DEFECTIVE column
        
        Return: Table Schema format (each table might not the same)
    """
    
    if table_name and lotno:
        # Find Mapping Data for target table
        find_mapping_qry = text(f"""
                    SELECT DEPARTMENT, PROCESS_CODE, PRODUCT_CODE
                    FROM CONFIG_WAREHOUSE_TABLE
                    WHERE TABLE_NAME = '{table_name}'
                """)
        
        pg_engine = get_pg_engine()
        pg_session = sessionmaker(bind=pg_engine)

        with pg_session() as pg_session:
            find_mapping = pg_session.execute(find_mapping_qry).fetchone()
            
        # Check if mapping data was found
        if not find_mapping:
            pg_session.close()
            pg_engine.dispose()
            return []
            
        # Define temp mapping_data
        mapping_data = {}
        mapping_data["lotno"] = lotno
        mapping_data["table_name"] = table_name
        mapping_data["product_code"] = find_mapping.product_code
        mapping_data["process_code"] = find_mapping.process_code
        mapping_data["department"] = find_mapping.department
        
            
        # Query for LOT_INFO
        lot_info_qry = text(f"""
                    SELECT *
                    FROM {mapping_data["table_name"]}
                    WHERE LOTNO = '{mapping_data["lotno"]}'
                """)
        
        # Query for excluding column
        exclude_column_qry = text(f"""
                    SELECT LINK_CODE_MAIN
                    FROM CONFIG_LINK_CODE
                    WHERE PRODUCT_SUBGROUP = '{mapping_data["product_code"]}'
                    AND DG_PROCESS_CODE = '{mapping_data["process_code"]}'
                    AND DG_DEPARTMENT = '{mapping_data["department"]}'
                    AND SPECIAL_DATA_TYPE = 'Defective'
                          
                """)
        
        pg_engine = get_pg_engine()
        pg_session = sessionmaker(bind=pg_engine)
        
        # Query for Mapping Data (product, process, department)
        with pg_session() as pg_session:
            process_data = pg_session.execute(lot_info_qry).fetchall()
            exclude_column_result = pg_session.execute(exclude_column_qry).fetchall()
            exclude_column = [row[0] for row in exclude_column_result]

        pg_session.close()
        pg_engine.dispose()
        
        # Filter column, remove column from "process_data" that exist in "exclude_column" list
        if exclude_column:
            filtered_data = []
            for row in process_data:
                row_dict = dict(row._mapping)
                filtered_row = {k: v for k, v in row_dict.items() if k.lower() not in exclude_column}
                filtered_data.append(filtered_row)
                
            
            # Rename "filtered_data" column by using "rename_list"
            rename_list = column_view_mapper(mapping_data=mapping_data, defective_flag=False)
            
            if isinstance(rename_list, dict):
                filtered_data = [{rename_list.get(k, k): v for k, v in row.items()} for row in filtered_data]
                
            return filtered_data
        else:
            return [dict(row._mapping) for row in process_data]
    else:
        return []

def process_retrieving_data_defective(
    table_name: str,
    lotno: str,
):
    """
        Process Retrieving Data Defective Function: A component function use for execute defective/NG/NC Reject data
        from each table based on table_name & lotno from mapping data.
        Remark: Only DEFECTIVE column
        
        Return: Table Schema format (each table might not the same)
    """
    
    if table_name and lotno:
        # Find Mapping Data for target table
        find_mapping_qry = text(f"""
                    SELECT DEPARTMENT, PROCESS_CODE, PRODUCT_CODE
                    FROM CONFIG_WAREHOUSE_TABLE
                    WHERE TABLE_NAME = '{table_name}'
                """)
        
        pg_engine = get_pg_engine()
        pg_session = sessionmaker(bind=pg_engine)

        with pg_session() as pg_session:
            find_mapping = pg_session.execute(find_mapping_qry).fetchone()
            
        # Check if mapping data was found
        if not find_mapping:
            pg_session.close()
            pg_engine.dispose()
            return []
            
        # Define temp mapping_data
        mapping_data = {}
        mapping_data["lotno"] = lotno
        mapping_data["table_name"] = table_name
        mapping_data["product_code"] = find_mapping.product_code
        mapping_data["process_code"] = find_mapping.process_code
        mapping_data["department"] = find_mapping.department
        
            
        # Query for LOT_INFO
        lot_info_qry = text(f"""
                    SELECT *
                    FROM {mapping_data["table_name"]}
                    WHERE LOTNO = '{mapping_data["lotno"]}'
                """)
        
        # Query for excluding column
        exclude_column_qry = text(f"""
                    SELECT LINK_CODE_MAIN
                    FROM CONFIG_LINK_CODE
                    WHERE PRODUCT_SUBGROUP = '{mapping_data["product_code"]}'
                    AND DG_PROCESS_CODE = '{mapping_data["process_code"]}'
                    AND DG_DEPARTMENT = '{mapping_data["department"]}'
                    AND SPECIAL_DATA_TYPE != 'Defective'
                          
                """)
        
        pg_engine = get_pg_engine()
        pg_session = sessionmaker(bind=pg_engine)
        
        # Query for Mapping Data (product, process, department)
        with pg_session() as pg_session:
            process_data = pg_session.execute(lot_info_qry).fetchall()
            exclude_column_result = pg_session.execute(exclude_column_qry).fetchall()
            exclude_column = [row[0] for row in exclude_column_result]

        pg_session.close()
        pg_engine.dispose()
        
        # Filter column, remove column from "process_data" that exist in "exclude_column" list
        if exclude_column:
            filtered_data = []
            for row in process_data:
                row_dict = dict(row._mapping)
                filtered_row = {k: v for k, v in row_dict.items() if k.lower() not in exclude_column}
                filtered_data.append(filtered_row)
                
            
            # Rename "filtered_data" column by using "rename_list"
            rename_list = column_view_mapper(mapping_data=mapping_data, defective_flag=True)
            
            if isinstance(rename_list, dict):
                filtered_data = [{rename_list.get(k, k): v for k, v in row.items()} for row in filtered_data]
                
            return filtered_data
        else:
            return [dict(row._mapping) for row in process_data]
    else:
        return []
  


def column_view_mapper(
    mapping_data: dict,
    defective_flag: bool,
):
    """
        Column View Mapper Function: A component function use for mapping actual column_name with view_column for user.
        Return: mapping dict of link_code_main and view_column
    """
    
    if mapping_data:
        
        # Check defective_flag is TRUE or not
        if not defective_flag:
            # Query for excluding column
            view_column_qry = text(f"""
                        SELECT LINK_CODE_MAIN, VIEW_COLUMN
                        FROM CONFIG_LINK_CODE
                        WHERE PRODUCT_SUBGROUP = '{mapping_data["product_code"]}'
                        AND DG_PROCESS_CODE = '{mapping_data["process_code"]}'
                        AND DG_DEPARTMENT = '{mapping_data["department"]}'
                        AND SPECIAL_DATA_TYPE != 'Defective'
                    """)
        else:
            # Query for excluding column
            view_column_qry = text(f"""
                        SELECT LINK_CODE_MAIN, VIEW_COLUMN
                        FROM CONFIG_LINK_CODE
                        WHERE PRODUCT_SUBGROUP = '{mapping_data["product_code"]}'
                        AND DG_PROCESS_CODE = '{mapping_data["process_code"]}'
                        AND DG_DEPARTMENT = '{mapping_data["department"]}'
                        AND SPECIAL_DATA_TYPE = 'Defective'
                    """)
            
        pg_engine = get_pg_engine()
        pg_session = sessionmaker(bind=pg_engine)
        
        # Query for Mapping Data (product, process, department)
        with pg_session() as pg_session:
            view_column = pg_session.execute(view_column_qry).fetchall()
            
        pg_session.close()
        pg_engine.dispose()
            
        # Create dict in format "{link_code_main: view_column}"
        view_column_dict = {row.link_code_main: row.view_column for row in view_column}

        return view_column_dict
    else:
        return []
        

################################################ Helper Function #########################################################
### Mostly, Helper Function use for Helper Agent that map & prepare actual data before sent to Main Agent

# [HELPER] - Mapping Info Function
def helper_mapping_info_func(
    chatInput: str | None = None,
)-> dict:
    f"""
        Input: A description containing [Lot No., Process Name, Process Code, Product Code, Table Name, Department].
        Example: 'Get data about 2354ABC of Racking Process'
        
        Output: Actual Parameter Name within JSON object / Dict
    """
    if chatInput:
        print("Calling tool: [Helper] Mapping Information Tool")
        
        # Init Info Mapping
        info_map_list = info_mapping()   
        
        # 1. Map the Parameter Code (Fuzzy or Dictionary lookup)
        # We convert to lowercase to handle 'Racking' vs 'racking'
        mapping_dict = {}
        for row in info_map_list:
            
            # Finding Table
            if (row[0] and row[0].lower() in chatInput.lower()):
                
                mapping_dict["table_name"] = row[0]
                mapping_dict["department"] = row[1]
                mapping_dict["product_code"] = row[2]
                mapping_dict["process_code"] = row[3]
                mapping_dict["process_name"] = row[4]
                break

        # 2. Extract the Lot Number using Regex (Pattern Matching)
        # First try to find after 'lotno' keyword, then fallback to alphanumeric pattern
        lot_match = re.search(r'lotno[:\s=]+([A-Z0-9]{8,12})', chatInput, re.IGNORECASE)
        if not lot_match:
            lot_match = re.search(r'\b([A-Z0-9]{8,12})\b', chatInput, re.IGNORECASE)
        lot_no = lot_match.group(1) if lot_match else "-"
        
        mapping_dict["lotno"] = lot_no
    

        return {
            "success": True,
            "content": f"Mapping found: '{chatInput}',  Mapping Param: {mapping_dict}",
        }   
    else:
        return {
            "success": False,
            "content": "No Mapping Prompt Provided, Please tell user to try again.",
        }

# [HELPER] - Process Mapper Function
def helper_process_mapper_func(
    chatInput: str | None = None,
    arguments: dict | None = None,
)->dict:
    f""""
        Tool for finding list of table (process) that match with the target LOTNO
        
        Input: Relation with the LOTNO
        Remark: Run this tool if user's want to get data with condition to filter with LOTNO
        Example: 'Find process list that relate with lotno 25XPB0062' (find list of tables before goes summary data)
        Output: process_table_lists
    """
    if chatInput:
        print("Calling tool: [Helper] Process Mapper Tool")
        
                # Check if arguments and mapping_data exist
        if  not arguments or "mapping_data" not in arguments or \
            not arguments["mapping_data"] or \
            'lotno' not in arguments["mapping_data"]:
            return {
                "success": False,
                "content": [],
            }
            
        mapping_data = lot_mapper(arguments["mapping_data"]["lotno"])
        
        # Check if mapping_data is valid dict
        if not mapping_data or not isinstance(mapping_data, dict):
            return {
                "success": False,
                "content": [],
            }
            
        # Get Table Name within the product target
        table_list = process_mapper(product_code=mapping_data["product_code"], department=mapping_data["department"])
        
        if table_list:
            return {
                "success": True,
                "content": table_list,
            }
        else:
            return {
                "success": False,
                "content": [],
            }
    else:
        return {
            "success": False,
            "content": [],
        }
            

################################################ Main Function #########################################################
### Mostly, Main Function use for Main Agent that will execute, summary, analyze data in DWH with information from Helper Agent

# [MAIN] - Execute SQL Query Function
def main_execute_sql_func(
    chatInput: str | None = None,
    arguments: dict | None = None,
)-> dict:
    f""""
        Tool for executing sql query base-on sql_statement, user's input and mapping information from DWH database
        
        Input: User request to get data with mapping parameter.
        Example: 'Get data from pac_1000'
        
        Output: Example data of target table within JSON object / Dict
    """
    

    if chatInput:
        print("Calling tool: [MAIN] Executing SQL Query Tool")
        
        # Check if arguments and mapping_data exist
        if not arguments or "mapping_data" not in arguments or not arguments["mapping_data"] or 'table_name' not in arguments["mapping_data"]:
            return {
                "success": False,
                "content": [],
            }
            
        
        mapping_data = arguments["mapping_data"]
        
        # CONDITION: if user's input contain 'lotno' parameter
        if( 'lotno' in mapping_data) and (mapping_data['lotno'] != '-'):
            sql_statement = f"SELECT * FROM {mapping_data['table_name']} WHERE lotno = '{mapping_data['lotno']}' LIMIT 5;"
        else: 
            sql_statement = f"SELECT * FROM {mapping_data['table_name']} LIMIT 5;"
            
        pg_engine = get_pg_engine()
        pg_session = sessionmaker(bind=pg_engine)
        
        
        with pg_session() as pg_session:
            sql_output = pg_session.execute(text(sql_statement)).fetchall()

        pg_session.close()
        pg_engine.dispose()            
        
        return {
            "success": True,
            "content": sql_output,
        }
    else:
        return {
            "success": False,
            "content": [],
        }



# [MAIN] - Summary Lot Data Function
def main_summary_each_process_data_func(
    chatInput: str | None = None,
    arguments: dict | None = None,
)->dict:
    f""""
        Tool for summary lot data base-on sql_statement, user's input and mapping information from DWH database
        
        Input: User request to summry lot data with mapping parameter.
        Remark: User's input contain "summary" and have only "lotno" in the request
        Example: 'Summary data of 25XPB0062'
        Output: Example data of target table within JSON object / Dict
    """
    if chatInput:
        print("Calling tool: [MAIN] Summary Lot Data Tool")
        
        
                # Check if arguments and mapping_data exist
        if  not arguments or "mapping_data" not in arguments or \
            not arguments["mapping_data"] or \
            'lotno' not in arguments["mapping_data"] or "table_list" not in arguments:
            return {
                "success": False,
                "content": [],
            }
            
        table_list = arguments["table_list"]
        
        
            
        joined_data = []
        for table in table_list:
            
            # For-loop each process data filter by lotno
            output_data = process_retrieving_data(table_name= table, 
                                                  lotno=arguments["mapping_data"]["lotno"])
            joined_data.append(output_data)
        
        
        return {
            "success": True,
            "content": joined_data,
        }
    else:
        return {
            "success": False,
            "content": [],
        }

# [MAIN] - Summary Lot Data Defective Function
def main_summary_each_process_data_def_func(
    chatInput: str | None = None,
    arguments: dict | None = None,
)->dict:
    f""""
        Tool for summary lot data base-on sql_statement, user's input and mapping information from DWH database
        
        Input: User request to summry lot data with mapping parameter.
        Remark: User's input contain "summary" and have only "lotno" in the request (Defective)
        Example: 'Summary data of 25XPB0062'
        Output: Example data of target table within JSON object / Dict
    """
    if chatInput:
        print("Calling tool: [MAIN] Summary Lot Data Defective Tool")
        
        
                # Check if arguments and mapping_data exist
        if  not arguments or "mapping_data" not in arguments or \
            not arguments["mapping_data"] or \
            'lotno' not in arguments["mapping_data"] or "table_list" not in arguments:
            return {
                "success": False,
                "content": [],
            }
            
        table_list = arguments["table_list"]
        
        
            
        joined_data = []
        for table in table_list:
            
            # For-loop each process data filter by lotno
            output_data = process_retrieving_data_defective(table_name= table, 
                                                  lotno=arguments["mapping_data"]["lotno"])
            joined_data.append(output_data)
        
        
        return {
            "success": True,
            "content": joined_data,
        }
    else:
        return {
            "success": False,
            "content": [],
        }





# # Generate SQL Query Function
# def generate_sql_func(
#     chatInput: str | None = None,
#     sql_statement: str | None = None,
#     mapping_data: dict | None = None,
# )-> dict:
#     f""""
#         Tool for generating sql query base-on user's input and mapping information from DWH database
#         include all information such as column_name, data, total rows
        
#         Generate SQL based-on user's input and mapping information such as column_name, parameter and table's name
#     """

#     if chatInput:
#         print("Calling tool: Generating SQL Query Tool")
        
#         if not mapping_data:
#             return {
#                 "success": False,
#                 "generate_sql_prompt": "No mapping data provide, try to call mapping tool again...",
#             }
        
#         if mapping_data["table_name"]:
        
#             # Query for COLUMN_NAME, TABLE_NAME
#             column_info_query = text(f"""
#                 SELECT DISTINCT COLUMN_NAME, ORDINAL_POSITION
#                 FROM INFORMATION_SCHEMA.COLUMNS
#                 WHERE TABLE_NAME = '{mapping_data["table_name"]}'
#                 AND TABLE_SCHEMA = 'public'
#                 ORDER BY ORDINAL_POSITION
#             """)

        
#             pg_engine = get_pg_engine()
#             pg_session = sessionmaker(bind=pg_engine)

#             with pg_session() as pg_session:
#                 lot_info = pg_session.execute(column_info_query).fetchall()

#             pg_session.close()
#             pg_engine.dispose()


#             prompt = f"""
#                 Generate SQL Query that output format is ready to be executed without any error
#                 By using user's input as purpose of the query and use support information to generate a correct and complete sql query string.
#                 Return as SQL Query format
                
#                 User's input: {chatInput}
#                 Support Information: {lot_info}
#             """

#             return {
#                 "success": True,
#                 "generate_sql_prompt": prompt,
#             }
#         else:
#             # Query for COLUMN_NAME, TABLE_NAME
#             column_info_query = text(f"""
#                 SELECT DISTINCT COLUMN_NAME, ORDINAL_POSITION, TABLE_NAME
#                 FROM INFORMATION_SCHEMA.COLUMNS
#                 WHERE TABLE_SCHEMA = 'PUBLIC'
#                 AND TABLE_NAME IN (
#                     SELECT DISTINCT TABLE_NAME 
#                     FROM INFORMATION_SCHEMA.TABLES 
#                     WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'PUBLIC'
#                 )
#                 ORDER BY ORDINAL_POSITION
#             """)

        
#             pg_engine = get_pg_engine()
#             pg_session = sessionmaker(bind=pg_engine)

#             with pg_session() as pg_session:
#                 lot_info = pg_session.execute(column_info_query).fetchall()

#             pg_session.close()
#             pg_engine.dispose()


#             prompt = f"""
#                 Generate SQL Query that output format is ready to be executed without any error
#                 By using user's input as purpose of the query and use support information to generate a correct and complete sql query string.
#                 Return as SQL Query format
                
#                 User's input: {chatInput}
#                 Support Information: {lot_info}
#             """

#             return {
#                 "success": True,
#                 "generate_sql_prompt": prompt,
#             }
#     else:
#         return {
#             "success": False,
#             "generate_sql_prompt": "No User's input provide, Please tell user to try again",
#         }