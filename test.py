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

from fastapi import APIRouter, HTTPException, Depends

def info_mapping():
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
        

#########################################################################################################
def process_mapper(
    product_code: str,
    department: str,
):
    """
        Process Mapper Function: A helper function use for listing all table that include in the same product.
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
    lotno: str
):
    """
        Process Retrieving Data Function: A helper function use for execute data from each table based on 
        table_name & lotno from mapping data.
        
        Return: Table Schema format (each table might not the same)
    """
    
    if table_name and lotno:
        # Query for LOT_INFO
        lot_info_qry = text(f"""
                    SELECT *
                    FROM {table_name}
                    WHERE LOTNO = '{lotno}'
                """)

        pg_engine = get_pg_engine()
        pg_session = sessionmaker(bind=pg_engine)

        with pg_session() as pg_session:
            process_data = pg_session.execute(lot_info_qry).fetchall()

        pg_session.close()
        pg_engine.dispose()

        return process_data

    else:
        return []
    
mapping_data = {}
mapping_data["product_code"] = "PAC"
mapping_data["department"] = "MT900"
mapping_data["lotno"] = "25XPB2941"
    
# Get Table Name within the product target
table_list = process_mapper(product_code=mapping_data["product_code"], department=mapping_data["department"])

# # For-loop each process data filter by lotno
# joined_data = []

# for item in table_list:
#     each_data = process_retrieving_data(table_name= item.table_name, lotno=mapping_data["lotno"])
    
#     # Convert Row objects to dicts and add table_name
#     for row in each_data:
#         row_dict = dict(row._mapping)
#         row_dict["table_name"] = item.table_name
#         joined_data.append(row_dict)


print(table_list)

# [('PAC_1000',), ('PAC_1020',), ('PAC_1080',), ('PAC_1090',), ('PAC_1110',), ('PAC_1112',),
# ('PAC_1171',), ('PAC_1190',), ('PAC_1210',), ('PAC_1220',), ('PAC_1230',), ('PAC_2000',),
# ('PAC_2020',), ('PAC_2060',), ('PAC_2070',), ('PAC_2090',), ('PAC_2150',), ('PAC_2160',), ('PAC_2191',), ('PAC_2200',)]
# 259PB0557