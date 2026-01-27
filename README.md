# MCP Tools for Apply Gen-AI with Data Warehouse V2.0
MCP Tools for supporting Generative AI Chatbot with connection with database (Data Warehouse)

### Tools
- Mapping Data: Use for mapping user's input with the actual value in database like table_name, column_name or parameter_name.

### Command

#### Production
uvicorn main:app --host 0.0.0.0 --port 8080

#### Test + Auto-Reload
uvicorn main:app --host 0.0.0.0 --port 8080 --reload 
