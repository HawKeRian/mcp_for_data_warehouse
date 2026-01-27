from datetime import datetime
from pydantic import BaseModel, Field, field_serializer
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer, String, DateTime, Boolean
from sqlalchemy.sql import func
from typing import Optional, Union
import uuid  # Make sure this import exists
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Session, sessionmaker, DeclarativeBase
from enum import Enum

# database model classes for PostgreSQL
class PgBase(DeclarativeBase):
    pass



# Add User Role Enum
class UserRole(str, Enum):
    ADMIN = "admin"
    USER = "user"
    MAINTAINER = "maintainer"
    OPERATOR = "operator"


class DbUser(PgBase):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, server_default=func.gen_random_uuid())
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=True, index=True)
    password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    role = Column(String(20), default="user", nullable=False)  # 🔒 Add role field
    created_date = Column(DateTime(timezone=True), server_default=func.current_timestamp())
    updated_date = Column(DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp())


# Request/Response Models
class UserBase(BaseModel):
    username: str
    email: Optional[str] = None
    password: str
    role: Optional[UserRole] = UserRole.USER  # 🔒 Add role field


class UserCreate(BaseModel):
    username: str
    email: Optional[str] = None
    password: str
    role: Optional[UserRole] = UserRole.USER  # 🔒 Add role field


class UserLogin(BaseModel):
    username: str
    password: str


class UserDisplayBase(BaseModel):
    # id: str
    username: str
    email: Optional[str] = None
    is_active: bool
    role: str
    created_date: datetime

    # @field_serializer("id")
    # def serialize_id(self, value):
    #     """Convert UUID to string"""
    #     if isinstance(value, uuid.UUID):
    #         return str(value)
    #     return str(value)

    @field_serializer("created_date")
    def serialize_created_date(self, value):
        """Ensure datetime is properly serialized"""
        if isinstance(value, datetime):
            return value
        return value

    class Config:
        from_attributes = True
        # Remove json_encoders as field_serializer should handle it
        arbitrary_types_allowed = True


class Token(BaseModel):
    access_token: str
    token_type: str
    user_id: str
    username: str
    role: str  # 🔒 Add role field


class TokenData(BaseModel):
    username: Optional[str] = None
