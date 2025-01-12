from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Text, CheckConstraint, PrimaryKeyConstraint

# Initialize SQLAlchemy components
Base = declarative_base()

# Define the sql_script_store table
class SQLScriptStore(Base):
    __tablename__ = "sql_script_store"

    zone = Column(
        String,
        CheckConstraint("zone IN ('COMMON','BRONZE','SILVER','GOLD')"),
        nullable=False
    )
    query = Column(Text, nullable=False)
    query_type = Column(
        String,
        CheckConstraint("query_type IN ('SELECT','UPDATE','DELETE','CREATE','DROP','OTHER')"),
        nullable=False
    )
    table_name = Column(String, nullable=False)
    data_columns = Column(Text)  # Added data_columns to store the list of columns

    __table_args__ = (
        CheckConstraint("zone IN ('COMMON', 'BRONZE', 'SILVER', 'GOLD')"),
        CheckConstraint("query_type IN ('SELECT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'OTHER')"),
        PrimaryKeyConstraint("table_name", "query_type"),  # Define composite primary key here
    )