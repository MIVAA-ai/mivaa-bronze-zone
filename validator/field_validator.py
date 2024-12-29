import pandas as pd
import pandera as pa
from pandera.typing import Series
from datetime import datetime
import os
from models.bronze_validation_results import log_field_bronze_table
from models.validation_errors import log_errors_to_db
from utils.db_util import con, fetch_data_conditionaly
import traceback

# List to store validation errors
validation_errors = []
error_index = []

field_column_list = ['FieldName', 'FieldType', 'DiscoveryDate', 'X', 'Y', 'CRS', 'Source', 'ParentFieldName']

# Define the Pandera Schema with All Validation Rules
class FieldDataSchema(pa.DataFrameModel):
    # Define schema for the input data
    FieldName: Series[str] = pa.Field(nullable=False, coerce=True)
    FieldType: Series[str] = pa.Field(nullable=True, coerce=True)
    X: Series[float] = pa.Field(nullable=True, coerce=True)
    Y: Series[float] = pa.Field(nullable=True, coerce=True)
    CRS: Series[str] = pa.Field(
        nullable=True,
        regex=r"^(Geographic2D|Projected|Vertical):EPSG::\d+$|^(BoundGeographic2D|BoundProjected):EPSG::\d+_EPSG::\d+$",
        coerce=True
    )
    DiscoveryDate: Series[pd.Timestamp] = pa.Field(nullable=True, coerce=True)
    Source: Series[str] = pa.Field(nullable=True)
    ParentFieldName: Series[str] = pa.Field(nullable=True)

    # Validate DiscoveryDate is <= today
    @pa.dataframe_check
    def validate_discovery_date(cls, df: pd.DataFrame) -> bool:
        """Check if DiscoveryDate is not in the future."""
        today = pd.Timestamp(datetime.now().date())
        invalid_rows = df[df["DiscoveryDate"] > today]
        if not invalid_rows.empty:
            for idx in invalid_rows.index:
                error_index.append(idx)
                validation_errors.append({
                    "row_index": str(idx),
                    "field_name": "DiscoveryDate",
                    "error_type": "row_validation",
                    "error_message": f"DiscoveryDate is in the future"
                })
        return True

    # Ensure FieldType and DiscoveryDate are consistent for each FieldName
    @pa.dataframe_check
    def validate_consistency(cls, df: pd.DataFrame) -> bool:
        """Check consistency of FieldType and DiscoveryDate within FieldName."""
        for fieldname, group in df.groupby("FieldName"):
            if group["FieldType"].nunique() > 1 or group["DiscoveryDate"].nunique() > 1:
                error_index.extend(group.index.tolist())
                for idx in group.index.tolist():
                    validation_errors.append({
                        "row_index": str(idx),
                        "field_name": fieldname,
                        "error_type": "group_validation",
                        "error_message": f"Inconsistent FieldType or DiscoveryDate"
                    })
        return True

    # Validate Polygon Completeness (X, Y, CRS must all be present or null)
    @pa.dataframe_check
    def validate_polygon_completeness(cls, df: pd.DataFrame) -> bool:
        """Ensure X, Y, CRS are either all present or all null."""
        for fieldname, group in df.groupby("FieldName"):
            condition = (
                (group["X"].isnull() == group["Y"].isnull()) &
                (group["Y"].isnull() == group["CRS"].isnull())
            )
            if not condition.all():
                error_index.extend(group.index.tolist())
                for idx in group.index.tolist():
                    validation_errors.append({
                        "row_index": str(idx),
                        "field_name": fieldname,
                        "error_type": "group_validation",
                        "error_message": f"Incomplete Polygon Data"
                    })
        return True

    # Validate Polygon Closure (First and last X, Y must match)
    @pa.dataframe_check
    def validate_polygon_closure(cls, df: pd.DataFrame) -> bool:
        """Ensure the first and last coordinates of a polygon match."""
        for fieldname, group in df.groupby("FieldName"):
            group = group.dropna(subset=["X", "Y"])
            if len(group) >= 2 and not (
                (group.iloc[0]["X"] == group.iloc[-1]["X"]) and
                (group.iloc[0]["Y"] == group.iloc[-1]["Y"])
            ):
                error_index.extend(group.index.tolist())
                for idx in group.index.tolist():
                    validation_errors.append({
                        "row_index": str(idx),
                        "field_name": fieldname,
                        "error_type": "group_validation",
                        "error_message": f"Polygon not closed"
                    })
        return True

# Main Validation Function
def validate_field(df,file_id,file_name):
    """Main function to validate data and log results."""
    try:
        # Convert DiscoveryDate to datetime with dayfirst=True
        df['DiscoveryDate'] = pd.to_datetime(df['DiscoveryDate'], errors='coerce', dayfirst=True)

        # Validate the DataFrame against the schema
        FieldDataSchema.validate(df, lazy=True)

    except pa.errors.SchemaErrors as e:
        # Log schema validation errors
        for _, error in e.failure_cases.iterrows():
            error_index.append(error.get("index"))
            validation_errors.append({
                "row_index": str(error.get("index")),
                "field_name": error.get("column"),
                "error_type": "row_validation",
                "error_message": error.get("check")
            })
    except Exception:
        # Log unexpected errors
        print("Unexpected Error:", traceback.format_exc())

    finally:

        # Convert the list to a set
        error_index_set = set(error_index)

        # Log validation results
        log_field_bronze_table(df, file_id, error_index_set)

        # Log validation errors to the database
        log_errors_to_db(validation_errors, file_id)

        validation_errors.clear()
        error_index.clear()

        query = f"""
            SELECT 
                t1.id,
                t1.row_index,
                t1.file_id,
                t1.validation_status,
                t1.FieldName,
                t1.FieldType,
                t1.DiscoveryDate,
                t1.X,
                t1.Y,
                t1.CRS,
                t1.Source,
                t1.ParentFieldName,
                t1.validation_timestamp,
                STRING_AGG(t2.error_message, ', ') AS error_message
            FROM field_bronze_table AS t1
            LEFT JOIN validation_errors AS t2
            ON t1.row_index = t2.row_index AND t1.file_id = t2.file_id
            WHERE t1.file_id = {file_id}
            GROUP BY 
                t1.id,
                t1.row_index,
                t1.file_id,
                t1.validation_status,
                t1.FieldName,
                t1.FieldType,
                t1.DiscoveryDate,
                t1.X,
                t1.Y,
                t1.CRS,
                t1.Source,
                t1.ParentFieldName,
                t1.validation_timestamp,
            ORDER BY t1.id;
        """
        result = con.execute(query).fetchdf()

        # VERIFY RESULTS
        # Set option to display all columns
        pd.set_option('display.max_columns', None)
        print(result)

        # Ensure the 'output' directory exists
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)

        # Save the results to a CSV file in the 'output' directory
        result.to_csv(f"{output_dir}/{file_name}_validation_results.csv", index=False)

        print(f"CSV file saved as '{output_dir}/{file_name}_validation_results.csv'.")
        return