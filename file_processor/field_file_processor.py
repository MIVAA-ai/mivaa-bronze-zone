from bronze.validators.field_data_validator import validate_field
from file_processor.file_processor import FileProcessor


class FieldFileProcessor(FileProcessor):

    def validate(self, dataframe, result):
        print("Validating field file...")
        # Add field-specific validation logic
        # Perform field validation
        validate_field(dataframe, result.id, result.filename)

    def process(self):
        print("Processing field file...")
        # Add field-specific processing logic

