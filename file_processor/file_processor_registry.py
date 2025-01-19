from file_processor.field_file_processor import FieldFileProcessor


class FileProcessorRegistry:
    _registry = {}

    @classmethod
    def register(cls, file_type, processor_class):
        cls._registry[file_type] = processor_class

    @classmethod
    def get_processor(cls, file_type):
        processor_class = cls._registry.get(file_type)
        if not processor_class:
            raise ValueError(f"Unknown file type: {file_type}")
        return processor_class(file_type)


# Register processors
FileProcessorRegistry.register("FIELD", FieldFileProcessor)
