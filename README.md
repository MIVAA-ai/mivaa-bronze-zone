# MIVAA Bronze Zone Tool

OSDU Sync is a robust data synchronization tool designed to streamline data management within the Open Subsurface Data Universe (OSDU) ecosystem. This tool ensures seamless integration and synchronization between local systems and OSDU-compliant environments, making subsurface and energy data management more efficient and reliable.

---

## Features

- **Data Synchronization**: Syncs files and metadata between local systems and OSDU.
- **Validation Engine**: Custom validation checks ensure data integrity.
- **Error Logging**: Detailed logging of warnings and errors.
- **Configurable Folders**: Manage uploads, outputs, and database files dynamically.
- **Docker Integration**: Easily deploy using Docker Compose.

---

## Installation

### Prerequisites

- Python 3.9+
- Docker & Docker Compose
- Git

### Clone the Repository

```bash
git clone https://github.com/MIVAA-ai/osdu-sync.git
cd osdu-sync
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Setup Environment Variables

Create or update the `.env` file in the root directory:

```
UPLOADS_DIR=path/to/uploads
OUTPUT_DIR=path/to/output
DB_DIR=path/to/db_files
```

Use the provided batch script to initialize directories and update the `.env` file dynamically:

```bash
setup.bat D:/MIVAA-ai/osdu-sync-field
```

---

## Usage

### Initialize the Database

```bash
python startup.py
```

Logs will indicate the progress of database initialization and application startup:

```plaintext
INFO - Initializing database from JSON Schema file.
INFO - Database initialization completed successfully.
INFO - Starting application...
```

### Run Docker Compose

To start the application using Docker Compose:

```bash
docker-compose --env-file .env up --build
```

---

## Validation Engine

The validation engine performs checks on subsurface data, ensuring compliance with OSDU standards. Key features include:

- **Custom Checks**:
  - `validate_discovery_date`: Ensures discovery dates are not in the future.
  - `validate_consistency`: Validates consistency of `FieldType` and `DiscoveryDate`.
  - `validate_polygon_completeness`: Verifies polygons are properly defined.
  - `validate_polygon_closure`: Ensures polygons are closed geometrically.

- **Error Logging**: Errors are logged in the database with severity levels (`WARNING`, `ERROR`).

---

## Project Structure

```
osdu-sync/
├── app.py               # Main application entry point
├── crawler/             # Contains polling and file watching utilities
├── models/              # SQLAlchemy models for database tables
├── utils/               # Utility scripts (DB, validation, logging)
├── validator/           # Data validation logic
├── docker-compose.yml   # Docker Compose configuration
├── requirements.txt     # Python dependencies
├── .env                 # Environment variables
```

---

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -m "Add feature"`).
4. Push to the branch (`git push origin feature-name`).
5. Open a pull request.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Contact

For questions or support, please contact [support@mivaa-ai.com](mailto:support@mivaa-ai.com).
