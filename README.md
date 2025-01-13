# MIVAA Bronze Zone Tool

This repository contains a robust software utility designed to validate and process field boundaries data in CSV format. Built with a focus on ensuring data quality, the utility plays a critical role in preparing high-quality data for ingestion into platforms like OSDU, adhering to the principles of the Medallion Architecture. Currently, this utility implements only bronze zone validations.

---

## Prerequisites

1. **Download the Repository**:
   - [Clone](https://github.com/MIVAA-ai/mivaa-bronze-zone.git) or download the repository as a [ZIP](https://github.com/MIVAA-ai/mivaa-bronze-zone/archive/refs/heads/main.zip) file.

2. **Unzip the Repository**:
   - Extract the downloaded ZIP file to a folder on your system.

3. **Install Python**:
   - Ensure Python 3.9+ is installed on your machine. You can download Python [here](https://www.python.org/downloads/).

4. **Install Docker**:
   - Ensure Docker and Docker Compose are installed and running on your machine. You can download Docker [here](https://www.docker.com/).

---

## Steps to Run the Application Using the Startup Script

### 1. Setup the Environment

#### For Windows:
1. Open Command Prompt.
2. Navigate to the repository directory and run:
   ```cmd
   setup.bat D:/MIVAA-ai/mivaa-bronze-directory
   ```
   Replace `D:/MIVAA-ai/mivaa-bronze-directory` with your desired base directory.

#### For Linux:
1. Open a terminal.
2. Navigate to the repository directory.
3. Make the script executable (only needed the first time):
   ```bash
   chmod +x setup.sh
   ```
4. Run the command:
   ```bash
   ./setup.sh /path/to/mivaa-bronze-directory
   ```
   Replace `/path/to/mivaa-bronze-directory` with your desired base directory.

### 2. Initialize the Database

Run the following command in your terminal:
```bash
python startup.py
```
This will initialize the database and prepare the application for use. Example log output:
```plaintext
INFO - Initialize the database from the JSON Schema file.
INFO - Database initialization completed successfully.
INFO - Starting application...
```

### 3. Start the Application Using Docker Compose

Run the following command to start the application:
```bash
docker-compose --env-file .env up --build
```

### Error Logging
Errors are logged in the database with severity levels (`WARNING`, `ERROR`). Detailed logs are generated to help users identify and resolve issues efficiently.

---

## Accessing Logs and Outputs

- **Uploads Directory**:
  Place your CSV files in the directory specified in the `UPLOADS_DIR` path in your `.env` file.
- **Processed Directory**:
  The validated and processed files will be saved in the directory specified in the `OUTPUT_DIR` path.
- **Error Logs**:
  Detailed error logs are saved in the database and corresponding output folders for review.

---

## Project Structure

```
mivaa-fieldboundaries-bronze-zone/
├── app.py                # Main application entry point
├── config/               # Configuration files and utilities
│   ├── settings.py       # Application-level settings and environment variables
│   └── logger.py         # Logging configuration
├── crawler/              # File synchronization and polling utilities
│   ├── file_watcher.py   # Watches for changes in input directories
│   └── sync_service.py   # Synchronizes files between local and remote locations
├── models/               # Database models and schema
│   ├── base_model.py     # Base model definition
│   └── field_models.py   # Models specific to field boundaries
├── utils/                # Helper functions and utilities
│   ├── db_utils.py       # Database connection and query utilities
│   ├── validation_utils.py # Validation logic for input data
│   └── file_utils.py     # File handling and path management
├── validator/            # Core data validation logic
│   ├── validations.py    # Custom validation functions
│   ├── polygon_checks.py # Polygon-specific validation logic
│   └── date_checks.py    # Date-specific validation logic
├── migrations/           # Database migrations and versioning
│   ├── 0001_initial.py   # Initial database schema
│   └── alembic.ini       # Alembic configuration for migrations
├── tests/                # Unit and integration tests
│   ├── test_validations.py # Tests for validation logic
│   ├── test_crawler.py   # Tests for file synchronization
│   └── test_endpoints.py # API or main app testing
├── docker/               # Docker-related files
│   ├── Dockerfile        # Docker image configuration
│   └── docker-compose.yml # Docker Compose configuration
├── .env                  # Environment variables
├── requirements.txt      # Python dependencies
├── setup.bat             # Windows setup script
├── setup.sh              # Linux setup script
└── README.md             # Project documentation

```

---

## Troubleshooting

- **Check Environment Variables**:
  Ensure the directories specified in the `.env` file exist and are accessible.
- **Inspect Docker Logs**:
  Run the following command to view Docker logs:
  ```bash
  docker-compose logs
  ```
- **Rebuild Containers**:
  If you encounter issues, rebuild the containers using:
  ```bash
  docker-compose --env-file .env up --build
  ```

---

## Additional Resources
- **Blog**: Read the detailed blog post about this application: 
- **Medallion Architecture**: Learn more about the principles of Medallion Architecture [here](https://example.com).
- **OSDU Standards**: Read about OSDU standards [here](https://osduforum.org/).

---

## Notes

--This application requires Docker Compose.

--Ensure your CSV files are properly formatted for successful processing.

--Avoid editing any files that are being actively processed, as it might cause conflicts or errors.

--This application is tested in both Windows and Linux environments. In case of issues, feel free to reach out.

Feel free to raise any issues or suggestions for improvement! Reach out at support@mivaa-ai.com for more help, comments, or feedback.

