OSDU Sync
OSDU Sync is a solution designed to process, validate, and manage files for subsurface data management workflows. It leverages DuckDB, Python, and Docker to streamline data validation and integration.

Features
File Polling: Automatically detects new .csv files in a configured folder and processes them.
Data Validation: Validates subsurface data using schema rules defined with Pandera.
Database Integration: Utilizes DuckDB for lightweight and efficient data storage.
Dockerized Environment: Fully containerized for consistent deployment across environments.
Configurable Paths: Supports customizable upload and output directories through .env configuration.
Project Structure
bash
Copy code
osdu-sync/
│
├── crawler/                   # Components for file polling
│   ├── crawlerconfig.py       # Configuration for the polling mechanism
│   ├── watcher.py             # Watches and processes new files
│
├── models/                    # Database models and error logging
│   ├── bronze_validation_results.py
│   ├── validation_errors.py
│
├── output/                    # Stores processed validation results
│
├── uploads/                   # Default directory for input files
│
├── utils/                     # Utility functions
│   ├── db_util.py             # Handles DuckDB interactions
│   ├── checksum_util.py       # Utility functions for data integrity
│
├── validator/                 # Data validation logic
│   ├── field_validator.py     # Validates input fields using Pandera
│
├── .env                       # Environment configuration
├── Dockerfile                 # Docker setup for containerization
├── docker-compose.yml         # Docker Compose configuration
├── requirements.txt           # Python dependencies
├── app.py                     # Entry point for the application
├── README.md                  # Documentation
└── my_database.db             # DuckDB database file
Getting Started
Prerequisites
Python: Version 3.9 or later.
Docker: Latest version of Docker and Docker Compose.
Git: For cloning the repository.
Installation
Clone the Repository:

bash
Copy code
git clone https://github.com/MIVAA-ai/osdu-sync.git
cd osdu-sync
Install Python Dependencies: Create a virtual environment and install required dependencies:

bash
Copy code
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
Environment Configuration
Create a .env File: Add the following configuration to your .env file:

env
Copy code
UPLOADS_DIR=./uploads
OUTPUT_DIR=./output
Customize Directories: Update UPLOADS_DIR and OUTPUT_DIR paths as needed.

Usage
1. Start Locally
Run the application locally:

bash
Copy code
python app.py
2. Start with Docker
Build and run the Docker container:

bash
Copy code
docker-compose up --build
3. Upload Files
Place .csv files in the folder specified by UPLOADS_DIR (default is ./uploads).

4. View Results
Processed validation results will be available in the folder specified by OUTPUT_DIR (default is ./output).

Features in Detail
1. File Polling
Automatically polls the UPLOADS_DIR for new .csv files.
Triggers validation tasks for detected files.
2. Data Validation
Schema-based validation using Pandera:
Ensures compliance with predefined rules.
Logs validation errors to the database.
3. Lightweight Database
DuckDB: A high-performance embedded database for managing validation results and error logs.
4. Dockerized Deployment
Containerized setup ensures consistency across environments.
Volumes for /uploads and /output allow data persistence.
Volumes
Uploads Directory: Mounted to /uploads inside the container.
Output Directory: Mounted to /output inside the container.
Development
Run Tests
To test the application logic:

bash
Copy code
pytest
Code Linting
Ensure code adheres to Python standards:

bash
Copy code
flake8 .
Contributing
Contributions are welcome! Please follow these steps:

Fork the repository.
Create a new branch for your feature:
bash
Copy code
git checkout -b feature/my-feature
Commit changes and push to your fork:
bash
Copy code
git push origin feature/my-feature
Open a pull request.
License
This project is licensed under the MIT License.

Acknowledgments
Pandera for data validation.
DuckDB for lightweight data storage.
Docker for containerization.
