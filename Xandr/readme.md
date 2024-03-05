# Xandr Data Fetch and Processing Script

## Author: Deepika S
## Created: 03/5/2023
## Modified: 

This Python script is designed to fetch data from the Xandr platform reports using APIs, process the data, and then push it to S3 and Redshift.

### Features:
- Fetches data from Xandr platform reports using APIs.
- Processes the data and pushes it to S3 in raw, cleaned, and curated formats.
- Inserts the curated data into Redshift.

### Dependencies:
- `requests`: For making HTTP requests.
- `json`: For handling JSON data.
- `pandas`: For data manipulation and analysis.
- `psycopg2`: For connecting to PostgreSQL databases.
- `boto3`: For interacting with Amazon Web Services (AWS) resources.
- `datetime`: For working with dates and times.

### Usage:
1. Ensure all dependencies listed in `requirements.txt` are installed.
2. Configure `config.py` with your Xandr and AWS credentials.
3. Run the script `xandr_data_fetch.py`.

### Configuration:
You need to configure `config.py` with the following parameters:
- `AUTH_URL`: Xandr authentication URL.
- `DATASOURCE`: Xandr datasource.
- `...` (List all other configuration parameters)

### License:
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

For any questions or inquiries, please contact Nayana N N at [nayanan@example.com](mailto:nayanan@example.com).
