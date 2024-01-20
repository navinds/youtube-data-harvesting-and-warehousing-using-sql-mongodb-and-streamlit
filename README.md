# YouTube Data Harvesting and Warehousing

YouTube Data Harvesting and Warehousing is a project designed to empower users to access and analyze data from various YouTube channels. The application, developed using SQL, MongoDB, and Streamlit, provides a user-friendly interface for retrieving, saving, and querying YouTube channel and video data.

## Tools and Libraries Used

This project leverages the following components:

- **Streamlit:** The Streamlit library is utilized to create an intuitive UI, enabling users to interact with the application for data retrieval and analysis.

- **Python:** As the primary programming language, Python is employed for the complete application development, including data retrieval, processing, analysis, and visualization.

- **Google API Client:** The googleapiclient library in Python facilitates communication with YouTube's Data API v3, allowing seamless retrieval of essential information like channel details, video specifics, and comments.

- **MongoDB:** MongoDB, a scalable document database, is used for storing structured or unstructured data in a JSON-like format.

- **MySQL:** MySQL, an advanced and scalable open-source DBMS, is employed for efficient storage and management of structured data, offering support for various data types and advanced SQL capabilities.

## YouTube Data Scraping and Ethical Perspective

When scraping YouTube content, ethical considerations are paramount. Adhering to YouTube's terms, obtaining proper authorization, and complying with data protection regulations are fundamental. Responsible handling of collected data, ensuring privacy, confidentiality, and preventing misuse, is crucial. Additionally, considering the impact on the platform and its community fosters a fair and sustainable scraping process.

## Required Libraries

1. googleapiclient.discovery
2. streamlit
3. sqlalchemy
4. pymongo
5. pandas

## Features

The YouTube Data Harvesting and Warehousing application offers the following functions:

- Retrieval of channel and video data from YouTube using the YouTube API.
- Storage of data in a MongoDB database as a data lake.
- Migration of data from the data lake to a SQL database for efficient querying and analysis.
- Search and retrieval of data from the SQL database using different search options.

## Installation and Setup

To run the YouTube Data Harvesting and Warehousing project, follow these steps:

1. **Install Python:** Ensure that the Python programming language is installed on your machine.

2. **Install Required Libraries:**
    ```
    pip install streamlit pymongo sqlalchemy PyMySQL pandas google-api-python-client
    ```

3. **Set Up Google API:**
    - Create a Google API project on the [Google Cloud Console](https://console.cloud.google.com/).
    - Obtain API credentials (JSON file) with access to the YouTube Data API v3.
    - Place the API credentials file in the project directory under the name `google_api_credentials.json`.

4. **Configure Database:**
    - Set up a MongoDB database and ensure it is running.
    - Set up a MySQL database and ensure it is running.
  
5. **Configure Application:**
    - Copy the `config.sample.ini` file and rename it to `config.ini`.
    - Update the `config.ini` file with your Google API credentials, MongoDB, and MySQL connection details.

6. **Run the Application:**
    ```
    streamlit run yourfilename.py
    ```
   Access the Streamlit application at `http://localhost:8501` in your web browser.

## Confidential Credentials

Ensure that confidential credentials are securely managed. Replace the placeholder values in the configuration files with your actual credentials. Do not expose sensitive information in public repositories.

**Note:** Ensure to replace placeholder text with specific details about your project.
**Note:** Follow ethical scraping practices, obtain necessary permissions, and comply with YouTube's terms of service when using the YouTube API.


## Contribution Guidelines

If you wish to contribute to the project, you are always welcome. If you encounter any issues or have suggestions for improvements, please feel free to reach me.

## License

This project is licensed under the [MIT License](LICENSE).

[MIT License](https://opensource.org/licenses/MIT)


## Contact

- **LinkedIn:** [Navin](https://www.linkedin.com/in/navinkumarsofficial/)
- **Email:** navinofficial1@gmail.com

Feel free to connect with me on LinkedIn or reach out via email for any inquiries or collaboration opportunities.



