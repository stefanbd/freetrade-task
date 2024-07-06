# ApiRunner

ApiRunner is a Python application that fetches user data from an API, processes it, and uploads it to Google Cloud Storage in Parquet format.

## Prerequisites

- Python 3.7 or higher

## Installation

1. **Clone the repository:**

    ```sh
    git clone https://github.com/stefanbd/stefan-freetrade-task.git
    cd stefan-freetrade-task
    ```

2. **Create a virtual environment:**

    ```sh
    python3 -m venv venv
    source venv/bin/activate
    ```

3. **Install the required packages:**

    ```sh
    pip install -r requirements.txt
    ```


4. **Optional settings:**

    Edit the `app/settings/settings.py` file to increase or decrease the `BATCH_SIZE`.


## Usage

1. **Run the application:**

    ```sh
    python src/entrypoint.py
    ```
