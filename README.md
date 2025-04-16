# TechHub Data Studio - Setup Instructions

Follow these steps to set up and run the Data Studio application locally:

## 1. Create a Virtual Environment
First, create a virtual environment to isolate your project dependencies:
Make sure Python 12 is installed on your machine. Using command **python --version**

```bash
python -m venv .venv
```

## 2. Activate the Virtual Environment
For Windows:
```bash
.\.venv\Scripts\Activate
```

For Linux:
```bash
source .venv/bin/activate
```

## 3. Install Dependencies from Poetry
Install all required packages for this application using Poetry.
If you do not have poetry installed, Install it using command **pip install poetry**

```bash
poetry install --no-root
```

## 4. Create and Populate the Environment File
```bash
cp .env-example .env
```
Edit the .env file and replace placeholder values with actual configurations.

## 5. Create the Database and necessary tables
Install PostgreSQL on your local machine and, if necessary, create the user and password. There is no need to create the database; simply run the script to set it up.

```bash
python scripts/database.py
```

## 4. Run the Data Studio App

```bash
python app/main.py
```