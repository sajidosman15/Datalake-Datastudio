# TechHub Data Studio - Setup Instructions

Follow these steps to set up and run the Data Studio application locally:

## 1. Create a Virtual Environment
First, create a virtual environment to isolate your project dependencies:

```bash
python -m venv .venv
```

## 2. Activate the Virtual Environment

```bash
.\.venv\Scripts\Activate
```

## 3. Install Dependencies from Poetry
```bash
poetry install --no-root
```

## 4. Run the Data Studio App
```bash
python app/main.py
```