import subprocess

def run_streamlit():
    command = [
        "uvicorn", "app.api.v1.routes:app", "--host", "0.0.0.0", "--port", "8000"
    ]
    subprocess.run(command)

if __name__ == "__main__":
    run_streamlit()
