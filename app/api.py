import subprocess

def run_streamlit():
    command = [
        "uvicorn", "app.api.v1.routes:app"
    ]
    subprocess.run(command)

if __name__ == "__main__":
    run_streamlit()
