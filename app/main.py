import subprocess

def run_streamlit():
    command = [
        "poetry", "run", "python", "-m", "streamlit", "run", "app/views/ui.py", "--server.enableXsrfProtection=false"
    ]
    subprocess.run(command)

if __name__ == "__main__":
    run_streamlit()

