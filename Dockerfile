FROM htcondor/htc-minimal-notebook:latest

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install -r /tmp/requirements.txt
