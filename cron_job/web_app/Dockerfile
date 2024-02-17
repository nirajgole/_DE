FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/nirajgole/_DE.git .

RUN pip3 install -r /app/cron_job/web_app/requirements.txt

EXPOSE 8501

# HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "/app/cron_job/web_app/rates_app.py"]