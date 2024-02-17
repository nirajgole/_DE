FROM python:3.10
RUN apt-get update && apt-get -y install cron && apt-get clean
WORKDIR /app
COPY requirements.txt /app/requirements.txt
COPY rates.py /app/rates.py
COPY crontab /etc/cron.d/crontab
RUN pip install --upgrade pip && pip install -r /app/requirements.txt
RUN chmod +x /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab
RUN echo $PYTHONPATH
# run crond as main process of container
CMD ["cron", "-f"]