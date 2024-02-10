FROM python:3.10
RUN apt-get update && apt-get -y install cron vim && apt-get clean
WORKDIR /app
COPY crontab /etc/cron.d/crontab
COPY requirements.txt /app/
RUN pip install --requirement /app/requirements.txt
COPY script.py /app/
RUN chmod +x /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab

# run crond as main process of container
CMD ["cron", "-f"]