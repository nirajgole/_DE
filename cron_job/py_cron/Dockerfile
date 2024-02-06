FROM alpine:3.19

# Install python/pip
# ENV PYTHONUNBUFFERED=1
# RUN apk add --update --no-cache python3 && ln -sf python3 /usr/bin/python
# RUN python3 -m ensurepip
# RUN pip3 install --no-cache --upgrade pip setuptools

RUN apk -U upgrade
RUN apk add --update --no-cache python3
COPY script.py /usr/
RUN chmod +x /usr/script.py
ADD root /var/spool/cron/crontabs/root
CMD ["/usr/sbin/crond","-f","-l","8"]