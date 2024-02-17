For local environment install docker desktop.
```shell
docker build -t py-cron .
```
```shell
docker run -it --rm py-cron
```
```shell
docker-compose up -d
```
```shell
docker build -t web-app . --no-cache
```

for web_app configure your postgres/db credentials in .streamlit/config.toml file