# redis-hello

## Setup

Set bF API keys

```sh
cp .envrc.sample .envrc
direnv edit .
```

Install dependencies

```sh
pipenv install
```


## Run

```sh
docker-compose up -d
pipenv run python main.py
```
