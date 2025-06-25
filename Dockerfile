FROM python:3.13-slim

RUN apt-get update && apt-get install -y git

RUN pip install flake8

WORKDIR /code/
COPY requirements.txt .

RUN pip install -r requirements.txt

COPY src/ src
COPY tests/ tests
COPY scripts/ scripts
COPY flake8.cfg .
COPY deploy.sh .

CMD ["python", "-u", "/code/src/component.py"]
