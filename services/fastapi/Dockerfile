FROM python:3.10

RUN mkdir -p /fastapi
WORKDIR /fastapi
RUN apt-get update
RUN apt-get install -y curl

COPY requirements.txt /fastapi/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /fastapi/requirements.txt

COPY . /fastapi

COPY entrypoint.sh /fastapi/entrypoint.sh
RUN chmod -R +x /fastapi
ENTRYPOINT [ "/fastapi/entrypoint.sh" ]