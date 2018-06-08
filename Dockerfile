FROM python:latest

ADD . /code
WORKDIR /code

RUN pip install .
CMD ["target-datadotworld"]
