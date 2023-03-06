FROM python:3.11

WORKDIR /db_tests
COPY . .

RUN pip install -U pip && pip install -r test-requirements.txt --no-cache-dir

ENV PYTHONPATH=.

ENTRYPOINT ["pytest", "-k", "test_create_table", "-s"]