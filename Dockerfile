FROM python:3.11

WORKDIR /db_tests
COPY . .

RUN pip install -U pip && pip install -r test-requirements.txt --no-cache-dir

ENV PYTHONPATH=.

ENTRYPOINT pytest --ip ${POSTGRES_CONTAINER_NAME} \
                  --port ${POSTGRES_PORT} \
                  --database ${POSTGRES_DB} \
                  --username ${POSTGRES_USER} \
                  --password ${POSTGRES_PASSWORD} \
                  --alluredir=WORKDIR/allure-results
