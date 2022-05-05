FROM python:3.7.6

#test-market
WORKDIR /market-test-stock-data
# WORKDIR /demo-check

COPY requirements.txt .

ADD main.py .

RUN pip install -r requirements.txt

COPY ./app ./app
EXPOSE 9004
HEALTHCHECK --interval=5s --timeout=3s --retries=3 CMD curl -f / https://thormarketdata.extrapreneursindia.com/stock-test:9004 || exit 1
CMD ["python" , "./main.py"]