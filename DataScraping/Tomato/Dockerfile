FROM umihico/aws-lambda-selenium-python:latest

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY main.py ./
CMD [ "main.lambda_handler" ]
