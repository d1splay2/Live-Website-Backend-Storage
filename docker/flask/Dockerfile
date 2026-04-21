FROM python:3.15-rc-alpine

RUN mkdir -p /opt/flask

WORKDIR /opt/flask

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD [ "python", "app.py" ]
