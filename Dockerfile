FROM python:3.14

WORKDIR /app

# Installer afhængigheder først, så Docker bedre kan genbruge cache
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Kopiér resten af programmet
COPY overkiz2mqtt.py .

# Kør programmet
CMD ["python", "overkiz2mqtt.py"]
