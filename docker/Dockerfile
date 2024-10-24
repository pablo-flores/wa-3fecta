#FROM python:3.9-slim
FROM registry.access.redhat.com/ubi9/python-39:latest
#FROM registry.access.redhat.com/ubi9/python-39:1-117.1684741281

#WORKDIR /app
WORKDIR /projects


# Copy the content of the local src directory to the working directory
COPY . .

# Install any dependencies
RUN \
  if [ -f requirements.txt ]; \
    then pip install -r requirements.txt; \
  elif [ `ls -1q *.txt | wc -l` == 1 ]; \
    then pip install -r *.txt; \
  fi

#COPY requirements.txt ./
#RUN pip install -r requirements.txt

COPY . .

EXPOSE 8081/tcp

#CMD ["gunicorn", "--bind", "0.0.0.0:8081", "app:app"]
# Variables de entorno recomendadas para OCP
ENV PYTHONUNBUFFERED=1 \
    FLASK_ENV=production \
    PORT=8081 \
    FLASK_PORT=8081


# Comando para ejecutar waitress y servir la aplicación Flask
#CMD ["waitress-serve", "--port=8081", "--call", "app:create_app"]
#CMD ["waitress-serve", "--listen=0.0.0.0:8081", "app:app"]

# Specify the command to run on container start
CMD [ "python", "./wa_cierra_enmascarados.py" ]
