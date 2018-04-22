FROM python:3.6-stretch

# Upgrade system
RUN apt-get -yqq update
RUN apt-get -yqq upgrade

# Upgrade pip and install pipenv
RUN echo $(python --version)
RUN echo $(pip --version)
RUN python -m pip install --upgrade pip setuptools wheel
RUN echo $(pip --version)
RUN pip install --upgrade pipenv

# Create pipenv environment
RUN mkdir /app
WORKDIR /app
ADD Pipfile Pipfile
RUN pipenv install

# Copy code
ADD *.py ./
