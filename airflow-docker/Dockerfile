FROM apache/airflow:2.8.4

# Copy the files json
COPY files/students.json /students.json
COPY files/missed_days.json /missed_days.json

# Copy the requirements files and installs it
COPY files/requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt