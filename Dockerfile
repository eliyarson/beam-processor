FROM apache/beam_python3.10_sdk:2.48.0

COPY . .
RUN pip install -r requirements.txt

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]
