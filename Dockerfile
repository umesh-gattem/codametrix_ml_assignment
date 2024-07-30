FROM spark

WORKDIR /opt/umesh/

# copy py files 
COPY test_cases.py /opt/umesh/

# necessary to install dependencies using pip and use apt-get install
USER root

RUN apt-get --assume-yes --no-install-recommends install python3

RUN pip install pyspark==3.5.0 && pip install pytest==7.4.0


# Set the working directory
WORKDIR /opt/umesh

ENTRYPOINT pytest