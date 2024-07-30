### CodaMetrix ML Assignment.

Install Requirements:

```buildoutcfg
pip install pyspark==3.5.0
pip install pytest==7.4.0
```

Run Pytests:

```shell
pytest
```

Build Docker and push:

```shell
source deploy.sh
```

Pull docker and run:

```shell
docker pull umesh/cdm_assignment:latest
docker run -it umesh/cdm_assignment:latest 
```






