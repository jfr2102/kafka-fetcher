# kafka-fetcher
Meant to fetch result data of example topology in: [jfr212/storm-topology](https://github.com/jfr2102/storm-topology)
Included in kafka image in [jfr212/storm-cluster](https://github.com/jfr2102/storm-cluster)
Consumes Kafka messages on topic "results" and writes them with metadata information into file.
### build:
```bash
mvn assembly:assembly
```
### run:
```bash
java -jar jarname-with-dependencies.jar
```
