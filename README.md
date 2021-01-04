# Big-Data-Projekt-2

### Utworzenie tabel hurtowni:
```
spark-shell -i hurtownia.scala
```

### Załadowanie danych do tabeli pogoda (plik pogoda-etl.jar)
```
spark-submit --class com.example.bigdata.PogodaETL \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 pogoda-etl.jar project/uk-trafic
(zamiast project/uk-trafic podać ścieżkę do katalogu w HDFS, w którym znajdują się pliki z danymi)
```
