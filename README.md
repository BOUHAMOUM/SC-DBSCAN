# SC-DBSCAN

## Compilation
Ce projet est compilable avec maven dans un environnement Java 8.
```bash
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/"
mvn package
```

## Génération d'un jar incluant les dépendances
```bash
mvn clean package assembly:single
```
