"""Exercice 6 : weather_transformed -> HDFS.

Chaque message recu est ajoute en une ligne JSON dans le fichier de sa ville :
/hdfs-data/{country}/{city}/alerts.json  (append-only, jamais reecrit)
Lancement :  docker exec -d pyspark_notebook python work/exo6.py
Arret :      docker exec pyspark_notebook pkill -f exo6.py
"""
import glob
import json
import os
import sys

# pyspark n'est pas dans le PYTHONPATH des shells docker exec : on l'ajoute
os.environ["HADOOP_USER_NAME"] = "root"   # utilisateur HDFS de ces images
SPARK_HOME = os.environ["SPARK_HOME"]
sys.path.extend([f"{SPARK_HOME}/python",
                 glob.glob(f"{SPARK_HOME}/python/lib/py4j-*-src.zip")[0]])

from kafka import KafkaConsumer
from pyspark.sql import SparkSession

TOPIC = "weather_transformed"
BROKER = "kafka:9092"
RACINE = "/hdfs-data"

# Session locale uniquement pour le client HDFS natif (fs.append en RPC) :
# l'append WebHDFS est casse sur ce cluster mono-datanode (pipeline replacement)
spark = (SparkSession.builder.master("local[1]")
         .config("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

jvm = spark._jvm
Path = jvm.org.apache.hadoop.fs.Path
fs = jvm.org.apache.hadoop.fs.FileSystem.get(
    jvm.java.net.URI("hdfs://namenode:9000"), spark._jsc.hadoopConfiguration())

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    auto_offset_reset="earliest",   # premiere fois : rattrape tout l'historique
    group_id="exo6-hdfs",           # ensuite : reprend la ou il s'etait arrete
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

for msg in consumer:
    v = msg.value
    chemin = Path(f"{RACINE}/{v['country']}/{v['city']}/alerts.json")
    ligne = (json.dumps(v, ensure_ascii=False) + "\n").encode("utf-8")
    out = fs.append(chemin) if fs.exists(chemin) else fs.create(chemin)
    out.write(ligne)
    out.close()
    print(chemin.toString(), "<-", v["city"], v["temperature"], "C", flush=True)
