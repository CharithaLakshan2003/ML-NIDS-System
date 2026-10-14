# Running on 3 Separate Laptops

Each laptop runs one component. All must be on same LAN and reach each other.

## 1) Laptop A - kafka-process (Kafka + storage + backend)
Hosts Kafka (29092), Kafka UI (8080), MinIO, OpenSearch, Backend. Other laptops connect to Kafka here.

Update kafka-process/docker-compose.yml Kafka service - change advertised listener to Laptop A's IP:

`yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://192.168.1.100:29092
`

Replace 192.168.1.100 with your actual LAN IP of Laptop A.

Start:
`powershell
cd kafka-process
.\start-kafka-process.ps1
`

## 2) Laptop B - ml-engine (Flink jobs, NO local Kafka)
You should run ml-engine without its own Kafka. Options:
- Edit ml-engine/docker-compose.yml: remove kafka and kafka-ui services. Remove depends_on: kafka from jobmanager (or adjust). Keep only jobmanager/taskmanager.
- Update Kafka bootstrap for jobs. Jobs read from env KAFKA_BOOTSTRAP (see ml-engine/flink_jobs/common.py). Set env var in docker-compose for jobmanager/taskmanager or export.

In ml-engine/docker-compose.yml, under jobmanager/taskmanager environment, add:
`yaml
environment:
  - KAFKA_BOOTSTRAP=192.168.1.100:29092
`

Also start-ml-engine.ps1 creates topics on local ml-kafka and waits for it - skip if external. Easier: modify script to not require local Kafka. Or just run docker compose up for jobmanager/taskmanager after editing.

After edits, start:
`powershell
cd ml-engine
docker compose up -d --build
# then submit jobs manually or via adjusted script
docker compose exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/feature_engineering_job.py
docker compose exec jobmanager flink run -d -pyFiles /opt/flink/usrlib/flink_jobs/common.py -py /opt/flink/usrlib/flink_jobs/fusion_job.py
`

## 3) Laptop C - suricata-process (ingestion)
Produces to Kafka on Laptop A. Update configs to point to Laptop A IP.

- suricata-process/producers/cicflowmeter_producer.py: change default --bootstrap-servers from localhost:29092 to 192.168.1.100:29092 (or pass arg). Also start-suricata-process.ps1 passes localhost:29092 - update it.
- suricata-process/filebeat/filebeat.yml: change hosts: ["localhost:29092"] to hosts: ["192.168.1.100:29092"]

Update start-suricata-process.ps1 producer line to use Laptop A IP:
`powershell
python producers/cicflowmeter_producer.py --watch-dir '' --bootstrap-servers '192.168.1.100:29092'
`

Also set correct Suricata interface IP on Laptop C.

Start:
`powershell
cd suricata-process
.\start-suricata-process.ps1
`

## Notes
- Replace 192.168.1.100 with actual Laptop A LAN IP.
- Ensure ports open (29092, 8080, 8081, 8000, 9400, 9500 etc as needed).
- ml-engine's original stack runs its own Kafka - disable it when pointing externally.
- Topics are created on Laptop A (kafka-process).
