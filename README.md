# AIRFLOW-Dmytro-Kozak

# Clone repository

```bash
git clone git@github.com:gridu/AIRFLOW-Dmytro-Kozak.git
cd AIRFLOW-Dmytro-Kozak
```

# Create slack webhook token
[You can use this guide to create slack webhook token](https://api.slack.com/messaging/webhooks#)
You need to 
Go to Dockerfile and paste slack webhook url near `ENV slack_webhook_url=`

# Initialize database and run docker container
```bash
docker-compose up initdb
docker-compose up
```

# Airflow WebUI
## Login
password:login to account - admin:admin
## Create path_to_run variable
Go to Admin -> variables -> create new variable  
key - path_to_run  
value - /opt/airflow/files  

## Running dags
1. Unpause dags:
  - dag_id_1
  - trigger_dag
  - all dags with name smart_sensor_group_shard_*
2. Trigger trigger_dag without config
3. Go to project and create run file in files folder e.g. `touch run`
4. Wait for a message in slack with  all done text
