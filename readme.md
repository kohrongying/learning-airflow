# Learning Airflow

Install airflow first

# Run airflow locally
```
airflow standalone
```

# Unpause dag and trigger
1) Use UI @ localhost:8080 
2) Use airflow CLI
```
airflow cheat-sheet
airflow dags unpause <dag id>
airflow dags trigger <dag id>
airflow tasks test <dag id> <task id>

```

# Tips
Remove local `airflow.db` to reset database.
`airflow.cfg` Executor -- Sequential. May affect dags running.

# Resources
[Operator Documentation](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/index.html)