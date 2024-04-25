# Rental-Price-Prediction
## Setting up

1. Create a `.env` file according to `.env.example` and fill up the variables

2. Put the service account key `.json` file in the `auth` folder

## Running

1. Make sure Docker Desktop is running

2. Start all the services
```bash
docker compose up
```

3. Access the Airflow UI:

Visit `localhost:8080` in your browser and log in with the login `airflow` and the password `airflow`. Filter DAGs by tag `project` and enable the resultant DAGs displayed.