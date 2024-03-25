# Airflow-Docker
## Setting up
Refer to this page for a more detailed explanation: [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html). This part is done in a Linux environment.

1. Fetch the docker-compose.yaml file for Airflow 2.8.1
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.3/docker-compose.yaml'
```

2. Initialize the environment with the docker-compose.yaml file
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

3. Make sure docker compose is installed
```bash
docker compose version 
```

4. Make sure docker daemon is running
```bash
docker info
```

3. Start the Airflow environment
```bash
docker compose up airflow-init
```

4. Start all the services
```bash
docker compose up
```

## Adding dependencies via requirements.txt file
1. Comment out the `image: ...` line and remove comment from the `build: .` line in the `docker-compose.yaml` file. The relevant part of the docker-compose file of yours should look similar to (use correct image tag):
```yaml
#image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.8.3}
build: .
```

2. Create `Dockerfile` in the same folder your `docker-compose.yaml` file is with content similar to:
```Dockerfile
FROM apache/airflow:2.8.3
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
```

3. Place `requirements.txt` file in the same directory.
Run `docker compose build` to build the image, or add `--build` flag to `docker compose up` or `docker compose run` commands to build the image automatically as needed.

## Modifications to support Kaggle API
1. Provide your Kaggle username and token in the `.env` file. Refer to the `.env.example` file as a guide.
```bash
KAGGLE_USERNAME=xxxxxxxxxxxxxx
KAGGLE_KEY=xxxxxxxxxxxxxx
```

2. Add the following lines under the `x-airflow-common:` and `environment:` section to the `docker-compose.yaml` file to export your Kaggle username and token to the environment:
```yaml
x-airflow-common:
  ...
  environment:
    ...
    KAGGLE_USERNAME: ${KAGGLE_USERNAME}
    KAGGLE_KEY: ${KAGGLE_KEY}
```

## Set up to run in VS Code Dev Container
The purpose of Dev Container is to allow for auto-completion when writing code. Refer to [this video](https://youtu.be/fsMKV9A1B-I?si=5Eqf3j0CVUZbT6fD) for more details.

1. Add VS Code Extension "Dev Containers" to your VS Code.

2. Click on the blue "><" button on the bottom right of your VS Code window.

3. Select "Reopen in Container".

4. Select "From 'Dockerfile'".

5. Select "OK".

6. This generates the `.devcontainer/devcontainer.json`. Inside this file, you can add customizations like VS Code Extensions to run in your Dev Container. I added:
```json
{
    ...
    "customizations": {
		"vscode": {
			"extensions": [
				"ms-azuretools.vscode-docker",
				"ms-python.python",
				"ms-python.vscode-pylance"
			]
		}
	}
}
```
You have to rebuild the container for the changes to take effect. A pop-up should appear to prompt you to do this, but if not you can click on the blue "><" button on the bottom right of your VS Code window for the option "Rebuild Container".

Click on the blue "><" button on the bottom right of your VS Code window for options to "Reopen in Container" and "Reopen Folder Locally" to switch between local environment and Dev Container environment.