up: 
	docker compose up --build -d

format: 
	docker exec glue_aws python -m black -S --line-length 79 .

isort:
	docker exec glue_aws isort .


type:
	docker exec glue_aws mypy --ignore-missing-imports .


lint: 
	docker exec glue_aws flake8 .


ci: isort format type lint 


infra-init: 
	docker compose -f docker-compose.yaml run --rm terraform -chdir=./terraform init

infra-apply: 
	docker compose -f docker-compose.yaml run --rm terraform -chdir=./terraform apply --auto-approve


down: 
	docker compose down