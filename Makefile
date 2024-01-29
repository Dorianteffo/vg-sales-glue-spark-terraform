infra-init: 
	terraform -chdir=./terraform init

infra-up: 
	terraform -chdir=./terraform apply --auto-approve

infra-down: 
	terraform -chdir=./terraform destroy --auto-approve