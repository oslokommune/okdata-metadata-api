.PHONY: init
init:
	python3 -m pip install tox pip-tools yq
	pip-compile

.PHONY: test
test: init
	python3 -m tox -p auto

.PHONY: deploy
deploy: init test
	sls deploy


.PHONY: deploy-prod
deploy-prod: init test
	sls deploy --stage prod


.PHONY: put-parameter
put-parameter: init
	url=$$(sls info --verbose | grep -Ev "Stack Outputs|Service Information" | yq .ServiceEndpoint) &&\
	aws --region eu-west-1 ssm put-parameter \
	--name "/dataplatform/metadata-api/url" \
	--value $$url \
	--type "String" \
	--overwrite

