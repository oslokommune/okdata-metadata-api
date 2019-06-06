.PHONY: init
init:
	python3 -m pip install tox pip-tools
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


.PHONY: update-ssm
update-ssm:
	url=$$(sls info -s $$STAGE --verbose | grep ServiceEndpoint | cut -d' ' -f2) &&\
	aws --region eu-west-1 ssm put-parameter --overwrite \
	--cli-input-json "{\"Type\": \"String\", \"Name\": \"/dataplatform/metadata-api/url\", \"Value\": \"$$url\"}"
