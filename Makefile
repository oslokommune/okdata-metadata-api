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

