CASSANDRA_VERSION ?= 3.7

.PHONY: clean test codestyle install-cassandra start-cassandra stop-cassandra  

clean:
	find bndl_cassandra -name '*.pyc' -exec rm -f {} +
	find bndl_cassandra -name '*.pyo' -exec rm -f {} +
	find bndl_cassandra -name '*.c' -exec rm -f {} +
	find bndl_cassandra -name '*.so' -exec rm -f {} +
	find bndl_cassandra -name '*~' -exec rm -f {} +
	find bndl_cassandra -name '__pycache__' -exec rm -rf {} +
	rm -rf build
	rm -rf dist
	rm -rf .coverage .coverage.* htmlcov


test:
	rm -fr .coverage .coverage.* htmlcov
	BNDL_SUPERVISOR_ONSIGTERM=raise_exit \
	COVERAGE_PROCESS_START=.coveragerc \
	coverage run -m pytest bndl_cassandra
	coverage combine
	coverage html

codestyle:
	pylint bndl_cassandra > build/pylint.html
	flake8 bndl_cassandra > build/flake8.txt


install-cassandra:
	pip install ccm
	ccm list | grep bndl_test || ccm create bndl_test -v binary:$(CASSANDRA_VERSION) -n 1

start-cassandra: install-cassandra
	ccm switch bndl_test
	ccm start --wait-for-binary-proto

stop-cassandra:
	ccm stop || :
	ccm remove || :
