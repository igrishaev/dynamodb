
repl:
	lein repl

.PHONY: test
test:
	lein test

docker-up:
	docker run -p 8000:8000 amazon/dynamodb-local
