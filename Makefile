
repl:
	lein repl

.PHONY: test
test:
	lein test

docker-up:
	docker run -p 8000:8000 amazon/dynamodb-local

lint:
	clj-kondo --lint .
	lein cljfmt check

lint-fix:
	lein cljfmt fix
