
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

release:
	lein release


toc-install:
	npm install --save markdown-toc

toc-build:
	node_modules/.bin/markdown-toc -i README.md
