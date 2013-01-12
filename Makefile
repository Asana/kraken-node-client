MOCHA = ./node_modules/.bin/mocha

integration:
	$(MOCHA) --timeout 2000 --reporter list test/integration/*

.PHONY: integration
