build:
	go build cmd/kostanza/main.go

run: build
	./main collect \
		--kubeconfig=$(HOME)/.kube/config \
		-v \
		--interval 5s \
		--config .private/config.json \
		--pubsub-flush-interval 300s \
		--pubsub-project planet-k8s-staging \
		--pubsub-topic kostanza

aggregate: build
	./main aggregate \
		-v \
		--config .private/config.json \
		--listen-addr :5050 \
		--pubsub-project planet-k8s-staging \
		--pubsub-subscription kostanza \
		--pubsub-topic kostanza \
		--bigquery-project planet-k8s-staging \
		--bigquery-table kostanza_12_2018 \
		--bigquery-dataset kostanza_12_2018

test:
	go test -v ./...

lint:
	gometalinter --fast --vendored-linters --vendor ./... --deadline 5m

container:
	docker build -t us.gcr.io/planet-gcr/kostanza:$(shell git rev-parse --short HEAD) .
	docker push us.gcr.io/planet-gcr/kostanza:$(shell git rev-parse --short HEAD)
