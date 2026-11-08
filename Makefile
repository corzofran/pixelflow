.PHONY: build test race cover data demo bench clean web
build:
	go build -o bin/pixelflow ./cmd/pixelflow
	go build -o bin/genimages ./cmd/genimages
	go build -o bin/pixelflow-web ./cmd/pixelflow-web
test:
	go vet ./... && test -z "$$(gofmt -l . | grep -v ^vendor/)" && go test ./...
race:
	go test -race -count=3 ./...
cover:
	go test -coverprofile=coverage.out ./internal/... && go tool cover -func=coverage.out | tail -1
data: build
	./bin/genimages -out data/in -n 120 -w 800 -h 600
demo: data
	./bin/pixelflow -in data/in -out data/out -filtros gray,blur:2,resize:50% -workers 4
bench:
	./scripts/bench.sh 5
clean:
	rm -rf bin data coverage.out
web: build
	./bin/pixelflow-web
