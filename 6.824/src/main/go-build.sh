go build -race -buildmode=plugin ../mrapps/wc.go
sudo go run -race mrworker.go wc.so