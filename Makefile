test:
	go test ./...

test_single:
	go test -run $(name) ./... -v
