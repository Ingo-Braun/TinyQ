echo "running tests with coverage"
go test -v -coverprofile=coverage.out ./...
	
go tool cover -html=./coverage.out -o ./coverage.html