package schema

// NOTE: assert that the included data and generated data are the same.
//
// Update bindata.go using:
//   go get -u github.com/go-bindata/go-bindata
//   go-bindata -pkg schema -nometadata -prefix descriptions descriptions
//
//go:generate go-bindata -pkg schema -nometadata -prefix descriptions -o /tmp/current-bindata.go descriptions
//go:generate diff -q bindata.go /tmp/current-bindata.go
