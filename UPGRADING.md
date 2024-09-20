# Upgrading

## v0.15 -> v1.0

There's now a `goleveldb` build flag that must be used when using goleveldb. If
you're using `pebbledb`, you don't need a build flag anymore.

```sh
go build -tags goleveldb
```
