// Package tests boots the kv plugin inside an Endure container and drives it
// over the real goridge rpc codec: storage routing across the memory and boltdb
// drivers, the error a client sees for an unknown storage name, boltdb data
// surviving a container restart, and a boltdb storage configured without a gc
// interval.
package tests
