package main

import (
	"testing"
)

func TestReadUncommited(t *testing.T) {
	database := newDatabase()
	database.defaultIsolation = ReadUncommitedIsolation

	c1 := database.newConnection()
	c1.mustExecCommand("begin", nil)

	c2 := database.newConnection()
	c2.mustExecCommand("begin", nil)

	c1.mustExecCommand("set", []string{"x", "hey"})

	// update is visible to self.
	res := c1.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c1 get x")

	// But since read uncommitted, also available to everyone else.
	res = c2.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")

	// And if we delete, that should be respected.
	res = c1.mustExecCommand("delete", []string{"x"})
	assertEq(res, "", "c1 delete x")

	res, err := c1.execCommand("get", []string{"x"})
	assertEq(res, "", "c1 sees no x")
	assertEq(err.Error(), "cannot get key that does not exist", "c1 sees no x")

	res, err := c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 sees no x")
	assertEq(err.Error(), "cannot get key that does not exist", "c2 sees no x")
}
