package main

import (
	"fmt"
	"os"
	"slices"

	"github.com/tidwall/btree"
)

func assert(b bool, msg string) {
	if !b {
		panic(msg)
	}
}

func assertEq[C comparable](a C, b C, prefix string) {
	if a != b {
		panic(fmt.Sprintf("%s '%v' != '%v'", prefix, a, b))
	}
}

var DEBUG = slices.Contains(os.Args, "--debug")

func debug(a ...any) {
	if !DEBUG {
		return
	}

	args := append([]any{"[DEBUG]"}, a...)
	fmt.Println(args...)
}

type Value struct {
	txStartId uint64
	txEndId   uint64
	value     string
}

type TransactionState uint8

const (
	InProgressTransaction TransactionState = iota
	AbortedTransaction
	CommittedTransaction
)

// Loosest isolation at the top, strictiest isolation at the bottom
type IsolationLevel uint8

const (
	ReadUncommitedIsolation IsolationLevel = iota
	ReadCommitedIsolation
	RepeatableReadIsolation
	SnapshotIsolation
	SerializableIsolation
)

/*
We'll get into detail abou the meaning of the levels later.
A transaction has an isolation level, an id (monotonic increasing integer) and a
current state. And although we won't make use of this data yet, transactions
at stricter isolation levels will need some extra info. Specifically, stricer
isolation levels need to know about other transactions that were in-progress when
this one started. And stricter isolation
levels need to know about all keys read and written by a transaction
*/
type Transaction struct {
	isolation IsolationLevel
	id        uint64
	state     TransactionState

	// Used only by Repeatable Read and stricter
	inprogress btree.Set[uint64]

	// Used only by Snapshot Isolation and stricter.
	writerset btree.Set[string]
	readset   btree.Set[string]
}

/*
We'll discuss why later
Finally, the database itself will have a default isolation level that each
transaction will inherit (for our own convenicen in tests).
The database will have a mapping of keys to an array of value.

The database will also store the next free transaction id it will use to assign
ids to new transactions.
*/
type Database struct {
	defaultIsolation  IsolationLevel
	store             map[string][]Value
	transactions      btree.Map[uint64, Transaction]
	nextTransactionId uint64
}

func newDatabase() Database {
	return Database{
		defaultIsolation: ReadCommitedIsolation,
		store:            map[string][]Value{},
		// The `0` transaction id will be used to mean
		// that the id was not set. So all valid transaction ids
		// must start at 1.
		nextTransactionId: 1,
	}
}

/*
To be thread-safe, store, transactions, and nextTransactionId should be guarded
by a mutex. But to keep the code small, this post will not use goroutines and
thus does not need mutexts
*/

/*
There's abit of book-keeping when creating a transaction, so we'll make a dedicted
method for this. We must give the new transation id, store all in-progress
transactions, and add it to the database transaction history.
*/

func (d *Database) inprogress() btree.Set[uint64] {
	var ids btree.Set[uint64]
	iter := d.transactions.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if iter.Value().state == InProgressTransaction {
			ids.Insert(iter.Key())
		}
	}
	return ids
}

func (d *Database) newTransaction() *Transaction {
	t := Transaction{}
	t.isolation = d.defaultIsolation
	t.state = InProgressTransaction

	// Assign and increment transaction id.
	t.id = d.nextTransactionId
	d.nextTransactionId++

	// Store all inprogress transaction ids
	t.inprogress = d.inprogress()

	// Add this transaction to history
	d.transactions.Set(t.id, t)

	debug("starting transaction", t.id)

	return &t
}

/*
And we'll add a few more helpers for completing a transaction, for
fetching a transaction by id, and for validating a transaction
*/

func (d *Database) completeTransaction(t *Transaction, state TransactionState) error {
	debug("completing transactions ", t.id)

	//Update transactions
	t.state = state
	d.transactions.Set(t.id, *t)

	return nil
}

func (d *Database) transactionState(txId uint64) Transaction {
	t, ok := d.transactions.Get(txId)
	assert(ok, "valid transaction")
	return t
}

func (d *Database) assertValidTransaction(t *Transaction) {
	assert(t.id > 0, "valid id")
	assert(d.transactionState(t.id).state == InProgressTransaction, "in progress")
}

/*
The final bit of scaffolding we'll set up is an abstraction for database connection. A
A connection will have at most assocated one transaction. Users must ask the
database for a new connection. Then within the connection thye can manage a
transaction.
*/

type Connection struct {
	tx *Transaction
	db *Database
}

func (c *Connection) execCommand(command string, args []string) (string, error) {
	debug(command, args)

	/*
		When a user asks to begin a transaction, we ask the db for a new
		transaction and assign it to the current connection
	*/
	if command == "begin" {
		assertEq(c.tx, nil, "no running transactions")
		c.tx = c.db.newTransaction()
		c.db.assertValidTransaction(c.tx)
		return fmt.Sprintf("%d", c.tx.id), nil
	}

	/*
		To abort a transaction, we call the completTransaction method
		(which makes sure the database transaction history gets updated)
		with the AbortedTransaction state
	*/
	if command == "abort" {
		c.db.assertValidTransaction(c.tx)
		err := c.db.completeTransaction(c.tx, AbortedTransaction)
		c.tx = nil
		return "", err
	}

	/* commit a transaction */
	if command == "commit" {
		c.db.assertValidTransaction(c.tx)
		err := c.db.completeTransaction(c.tx, CommittedTransaction)
		c.tx = nil
		return "", err
	}

	//TODO:
	return "", fmt.Errorf("unimplemented")
}

func (c Connection) mustExecCommand(cmd string, args []string) string {
	res, err := c.execCommand(cmd, args)
	assertEq(err, nil, "unexpected error")
	return res
}

func (d *Database) newConnection() *Connection {
	return &Connection{
		db: d,
		tx: nil,
	}
}

func main() {
	panic("unimplemented")
}
