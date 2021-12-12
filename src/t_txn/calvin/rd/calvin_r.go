package rd

import (
	"t_txn/calvin/utils/qlock"
	// "t_log"
)

// each record has a lock list
type Record struct {
	qlock *qlock.QueueLock
}

func NewRecord() *Record {
	ql := qlock.NewQueueLock()
	return &Record{ql}
}


/*
calvin first phase
*/
func (r *Record) Reserve(txn_id int, is_write bool) {
	r.qlock.AddLock(txn_id, is_write)
	// t_log.Log(t_log.DEBUG, "%v\n", r.LockListString())
}


/*
calvin second phase
unsync write so not txn_id
routine can imporve this since routine can not suffer dead lock
*/
func (r *Record) Write(txn_id int) bool {
	return r.qlock.WriteTicket(txn_id)
}

func (r *Record) Read(txn_id int) bool {
	return r.qlock.ReadTicket(txn_id)
}

func (r *Record) LockListString() string {
	return r.qlock.LockListString()
}