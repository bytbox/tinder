package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"code.google.com/p/gosqlite/sqlite"
	"github.com/bytbox/format.go/format"
)

const (
	RCHAN_BUF_LEN = 1024
)

var (
	initialize = flag.Bool("init", false, "Initialize the db")
	dbFile = flag.String("db", "tinder.db", "Database file")
	configFile = flag.String("config", "tinder.cfg", "Configuration file (unimplemented)")

	logFile = flag.String("log", "", "Log file (incompatible with -config)")
	logFmt = flag.String("fmt", "[${datetime}] ${msg}", "Log format (use with -log)")
	dateLayout = flag.String("date", "2006-01-02 15:04", "Layout of the datetime string")
	compact = flag.Bool("compact", false, "Record less data")
	relax = flag.Bool("relax", false, "Don't warn on parse errors")
)

var (
	db *sqlite.Conn
)

func main() {
	flag.Parse()

	defer func() {
		err := recover()
		if err != nil {
			switch e := err.(type) {
			case string:
				log.Fatalf(e)
			case error:
				log.Fatalf(e.Error())
			}
		}
	}()

	openDB()
	defer closeDB()

	if len(*logFile) > 0 {
		readLog(*logFile, *logFmt)
	} else {
		log.Printf("Config file not yet implemented - taking no additional action")
	}
}

func handlePanic(err error) {
	if err != nil {
		panic(err)
	}
}

func logName(fname string) string {
	return fname
}

func getLog(lname, fname string) (id int) {
	findStmt, err := db.Prepare(`SELECT log_id FROM logs WHERE log_name=?`)
	handlePanic(err)
	err = findStmt.Exec(lname)
	handlePanic(err)
	if findStmt.Next() {
		err = findStmt.Scan(&id)
		handlePanic(err)
		log.Printf(`Using log "%s" (%d)`, lname, id)
		err = findStmt.Finalize()
		if err != nil {
			log.Printf(`WARN: Finalize(): %s`, err.Error())
		}
		return
	}
	handlePanic(findStmt.Error())
	err = db.Exec(`INSERT INTO logs (log_name, filename) VALUES (?, ?)`, lname, fname)
	handlePanic(err)
	err = findStmt.Reset()
	handlePanic(err)
	err = findStmt.Exec(lname)
	handlePanic(err)
	if findStmt.Next() {
		err = findStmt.Scan(&id)
		handlePanic(err)
		log.Printf(`Created log "%s" (%d)`, lname, id)
		err = findStmt.Finalize()
		if err != nil {
			log.Printf(`WARN: Finalize(): %s`, err.Error())
		}
		return
	}
	handlePanic(findStmt.Error())
	panic(`INSERT failed`)
}

func readLog(fname, logFmt string) {
	err := db.Exec(`BEGIN EXCLUSIVE TRANSACTION`)
	handlePanic(err)
	success := false
	defer func() {
		if !success {
			log.Printf(`Something went wrong: rolling back transaction`)
			err := db.Exec(`ROLLBACK TRANSACTION`)
			if err != nil {
				log.Printf(`WARN: ROLLBACK failed: %s`, err.Error())
			}
		}
	}()

	lname := logName(fname)
	log_id := getLog(lname, fname)

	log.Printf(`Reading log file: "%s"`, fname)
	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}

	r := bufio.NewReader(f)
	lines := make(chan string, RCHAN_BUF_LEN)
	go func() {
		defer close(lines)
		line := ""
		l, isPrefix, err := r.ReadLine()
		for err == nil {
			line += string(l)
			if !isPrefix {
				lines <- line
				line = ""
			}
			l, isPrefix, err = r.ReadLine()
		}
		if err != io.EOF {
			panic(err)
		}
	}()

	for line := range lines {
		addLine(log_id, logFmt, line)
	}
	success = true
	err = db.Exec(`COMMIT TRANSACTION`)
	handlePanic(err)
}

func entryId(log_id int, line string) string {
	// generate eight random bytes
	rbs := make([]byte, 8)
	for i := 0; i < 2; i++ {
		rbs[i] = byte(rand.Int())
	}
	h := md5.New()
	h.Write(rbs)
	h.Write([]byte(strconv.Itoa(log_id)))
	h.Write([]byte(line))
	var o []byte
	o = h.Sum(o)
	return hex.EncodeToString(o)
}

func addLine(log_id int, logFmt, line string) {
	data := map[string]string{}
	err := format.Read(logFmt, line, &data)
	if err != nil {
		if !*relax {
			log.Printf(`error reading line: "%s"`, line)
		}
		return
	}
	entry_id := entryId(log_id, line)
	dt, ok := data["datetime"]
	date := time.Now()
	if ok {
		date, err = time.Parse(*dateLayout, dt)
		if err != nil {
			log.Printf(`error parsing date: "%s"`, date)
		}
	}
	entry_full := line
	if *compact {
		entry_full = ""
	}
	err = db.Exec(`INSERT INTO entries (log_id, entry_id, entry_full, entry_time) VALUES (?, ?, ?, ?)`, log_id, entry_id, entry_full, date.Unix())
	handlePanic(err)
	for k, v := range data {
		err := db.Exec(`INSERT INTO strings (column_name, entry_id, string_value) VALUES (?, ?, ?)`, k, entry_id, v)
		handlePanic(err)
	}
}

var initStmts = []string{
	`BEGIN EXCLUSIVE TRANSACTION`,

	`CREATE TABLE logs (log_id INTEGER PRIMARY KEY AUTOINCREMENT, log_name STRING, filename STRING)`,

	`CREATE TABLE entries (log_id INTEGER, entry_id STRING PRIMARY KEY, entry_full STRING, entry_time INTEGER)`,
	`CREATE INDEX entries_log_id ON entries (log_id)`,
	`CREATE INDEX entries_entry_time ON entries (entry_time)`,

	`CREATE TABLE stats (column_name STRING, entry_id INTEGER, stat_value INTEGER, UNIQUE (column_name, entry_id))`,
	`CREATE INDEX stats_column_name ON stats (column_name)`,
	`CREATE INDEX stats_entry_id ON stats (entry_id)`,

	`CREATE TABLE strings (column_name STRING, entry_id INTEGER, string_value STRING, UNIQUE (column_name, entry_id))`,
	`CREATE INDEX strings_column_name ON strings (column_name)`,
	`CREATE INDEX strings_entry_id ON strings (entry_id)`,

	`COMMIT TRANSACTION`,
}

func openDB() {
	if *initialize {
		// Delete the old database file
		log.Printf(`Removing old db file`)
		err := os.Remove(*dbFile)
		if err != nil && !os.IsNotExist(err) {
			panic(err)
		}
	}
	log.Printf(`Opening sqlite (%s) db file: "%s"`, sqlite.Version(), *dbFile)
	var err error
	db, err = sqlite.Open(*dbFile)
	if err != nil {
		panic(err)
	}
	if *initialize {
		// Initialize database structure
		log.Printf(`Initializing database`)
		for _, stmt := range initStmts {
			err := db.Exec(stmt)
			if err != nil {
				panic(err)
			}
		}
	}
}

func closeDB() {
	log.Printf(`Closing db`)
	err := db.Close()
	if err != nil {
		log.Printf(`ERR: Close(): %s`, err.Error())
	}
}

