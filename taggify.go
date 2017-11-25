package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"os"
	"strings"

	"github.com/influxdata/influxdb/models"
	"github.com/pkg/errors"
)

const startLine = "# writing tsm data"
const stopLine = "# writing wal data"

func main() {
	from := flag.String("from", "", "file containing data in line-protocol format")
	to := flag.String("to", "", "file to output the result to (defaults to stdout if not specified)")
	flag.Parse()

	if *from == "" {
		log.Fatal("-from flag must be specified")
	}

	var in io.Reader
	var out io.Writer = os.Stdout

	if *to != "" && *to == *from {
		f, err := os.OpenFile(*from, os.O_RDWR, 0)
		if err != nil {
			log.Fatalf("Failed to open file for read/write at %s: %s", *from, err)
		}
		defer f.Close()
		in = f
		out = f
	} else {
		f, err := os.OpenFile(*from, os.O_RDONLY, 0)
		if err != nil {
			log.Fatalf("Failed to open file for read at %s: %s", *from, err)
		}
		defer f.Close()
		in = f

		if *to != "" {
			f, err := os.OpenFile(*to, os.O_WRONLY, 0)
			if err != nil {
				log.Fatalf("Failed to open file for writing at %s: %s", *to, err)
			}
			defer f.Close()
			out = f
		}
	}
	if err := taggify(in, out, flag.Args()...); err != nil {
		log.Fatalf("Failed to convert data: %s", err)
	}
}

func parseMap(s string) (map[string]string, error) {
	m := make(map[string]string)
	for _, p := range strings.Split(s, ",") {
		var n int
		for {
			i := strings.Index(p[n:], "=")
			if i == -1 {
				return nil, errors.New("wrong format, '=' not found")
			}
			// actual index in p
			n += i
			if p[n-1] != '\\' {
				m[p[:n]] = p[n+1:]
				break
			}
			n++
		}
	}
	return m, nil
}

type stringReader interface {
	ReadString(byte) (string, error)
}

type stringWriter interface {
	WriteString(string) (int, error)
}

func writeLine(w stringWriter, line string, appendNewline bool) error {
	if appendNewline && len(line) > 1 && line[len(line)-1] != '\n' {
		line += string('\n')
	}

	n, err := w.WriteString(line)
	if err != nil {
		return errors.Wrapf(err, "failed to write line '%s'", line)
	}
	if n < len(line) {
		return errors.Errorf("short write, wrote %d bytes instead of %d", n, len(line))
	}
	return nil
}

func parseLine(line string) (key string, fields map[string]string, timestamp string, err error) {
	b := []byte(line)
	// scan the first block which is measurement[,tag1=value1,tag2=value=2...]
	pos, keyBytes, err := scanKey(b, 0)
	if err != nil {
		return "", nil, "", err
	}
	// measurement name is required
	if len(keyBytes) == 0 {
		return "", nil, "", errors.New("missing measurement")
	}
	if len(keyBytes) > models.MaxKeyLength {
		return "", nil, "", errors.Errorf("max key length exceeded: %v > %v", len(key), models.MaxKeyLength)
	}

	// scan the second block which is field1=value1[,field2=value2,...]
	pos, fieldBytes, err := scanFields(b, pos)
	if err != nil {
		return "", nil, "", err
	}
	// at least one field is required
	if len(fieldBytes) == 0 {
		return "", nil, "", errors.New("missing fields")
	}
	fields, err = parseMap(string(fieldBytes))
	if err != nil {
		return "", nil, "", errors.Wrap(err, "failed to parse fields")
	}

	timestamp = string(b[pos+1:])
	if timestamp == "" {
		return "", nil, "", errors.New("missing timestamp")
	}
	return string(keyBytes), fields, timestamp, nil
}

func taggify(r io.Reader, w io.Writer, names ...string) (err error) {
	buf := bufio.NewReadWriter(bufio.NewReader(r), bufio.NewWriter(w))
	defer func() {
		if ferr := buf.Flush(); ferr != nil {
			if err == nil {
				err = ferr
				return
			} else {
				log.Printf("Failed to write data: %s", ferr)
			}
		}
	}()

	sc := bufio.NewScanner(buf)

	nextSection := false
	for sc.Scan() {
		if err := writeLine(buf, sc.Text(), true); err != nil {
			return err
		}
		if strings.HasPrefix(sc.Text(), startLine) {
			nextSection = true
			break
		}
	}
	if err = sc.Err(); err != nil {
		return errors.Wrap(err, "failed to read input")
	}
	if !nextSection {
		return errors.New("unexpected end of input while reading header section")
	}
	nextSection = false

	// measurement[,tag1=value1,tag2=value=2...] -> timestamp -> field1=value1[,field2=value2,...]
	entries := make(map[string]map[string]map[string]string)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), stopLine) {
			nextSection = true
			break
		}
		key, fields, timestamp, err := parseLine(sc.Text())
		if err != nil {
			return errors.Wrapf(err, "failed to parse line %s", sc.Text())
		}
		// by measurement+tags
		rows, ok := entries[key]
		if !ok {
			rows = make(map[string]map[string]string)
			entries[key] = rows
		}

		// by timestamp
		row, ok := rows[timestamp]
		if !ok {
			row = make(map[string]string)
			rows[timestamp] = row
		}

		for k, v := range fields {
			row[k] = v
		}
	}
	if err = sc.Err(); err != nil {
		return errors.Wrap(err, "failed to reading input")
	}
	if !nextSection {
		return errors.New("unexpected end of input while reading data section")
	}
	nextSection = false

	for key, rows := range entries {
		for timestamp, fields := range rows {
			line := key
			for _, name := range names {
				if v, ok := fields[name]; ok {
					line += "," + name + "=" + strings.Trim(v, `"'`)
					delete(fields, name)
				}
			}
			line += " "

			suffix := ""
			if timestamp != "" {
				suffix = " " + timestamp
			}
			for k, v := range fields {
				if err = writeLine(buf, line+k+"="+v+suffix, true); err != nil {
					return err
				}
			}
		}
	}

	for {
		line := sc.Text()
		hasNext := sc.Scan()
		if !hasNext {
			if err = sc.Err(); err != nil {
				return errors.Wrap(err, "failed to read input")
			}
			if err = writeLine(buf, line, false); err != nil {
				return err
			}
			return nil
		}
		if err := writeLine(buf, sc.Text(), true); err != nil {
			return err
		}
	}
}
