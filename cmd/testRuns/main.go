package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	log2 "log"
	"os"
)

const lenWidth = 8

func mywrite(file *os.File, p []byte) (int, error) {

	bufWrite := bufio.NewWriter(file)

	enc := binary.BigEndian

	err := binary.Write(bufWrite, enc, uint64(len(p)))

	if err != nil {
		log2.Fatal("ble", err)
	}
	if _, err := bufWrite.Write(p); err != nil {
		log2.Fatal("a", err)

	}

	if err := bufWrite.Flush(); err != nil {
		log2.Fatal("failed to flush", err)
	}
	amount := lenWidth + len(p)

	return amount, err
}
func main() {

	fmt.Println(os.Getwd())
	fi, err := os.OpenFile("./cmd/testRuns/empty.txt", os.O_RDWR, 0644)
	if err != nil {
		log2.Fatal("Oh no", err)

	}
	defer fi.Close()
	//amount1, err := mywrite(fi, []byte{0x41, 0x42, 0x43})
	//
	//fmt.Println(amount1)
	//
	//amount2, err := mywrite(fi, []byte{0x51, 0x52, 0x53})
	//
	//fmt.Println(amount2)
	//

	size := make([]byte, lenWidth)

	n, err := fi.ReadAt(size, 11)

	fmt.Println(n)
	fmt.Println(size)
	fmt.Println(binary.BigEndian.Uint64(size))
	b := make([]byte, binary.BigEndian.Uint64(size))

	n2, err := fi.ReadAt(b, 11+lenWidth)
	fmt.Println(n2)
	fmt.Println(b)
}
