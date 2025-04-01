package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	log2 "log"
	"math"
	"os"
	"strings"
)

const lenWidth = 8

func maxVal(bitAmount int) int {
	res := float64(0)
	for i := bitAmount - 1; i >= 0; i-- {
		res += math.Pow(2, float64(i))
	}
	return int(res)

}
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

	//fmt.Println(os.Getwd())
	//fi, err := os.OpenFile("./cmd/testRuns/empty.txt", os.O_RDWR, 0644)
	//if err != nil {
	//	log2.Fatal("Oh no", err)
	//
	//}
	//defer fi.Close()
	////amount1, err := mywrite(fi, []byte{0x41, 0x42, 0x43})
	////
	////fmt.Println(amount1)
	////
	////amount2, err := mywrite(fi, []byte{0x51, 0x52, 0x53})
	////
	////fmt.Println(amount2)
	////
	//
	//size := make([]byte, lenWidth)
	//
	//n, err := fi.ReadAt(size, 11)
	//
	//fmt.Println(n)
	//fmt.Println(size)
	//fmt.Println(binary.BigEndian.Uint64(size))
	//b := make([]byte, binary.BigEndian.Uint64(size))
	//
	//n2, err := fi.ReadAt(b, 11+lenWidth)
	//fmt.Println(n2)
	//fmt.Println(b)

	//enc := binary.BigEndian

	brr := make([]byte, 20)

	fmt.Println("brr", brr)

	fmt.Println("brr", brr)

	fmt.Printf("%-5s %-10s %-30s\n", "Length", "Decimal", "Binary")
	fmt.Println(strings.Repeat("-", 35))
	fmt.Printf("%-5[1]d %-10[2]v %-20[2]b\n", 8, maxVal(8))
	fmt.Printf("%-5[1]d %-10[2]v %-20[2]b\n", 16, maxVal(16))
	fmt.Printf("%-5[1]d %-10[2]v %-20[2]b\n", 32, maxVal(32))
	fmt.Print(uint16(65535))

	//fmt.Printf("%v   %d", maxVal(32), "\n")
	fmt.Println()

	f, err := os.OpenFile("./cmd/testRuns/empty2.txt", os.O_RDWR, 0644)
	if err != nil {
		log2.Fatal("Oh no")
	}

	//fmt.Println(f)
	fmt.Println(f.Name())

	defer fmt.Println(os.O_RDWR)

}
