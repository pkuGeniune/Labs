package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//


import "time"
import "fmt"
import "sync"

func main() {
	
	var a int
	var mu sync.Mutex
	ch := make(chan int)

	go func (){
		time.Sleep(2 * time.Second)
		mu.Lock()
		a += 1
		mu.Unlock()
	}()

	go func (){
		mu.Lock()
		a += 1
		mu.Unlock()
	}()

	go func (){
		for {
			mu.Lock()
			if a == 2 {
				ch <- 0
			}
			mu.Unlock()
		}
	}()


	select {
	case <-ch :
		fmt.Println("Case a == 2")
	case <-time.After(1 * time.Second):
		fmt.Println("time out")
	}



}
