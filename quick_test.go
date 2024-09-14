package bolt_test

import (
	"fmt"
	"log"
	"time"

	"github.com/NandVinchhi/bolt"
)

func init() {
	fmt.Println("Starting insertion of 1M keys...")
	db, err := bolt.Open("test.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	start := time.Now()

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("testbucket"))
		if err != nil {
			return err
		}

		for i := 0; i < 1000000; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d", i))
			err = b.Put(key, value)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	fmt.Printf("Insertion of 1M keys took %s\n", elapsed)
}
