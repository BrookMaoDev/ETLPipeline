package main

import (
	"fmt"
	"log"

	"github.com/BrookMaoDev/ETLPipeline/internal/extract"
)

func main() {
	cik := "0000320193" // Example CIK for Apple Inc.

	info, err := extract.ExtractCompanyInfo(cik)
	if err != nil {
		log.Fatalf("Failed to extract company info: %v", err)
	}

	fmt.Println(string(info))
}
