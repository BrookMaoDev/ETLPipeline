package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/BrookMaoDev/ETLPipeline/internal/extract"
	"github.com/BrookMaoDev/ETLPipeline/internal/storage"
)

// validateCIK checks if the CIK is exactly 10 digits long.
func validateCIK(cik string) bool {
	match, _ := regexp.MatchString(`^\d{10}$`, cik)
	return match
}

func main() {
	// Define the CIK flag
	cik := flag.String("cik", "", "CIK of the company (must be 10 digits)")
	flag.Parse()

	// Check if CIK is provided and valid
	if *cik == "" || !validateCIK(*cik) {
		fmt.Println("Error: Please provide a valid 10-digit CIK using -cik=<CIK>.")
		os.Exit(1)
	}

	// Extract company information
	info, err := extract.ExtractCompanyInfo(*cik)
	if err != nil {
		log.Fatalf("Failed to extract company info: %v", err)
	}

	// Define the storage bucket and file path
	bucketName := "edgar_sec"
	filePath := fmt.Sprintf("%s-info.json", *cik)

	// Upload the extracted information to Google Cloud Storage
	if err := storage.UploadBytes(info, bucketName, filePath); err != nil {
		log.Fatalf("Failed to upload company info: %v", err)
	}

	log.Printf("Successfully uploaded company info for CIK %s", *cik)
}
