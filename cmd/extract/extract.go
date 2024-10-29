package main

import (
	"fmt"
	"log"

	"github.com/BrookMaoDev/ETLPipeline/internal/extract"
	"github.com/BrookMaoDev/ETLPipeline/internal/storage"
)

func main() {
	cik := "0000320193" // Example CIK for Apple Inc.

	// Extract company information
	info, err := extract.ExtractCompanyInfo(cik)
	if err != nil {
		log.Fatalf("Failed to extract company info: %v", err)
	}

	// Define the storage bucket and file path
	bucketName := "edgar_sec"
	filePath := fmt.Sprintf("%s-info.json", cik)

	// Upload the extracted information to Google Cloud Storage
	if err := storage.UploadBytes(info, bucketName, filePath); err != nil {
		log.Fatalf("Failed to upload company info: %v", err)
	}

	log.Printf("Successfully uploaded company info for CIK %s", cik)
}
