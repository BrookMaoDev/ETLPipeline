package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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

// RequestPayload defines the expected structure of the request body.
type RequestPayload struct {
	CIK string `json:"cik"`
}

// CompanyInfoHandler handles POST requests to extract and upload company info.
func CompanyInfoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the JSON body
	var payload RequestPayload
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Validate CIK
	if !validateCIK(payload.CIK) {
		http.Error(w, "Invalid CIK: must be a 10-digit number", http.StatusBadRequest)
		return
	}

	// Extract company information
	info, err := extract.ExtractCompanyInfo(payload.CIK)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to extract company info: %v", err), http.StatusInternalServerError)
		return
	}

	// Define the storage bucket and file path
	bucketName := "edgar_sec"
	filePath := fmt.Sprintf("%s-info.json", payload.CIK)

	// Upload the extracted information to Google Cloud Storage
	if err := storage.UploadBytes(info, bucketName, filePath); err != nil {
		http.Error(w, fmt.Sprintf("Failed to upload company info: %v", err), http.StatusInternalServerError)
		return
	}

	// Respond with a success message
	response := map[string]string{
		"message": fmt.Sprintf("Successfully uploaded company info for CIK %s", payload.CIK),
		"file":    filePath,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Default to 8080 if PORT isn't set
	}

	http.HandleFunc("/", CompanyInfoHandler)

	log.Printf("Starting server on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
