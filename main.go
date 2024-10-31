package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/BrookMaoDev/ETLPipeline/internal/extract"
	"github.com/BrookMaoDev/ETLPipeline/internal/storage"
	"github.com/joho/godotenv"
)

// WeatherDataHandler handles POST requests to extract and upload weather data.
func WeatherDataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { // Only allow POST requests
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract weather data
	data, err := extract.ExtractWeatherData()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to extract weather data: %v", err), http.StatusInternalServerError)
		return
	}

	// Define the storage bucket and file path using environment variables
	bucketName := os.Getenv("GCS_BUCKET")
	filePath := os.Getenv("GCS_FILE_PATH")
	if bucketName == "" || filePath == "" {
		http.Error(w, "Storage bucket or file path environment variable not set", http.StatusInternalServerError)
		return
	}

	// Upload the extracted weather data to Google Cloud Storage
	if err := storage.UploadBytes(data, bucketName, filePath); err != nil {
		http.Error(w, fmt.Sprintf("Failed to upload weather data: %v", err), http.StatusInternalServerError)
		return
	}

	// Respond with a success message
	response := map[string]string{
		"message": "Successfully uploaded weather data",
		"file":    filePath,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func main() {
	// Load .env file for environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("Warning: .env file not found, defaulting to environment variables")
	}

	// Retrieve the port from environment variables, default to 8080 if not set
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/", WeatherDataHandler)

	log.Printf("Starting server on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
