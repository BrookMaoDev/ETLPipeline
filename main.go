package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/BrookMaoDev/ETLPipeline/internal/extract"
	"github.com/BrookMaoDev/ETLPipeline/internal/load"
	"github.com/BrookMaoDev/ETLPipeline/internal/storage"
	"github.com/BrookMaoDev/ETLPipeline/internal/transform"
	"github.com/joho/godotenv"
)

// WeatherDataHandler handles POST requests to extract, transform, and upload weather data.
func WeatherDataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Step 1: Extract raw weather data from the NASA API
	data, err := extract.ExtractWeatherData()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to extract weather data: %v", err), http.StatusInternalServerError)
		return
	}

	// Step 2: Transform the raw JSON data into the desired WeatherData structure
	weatherData, err := transform.TransformWeatherData(data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to transform weather data: %v", err), http.StatusInternalServerError)
		return
	}

	// Step 3: Convert the transformed WeatherData slice into NDJSON bytes for storage
	transformedNDJSON, err := transform.ConvertToNDJSON(weatherData)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to convert transformed data to NDJSON: %v", err), http.StatusInternalServerError)
		return
	}

	// Step 4: Define the storage bucket and file path using environment variables
	bucketName := os.Getenv("GCS_BUCKET")
	filePath := os.Getenv("GCS_FILE_PATH")
	if bucketName == "" || filePath == "" {
		http.Error(w, "Storage bucket or file path environment variable not set", http.StatusInternalServerError)
		return
	}

	// Step 5: Upload the transformed JSON data to Google Cloud Storage
	if err := storage.UploadBytes(transformedNDJSON, bucketName, filePath); err != nil {
		http.Error(w, fmt.Sprintf("Failed to upload weather data: %v", err), http.StatusInternalServerError)
		return
	}

	// Step 6: Load the data into BigQuery
	projectID := os.Getenv("BQ_PROJECT_ID")
	datasetID := os.Getenv("BQ_DATASET_ID")
	tableID := os.Getenv("BQ_TABLE_ID")
	if err := load.LoadDataFromGCS(projectID, datasetID, tableID, bucketName, filePath); err != nil {
		http.Error(w, fmt.Sprintf("Failed to load data into BigQuery: %v", err), http.StatusInternalServerError)
		return
	}

	// Step 7: Respond with a success message
	response := map[string]string{
		"message": "Successfully uploaded and loaded transformed weather data into BigQuery",
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
