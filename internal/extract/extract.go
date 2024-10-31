package extract

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
)

// ExtractWeatherData retrieves JSON-formatted weather data from the NASA InSight API.
// Returns a byte slice containing the JSON data from the NASA API, or an error if the request fails.
func ExtractWeatherData() ([]byte, error) {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("Warning: .env file not found, defaulting to environment variables")
	}

	// Get NASA API key from environment variable, defaulting to "DEMO_KEY" if not set
	apiKey := os.Getenv("NASA_API_KEY")
	if apiKey == "" {
		apiKey = "DEMO_KEY"
	}

	// Construct the API URL
	url := fmt.Sprintf("https://api.nasa.gov/insight_weather/?api_key=%s&feedtype=json&ver=1.0", apiKey)

	// Create a new HTTP client and request
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check if the request was successful
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return body, nil
}
