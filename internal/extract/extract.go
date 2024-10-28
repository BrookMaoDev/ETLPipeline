package extract

import (
	"fmt"
	"io"
	"net/http"
)

// ExtractCompanyInfo retrieves JSON-formatted company information from the SEC database
// using the company's Central Index Key (CIK).
//
// Parameters:
// - cik: A string representing the company's CIK (must be padded to 10 digits).
//
// Returns:
// - A byte slice containing the JSON data from the SEC API, or an error if the request fails.
func ExtractCompanyInfo(cik string) ([]byte, error) {
	// Construct the API URL using the provided CIK
	url := fmt.Sprintf("https://data.sec.gov/api/xbrl/companyfacts/CIK%s.json", cik)

	// Create a new HTTP request
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Use an HTTP client to execute the request
	client := &http.Client{}
	resp, err := client.Do(req)
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
