package transform

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// WeatherData represents a structured format for Mars weather data per Sol (Martian day).
type WeatherData struct {
	Sol          int     `json:"sol"`
	StartTime    string  `json:"starttime"`
	EndTime      string  `json:"endtime"`
	TempAvg      float64 `json:"temp_avg"`
	TempMin      float64 `json:"temp_min"`
	TempMax      float64 `json:"temp_max"`
	PressureAvg  float64 `json:"pressure_avg"`
	PressureMin  float64 `json:"pressure_min"`
	PressureMax  float64 `json:"pressure_max"`
	WindSpeedAvg float64 `json:"wind_speed_avg"`
	WindSpeedMin float64 `json:"wind_speed_min"`
	WindSpeedMax float64 `json:"wind_speed_max"`
}

// TransformWeatherData takes raw JSON data (as bytes) from the NASA API,
// parses it, and transforms it into a slice of WeatherData.
func TransformWeatherData(rawJSON []byte) ([]WeatherData, error) {
	// Unmarshal the JSON data into a generic map
	var rawData map[string]interface{}
	if err := json.Unmarshal(rawJSON, &rawData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON data: %w", err)
	}

	solKeys, ok := rawData["sol_keys"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to extract sol_keys from raw data")
	}

	var weatherData []WeatherData
	for _, sol := range solKeys {
		solStr, _ := sol.(string)
		solData, ok := rawData[solStr].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to extract data for sol %s", solStr)
		}

		// Extract atmospheric temperature data
		tempData, _ := solData["AT"].(map[string]interface{})
		tempAvg, _ := tempData["av"].(float64)
		tempMin, _ := tempData["mn"].(float64)
		tempMax, _ := tempData["mx"].(float64)

		// Extract atmospheric pressure data
		pressureData, _ := solData["PRE"].(map[string]interface{})
		pressureAvg, _ := pressureData["av"].(float64)
		pressureMin, _ := pressureData["mn"].(float64)
		pressureMax, _ := pressureData["mx"].(float64)

		// Extract horizontal wind speed data
		windData, _ := solData["HWS"].(map[string]interface{})
		windSpeedAvg, _ := windData["av"].(float64)
		windSpeedMin, _ := windData["mn"].(float64)
		windSpeedMax, _ := windData["mx"].(float64)

		// Extract timestamps for start and end times
		startTime, _ := solData["First_UTC"].(string)
		endTime, _ := solData["Last_UTC"].(string)

		// Parse sol number as an integer
		solNum, err := strconv.Atoi(solStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sol number %s: %v", solStr, err)
		}

		// Append structured data for each sol
		weatherData = append(weatherData, WeatherData{
			Sol:          solNum,
			StartTime:    startTime,
			EndTime:      endTime,
			TempAvg:      tempAvg,
			TempMin:      tempMin,
			TempMax:      tempMax,
			PressureAvg:  pressureAvg,
			PressureMin:  pressureMin,
			PressureMax:  pressureMax,
			WindSpeedAvg: windSpeedAvg,
			WindSpeedMin: windSpeedMin,
			WindSpeedMax: windSpeedMax,
		})
	}
	return weatherData, nil
}

// ConvertToNDJSON converts a slice of WeatherData to a newline-delimited JSON format.
func ConvertToNDJSON(weatherData []WeatherData) ([]byte, error) {
	var ndjsonData []byte

	for _, data := range weatherData {
		line, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to convert WeatherData to JSON: %w", err)
		}
		ndjsonData = append(ndjsonData, line...)
		ndjsonData = append(ndjsonData, '\n') // Add newline character after each JSON object
	}

	return ndjsonData, nil
}
