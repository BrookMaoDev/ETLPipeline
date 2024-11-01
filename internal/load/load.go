package load

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
)

// LoadDataFromGCS loads data from a newline-delimited JSON file in Google Cloud Storage into BigQuery.
func LoadDataFromGCS(projectID, datasetID, tableID, bucketName, filePath string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%s/%s", bucketName, filePath))
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.AutoDetect = true

	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate // Overwrite the existing table

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return fmt.Errorf("job completed with error: %v", status.Err())
	}

	return nil
}
