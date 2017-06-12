package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"os"
	"time"
)

func getShards(svc *kinesis.Kinesis, streamName string) ([]string, error) {
	params := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}

	resp, err := svc.DescribeStream(params)
	if err != nil {
		return nil, err
	}

	shards := make([]string, 0, len(resp.StreamDescription.Shards))

	for _, s := range resp.StreamDescription.Shards {
		shards = append(shards, *s.ShardId)
	}
	return shards, nil
}

func getInitialShardIterator(svc *kinesis.Kinesis, streamName string, shardId string) (*string, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardId),
		ShardIteratorType: aws.String("LATEST"),
		StreamName:        aws.String(streamName),
		Timestamp:         aws.Time(time.Now()),
	}

	resp, err := svc.GetShardIterator(params)

	if err != nil {
		return nil, err
	}

	return resp.ShardIterator, nil
}

func readStream(messages chan string, svc *kinesis.Kinesis, streamName string, shardId string) {
	nextShardIterator, err := getInitialShardIterator(svc, streamName, shardId)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}

	for {
		getParams := &kinesis.GetRecordsInput{
			ShardIterator: nextShardIterator,
		}

		resp, err := svc.GetRecords(getParams)

		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			return
		}

		for _, e := range resp.Records {
			messages <- string(e.Data)
		}

		nextShardIterator = resp.NextShardIterator
		if len(resp.Records) == 0 {
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func mkKinesisService() (*kinesis.Kinesis, error) {
	conf := aws.NewConfig()

	if os.Getenv("AWS_REGION") == "" {
		conf = conf.WithRegion(endpoints.UsEast1RegionID)
	}

	sess, err := session.NewSession(conf)
	if err != nil {
		return nil, err
	}

	svc := kinesis.New(sess)
	return svc, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: kinesis-tail [stream-name]\n")
		os.Exit(1)
	}
	streamName := os.Args[1]

	svc, err := mkKinesisService()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	shardIds, err := getShards(svc, streamName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	messages := make(chan string, 10)

	for _, shardId := range shardIds {
		go readStream(messages, svc, streamName, shardId)
	}

	for {
		fmt.Println(<-messages)
	}
}
