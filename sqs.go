package sqs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Queue defines the behavious of an order queue.
// This interface allows us to implement a mock queue service for development and
// unit testing
type Queue interface {
	SendMessage(ctx context.Context, delay time.Duration, payload []byte) error
	ReceiveMessage(ctx context.Context) (Message, error)
}

// Message defines the beavious of a message of an order queue.
type Message interface {
	Payload() []byte

	// Resets the remaining visibility timeout to the given value.
	ChangeVisibility(ctx context.Context, remainingInvisibility time.Duration) error

	// Deletes the message. Once this is called, no further methods may be called
	// on this message.
	Delete(ctx context.Context) error
}

// An SQS backed queue implementation.
type sqsQueue struct {
	// The AWS SQK SQS client.
	cli *sqs.SQS
	// The queue url.
	url string
	// The maximum time to wait on ReceiveMessage.
	waitTime time.Duration
}

// Config defines dependent variables and values for initialising a sqsQueue client.
type Config struct {
	// A valid aws region string must be provided to initialise a sqs client.
	// The available regions are:
	// https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
	Region string
	// the endpoint of the queue.
	Endpoint string
	// wait time is important for reducing cost by enabling long pooling.
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/reducing-costs.html
	WaitTime time.Duration
	// Using default aws credentials for live environment.
	// Create a static credentials for local development.
	// https://docs.aws.amazon.com/sdk-for-go/api/aws/credentials/
	Credentials *credentials.Credentials
}

// Validate validates the config object and returns an error on invalid config.
func (c Config) Validate() error {
	if len(c.Region) == 0 {
		return errors.New("aws region cannot be empty")
	}
	if len(c.Endpoint) == 0 {
		return errors.New("sqs endpoint url cannot be empty")
	}
	if c.WaitTime < 0 || c.WaitTime > 20*time.Second {
		return fmt.Errorf("sqs waitTime must be a positive number of at most 20 seconds, got %s", c.WaitTime)
	}
	return nil
}

// NewSqsQueue creates a new SQS queue instance bound to the specified url. waitTime is the number of seconds
// that an sqsl.ReceiveMessage() should wait, at most, for a message to arrive. If it is set to a
// non-zero number then long-polling is enabled, as described here:
// http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
func NewSqsQueue(sess *session.Session, c Config) *sqsQueue {
	return &sqsQueue{
		cli:      sqs.New(sess),
		url:      c.Endpoint,
		waitTime: c.WaitTime,
	}
}

// The SQS API uses strings to represent message payloads. This function encodes a byte slice to a string.
func encodePayload(payload []byte) string {
	return base64.StdEncoding.EncodeToString(payload)
}

// The SQS API uses strings to represent message payloads. This function decodes a message payload
// into a byte slice.
func decodePayload(msgPayload string) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(msgPayload)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// SendMessage wraps the sqs SendMessage methods and enqueue a message with the given payload.
func (s *sqsQueue) SendMessage(ctx context.Context, delay time.Duration, payload []byte) error {
	params := &sqs.SendMessageInput{
		MessageBody:  aws.String(encodePayload(payload)),
		QueueUrl:     aws.String(s.url),
		DelaySeconds: aws.Int64(int64(delay / time.Second)),
	}
	if _, err := s.cli.SendMessage(params); err != nil {
		log.Printf("%+v", params)
		return err
	}
	return nil
}

// SendMessage wraps the sqs ReceiveMessage methods and returns a dequeued message.
func (s *sqsQueue) ReceiveMessage(ctx context.Context) (Message, error) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.url),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(int64(s.waitTime / time.Second)),
	}

	// Poll AWS for a message. Each call to the ReceiveMessage endpoint will block for at most
	// s.waitTime.
	awsMsg, err := func() (*sqs.Message, error) {
		for i := 1; true; i++ {
			resp, err := s.cli.ReceiveMessage(params)
			if err != nil {
				return nil, err
			}
			if len(resp.Messages) == 1 {
				return resp.Messages[0], nil
			}
			if len(resp.Messages) > 1 {
				return nil, fmt.Errorf("Error: Got %d messages from SQS, expected at most 1", len(resp.Messages))
			}
		}
		return nil, errors.New("unreachable statement")
	}()
	if err != nil {
		return nil, err
	}

	payload, err := decodePayload(*awsMsg.Body)
	if err != nil {
		return nil, err
	}

	msg := &sqsMsg{
		q:             s,
		messageID:     *awsMsg.MessageId,
		receiptHandle: *awsMsg.ReceiptHandle,
		payload:       payload,
	}
	return msg, nil
}
