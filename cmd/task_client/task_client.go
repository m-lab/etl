// Sample
package main

import (
	"flag"
	"fmt"
	"time"

	"context"
	"github.com/kr/pretty"
	"golang.org/x/oauth2/google"
	taskqueue "google.golang.org/api/taskqueue/v1beta2"
)

var (
	queue   = flag.String("queue", "", "Pull TaskQueue name.")
	project = flag.String("project", "mlab-sandbox", "GCP Project name.")
	payload = flag.String("payload", "", "A base64 encoded payload.")
)

// Create taskqueue.Service for communicating with the taskqueue services.
func CreateTaskQueueService() *taskqueue.Service {
	// NOTE: google.DefaultClient authenticates the returned client with
	// "Application Default Credentials". For AppEngine Flex and GCE VMs, the
	// application default credentials are associated with the default GCE
	// service account. Credentials are scoped to limit their use to specific
	// APIs. So, configure necessary scopes in app.yaml, or when creating a GCE
	// VM; scopes cannot be changed at runtime.
	//
	// For more details and background see:
	//   https://developers.google.com/identity/protocols/application-default-credentials
	client, err := google.DefaultClient(context.Background(),
		taskqueue.TaskqueueScope, taskqueue.TaskqueueConsumerScope)
	if err != nil {
		fmt.Printf("Unable to get default taskqueue client: %v \n", err)
		return nil
	}

	service, err := taskqueue.New(client)
	if err != nil {
		fmt.Printf("Unable to create taskqueue service: %v\n", err)
		return nil
	}
	return service
}

func NewTask(queueName, payload string) *taskqueue.Task {
	t := &taskqueue.Task{
		// Set first available lease time to now.
		LeaseTimestamp: time.Now().Unix(),
		PayloadBase64:  payload,
		QueueName:      queueName,
	}
	return t
}

func main() {
	flag.Parse()

	api := CreateTaskQueueService()
	pretty.Print(api)

	req := NewTask(*queue, *payload)
	pretty.Print(req)

	// WARNING: NOTE: The v1beta2 taskqueue REST API requires the prefix "p~"
	// before the project name. Without this prefix, Insert, Lease, and Delete
	// operations fail with 403 errors:
	//    "you are not allowed to make this api call".
	task, err := api.Tasks.Insert(fmt.Sprintf("p~%s", *project), *queue, req).Do()
	if err != nil {
		panic(err)
	}
	pretty.Print(task)
}
