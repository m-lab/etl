// Sample
package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/kr/pretty"
	"golang.org/x/net/context"
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

	// NOTE: DefaultClient uses "Application Default Credentials", which derive
	// from the GCE VM service account. Appropriate scopes must be set in the
	// app.yaml configuration, or when the VM is created.
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
		// Set first available least time to now.
		LeaseTimestamp: time.Now().Unix(),
		PayloadBase64:  payload,
		QueueName:      queueName,
	}
	return t
}

func main() {
	api := CreateTaskQueueService()
	pretty.Print(api)

	req := NewTask(*queue, *payload)
	pretty.Print(req)

	task, err := api.Tasks.Insert(fmt.Sprintf("p~%s", *project), *queue, req).Do()
	if err != nil {
		panic(err)
	}
	pretty.Print(task)
}
