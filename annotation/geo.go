package annotation

var (
	// AnnotatorURL holds the https address of the annotator.
	AnnotatorURL string

	// BaseURL provides the base URL for single annotation requests
	BaseURL string

	// BatchURL provides the base URL for batch annotation requests
	BatchURL string
)

func SetupURLs(project string) {
	AnnotatorURL = "https://annotator-dot-" + project + ".appspot.com"
	BaseURL = AnnotatorURL + "/annotate?"
	BatchURL = AnnotatorURL + "/batch_annotate"
}
