package main

import "net/http"

func (app *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/healthcheck", app.healthcheckHandler)
	mux.HandleFunc("POST /v1/images", app.createImageHandler)
	mux.HandleFunc("GET /v1/jobs/{id}", app.getJobHandler)
	// [V3] EventSource endpoint. Short and long polling stay on the route above.
	mux.HandleFunc("GET /v1/jobs/{id}/events", app.jobEventsHandler)
	mux.HandleFunc("GET /v1/images/{image_id}/variants/{name}", app.getVariantHandler)

	// Serves index.html/app.js/style.css etc.
	mux.Handle("/", http.FileServer(http.Dir("./ui/static")))

	return mux
}
