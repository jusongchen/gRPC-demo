//Package console offer an web server console
package console

import (
	"fmt"
	"net/http"
	"time"

	"log"

	"github.com/julienschmidt/httprouter"
	"github.com/jusongchen/goLearning/goWeb/views"
)

var (
	index, contact, msg *views.View
)

//Start starts an http Server
func Start(port int) {
	index = views.NewView("bootstrap", "views/index.gohtml")
	contact = views.NewView("bootstrap", "views/contacts.gohtml")
	msg = views.NewView("bootstrap", "views/message.gohtml")

	router := httprouter.New()
	router.GET("/", indexHandler)
	router.GET("/contact", contactHandler)
	router.GET("/message", msgHandler)
	router.POST("/message", msgHandler)

	router.GET("/dashboard", dashboardHandler)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), router))

}

func dashboardHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprintf(w, "<p>time %v</p>", time.Now())
}

func indexHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	index.Render(w, nil)
}

func contactHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	contact.Render(w, nil)
}

func msgHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	switch r.Method {
	case "GET":
		msg.Render(w, nil)
	case "POST":
		msg := fmt.Sprintf("Post called:%s message: %s", r.FormValue("email"), r.FormValue("message"))
		fmt.Printf(msg)

		fmt.Fprintf(w, "Get form values: %v", r.Form)
	}
}
