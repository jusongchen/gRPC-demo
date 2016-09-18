//Package console offer an web server console
package cli

import (
	"fmt"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/jusongchen/gRPC-demo/cli/views"
)

var (
	index, contact, msg *views.View
	client              *Client
	viewsDir            = "cli/views/"
)

//Start starts an http Server
func (c *Client) openConsole() error {
	client = c
	index = views.NewView("bootstrap", viewsDir+"index.gohtml")
	contact = views.NewView("bootstrap", viewsDir+"contacts.gohtml")
	msg = views.NewView("bootstrap", viewsDir+"message.gohtml")

	router := httprouter.New()
	router.GET("/", indexHandler)
	router.GET("/contact", contactHandler)
	router.GET("/message", msgHandler)
	router.POST("/message", msgHandler)

	router.GET("/dashboard", dashboardHandler)

	return http.ListenAndServe(fmt.Sprintf(":%d", c.consolePort), router)

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
