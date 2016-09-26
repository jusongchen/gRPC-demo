//Package cli get user input and broacast to all other peers
package cli

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"github.com/jusongchen/gRPC-demo/cli/views"
	pb "github.com/jusongchen/gRPC-demo/replica"
	"github.com/pkg/errors"
)

var (
	index, contact, msg *views.View
	dashboard           *template.Template
	client              *Client

	viewsDir = "cli/views/"
)

//Start starts an http Server
func (c *Client) openConsole() error {
	client = c
	index = views.NewView("bootstrap", viewsDir+"index.gohtml")
	contact = views.NewView("bootstrap", viewsDir+"contacts.gohtml")
	msg = views.NewView("bootstrap", viewsDir+"message.gohtml")
	var err error
	dashboard, err = template.ParseFiles(viewsDir + "dashboard.gohtml")
	if err != nil {
		log.Fatal(errors.Wrapf(err, "template.ParseFiles(%s)", viewsDir+"dashboard.gohtml"))
	}

	router := httprouter.New()
	router.GET("/", indexHandler)
	router.GET("/contact", contactHandler)
	router.GET("/message", msgHandler)
	router.POST("/message", msgHandler)

	router.GET("/dashboard", dashboardHandler)

	return http.ListenAndServe(fmt.Sprintf(":%d", c.ConsolePort), router)

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
		msg := fmt.Sprintf("Post called:%s message: %s", r.FormValue("userid"), r.FormValue("message"))
		fmt.Printf(msg)

		fmt.Fprintf(w, "Get form values: %v", r.Form)

		records := []*pb.ChatMsg{}
		userid, err := strconv.Atoi(r.FormValue("userid"))
		if err != nil {
			fmt.Fprintf(w, "Can not convert to number:%s", err)

		}
		records = append(records, &pb.ChatMsg{
			Userid:  int32(userid),
			Message: r.FormValue("message"),
		})

		err = client.PromoteDataChange(records)
		if err != nil {
			fmt.Fprintf(w, "%v", err)
		} else {

		}
	}
}

func dashboardHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// log.Printf("dashboardHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params)")

	// dashboard.ExecuteTemplate(os.Stdout, "dashboard.gohtml", client)
	if err := dashboard.ExecuteTemplate(w, "dashboard.gohtml", client); err != nil {
		log.Fatal(errors.Wrap(err, "dashboardHandler"))
	}
	// fmt.Fprintf(w, "%v", client)

}
