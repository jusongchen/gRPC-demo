//Package console get user input and broacast to all other peers
package console

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"github.com/jusongchen/gRPC-demo/cli"
	"github.com/jusongchen/gRPC-demo/console/views"
	pb "github.com/jusongchen/gRPC-demo/replica"
	"github.com/jusongchen/gRPC-demo/svr"
	"github.com/pkg/errors"
)

//Console is not exported
type Console struct {
	ConsolePort int
	Cli         *cli.Client
	Svr         *svr.Server
}

var (
	index, contact, msg *views.View
	dashboard           *template.Template
	console             Console
	viewsDir            = "console/views/"
)

//Start starts an http Server
func Start(consolePort int, client *cli.Client, server *svr.Server) error {

	console = Console{
		ConsolePort: consolePort,
		Cli:         client,
		Svr:         server,
	}

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

	return http.ListenAndServe(fmt.Sprintf(":%d", console.ConsolePort), router)

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

		err = console.Cli.PromoteDataChange(records)
		if err != nil {
			fmt.Fprintf(w, "%v", err)
		} else {

		}
	}
}

func dashboardHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// log.Printf("dashboardHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params)")

	// dashboard.ExecuteTemplate(os.Stdout, "dashboard.gohtml", client)
	if err := dashboard.ExecuteTemplate(w, "dashboard.gohtml", console); err != nil {
		log.Fatal(errors.Wrap(err, "dashboardHandler"))
	}
	// fmt.Fprintf(w, "%v", client)

}

// func dashboardHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
// 	if err := dashboard.ExecuteTemplate(w, "dashboard.gohtml", stat); err != nil {
// 		log.Fatal(errors.Wrap(err, "dashboardHandler"))
// 	}

// }
