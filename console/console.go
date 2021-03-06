//Package console get user input and broacast to all other peers
package console

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
	clu "github.com/jusongchen/gRPC-demo/cluster"
	pb "github.com/jusongchen/gRPC-demo/clusterpb"
	"github.com/jusongchen/gRPC-demo/console/views"
	"github.com/pkg/errors"
)

//Console is not exported
type Console struct {
	// ConsolePort int
	*clu.Client
	*clu.Server
}

var (
	console Console

	index, msg *views.View
	dashboard  *template.Template
	viewsDir   = "console/views/"
)

//Start starts an http Server
func Start(client *clu.Client, server *clu.Server) error {

	console = Console{
		// ConsolePort: consolePort,
		Client: client,
		Server: server,
	}

	index = views.NewView("bootstrap", viewsDir+"index.gohtml")
	msg = views.NewView("bootstrap", viewsDir+"message.gohtml")
	var err error
	dashboard, err = template.ParseFiles(viewsDir + "dashboard.gohtml")
	if err != nil {
		log.Fatal(errors.Wrapf(err, "template.ParseFiles(%s)", viewsDir+"dashboard.gohtml"))
	}

	router := httprouter.New()
	router.GET("/", indexHandler)
	router.GET("/message", msgGetHandler)
	router.GET("/server/dashboard", dashboardHandler)
	router.GET("/server/last-update-TS", srvUpdateHandler)

	router.POST("/message", msgPostHandler)

	return http.ListenAndServe(fmt.Sprintf(":%d", client.Node.ConsolePort), router)

}

func indexHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	index.Render(w, nil, nil)
}

func srvUpdateHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	switch r.Method {
	case "GET":

		lastChangeAt := console.LastMsgReceivedAt
		if console.LastMsgSentAt.After(lastChangeAt) {
			lastChangeAt = console.LastMsgSentAt
		}

		if console.LastNodeChangeAt.After(lastChangeAt) {
			lastChangeAt = console.LastNodeChangeAt
		}
		fmt.Fprintf(w, "%s", lastChangeAt)
	default:
		log.Fatalf("srvUpdateHandler:unknown http method %v", r.Method)
	}
}

func msgGetHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	msg.Render(w, console, nil)
}

func msgPostHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	flashes := map[string]string{}
	err := postMsg(r.FormValue("userid"), r.FormValue("message"))
	if err != nil {
		flashes["danger"] = err.Error()
	}
	msg.Render(w, console, flashes)
}

func postMsg(uid, msg string) error {

	if uid == "" || msg == "" {
		return errors.New("User ID and Message must be filled!")
	}

	userid, err := strconv.Atoi(uid)
	if err != nil {
		return errors.Wrapf(err, "User ID %q must be a number!", uid)
	}

	records := []*pb.ChatMsg{}
	records = append(records, &pb.ChatMsg{
		Userid:  int32(userid),
		Message: msg,
	})

	return console.PromoteDataChange(records)
}

func dashboardHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// log.Printf("dashboardHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params)")

	// dashboard.ExecuteTemplate(os.Stdout, "dashboard.gohtml", client)
	if err := dashboard.ExecuteTemplate(w, "dashboard.gohtml", console); err != nil {
		log.Fatal(errors.Wrap(err, "dashboardHandler"))
	}
	// fmt.Fprintf(w, "%v", client)

}
