package views

import (
	"html/template"
	"log"
	"net/http"
	"path/filepath"

	"github.com/pkg/errors"
)

type View struct {
	Template *template.Template
	Layout   string
}

type ViewData struct {
	Flashes map[string]string
	Data    interface{}
}

var LayoutDir string = "cli/views/layouts"

func NewView(layout string, files ...string) *View {
	files = append(files, layoutFiles()...)
	t, err := template.ParseFiles(files...)
	if err != nil {
		panic(err)
	}

	return &View{
		Template: t,
		Layout:   layout,
	}
}

func (v *View) Render(w http.ResponseWriter, data interface{}) error {
	vd := ViewData{
		Flashes: flashes(),
		Data:    data,
	}
	err := v.Template.ExecuteTemplate(w, v.Layout, vd)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "View.Render:%s", v.Template.Name()))
	}
	return nil

}

func layoutFiles() []string {
	files, err := filepath.Glob(LayoutDir + "/*.gohtml")
	if err != nil {
		panic(err)
	}
	return files
}

var flashRotator int = 0

func flashes() map[string]string {
	flashRotator = flashRotator + 1
	if flashRotator%3 == 0 {
		return map[string]string{
			"warning": "You are about to exceed your plan limts!",
		}
	}
	return map[string]string{}
}
