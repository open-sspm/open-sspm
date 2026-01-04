package viewmodels

type LoginViewData struct {
	CSRFToken     string
	Email         string
	Next          string
	ErrorMessage  string
	SetupRequired bool
	Toast         *ToastViewData
}
