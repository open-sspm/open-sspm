package viewmodels

type SettingsUsersAlert struct {
	Title       string
	Message     string
	Destructive bool
}

type SettingsUsersForm struct {
	Email string
	Role  string
}

type SettingsUsersUserItem struct {
	ID             int64
	Email          string
	Role           string
	IsActive       bool
	LastLogin      string
	LastLoginTitle string
	IsSelf         bool
	IsLastAdmin    bool
	CanEditRole    bool
	CanDelete      bool
}

type SettingsUsersEditForm struct {
	ID                 int64
	Email              string
	Role               string
	RoleDisabled       bool
	RoleDisabledReason string
}

type SettingsUsersDeleteViewData struct {
	ID    int64
	Email string
}

type SettingsUsersViewData struct {
	Layout     LayoutData
	Users      []SettingsUsersUserItem
	HasUsers   bool
	OpenAdd    bool
	OpenEdit   bool
	OpenDelete bool
	Form       SettingsUsersForm
	EditForm   SettingsUsersEditForm
	Delete     SettingsUsersDeleteViewData
	Alert      *SettingsUsersAlert
}
