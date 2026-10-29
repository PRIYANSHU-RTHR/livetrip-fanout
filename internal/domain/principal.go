package domain

// Role decides which topics a principal may subscribe to.
type Role string

const (
	RoleEmployee    Role = "employee"
	RoleOfficeAdmin Role = "office_admin"
	RoleSecurity    Role = "security"
	RoleOperator    Role = "operator"
)

func (r Role) Valid() bool {
	switch r {
	case RoleEmployee, RoleOfficeAdmin, RoleSecurity, RoleOperator:
		return true
	}
	return false
}

// Principal is the authenticated caller, taken from a verified token.
// OfficeID is empty for roles that are not tied to one office.
type Principal struct {
	Tenant   string
	UserID   string
	Role     Role
	OfficeID string
}
