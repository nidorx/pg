package pg

import (
	"errors"
	"fmt"
	"reflect"
)

var ErrUnsupportedDataType = errors.New("unsupported data type")

type Model[T any] struct {
	Data []T
}

func (m *Model[T]) Method(r *T) T {
	return m.Data[0]
}

func (m Model[T]) Create() *T {
	return new(T)
}

func NewModel[T any](data []T) *Model[T] { return &Model[T]{data} }

// table.With(Database)
// table.Insert(vales, where)
// table.Update(values, where)
// table.Upsert(values, where)
// table.Select(where)
// table.Delete(where)

type Table[T any] struct {
	schema     string
	table      string
	identifier string // "schema"."table"
	db         *Database
}

func (t *Table[T]) model() *T {
	return new(T)
}

func (t *Table[T]) Using(db *Database) *Table[T] {
	return &Table[T]{
		schema:     t.schema,
		table:      t.table,
		identifier: t.identifier,
		db:         db,
	}
}

func (t *Table[T]) getDb() (*Database, error) {
	if t.db == nil {
		return GetInstance()
	}
	return t.db, nil
}

//func (t *Table[T]) Insert(values ...T) (bool, error) {
//	if db, err := t.getDb(); err != nil {
//		return false, err
//	} else if exist, err := db.QueryForBoolean(tableQueryExists, t.schema, t.table); err != nil {
//		return false, errors.New(fmt.Sprintf("unable to check whether table " + t.identifier + " exists (cause: " + err.Error() + ")"))
//	} else {
//		return exist, nil
//	}
//}

// table.Insert(vales)

func NewTable[T any](schema, name string, model T) (*Table[T], error) {
	//if model == nil {
	//	return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, model)
	//}

	value := reflect.ValueOf(model)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		value = reflect.New(value.Type().Elem())
	}
	modelType := reflect.Indirect(value).Type()

	if modelType.Kind() == reflect.Interface {
		modelType = reflect.Indirect(reflect.ValueOf(model)).Elem().Type()
	}

	for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		if modelType.PkgPath() == "" {
			return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, model)
		}
		return nil, fmt.Errorf("%w: %s.%s", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
	}

	t := &Table[T]{
		schema:     schema,
		table:      name,
		identifier: QuoteIdentifier(schema) + "." + QuoteIdentifier(name),
	}

	return t, nil
}

type UserModel struct {
	Id    string // "id    VARCHAR(27)  NOT NULL PRIMARY KEY"
	Email string // "email VARCHAR(255) NOT NULL"
}

func (u *UserModel) talk() string {
	return "ola"
}

func teste() {

	//Users, _ := NewTable("auth", "t_user", UserModel{})
	//
	//u := Users.model()

	//Users.Insert(map[string]string{})
	//Users.Insert(UserModel{})

	// user, err := Users.Get(50)
	// user, err := Users.GetById(50)
	// user, err := Users.GetWhere("name <> ?", "jinzhu")

	// table.With(Database)

	// table.Select("name", "age")
	// table.Select([]string{"name", "age"})
	// table.Select("COALESCE(age,?)", 42)
	// table.Distinct("name", "age")

	// table.Order("age desc, name")
	// table.Limit(10)
	// table.Offset(2)

	// table.Insert(vales)
	// table.Update(where, values)
	// table.Upsert(values, where)
	// table.Select(where)
	// table.Delete(where)

	//print(u.talk())

	//model := NewModel([]int{1, 2, 3})
	//
	//var i int
	//i = model.Data[0]
	//i = model.Method(&i)
	//fmt.Sprintf("i = %d", model.Create())
	//fmt.Println(i) // [1 2 3]
}
