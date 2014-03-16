package sql_util

import (
    "database/sql"
    "fmt"
    "reflect"
    "strings"
    "sync"
)

const (
    TAG_NAME      = "db"
    TAG_SEPARATOR = ","
)

var (
    GoSqlUtil = &SqlUtil{
        cachedStructMaps: make(map[reflect.Type]*structInfo),
    }
)

type SqlUtil struct {
    cachedStructMaps  map[reflect.Type]*structInfo
    structsCacheMutex sync.Mutex
}

type structField struct {
    DBColumnName string
    FieldName    string
    Index        int
}

type structInfo struct {
    structFields map[string]*structField
}

func (s *SqlUtil) Scan(rows *sql.Rows, structs ...interface{}) error {
    return scan(rows, structs...)
}

func scan(rows *sql.Rows, structs ...interface{}) error {
    if err := rows.Err(); err != nil {
        return err
    }

    columns, err := rows.Columns()
    if err != nil {
        return err
    }

    targets, err := findScanTargets(columns, structs...)
    if err != nil {
        return err
    }

    rows.Scan(targets...)

    return nil
}

func findScanTargets(columns []string, structs ...interface{}) ([]interface{}, error) {
    var targets []interface{}

    for _, columnName := range columns {
        for _, dst := range structs {
            data, err := getStructFields(reflect.TypeOf(dst))
            structVal := reflect.ValueOf(dst).Elem()

            if err != nil {
                return nil, err
            }
            if field, present := data.(*structInfo).structFields[columnName]; present {
                fieldAddr := structVal.Field(field.Index).Addr().Interface()
                targets = append(targets, fieldAddr)
                break
            }
        }
    }

    return targets, nil
}

func getStructFields(destinationType reflect.Type) (interface{}, error) {
    if data, present := GoSqlUtil.cachedStructMaps[destinationType]; present {
        return data, nil
    }

    if destinationType.Kind() != reflect.Ptr {
        return nil, fmt.Errorf("SQL Util called with non-pointer destination %v", destinationType)
    }

    structType := destinationType.Elem()
    if structType.Kind() != reflect.Struct {
        return nil, fmt.Errorf("SQL Util called with pointer to non-struct %v", destinationType)
    }

    data := new(structInfo)
    data.structFields = make(map[string]*structField)

    numStructFields := structType.NumField()
    for i := 0; i < numStructFields; i++ {
        f := structType.Field(i)

        if f.PkgPath != "" {
            continue
        }

        if f.Type.Kind() == reflect.Ptr && f.Type.Elem().Kind() == reflect.Struct {
            // TODO - Add functionality to handle embedded structs
            continue
        } else {
            columnName := f.Name
            tags := strings.Split(f.Tag.Get(TAG_NAME), TAG_SEPARATOR)
            if len(tags) > 0 && tags[0] == "-" {
                continue
            }
            if len(tags) > 0 && tags[0] != "" {
                columnName = tags[0]
            }
            data.structFields[columnName] = &structField{
                DBColumnName: columnName,
                FieldName:    f.Name,
                Index:        i,
            }
        }
    }
    GoSqlUtil.structsCacheMutex.Lock()
    defer GoSqlUtil.structsCacheMutex.Unlock()
    GoSqlUtil.cachedStructMaps[destinationType] = data

    return data, nil
}
