package eg

import "reflect"

func FuncEqual(func1, func2 any) bool {
	f1Type := reflect.TypeOf(func1)
	if f1Type.Kind() != reflect.Func {
		panic("func1 is non-func type " + f1Type.String())
	}

	f2Type := reflect.TypeOf(func2)
	if f2Type.Kind() != reflect.Func {
		panic("func1 is non-func type " + f2Type.String())
	}

	return reflect.ValueOf(func1).Pointer() ==
		reflect.ValueOf(func2).Pointer()
}
