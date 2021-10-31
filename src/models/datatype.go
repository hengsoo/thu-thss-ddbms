package models

// enumeration of datatype
const (
	TypeInt32 = iota
	TypeInt64
	TypeFloat
	TypeDouble
	TypeBoolean
	TypeString
)

func compare(dataType int, operator string, valA interface{}, valB interface{}) bool {

	if operator == "==" {
		return valA == valB
	} else if operator == "!=" {
		return valA != valB
	}

	switch dataType {
	case TypeInt32:
		var valAI32 int32
		var valBI32 int32

		// Type conversion
		switch valA.(type) {
		case int:
			valAI32 = int32(valA.(int))
			break
		case int64:
			valAI32 = int32(valA.(int64))
			break
		case float32:
			valAI32 = int32(valA.(float32))
			break
		case float64:
			valAI32 = int32(valA.(float64))
			break
		default:
			valAI32 = valA.(int32)
		}

		switch valB.(type) {
		case int:
			valBI32 = int32(valB.(int))
			break
		case int64:
			valBI32 = int32(valB.(int64))
			break
		case float32:
			valBI32 = int32(valB.(float32))
			break
		case float64:
			valBI32 = int32(valB.(float64))
			break
		default:
			valBI32 = valB.(int32)
		}

		switch operator {
		case ">=":
			return valAI32 >= valBI32
		case ">":
			return valAI32 > valBI32
		case "<=":
			return valAI32 <= valBI32
		case "<":
			return valAI32 < valBI32
		}
	case TypeInt64:
		var valAI64 int64
		var valBI64 int64

		// Type conversion
		switch valA.(type) {
		case int:
			valAI64 = int64(valA.(int))
			break
		case int32:
			valAI64 = int64(valA.(int32))
			break
		case float32:
			valAI64 = int64(valA.(float32))
			break
		case float64:
			valAI64 = int64(valA.(float64))
			break
		default:
			valAI64 = valA.(int64)
		}

		switch valB.(type) {
		case int:
			valBI64 = int64(valB.(int))
			break
		case int32:
			valBI64 = int64(valB.(int32))
			break
		case float32:
			valBI64 = int64(valB.(float32))
			break
		case float64:
			valBI64 = int64(valB.(float64))
			break
		default:
			valBI64 = valB.(int64)
		}

		switch operator {
		case ">=":
			return valAI64 >= valBI64
		case ">":
			return valAI64 > valBI64
		case "<=":
			return valAI64 <= valBI64
		case "<":
			return valAI64 < valBI64
		}
	case TypeFloat:
		var valAF32 float32
		var valBF32 float32

		// Type conversion
		switch valA.(type) {
		case int:
			valAF32 = float32(valA.(int))
			break
		case int32:
			valAF32 = float32(valA.(int32))
			break
		case int64:
			valAF32 = float32(valA.(int64))
			break
		case float64:
			valAF32 = float32(valA.(float64))
			break
		default:
			valAF32 = valA.(float32)
		}

		switch valB.(type) {
		case int:
			valBF32 = float32(valB.(int))
			break
		case int32:
			valBF32 = float32(valB.(int32))
			break
		case int64:
			valBF32 = float32(valB.(int64))
			break
		case float64:
			valBF32 = float32(valB.(float64))
			break
		default:
			valBF32 = valB.(float32)
		}

		switch operator {
		case ">=":
			return valAF32 >= valBF32
		case ">":
			return valAF32 > valBF32
		case "<=":
			return valAF32 <= valBF32
		case "<":
			return valAF32 < valBF32
		}
	case TypeDouble:
		var valAF64 float64
		var valBF64 float64

		// Type conversion
		switch valA.(type) {
		case int:
			valAF64 = float64(valA.(int))
			break
		case int32:
			valAF64 = float64(valA.(int32))
			break
		case int64:
			valAF64 = float64(valA.(int64))
			break
		case float32:
			valAF64 = float64(valA.(float32))
			break
		default:
			valAF64 = valA.(float64)
		}

		switch valB.(type) {
		case int:
			valBF64 = float64(valB.(int))
			break
		case int32:
			valBF64 = float64(valB.(int32))
			break
		case int64:
			valBF64 = float64(valB.(int64))
			break
		case float32:
			valBF64 = float64(valB.(float32))
			break
		default:
			valBF64 = valB.(float64)
		}

		switch operator {
		case ">=":
			return valAF64 >= valBF64
		case ">":
			return valAF64 > valBF64
		case "<=":
			return valAF64 <= valBF64
		case "<":
			return valAF64 < valBF64
		}
	case TypeString:
		switch operator {
		case ">=":
			return valA.(string) >= valB.(string)
		case ">":
			return valA.(string) > valB.(string)
		case "<=":
			return valA.(string) <= valB.(string)
		case "<":
			return valA.(string) < valB.(string)
		}
	}

	return false
}
