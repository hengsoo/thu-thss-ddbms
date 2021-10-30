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
		switch operator {
		case ">=":
			return valA.(int32) >= valB.(int32)
		case ">":
			return valA.(int32) > valB.(int32)
		case "<=":
			return valA.(int32) <= valB.(int32)
		case "<":
			return valA.(int32) < valB.(int32)
		}
	case TypeInt64:
		switch operator {
		case ">=":
			return valA.(int64) >= valB.(int64)
		case ">":
			return valA.(int64) > valB.(int64)
		case "<=":
			return valA.(int64) <= valB.(int64)
		case "<":
			return valA.(int64) < valB.(int64)
		}
	case TypeFloat:
		switch operator {
		case ">=":
			return valA.(float32) >= valB.(float32)
		case ">":
			return valA.(float32) > valB.(float32)
		case "<=":
			return valA.(float32) <= valB.(float32)
		case "<":
			return valA.(float32) < valB.(float32)
		}
	case TypeDouble:
		switch operator {
		case ">=":
			return float64(valA.(int)) >= valB.(float64)
		case ">":
			return float64(valA.(int)) > valB.(float64)
		case "<=":
			return float64(valA.(int)) <= valB.(float64)
		case "<":
			return float64(valA.(int)) < valB.(float64)
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
