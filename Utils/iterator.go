package Utils

type Iterator interface {
	Next()
	Valid()
	Item()
}
type Item interface {
	Entry() *Entry
}
