package utils

import (
	"crypto/rand"
	"fmt"
)

// NewUUID 生成一个随机的UUID字符串
func NewUUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	// 设置版本为4（随机生成）
	b[6] = (b[6] & 0x0f) | 0x40
	// 设置变体为RFC4122
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
