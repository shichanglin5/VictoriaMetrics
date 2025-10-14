package main

import (
	"regexp"
	"strings"
)

func checkHost(host string) bool {
	// 如果包含 @ 符号，可能是邮箱格式，需要分离用户名和域名部分
	var domainPart string
	if strings.Contains(host, "@") {
		parts := strings.Split(host, "@")
		if len(parts) != 2 {
			return false // 无效的邮箱格式
		}

		username := parts[0]
		domainPart = parts[1]

		// 检查用户名是否是 UUID 格式
		if isUUID(username) {
			return false
		}

		// 检查用户名是否全数字
		if isAllNumeric(username) {
			return false
		}
	} else {
		domainPart = host
	}

	// 检查域名部分是否包含全数字标签
	if hasAllNumericLabels(domainPart) {
		return false
	}

	// 验证基础域名格式
	return isValidDomainFormat(domainPart)
}

// 检查字符串是否是 UUID 格式
func isUUID(s string) bool {
	uuidPattern := `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
	matched, _ := regexp.MatchString(uuidPattern, strings.ToLower(s))
	return matched
}

// 检查字符串是否全数字
func isAllNumeric(s string) bool {
	numericPattern := `^\d+$`
	matched, _ := regexp.MatchString(numericPattern, s)
	return matched
}

// 检查域名是否包含全数字标签
func hasAllNumericLabels(domain string) bool {
	// 分割域名标签
	labels := strings.Split(domain, ".")
	for _, label := range labels {
		// 跳过空标签
		if label == "" {
			continue
		}
		// 检查标签是否全数字
		if isAllNumeric(label) {
			return true
		}
	}
	return false
}

// 验证基础域名格式
func isValidDomainFormat(domain string) bool {
	// 支持域名+端口号格式：字母数字开头结尾，中间可包含连字符，可选端口号
	domainPattern := `^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,63}(:\d{1,5})?$`
	matched, _ := regexp.MatchString(domainPattern, domain)
	return matched
}
