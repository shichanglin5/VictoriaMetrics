package main

import (
	"fmt"
	"testing"
)

func TestCheckHost(t *testing.T) {
	testCases := []struct {
		host     string
		expected bool
	}{
		// 应该返回 false 的情况
		{"5dd72e9e-3d45-4f40-b792-b23a90aaf57c@ly.com", false}, // UUID 邮箱
		{"123456@example.com", false},                          // 全数字用户名
		{"000852a.02386800909.17u.net", false},                 // 全数字标签
		{"123.456.789.com", false},                             // 全数字子域名

		// 应该返回 true 的情况
		{"admin-wwwmonitor.t.elonghui.net:8081", true}, // 正常域名
		{"example.com", true},                          // 正常域名
		{"user@example.com", true},                     // 正常邮箱
		{"sub.domain.co.uk", true},                     // 多级域名
		{"test-123.example.org", true},                 // 包含数字但非全数字
		{"normal.user@company.com", true},              // 正常邮箱格式
		{"api.service123.com", true},                   // 包含数字的域名
		{"www.google.com", true},                       // 常见域名
	}

	fmt.Println("测试结果:")
	for _, tc := range testCases {
		result := checkHost(tc.host)
		if result != tc.expected {
			t.Fatalf("%s -> %v (期望: %v)\n", tc.host, result, tc.expected)
		}
	}
}
