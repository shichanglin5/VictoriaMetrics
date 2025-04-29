package config

import (
	"regexp"
	"testing"
)

func TestNilPointer(t *testing.T) {
	labelNames := VMSelectTreatDotsAsIsLabels.Load()
	if labelNames == nil || len(*labelNames) == 0 {
		t.Log("len == 0")
		return
	}
	for _, ln := range *labelNames {
		t.Log("ln: ", ln)
	}
}

func TestInvokeNilPointerMethod(t *testing.T) {
	labelNames := VMSelectTreatDotsAsIsLabels.Load()
	if labelNames == nil {
		t.Log("nil")
		return
	}
	if labelNames.Contains("hello") {
		t.Log("contains")
	}
}

type RegxTestCases struct {
	reverse  bool
	regex    string
	testStrs []string
}

func TestRegxMatch(t *testing.T) {
	cases := []*RegxTestCases{
		{
			reverse: false,
			regex:   "app_uk\\s*=",
			testStrs: []string{
				"{app_uk=\"demo.uk\"}",
				"{app_uk =\"demo.uk\"}",
				"{app_uk=~\"demo.uk\"}",
				"{app_uk =~\"demo.uk\"}",

				"{app_uk='demo.uk'}",
				"{app_uk ='demo.uk'}",
				"{app_uk=~'demo.uk'}",
				"{app_uk =~'demo.uk'}",

				"{app_uk =\".*\"}",
			},
		},
		{
			reverse: true,
			regex:   "app_uk\\s*=",
			testStrs: []string{
				"{app_uk1=\"demo.uk\"}",
				"{x=\"demo.uk\"}",
			},
		},
		{
			reverse: false,
			regex:   "app_uk\\s*=~\\s*['\"]{1}\\.\\*['\"]{1}",
			testStrs: []string{
				"{app_uk =~ \".*\"}",
				"{app_uk =~ \".*\"}",
				"{app_uk =~ \".*\"}",
			},
		},
		{
			reverse: false,
			regex:   "app_uk",
			testStrs: []string{
				"{app_uk=~'demo.uk'}",
			},
		},
		{
			reverse: true,
			regex:   "app_uk",
			testStrs: []string{
				"{app_ul=~'demo.uk'}",
			},
		},
		{
			reverse: true,
			regex:   "app_uk\\s*=~\\s*['\"]{1}\\.\\*['\"]{1}",
			testStrs: []string{
				"{app_uk=\"abc.*\"}",
				"{app_uk= \"abc\"}",
				"{app_uk =\".*abc\"}",
			},
		},
	}

	for _, testCase := range cases {
		compile, err := regexp.Compile(testCase.regex)
		if err != nil {
			t.Fatalf("compile regexp: %v", err)
		}
		for _, testStr := range testCase.testStrs {
			rlt := !compile.Match([]byte(testStr))
			if testCase.reverse && !rlt {
				t.Fatalf("reverse not match: %s => %s", testCase.regex, testStr)
			}
			if !testCase.reverse && rlt {
				t.Fatalf("not match: %s => %s", testCase.regex, testStr)
			}
		}
	}
}
