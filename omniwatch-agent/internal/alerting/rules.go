// OmniWatch — Agent
// Component: alerting (rule file)
// Phase: industry-ready (IND-5)
// Purpose: Load PrometheusRule YAML (groups + rules) for otelcol rule processor
// Inputs: configs/alerts/prometheusrules.yaml path
// Outputs: Parsed RuleFile with flattened rules
package alerting

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// DefaultRulesPath is the repo-relative path of the production rule file,
// loaded by the otelcol rule processor (no Prometheus server is added).
const DefaultRulesPath = "configs/alerts/prometheusrules.yaml"

// Rule is a single PrometheusRule alerting rule: the alert name, the PromQL
// expression evaluated by the otelcol rule processor, the pending duration,
// and the labels/annotations attached on fire.
type Rule struct {
	Alert       string            `yaml:"alert"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
}

// RuleGroup is one named group of rules evaluated on a shared interval.
type RuleGroup struct {
	Name     string `yaml:"name"`
	Interval string `yaml:"interval"`
	Rules    []Rule `yaml:"rules"`
}

// RuleFile mirrors the PrometheusRule shape: metadata plus spec.groups.
// Only the fields the agent reads are modeled; unknown keys are ignored.
type RuleFile struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Spec struct {
		Groups []RuleGroup `yaml:"groups"`
	} `yaml:"spec"`
}

// LoadRules parses the PrometheusRule YAML at path. It rejects empty group
// lists and rules missing alert/expr so a malformed file fails fast at boot
// instead of silently exporting zero rules.
func LoadRules(path string) (*RuleFile, error) {
	if path == "" {
		return nil, fmt.Errorf("alerting: rule path is empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("alerting: read rules %q: %w", path, err)
	}
	var rf RuleFile
	if err := yaml.Unmarshal(raw, &rf); err != nil {
		return nil, fmt.Errorf("alerting: parse rules %q: %w", path, err)
	}
	if len(rf.Spec.Groups) == 0 {
		return nil, fmt.Errorf("alerting: no rule groups in %q", path)
	}
	for _, g := range rf.Spec.Groups {
		for _, r := range g.Rules {
			if r.Alert == "" || r.Expr == "" {
				return nil, fmt.Errorf("alerting: rule in group %q missing alert/expr", g.Name)
			}
		}
	}
	return &rf, nil
}

// RuleCount returns the total number of rules across all groups.
func (f *RuleFile) RuleCount() int {
	n := 0
	for _, g := range f.Spec.Groups {
		n += len(g.Rules)
	}
	return n
}

// GroupedRule pairs a rule with its parent group name for export
// attributes. The embedded Rule promotes Alert/Expr/For/Labels so callers
// use gr.Alert directly.
type GroupedRule struct {
	Group string
	Rule
}

// Flatten returns every rule annotated with its group name.
func (f *RuleFile) Flatten() []GroupedRule {
	var out []GroupedRule
	for _, g := range f.Spec.Groups {
		for _, r := range g.Rules {
			out = append(out, GroupedRule{Group: g.Name, Rule: r})
		}
	}
	return out
}
